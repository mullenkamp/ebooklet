# EBooklet Architecture Assessment

## Executive Summary
This report presents a design-level architecture assessment of `ebooklet` (and its backing store `booklet`), specifically aimed at evaluating its readiness for the upcoming public RCG commons launch and the tethys data migration. 

The core local-first, explicit-push architecture is fundamentally sound. The `booklet` -> `ebooklet` -> `cfdb` layering assigns responsibilities correctly. However, all three hypothesized seams contain structural flaws that manifest as race conditions, torn reads, or silent data loss. Fortunately, they can be resolved within the constraints of plain object storage without adding sidecar files.

## Seam 1: Session/Lifecycle State
**Verdict: Targeted Refactor**
**Phase:** before-1.0

The implicit state machine driven by `flag='n'` overloaded the standard dbm mode with a destructive distributed operation (replacing a remote). While 0.9.4’s downgrade to `'w'` patches the resulting data-loss bug, the API surface remains fragile. Furthermore, the lack of `num_groups` persistence requires awkward warnings and manual user tracking until the first push.

**Design Sketch:**
1. **Explicit Lifecycle:** Deprecate `flag='n'` for remote replacement. Introduce explicit operations `ebooklet.create_remote(num_groups=...)` and `ebooklet.delete_remote()`. `open_ebooklet` should only accept `'r'`, `'w'`, and `'c'`.
2. **Local Persistence:** Store the initialized `num_groups` and other system state in a dedicated hidden key (e.g., `__ebooklet_sys_meta__`) within the local `booklet` file. Since `booklet` allows arbitrary keys, this requires zero sidecar files and perfectly bridges the gap until the first push commits the state to the remote `db_object`. `ebooklet.keys()` simply filters this key out, just as it does for user metadata.
**Cost on-disk:** Negligible (one extra key in the local file).

## Seam 2: Change Tracking by Wall Clock
**Verdict: Redesign**
**Phase:** before-1.0 (Can follow the tethys migration, as the 0.8.4 warning mitigates immediate risks)

Wall-clock change tracking is fundamentally unsound. Any local edit with an older timestamp (due to clock skew or explicit `set_timestamp` calls) is silently ignored. 

**Design Sketch:**
1. **Booklet Modification Sequence:** Add an 8-byte `modification_sequence` counter to `booklet`'s file header (which has ample reserved space). Add an 8-byte sequence field to every data block header. Every `booklet.set()` and `booklet.delete()` increments the file-level counter and stamps the block.
2. **Ebooklet Tracking:** `ebooklet` stores the `last_pushed_sequence` in the remote `db_object` metadata.
3. **Push Optimization:** `booklet` exposes an `iter_modified_since(seq)` method that performs a sequential scan of the data blocks (bypassing the bucket index). Given `booklet`'s ~0.3 µs/key iteration speed, a full sequential scan is well within acceptable latency bounds for an explicit network operation like `push()`.
**Cost on-disk:** +8 bytes per data block, +8 bytes in the file header. No sidecar files.

## Seam 3: Push Atomicity and Reader Consistency
**Verdict: Redesign**
**Phase:** before-migration (CRITICAL)

The current multi-object push sequence (`update_remote`) overwrites group objects in-place prior to committing the index. This results in a severe mid-push window where a reader using the old index fetches a new group object. Because the index offsets are stale, verification fails and the reader falls back to full-object recovery (`recover_group_members`). 
Worse, if a push aborts mid-way, this torn state is permanent: the new group data is served to readers, completely bypassing index snapshot isolation.

**Design Sketch (Embedded Manifest):**
1. **Immutable Group Objects:** Group objects become generation-numbered (e.g., `group_id_push_uuid`) and are never overwritten in-place.
2. **Embedded Manifest:** Instead of a separate manifest object, the remote index (`db_object`) payload is redefined to encapsulate all state atomically.
   *New `db_object` format:* `[MagicNumber][Manifest_Len: >I][Manifest_JSON][Metadata_Len: >I][Metadata_JSON][Remote_Index_Bytes]`
3. **Atomic Commit:** `update_remote` uploads the new group objects, atomically replaces the `db_object` (committing the new manifest, user metadata, and index all at once), and then deletes the old group objects.
4. **Zero Extra Network Cost:** Readers fetch the `db_object` as before, parse the manifest locally, and fetch the exact group objects required. This eliminates the `_metadata` S3 object entirely and guarantees perfect reader consistency.

## Findings Outside the Seams

1. **CRITICAL: Torn pushes permanently corrupt reader views.**
   *(Addressed by the Seam 3 redesign)*. A failed push leaves new group objects alongside an old index. Readers fall back to full-object recovery and materialize uncommitted data, violating snapshot isolation. This must be fixed before the high-volume tethys migration.
   *Citation:* `utils.py:633-636` (group objects are uploaded before the `db_object` is atomically replaced).
2. **MAJOR: `copy_remote` replicates orphaned garbage.**
   Currently, `copy_remote` uses `list_objects` to copy everything in the prefix. If pushes fail and leave orphaned objects, they are permanently replicated. 
   *Fix:* `copy_remote` should only copy the exact objects referenced by the active `db_object` and its manifest.
   *Citation:* `remote.py:400` (`source_resp = self._write_session.list_objects(...)`).
3. **MAJOR: User Metadata is susceptible to split-brain.**
   User metadata is packed into a separate `_metadata` S3 object, making it non-atomic relative to the database index. A failed push can leave metadata out of sync with the data. 
   *(Addressed by the Seam 3 redesign which embeds the metadata payload within the `db_object`).*
   *Citation:* `utils.py:516` (`upload_value` conditionally pushes to `_metadata`).
4. **MINOR: Inconsistent use of thread locks in remote session management.**
   Operations like `copy_remote` and cache handling lack explicit threading models compared to `main.py`.

## What Held Up (Assessed and Sound)

- **Local-first with explicit push:** The core paradigm is robust. Decoupling S3 latency from point reads/writes is the correct approach for the intended scale, and explicitly controlling pushes prevents intermittent network issues from blocking local work.
- **Layering (booklet -> ebooklet -> cfdb):** Clean separation of responsibilities. `booklet` handles byte-level persistence, `ebooklet` handles sync, and `cfdb` handles domain semantics.
- **Grouped Storage (num_groups):** The hash-based packing strategy correctly bounds object sizes and limits S3 `PUT` costs, making it highly effective for large datasets.

## Prioritized Roadmap

1. **Before Migration (~3 days)**
   - Implement the embedded group manifest + generation-numbered objects (Seam 3 redesign).
   - Embed user metadata within the `db_object` payload.
   - Refactor `copy_remote` to use the manifest rather than blind listing.
   *(Dependency: None. Required to prevent torn-push corruption and 404 races during the high-volume migration).*
2. **Before 1.0 (~3 days)**
   - Implement `modification_sequence` in `booklet` and transition `ebooklet` change tracking (Seam 2 redesign).
   - Formalize session lifecycle: deprecate `flag='n'`, implement `__ebooklet_sys_meta__`, and add `create_remote()` / `delete_remote()` (Seam 1 redesign).
   *(Dependency: Seam 3 should be completed first to stabilize the push pipeline).*
3. **Not-Worth-It / Post-1.0 (~1 day)**
   - Implement background garbage collection for orphaned S3 objects left behind by failed pushes (can be postponed indefinitely since object storage is cheap and they are isolated from active reads).

## What Was Not Examined
- The internal structure of `s3func` locks, retry layers, or credentials management (which was designated out-of-scope for the core architecture review).
- `envlib` catalogue semantics beyond its reliance on `ebooklet` RCG functionality.
- Live testing on B2 or MEGA (as per the ground rules forbidding network calls to S3 services).
