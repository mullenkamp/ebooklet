# Phase 0 Design Review: ebooklet 0.9.5 Hardening

## 1. Verdicts per Item

### Item 1: Exact-key deletes replacing prefix deletes
**Verdict: Needs Change (Minor)**
- **db-object-first for `delete_remote`**: Correct. This ensures readers immediately see clean absence (a 404 on the db object) rather than a torn state.
- **Excluding lock namespace**: Correct. It protects the active writer during a mid-'n'-push and avoids breaking other writers' locks during teardown.
- **Probe relocation**: Safe. No code parses group-ids blindly, so placing it in the namespace won't crash readers. 
- **Concrete Change Needed**: The brief proposes `self._write_session.delete_objects([self.write_db_key + '/' + key])`. Ensure that `purge=True` is explicitly passed (if it isn't the default in `s3func`) to guarantee all versions are deleted. Furthermore, the plural `delete_objects(keys)` method in `S3SessionWriter` (`remote.py:356-364`) used by legacy per-key deletes MUST also explicitly pass `purge=True`.
- **Failure contract**: Returning `error, None` from `upload_group` on an empty-group delete failure perfectly hooks into the existing `failures` dict, correctly retaining the keys in `deletes` for the next retry.

### Item 2: Deleted-member recovery routed through 0.9.3 re-check
**Verdict: Implementable as specified**
- **The boolean seam (`report_missing_members`)**: This is the correct design. It perfectly preserves the vital distinction between a missing *object* and a missing *member*. A missing group object (404) returns `MissingRemoteObject` regardless of the boolean, which rightfully fails the push and prevents the self-heal from permanently dropping the group's keys due to a transient S3 404. A missing member returns `None` during push, allowing the self-heal to run.
- **Partial materialization**: Completely safe. If a group partially recovers and then raises `RemoteIntegrityError`, the materialized keys simply rest in the local read cache (`_local_file`). They are valid remote data; leaving them cached is harmless.

### Item 3: format_version headroom
**Verdict: Needs Change (Minor)**
- **Guard location**: Raising at `_load_db_metadata` is perfect. It covers both session initialization and mid-session re-pulls. 
- **Concrete Change Needed**: Do not raise `RemoteIntegrityError`. A version mismatch is a compatibility fault, not an internal consistency fault (where the index contradicts the store). Raise a `ValueError` or a dedicated `UnsupportedFormatVersionError` so downstream consumers (`cfdb`) can distinguish it from a transient/recoverable integrity fault.

### Item 4: Close-time warning on pending deletions
**Verdict: Implementable as specified**
- Firing the `UserWarning` from `close()` rather than the weakref finalizer is correct (warnings during GC are notoriously flaky). It will successfully catch the `retained_deletes` case (a failed push leaves `self._deletes` populated).

### Item 5: Declare formats frozen
**Verdict: Implementable as specified**
- The 15-byte remote-index entry and the group pack layout are fully compatible with the planned Phase-1 redesign (which will embed a group manifest into the `db_object` payload rather than altering the index entries or group packing).

### Item 6: Warn on non-HTTPS db_url
**Verdict: Implementable as specified**
- Placing the warning inside `create_s3_read_session` correctly covers all choke points (both `S3Connection` and direct calls). `UserWarning` is appropriate.

## 2. Findings

1. **MINOR: Plural `delete_objects` missing explicit purge**
   *Citation: `ebooklet/remote.py:362`*
   The legacy per-key push path uses `S3SessionWriter.delete_objects`. If `s3func`'s exact-keys path doesn't default to `purge=True`, this will only insert delete markers. Update it to mirror the exact-keys purge logic proposed for the singular `delete_object`.
2. **MINOR: `copy_remote` will replicate probe orphans**
   *Citation: `ebooklet/remote.py:400`*
   Because `copy_remote` uses a blind prefix listing (`list_objects`), the relocated writability probe orphans (which are now inside the namespace) will be copied to the target. This is harmless garbage, but should be explicitly acknowledged as an accepted residual of Phase 0.
3. **MINOR: Fake S3 fidelity gap on versions**
   *Citation: `fake_s3.py` (Test Plan)*
   The brief notes the fake models only *one version per key*. This means the tests cannot prove that `purge=True` successfully sweeps *multiple* versions of a key (e.g., from concurrent writes or retries). The test plan should document this gap.

## 3. What Held Up

- **The re-check protocol (`_resolve_missing`)**: It absorbs the new member-level failures beautifully without requiring any structural changes to the point-read or batch-read pipelines.
- **Push-path self-heal safety**: By maintaining the distinction between 404-object and 404-member, the push path remains safe against destroying data during transient network outages.

## 4. Adjacent Items to Absorb or Defer

- **Defer**: The blind listing in `copy_remote`. Fixing it to parse the index/manifest rather than listing S3 is a larger structural change that belongs in Phase 1 alongside the embedded manifest. Phase 0 should explicitly defer it.
- **Absorb**: Explicitly document in the README that HTTP db_urls are discouraged but supported for local Minio testing, and note how to silence the `UserWarning` for that specific use case.
