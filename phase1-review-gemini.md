# Phase 1 Independent Review: EBooklet Generational Architecture

This report evaluates the proposed Phase 1 design brief for `ebooklet`, which transitions the backend to a generational architecture (V2) to address Phase 0 safety flaws.

## 1. Overall Assessment
The generational architecture—using immutable payload blocks ("gens") and an atomic db-object pointer swap—fundamentally solves the torn-push and prefix-delete data loss issues. The introduction of the `Journal` for state management (replacing implicit memory flags) is robust and resolves the read-your-writes and flag-'n'-then-'w' mid-session state tracking flaws.

However, three significant flaws were found in the read-path and journal-persistence mechanisms that will cause local data loss or crash read-only sessions if implemented strictly as written.

## 2. Critical Findings & Flaws

### 2.1 `_resolve_missing` Purge Unpushed Overwrite Deletion (Data Loss)
**Severity: Critical**  
**Root Cause:** The design brief identifies three read sites for the `read-your-writes` gate: `load_items`, `_load_item`, and the pull loop. It misses the implicit "read" path inside `_resolve_missing`.  
When `_resolve_missing` executes its clean-absence path (because an expected key 404s), it iterates over `marker.keys` and deletes them locally (`del self._local_file[k]`). If the user has just overwritten one of those keys locally, it will be in `journal.written` (unpushed). The remote index legitimately no longer claims it (e.g., deleted by a concurrent writer), but deleting it locally destroys the user's unpushed overwrite.  
**Fix:** The clean-absence purge inside `_resolve_missing` MUST be gated by `journal.written`:
```python
for k in marker.keys:
    if k in journal.written:
        continue  # Protected by read-your-writes gate
    if k == utils.metadata_key_str:
        ...
```

### 2.2 Read-Only Session Journal Hygiene
**Severity: Minor (Hygiene)**  
**Root Cause:** The plan mandates that the journal is persisted to slot 1 at `sync()`, `close()`, and in the finalizer. In `ebooklet`, the underlying `local_file` is always opened in `'w'` mode (even for `'r'` sessions, to allow local caching of remote data, per `utils.py:191`). Therefore, writing to the reserved slot won't strictly crash. However, since `'r'` sessions never mutate journal state, they should not perform unnecessary disk writes rewriting the unchanged journal.  
**Fix:** Journal persistence points (`sync()`, `close()`, `ebooklet_finalizer`) should be gated on the `ebooklet` session's own `_flag` (e.g., `if self._flag != 'r':`) to avoid unnecessary writes, though this is a hygiene fix rather than a crash fix.

### 2.3 Clarification on `journal.written` Clear Timing for Replacement Pushes
**Severity: Major (Clarification needed)**  
**Root Cause:** The plan states "On push, journal.written clears per successful group". If a replacement push (`replace_pending=True`) experiences a partial group failure, the plan correctly states it "commits NOTHING" (Phase C is skipped). If `journal.written` was cleared during Phase B for the successful groups, the next retry of the push (which re-evaluates the `not in written` purge gate) will delete those successfully-pushed keys from the local file.  
**Fix:** Explicitly constrain the `journal.written` clear to execute **only after Phase C succeeds**. For normal pushes, it clears the successfully committed groups. For replacement pushes, since Phase C is skipped on partial failure, nothing clears, keeping the local keys safe on retry.

### 2.4 Fsck Safety and `force_push`
**Severity: Minor**  
**Root Cause:** The brief questions whether `fsck` needs special logic to protect a crashed writer's pending retry (e.g., `force_push` after a failed commit).  
**Fix:** No special logic is needed. Because `journal.written` remains intact on a Phase C failure, any retry (forced or not) re-runs Phase B, packing the groups into entirely new gens. The old uncommitted gens are genuinely abandoned the moment the process crashes, meaning a 24-hour age gate for `fsck` sweeping them is completely safe.

## 3. Responses to Specific Architecture Questions

### Backend Integration
1. **Variable-length skip sites complete?** Yes. The three skip sites (`utils.py:554`, `utils.py:730`, and `main.py:803`) completely cover all key-enumeration paths. Unclean-close recovery calls `self.keys()`, which goes through the file iter, successfully skipping the reserved keys. `len()` bookkeeping relies solely on `_n_keys`. The API is sound.
2. **Finalizer persistence safety?** The finalizer ordering is safe. Because `local_file` is held by the finalizer's closure, it will not be garbage-collected before `ebooklet_finalizer` writes to it. (Note: Must gate with `if self.writable`).
3. **Per-successful-group clearing of journal.written?** Addressed in Finding 2.3. Safe only if executed *strictly after* the Phase C atomic db_object commit.
4. **Read-your-writes gate completeness?** Addressed in Finding 2.1. Incomplete; `_resolve_missing`'s clean-absence branch must be gated.
5. **Delete replay-on-swap index creation?** Yes, `initial open` and `_pull_remote_index` are the only creation sites.
6. **Skew normalization timestamp mutation?** The mutation is safe during the pull loop (no active booklet iterator). While `create_changelog` snapshots the timestamps *before* the pull loop, the payload builder does not read timestamps from the changelog. It reads them directly via `local_file.get_timestamp(key)`, effectively capturing the normalized timestamps for the payload.
7. **`set_reserved` lock-free?** Yes, but since `ebooklet` operates under `booklet`'s `'w'` lock guarantees, data corruption is intrinsically prevented.

### Data Layout & Schema
8. **Payload layout robustness?** The V2 payload layout is highly robust. The magic `b'ebooklet-db\x00'` completely discriminates against v1's `b'bklt\x01'`. The 8-byte unsigned integer lengths (`>Q`) are sufficient. Ranged GETs over S3 will correctly return HTTP 206, which must be supported alongside 200. Embedding metadata directly inside the payload eliminates the need for separate metadata objects.
9. **Push protocol crash states?** The protocol correctly isolates readers from Phase B (PUT gens). If Phase C fails, readers remain on the old state, and the writer can retry (generating new gens). If Phase D fails, orphans are left for `fsck` to sweep. It is robust.
10. **`get_remote_value` S3 vs http distinction?** Yes, routing based on `isinstance(remote_session, ...)` is straightforward and matches the existing dual-backend abstraction.
11. **Concurrency inside one OS process?** The `booklet` implementation acquires `portalocker.LOCK_EX` on `'w'` opens. The `fcntl` POSIX lock is per-process (or per file-descriptor depending on OS), but `ebooklet` forces exclusive ownership. Concurrent instances in the same process writing to the same file are fundamentally unsupported by `booklet`'s design.

### Validation Bounds
12. **S3 list_objects 1000 limit?** Yes, `list_objects_v2` paginates at 1000 items. Any group with more than 1000 abandoned gens (highly unlikely for a single group prefix) must be handled by `fsck` via pagination.
13. **F7 GroupTooLargeError bound?** The bound `4294967295` (2³² - 1) is correct for the 4-byte integer serialization used in `remote_index`. Returning the failure inside `upload_group` correctly flags the failure dict instead of breaking the entire push.
14. **Reserved slots count constraint?** `msgspec` encodes state succinctly. Slot length shouldn't hit block limits unless the journal itself exceeds 100s of MBs (which would hit memory bounds first).
15. **1-hour default break age for s3func?** It is a sensible default. Own-ticket exclusion is completely safe because a crashed-and-reopened session generates a new `lock_id` UUID, so it will accurately identify its own crashed tickets as foreign.
16. **`msgspec` strictness?** `msgspec` transparently handles int keys in JSON decoding if the struct is explicitly typed (e.g., `dict[int, str]`).

### Compatibility
17. **Consumers (`cfdb` / `envlib`) breakage?** The transition replaces the `flag = 'n'` to `'w'` downgrade with `replace_pending`. This leaves the semantic behavior intact without mutating `flag`, fully supporting push-mid-session and `failures dict` patterns used by `cfdb`.
18. **Phase 0 `format_version` behavior?** V2's payload checks naturally encompass the compatibility boundary. Unrecognized db_object payloads gracefully reject reading, forcing a protocol violation error instead of local corruption.

## 4. Conclusion
The Phase 1 generational model is beautifully designed. The shift to a journaled state fundamentally resolves the state-loss vectors from Phase 0. With the three core fixes applied (gating `_resolve_missing`'s purge, checking `writable` before persistence, and tightening the `journal.written` clear rules), the architecture is ready for implementation.
