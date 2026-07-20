# Gemini review report — ebooklet local-cache reconciliation (plan stage)

Run 2026-07-20 by Mike via agy against the neutral-worded brief
(`delete-reconcile-review-brief-gemini.md`), blind to the Claude reviewer. Verbatim.

---

## 1. Verdict

Implementable with changes. The core timestamp discriminator is sound and elegantly
distinguishes remotely-sourced values from unpushed local writes. However, two major
oversights need addressing before implementation: the optimization to skip reconciliation on
cached-index reuse leaves upgraded pre-fix caches vulnerable to republishing deleted data,
and a concurrency hazard with booklet iterators will crash the pull path if a background
fetch completes during reconciliation.

## 2. Findings

### MAJOR: The fetched-only rule leaves upgraded pre-fix caches vulnerable to republishing deleted keys

- Evidence: In _init_common (main.py:466-479), the plan proposes running reconciliation only
  if fetched is True. However, if a user upgrades an existing ebooklet (which already has
  stale, remotely-deleted keys in its local file) and opens it as a writer,
  check_local_remote_sync may return False (no new remote changes). Reconciliation is
  skipped. If they subsequently call push(), create_changelog will sweep these stale keys,
  find them missing from the remote_index, assign them remote-ts 0, and enter them into the
  changelog as NEW writes (utils.py:854). The pre-fix stale residue is silently republished
  to the remote.
- Recommended Change: Do not gate reconciliation on fetched in _init_common. Run
  reconcile_local_with_index unconditionally at open (using remote_state.remote_ts as
  prev_synced_ts). It is a cheap memory scan and guarantees that pre-fix residue is healed
  before any push() can republish it.

### MAJOR: Concurrent fetches mutate local_file outside _index_lock, crashing reconciliation

- Evidence: Call site A runs reconciliation inside _pull_remote_index while holding
  _index_lock. However, concurrent point reads (_load_item) explicitly release _index_lock
  before issuing the network fetch (main.py:1156). When the fetch completes,
  get_remote_value calls local_file.set() without acquiring _index_lock. Because
  booklet.keys() raises RuntimeError if mutated, a background fetch completing during
  reconciliation's `for key in local_file.keys():` scan will violently crash the pull.
- Recommended Change: Wrap the candidate collection step of reconcile_local_with_index in a
  retry loop that catches RuntimeError and restarts the scan. Since fetches are finite, this
  safely tolerates concurrent writes without degrading read-path concurrency by holding
  locks across I/O.

### NOTE: The unjournaled write residual loss window is slightly broader than claimed

- Evidence: You noted a residual window requires a "skewed/backdated timestamp". This is
  imprecise. A perfectly normal unjournaled local write (e.g. set() at T1.5) followed by an
  interrupted pull() (which successfully persists remote_state.remote_ts at T2 but crashes
  before updating _file_timestamp) will result in prev_synced_ts = T2 on reopen. Because
  T1.5 <= T2, reconciliation WILL delete the unjournaled local write, even without clock
  skew.
- Recommended Change: No code change necessary (unjournaled writes are inherently vulnerable
  to crashes by design), but adjust your mental model of the failure bounds.

  [Synthesis note: this scenario was checked against source and does NOT hold for API
  writes — `_pull_remote_index` calls `sync()` first (main.py:948), persisting the journal,
  so the write is journal-protected on reopen; a crash before any sync leaves the write's
  timestamp NEWER than the previous ingest, so the ts guard keeps it. The surviving form of
  the concern is the Claude reviewer's user-supplied-timestamp finding. See the synthesis.]

### NOTE: _resolve_missing handles reconciliation safely, but redundantly

- Evidence: _resolve_missing calls self._pull_remote_index(), which will run reconciliation
  and cleanly delete any index-absent keys. Later in _resolve_missing (main.py:1084), it
  explicitly deletes the missing keys again: del self._local_file[k].
- Recommended Change: No change required. Because `k in self._local_file` is checked first,
  this avoids KeyError and is harmlessly redundant.

## 3. Verified Author Claims

- keys() yields local keys unconditionally: Verified (main.py:635-647).
- __contains__ consults local file: Verified (main.py:740).
- _load_item serves local value on index absence: Verified. check_local_vs_remote returns
  None, completely bypassing the fetch and serving the local cache (main.py:1158).
- create_changelog republishes absent keys: Verified. It assigns them a 0 timestamp and adds
  them as new writes (utils.py:854).
- _file_timestamp is creation-time-stale for read-only caches: Verified. _init_common never
  sets it, meaning flag='r' sessions stay stale indefinitely.
- force=True from Change.discard is safe: Verified. Discarded writes are explicitly del'd
  locally prior to the force pull (main.py:189).
- Timestamps properties hold: Verified. Post-sync local writes use make_timestamp_int()
  (current UTC us) which is strictly newer than the remote index commit timestamp. Remote
  materialized values carry their original write timestamp (utils.py:1319), which is
  strictly older than or equal to the index commit timestamp.

## 4. What Was Checked and Found Clean

- Metadata Protection: utils.reserved_key_strs securely protects metadata and reserved
  slots from accidental deletion during the candidate scan.
- Interactions:
  - clear(): Truncates the local file before any sync operations occur. There are no
    candidates to reconcile, making this completely safe.
  - prune(): Removes items from local_file only. Reconcile iterates over surviving keys, so
    pruned keys are not candidates. Safe.
  - replace_pending (flag='n'): Suppresses index fetching entirely, leaving prev_synced_ts
    as None. Reconciliation behaves as a safe no-op.
  - journal.deletes: Deleted keys are immediately removed from local_file via __delitem__
    (main.py:1222). They will not appear in local_file.keys() during the scan.
- Concurrent _load_item resurrection: If a concurrent read is in-flight during a pull, and
  reconciliation finishes just before the fetch completes, the fetch will re-materialize
  the deleted key locally. This leaves a tiny residue, but it safely heals on the next
  pull. Tolerable and safe.

  [Synthesis note: the Claude reviewer rated this same race MAJOR because the "next pull"
  never comes while the remote is idle (freshness gate) — the stale key would serve until
  the next actual remote change. Mike ruled to CLOSE it in code (the R3 membership
  re-check before/after fetch completions).]
