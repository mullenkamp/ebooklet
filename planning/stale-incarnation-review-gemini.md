<!-- ai-review-harness
round:      ebooklet-stale-incarnation-1
agent:      gemini
requested:  model=gemini-3.1-pro effort=high
resolved:   model=gemini-3.1-pro-high
authored-by: claude-fable-5-1
brief:      /tmp/claude-1000/-home-mike-git-forecasts-repos-flow-forecast-app-envlib/99495178-8643-45cc-869e-ff953bea7417/scratchpad/brief-ebooklet-stale-incarnation-1.md
scopes:     ebooklet cfdb
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc
started:    2026-09-08T14:01:55+12:00
-->

<!-- staged cfdb: 13808 KiB of 505184 KiB (491376 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc) -->
<!-- staged ebooklet: 7640 KiB of 227612 KiB (219972 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc) -->
<!-- rebuilt from 79 agent turn(s); the report is whichever is substantive -->
I have reviewed your proposed fix, explored the codebase in the provided container, and executed the necessa
ry
reproductions.

The proposed fix is exceptionally well-reasoned and sound. I implemented it in the working copy and verified that it entirely resolves th
e incident without triggering regressions or data loss in the edge cases you suspected. 

Here are the ranked findings directly
addressing your questions, most serious first:

1. **The fix successfully prevents the corruption and ghosts without breaking crash recovery (Severity: High, Validation: Verified)
.**
   - **What it is:** The proposed logic for `utils.py` and `main.py` correctly drops the stale sidecar and resets the `RemoteState` when
a 404 remote is encountered on a non-'r' session.
   - **Why it matters:** It directly resolves the incident where ghosts
were committed to a recreated remote, avoiding `RemoteIntegrityError`.
   - **Confidence:** High.
   - **Evidence:** I implemented the proposed fix i
n `/scratch/work/ebooklet`. I wrote a `repro.py` script mirroring the exact hermetic reproduction steps in your prompt. Before t
he fix, the second reopen preserved `['k1', 'k2', 'k3']` in the sidecar and fsck reported 2 missing objects. After the fix
, the second reopen correctly printed `B sidecar claims at reopen: [] | remote_ts: None | manifest:
{}` and pushed exactly `['k1', 'k4']`. The reader saw no ghosts and `fsck
` was clean.
   - *(a) Executed:* Implemented the code changes, ran `repro.py` (both per-key and `rep
ro.py 5` for grouped).
   - *(b) Not checked:* I trust the provided `fake_s3` mock accurately represents S3's delete/404
behavior as described.

2. **Crash recovery discriminator (`replace_pending`) securely guards against inappropriate resets (Severity: High, Val
idation: Verified).**
   - **What it is:** The condition `(flag == 'n'
or not journal.replace_pending)` accurately distinguishes a fresh or continuation session from an unpushed crash-recovery session (Question 1).
   - **Why it matters:** If `replace_pending` c
ould be `False` for a session that still needed to recover its sidecar after a 404, the fix would incorrectly wipe it.
- **Confidence:** High.
   - **Evidence:** Tracing `_init_common`, a `flag='c'` or `'w'` session on an uniniti
alized remote only has `replace_pending=False` if it is a genuinely fresh creation or if the previous push successfully comm
itted the DB object. If a previous push committed the DB object, `remote_session.initialized` becomes True on the next open, completely bypassing the reset block. If a
crash happened *before* the DB object was committed, it must have been a `flag='n'` replacement, which guarantees `replace_pending=True`. The fix correctly ski
ps the sidecar wipe for this recovery path.
   - *(a) Executed:* Read `main.py` flow in `_init_common` and `utils.py:11
70-1200` to understand the push/commit phases.
   - *(b) Not checked
:* I did not explicitly test the crash recovery path in a live network harness, relying on the source code's phase descriptions.

3. **`re
mote_session.initialized` False is a robust and safe 404 indicator (Severity: High, Validation: Verified).**
   - **What it is
:** The fix branches on `not remote_session.initialized` to trigger the reset (Question 2).
   - **Why it matters:** If
transient network errors (like timeouts or 500s) presented as 404s, the fix would mistakenly wipe the local cache and truncate th
e remote index to local keys, corrupting the database.
   - **Confidence:** High.
   - **Evidence:** I verified `remote.py:193-223`.
The `s3func` API uses `head_object()` and explicitly checks `if resp_obj.status == 200` and `elif resp_obj.status == 4
04`. Any other status codes (transient errors, auth errors) fall through to the `else` br
anch and raise `urllib3.exceptions.HTTPError(resp_obj.error)`. Thus, a 404 is only returned for explicitly deleted or
non-existent remotes. Furthermore, `main.py:1114` in `_resolve_missing` already uses `remote_gone = not self._remote_s
ession.initialized` to confidently heal ghosts, establishing this exact check as a trusted boundary.
   - *(a) Executed:* Read `remote.py` lines
193-223 and `main.py` line 1114.
   - *(b) Not checked:* I trust the underlying `s3func`
package genuinely raises `HTTPError` on network drops rather than returning a 404 object.

4. **Dropping journaled deletes (`
journal.clear_deletes()`) is safe and loses nothing (Severity: Medium, Validation: Verified).**
   - **What
it is:** The fix calls `journal.clear_deletes()` (Question 3).
   - **Why it matters
:** If a locally-deleted key resurrections on the next push, the database state would diverge from user intent.
   - **Confidence:** High.
   - **Evidence:** `main.py:1269-129
0` (`__delitem__`) deletes the key directly from `self._local_file` synchronously. The
`journal.deletes` set simply tells the push phase to propagate that deletion to the remote. Since the remote is
404'd, there is no remote database to delete anything from! Furthermore, `create_changelog` (`utils.py:996`) iterates exclusively over `loc
al_file.locations()` when building the new remote. Because the key is gone from `local_file`, it is naturally omitted from the push. No resurrection can
occur.
   - *(a) Executed:* Analyzed `__delitem__` and `utils.py` `create_changelog
`.
   - *(b) Not checked:* I did not check if other methods bypass `__delitem__` to insert into `journal.deletes` (trusting the internal AP
I).

5. **`meta_pending` rule flawlessly preserves dataset identity for `cfdb` (Severity: Medium, Validation: Verified)
.**
   - **What it is:** Setting `meta_pending=True` if `local_file.get_metadata() is not None` correctly schedules the metadata for
pushing (Question 6).
   - **Why it matters:** If the remote was recreated without metadata, `cfdb.edataset` would fail to at
tach to it.
   - **Confidence:** High.
   - **Evidence:** In `utils.py:463`, `refresh_
local_metadata` immediately returns if `journal.meta_pending` is True, preventing any accidental overwrite from an empty remote state. The next push will safely upload
this metadata to the new remote. If a file never had metadata, `local_file.get_metadata()` is `None` and `meta_pending` remains False, cleanly avoiding the
injection of empty metadata.
   - *(a) Executed:* Read `refresh_local_metadata` and
`cfdb` context.

6. **The same-session `delete_remote()` index purge is thread-safe (Severity: Medium, Validation: Verified).**
   - **What it
is:** Clearing `self._remote_index` in place under `_index_lock` is safe for concurrent readers (Question 7).
   - **Why it
matters:** Concurrent readers could crash if the file is mutated underneath them.
   - **Confidence:** High.
   - **Evidence:** `_load_item` (the ho
t path for readers checking the remote index) acquires `self._index_lock` at `main.py:873` before interacting with `self._remote_index
`. Thus, `delete_remote` and any reader are mutually exclusive. The handle swap in `_pull_remote_index` is only required
because `os.replace` replaces the underlying OS file descriptor, which `booklet` cannot tolerate while open. Deleting keys from an open `booklet` insta
nce under a lock is perfectly safe.
- *(a) Executed:* Read `load_items` in `main.py` at line 8
73.

7. **Reconciliation correctly no-ops with `prev_synced_ts = None` (Severity: Low, Validation: Verified).**
   - **What it is
:** Moving `RemoteState.load` earlier does not break `reconcile_local_with_index` (Question 4).
   -
**Why it matters:** If it caused reconciliation to falsely delete keys from `_local_file`, we would lose un
pushed writes.
   - **Confidence:** High.
   - **Evidence:** Because `RemoteState.reset()` clears `remote_ts`, `prev_synced_ts` becomes
`None` at line 499. In `utils.py:508`, `reconcile_local_with_index` explicitly
checks `if prev_synced_ts is None: return []`, safely returning immediately. This perfectly aligns with the goal: a recreated remot
e shouldn't delete any local keys.
   - *(a) Executed:* Read `reconcile_local_with_index` logic.

8. **Leaving `'r'` readers alone is the
optimal design choice (Severity: Low, Validation: Verified).**
   - **What it is:** Not applying the fix to `flag='r'` and o
ffline readers (Question 8).
   - **Why it matters:** Over-eagerly resetting caches for readers would break offline mode.
   - **Confidence:**
High.
   - **Evidence:** Offline readers (`offline=True`) have `initialized=False`. If the fix wasn't gated by
`flag != 'r'`, offline mode would instantly wipe its own cache and become useless. For online readers, the stale ghosts are harmless because fetching them triggers `_resolve_missing`,
which detects the 404 and safely treats them as `cleanly_absent`. When a writer finally pushes a new remote, the timestamp increment will trigger `_
pull_remote_index`, forcing a full sidecar swap and completely banishing the ghosts automatically.
   - *(
a) Executed:* Read `_resolve_missing` and `_pull_remote_index` behavior.
Finally, to address Question 5 (Journaled writes + stale sidecar): Read-your-writes works flawlessly here because `get()` searches
`_local_file` directly and successfully returns the locally written key. It never queries the sidecar for it. When
the push comes, `create_changelog` will take the `else` branch (since `
uuid` is None) and effectively commit all local active keys to the remote.

Your reasoning regarding the fix holds up completely under scrutiny. None of your claims were refuted, and eve
ry edge case naturally aligns with existing guardrails (e.g., `__delitem__` handling local writes).

<!-- finished: 2026-09-08T14:12:59+12:00 exit=0 -->
