<!-- ai-review-harness
round:      ebooklet-wog-code-1
agent:      claude
requested:  model=claude-fable-5-1 effort=high
resolved:   model=claude-fable-5-1
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-code-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-base
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-07T09:38:22+13:00
-->

<!-- staged cfdb: 1736 KiB of 5488 KiB (3752 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8724 KiB of 230268 KiB (221544 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1464 KiB (708 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-base: 4368 KiB of 369548 KiB (365180 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 5 agent turn(s); the report is whichever is substantive -->
Still reading the core push/open paths; I have a first candidate data-loss path (the republish retry after a partial failure) and am now driving it against the fake S3.
Fuzz harness fixed; remaining failures all involve `del k; db[k]=v; del k`. Digging into one grouped-mode case (seed 1032) where a deleted key reappears after a partial push.
Remaining failures are all a harness artifact (my `prune()` eviction right after `delete_remote()`); masking that and rerunning.
Write-order allocation holds at 2000 keys (also after `prune()` and overwrites). Now checking the hydrated-file order, a candidate fix for the republish loss, and a batch of mutants.
# Review of ebooklet 0.11 write-order groups: findings

The core mechanism held up under everything I threw at it, but there are two verified data-loss paths in the migration (one in ebooklet itself, one in the migration script) and one verified lost-delete bug. Details and evidence below; all experiments ran against the working copy (ebooklet 0.11.0, booklet 0.12.10) over `fake_s3.py`, and 0.10.5 came from PyPI.

Baseline: the fake-S3 suite gives `203 passed, 2 warnings in 1.67s`, matching your number. The live suites were not run.

## Findings, most serious first

### 1. accuracy, data loss, verified: a republish that partly fails and is retried in the same session deletes the failed groups' only copy

- **What happens:** after hydrate + delete, the local values are the only copy and are not journaled. Push 1 creates the remote with one group failed. `_note_commit` does not mark the session initialised (`uuid` stays `None`), so push 2's pre-push re-check treats its own commit as "a remote that appeared" and force-pulls. `reconcile_local_with_index` then deletes every unjournaled local key the fresh index lacks.
- **Second route:** a failed group plus a crash right after the commit PUT, then a reopen, loses the same keys.
- **Reopening between the two pushes (no crash) is safe.**
- **Scope:** identical on 0.10.5, so the diff did not introduce it. The 0.11 upgrade recipe sends every grouped dataset through it, with multi-hour uploads where one failed group is likely.

Core of the script (`e1_republish_retry.py`, run as `python e1_republish_retry.py same|reopen`):

```python
work = copy of tests/fixtures/legacy_0105; store = pickle.load(store.pkl)
with FakeS3Connection(store, 'legacy').open('w') as s: s.delete_remote()
eb = open_ebooklet(conn(), work/'writer.blt', flag='w', group_bytes=64)
sess.put_object = failing   # 500 for keys starting 'legacy/1.'
r1 = eb.changes().push(); sess.put_object = orig
# 'reopen' mode: eb.close(); eb = open_ebooklet(..., flag='w', group_bytes=64)
r2 = eb.changes().push()
```

```
=== same
WARNING ebooklet.main: the remote was reported absent when this session opened but exists now; adopting its index before pushing ...
local keys before : ['k0', ... 'k9'] journal.written: []
push 1            : PushResult(updated=True, failures={1: "{'message': 'induced'}"})
push 2            : PushResult(updated=False, failures={})
local keys after 2: ['k0', 'k1', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
REMOTE keys       : ['k0', 'k1', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
=== reopen
push 2            : PushResult(updated=True, failures={})
REMOTE keys       : ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
```

The same sequence on 0.10.5 (`num_groups=5`): `0.10.5 local keys after 2: ['k0','k2','k3','k4','k6','k7','k8','k9']`, and the remote has the same eight.

Crash variant (group 1 fails, `_Crash` raised at "commit succeeded", then reopen):

```
republish: group 1 failed, then crash right after the commit PUT
  reopen: local keys ['k0', 'k1', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
  push: PushResult(updated=False, failures={})
  REMOTE keys: ['k0', 'k1', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
```

- **Fix for the main route (tested):** replace `remote_session._note_commit(...)` with `remote_session._load_db_metadata()`. The suite stays at 203 passed and the `same` run keeps all ten keys locally and remotely.
- **Fix for the crash route (not tested):** when a push creates the remote, journal every changelog key as written before uploading, so the reconcile's journal guard protects them.
- **(a) Executed:** all of the above.
- **(b) Not checked:** real S3; whether your production files match the fixture's state (hydrated, journal empty); the journal-all-keys fix.

### 2. accuracy, data loss, verified behaviour: `hydrate_and_delete.py` checks the local sidecar, not the remote's index

The script's "remote index" is `eb._remote_index`. If that sidecar is stale it reports CHECK PASSED and deletes a remote holding keys the local file lacks. I ran the real script under 0.10.5 with only `connection()` shimmed to the fake S3. Another machine had pushed `other0..2`, and the local stamp was set ahead of the remote's, as `test_lagging_remote_state…` does:

```
keys   : remote index 10, local 10, missing locally 0, older locally 0, local-only 0
CHECK PASSED: the local file holds every value of the remote.
DELETED: remote gone=True (objects left: 0)
local file keys: ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
```

- **Also (read from source, not run):** the delete runs in a second, unlocked raw session after the checked session closed, so a push between check and delete is lost.
- **Fix:** GET the db object fresh, parse it with `parse_db_payload`, and compare against that; delete while the checked session still holds the lock.
- **(a) Executed:** the script's behaviour on a stale sidecar.
- **(b) Not checked:** I manufactured the stale state. Reaching it through the 0.10.5 API with no pending changes needs a lost lock followed by `discard()`, or clock skew between two publishing machines. Confidence is high on behaviour and medium-low on likelihood.

### 3. accuracy, verified, pre-existing: `del k; db[k] = v; del k` loses the delete

This is the sibling of the discard bug you fixed. The re-set cancels the journaled delete, and the second `del` finds no sidecar entry and only discards the write.

```
group_bytes=None: journal deletes=[] written=[] "k" in eb=False
   push 1: PushResult(updated=False, failures={})
   fresh reader after push 1: k -> b'v1'
   push 2 (unrelated key): PushResult(updated=True, failures={})
   fresh reader after push 2: k -> None | remote objects: ['d/k', 'd/other', 'd/z']
group_bytes=64: (same; after push 2 k -> None)
0.10.5 num_groups=None / 5: del/set/del then push -> updated=False; fresh reader k -> b'v1'
```

- The remote keeps serving `k`, and a later unrelated commit silently drops it. In per-key mode the object `d/k` stays behind as an orphan.
- If an index re-fetch comes first, `k` is resurrected instead. My randomized run found this (seed 1032: `writer view != model: extra=['k008x']`).
- `clear()` after a del + re-set takes the same branch by my reading; I did not run it.
- **(b) Not checked:** a fix.

### 4. Smaller accuracy items

- **`db.group_bytes` is wrong for an offline per-key reader cache** (verified): `offline PER-KEY reader cache: db.group_bytes = 33554432 (sidecar len 15)`. Reader sessions record no mode, so the default (grouped) is reported.
- **`PushResult.updated=True` with nothing committed** (verified, pre-existing): `all groups failed -> PushResult(updated=True, failures={2: ...}) | db object changed: False`.
- **The open path never stamps the local file after a fetch** (verified, same on 0.10.5): a reader with an existing local file GETs the whole db object on every open after one remote change (`reader open #1..#4: db-object GETs = ['full']`). The new `remote_ts` comparison could gate this.
- **After a crash right after the commit, the next push re-uploads the crashed push's groups** (read from source): the journal is uncleared, so the keys go through the skew path with bumped timestamps. I observed only the misleading warning in the suite output ("…carried a timestamp at or before the remote's (clock skew…)" for key `c`).
- **A hydrated file republishes in download order, not original write order** (verified): `hydrated: 2000 keys in 128 groups; gid decreases along ORIGINAL write order: 62`. This matters only where an archive file was incomplete before hydration.
- **`group_bytes` took `num_groups`' positional slot** in `open_ebooklet`, `open_rcg`, both classes and `open_edataset` (read from signatures). A positional `num_groups=101` silently becomes a 101-byte target. Making it keyword-only would close this.
- **ingest-base update helpers against an absent remote now create a grouped remote** where they used to create per-key (read from source, low severity). Passing nothing on update is otherwise correct.

### 5. Tests that cannot fail (mutants that survive the fake suite)

Each mutant ran in a fresh copy with `PYTHONDONTWRITEBYTECODE=1`, and I confirmed `ebooklet.__file__` pointed at the copy. All of these ended `203 passed`:

| Mutant | What is unpinned |
|---|---|
| `tail_size_index_len` | the tail's projected size using updated local lengths |
| `pull_no_remote_ts` | the pull half of the commit-path fix ("an open or pull re-fetches") |
| `pull_no_legacy_raise` | the legacy refusal inside `_pull_remote_index` |
| `adopt_no_journal` | the journal update at pull-time mode adoption |
| `sidecar_no_suppressed_clause` | the `index_fetch_suppressed` clause of the layout rule |
| `no_layout_belt` | the 19-byte sidecar belt |
| `emptied_no_updated` | `updated = True` in the emptied-group loop |
| `lost_not_emptied` | the emptied branch after a lost-key drop |
| `no_deletes_filter`, `no_delete_loops`, `no_post_commit_delete_apply`, `no_commit_set_storage`, `legacy_hash_none` | dead code, see S1 |

`test_stale_index_belt` is the only test that kills `no_belt`, and it does so by hand-setting `remote_ts = 1`. The other twelve mutants I ran were killed, including the fsck fields, the range check, the `'n'` guard, the emptied-group GC and the shim.

## Simplicity

- **S1. Delete the dead delete machinery** (verified equivalent on what I ran). Remove `committed_delete_keys` (three sites), the `key in deletes` filter in `in_index`, the `if not keys_in_group` branch, the commit-time `journal.set_storage`, and `STORAGE_LEGACY_HASH`.
  - A combined build with all five removed, and `assert keys_in_group` in place of the branch, gave `203 passed` and 0 failures over 550 randomized seeds.
  - Deletes are already gone from the sidecar before the push. An affected group always has a locally present member, so a group cannot empty through a lost-key drop (your question 4).
  - Gives up: nothing reachable without concurrent mutation during a push.
- **S2. Replace `_note_commit` with `_load_db_metadata()`** and drop the two post-push reloads in `Change.push`. One HEAD per push, and it fixes finding 1's main route.
- **S3. `StaleIndexError`:** the check compares two cached values, so it cannot see a concurrent writer (`lock.verify()` does that), and I found no API path that reaches it. Keep the five-line check as an internal error like the layout belt; drop the public class, export and docs. Gives up: a typed name for a state that needs a bug to occur.
- **S4. Journal v1/v2 split:** the v2 stamp only lands on writer files. A 0.11 grouped reader cache has no journal at all, and 0.10.5 opens it offline and serves from it:

  ```
  grouped READER journal slot: None | sidecar len 19
  0.10.5 open gw.blt offline: ValueError: ... journal with version 2 ...
  0.10.5 open gr.blt offline: OK, reads {'k1': b'v1', 'k4': 'OfflineError'}
  0.10.5 open gr.blt online: UnsupportedFormatError: ... format_version 3 ...
  ```

  Always writing v1 with the extra `storage` field removes `JOURNAL_VERSION`/`_V1` and the conditional. Gives up: 0.10 refusing a 0.11 grouped writer file at the journal. I did not run that variant.
- **S5. cfdb and envlib sentinels:** `open_edataset(**kwargs)` and `publish(**open_kwargs)` already pass through to ebooklet. Letting `group_bytes`/`num_groups` ride them deletes both `_NOT_GIVEN` objects and three `if`s, and removes the positional hazard. Gives up: the named parameter in the signature.
- **S6. `plan_groups`** returns three values and its only production caller discards two. Return the assignment.
- **S7. fsck and `copy_remote`:** keep `dead_fraction`, since it backs the lazy-delete contract. `bad_entry_layout` has no producer I could find (about 9,700 clean mixed-mode pushes never set it); it is cheap either way. The `copy_remote` change is behaviour-neutral, as you said.
- **S8. A larger option (not tested):** stop editing the sidecar in `__delitem__` and hide deleted keys through `journal.deletes`. That would remove both delete replays and `discard()`'s forced re-pull, and would fix finding 3.
- **Test file size** is fine; the gap is the seven unpinned behaviours above, not excess.

## What I found sound, and how

- **Cross-version per-key (your unrun experiment):** 0.11 writes a per-key remote stamped `'format_version': '2'`, entry length 15, journal `{"v":1,…,"storage":"per_key"}`. 0.10.5 then:
  - read it from a fresh file;
  - read 0.11's reader cache;
  - opened 0.11's writer file `'w'`, wrote, deleted and pushed (`PushResult(updated=True, failures={})`);
  - ran a clean fsck.

  0.11 then reopened it, pushed again, and the stamp was still 2.
- **Randomized model check** (real open/push/read paths, two alternating writer files, a fresh reader and a long-lived reader cache compared with a dict model after every clean push, plus fsck):
  - Operations: set, update, delete, del + re-set, prune-evict, `clear()`, `discard()`, `delete_remote()`, flag `'n'` replacement, induced PUT/GET failures with retry or reopen, and a crash at "commit succeeded".
  - Result: `0 failing` over 1,400 seeds at `group_bytes` 1, 40, 64, 200 and None, with event counts in the hundreds each for crash, discard, clear and partial pushes.
  - Mixed-mode variant (a random mode kwarg on every open): `0 failing` over 900 seeds, about 5,800 format-3 and 4,000 format-2 clean pushes. Sidecar layout, stamp and `group_bytes` always agreed, so I found no path to format 2 with 19-byte entries or the reverse.
  - Masks: findings 1 and 3, and prune-evict straight after `delete_remote()` (which discards the only copy by design).
- **Write order:** 2,000 random-named keys over four sessions gave 126 groups with zero gid decreases along write order; the same held after `prune()` and after overwrites.
- **By reading only:** `group_entry_size` matches `pack_group`; a failed fresh gid is reused only with a new generation; a replacement's gid 0 cannot overwrite an old object; the tail's projected size counts live members.

## What all of this rests on that I did not check

- The fake S3: one version per key, no real lock contention, no real listing.
- No concurrent writers or readers in the randomized runs (serial sessions only), and no lost-lock case beyond your own test.
- Values were at most 120 bytes; nothing near 32 MiB or 4 GiB.
- cfdb, envlib and ingest-base were read, not executed.
- booklet 0.12.10 behaviour was taken as given.

One environment note: the Gmail, Google Calendar and Google Drive connectors need authorising in your claude.ai connector settings; I did not need them here.

<!-- finished: 2026-10-07T10:00:34+13:00 exit=0 -->
