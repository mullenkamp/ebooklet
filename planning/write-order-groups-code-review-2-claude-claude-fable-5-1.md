<!-- ai-review-harness
round:      ebooklet-wog-code-2
agent:      claude
requested:  model=claude-fable-5-1 effort=high
resolved:   model=claude-fable-5-1
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-code-2.md
scopes:     ebooklet cfdb envlib envlib-ingest-base
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-07T11:35:39+13:00
-->

<!-- staged cfdb: 1736 KiB of 5488 KiB (3752 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8912 KiB of 230564 KiB (221652 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1464 KiB (708 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-base: 4368 KiB of 369548 KiB (365180 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 4 agent turn(s); the report is whichever is substantive -->
I'll start by orienting in the ebooklet scope: the round-1 records, the diff since HEAD, and building the environment to run the suites.
Migration flow verified on both routes. Next I'm running the targeted mutants and reading envlib's `publish` signature.
Two more quick probes, then I'll write up: the flag-n crash-window variant where the journal was persisted once mid-session, and the HEAD-failure case for an ordinary (non-replacement) push.
## Verdict

One new data-loss path, introduced by A1, reproduced and fixed with a one-line move. Everything else I executed held: the A2 journaling, the D2 stamp, the D3 inference, the D1 result, the recorded `group_bytes`, and the 0.10.5 migration flow on both republish routes. Three of my nine mutants survived, two of them unreported by you, and both mark dead code rather than missing tests.

Baseline, as you observed:

```
218 passed, 10 warnings in 1.64s
```

## Findings, most serious first

### 1. accuracy, data loss. A replacement push whose post-commit HEAD fails is wiped by the retry. New in A1.

The reload at `ebooklet/utils.py:1855` runs after the journal has been cleared and persisted at line 1846, but before `Change.push` clears `replace_pending` at `ebooklet/main.py:337`. If the HEAD returns anything but 200 or 404, `_load_db_metadata` raises, the push propagates the error, and the journal on disk now says `replace_pending=True` with `written=[]`. The next push, in this session or after a reopen, treats every local key as stale replacement content, deletes all of them locally and from the index, commits an empty database, and sweeps the group objects. The docs tell the user to retry after a failed push, so this is the natural next step.

Evidence, a flag `'n'` session over an existing remote, the HEAD after the commit PUT answers 503 once, then a clean retry:

```
$ python a1_raise.py grouped
push 1 raised HTTPError after the commit: {'message': 'induced HEAD failure'}
remote index after push 1: ['k0', 'k1', 'k2', 'k3', 'k4']
journal after push 1: written=[] replace_pending=True
local keys before retry: ['k0', 'k1', 'k2', 'k3', 'k4']
push 2 result: PushResult(updated=True, failures={})
local keys after retry : []
remote index after retry: []
objects under d/: []
reader sees: []
```

Per-key is identical except the orphaned `d/k*` objects remain in the store. An ordinary `'w'` push with the same fault is safe: the retry hits the stale-index belt and a reopen pushes normally, as the second run below shows.

```
$ python a1_raise_w.py
push raised after the commit: {'message': 'induced'}
remote index: ['a', 'b', 'c'] | journal: [] | local stamp < remote ts: True
retry push in-session raised RuntimeError: internal error: this session's copy of the remote index is not the remote's current commit
reopen push: PushResult(updated=True, failures={}) | remote index: ['a', 'b', 'c', 'd']
```

Fix, verified: in `update_remote`, right after `clear_committed`, add `if replace_pending: journal.set_replace_pending(False)` before the single `journal.persist`. The replacement is finished the moment its commit lands, and the two facts belong in one write. With that patch the same experiment keeps every key in both modes, and the fake suites pass:

```
journal after push 1: written=[] replace_pending=False
[grouped] retry raised RuntimeError: ... re-open the file and push again.
[perkey]  push 2 result: PushResult(updated=False, failures={}) ; reader sees: ['k0'..'k4']
218 passed, 10 warnings in 1.63s
```

The `replace_pending` block in `Change.push` then becomes dead and can go. Consider also catching the reload failure and logging it, since a raise after a successful commit contradicts the `PushResult` docstring's "failures of the commit itself raise". Confidence: high.

- (a) Ran the experiments above against the fake S3, both modes, before and after the patch. Read the HEAD version of `Change.push`: the reload used to run after `set_replace_pending(False)`, so A1 opened this path.
- (b) The trigger is any non-404 error from the HEAD. I did not execute s3func's `head_object` against a real 5xx. The crash variant of the same window, a process dying between lines 1846 and 337, is pre-existing and derived from reading only. I did not test the patch with a replacement over a legacy remote.

### 2. accuracy, low. envlib `publish` still takes `group_bytes` positionally, in `num_groups`' old slot.

`envlib/catalogue.py:996` has `group_bytes=_NOT_GIVEN` as the fourth positional parameter, where 0.1.7 had `num_groups=None`. A caller that passed a group count positionally now gets a byte target that small, silently. This is the hazard B1 closed in `open_ebooklet`, `open_rcg` and `open_edataset`. Fix: a bare `*` before `group_bytes`. Confidence: high on the mechanism.

- (a) Read the signature and grepped the callers. Every caller in envlib's tests and in envlib-ingest-base uses the keyword.
- (b) No external callers checked; nothing executed.

### 3. accuracy, low. The recorded `group_bytes` is trusted on load.

A non-integer value makes every open fail, readers included. A zero does not crash: `plan_groups` opens a fresh group per key.

```
$ python g_malformed.py
reader open raised: ValueError invalid literal for int() with base 10: 'abc'
writer group_bytes with recorded 0: 0
push: PushResult(updated=True, failures={})
{'a': 0, 'k0': 1, 'k1': 2, 'k2': 3}
```

Only ebooklet writes the field, so I would not add much. A two-line guard at `remote.py:237` that treats an unparsable or out-of-range value as unrecorded, with a warning, turns a dataset-wide outage into a default. Your call.

### 4. simplicity. Three pieces of dead or unobservable code, found by mutation.

| Mutant | Result | Meaning |
|---|---|---|
| M4: `assert keys_in_group` removed, `utils.py:1605` | 218 passed | unreachable belt; keep or drop, no risk |
| M10: `and not index_fetch_suppressed` dropped, `main.py:558` | 218 passed | dead: a kind in {per_key, grouped} is never legacy, so never suppressed |
| M8: `copy_remote` branches on the manifest, `remote.py:499` | 218 passed | your known survivor; the emptied test still proves the copy works, just not the branch |

Also `fetched_manifest is not None` at `main.py:637` and `662` is implied by `index_fetched`, since `fetch_remote_index` returns a dict whenever it returns `True`. Drop the clause for one obvious condition. Nothing is lost by any of these cuts.

### 5. simplicity, on your question 9.

- The `deciding` block is fine. Two sources, one warning. I would not touch it.
- `_grouped_target` earns its place with two call sites.
- `committed` next to `updated` is right: one is the gate's input, the other its outcome. Keep both.
- A2 versus clearing `remote_ts`: keep A2. Clearing the watermark would also skip the reconciliation that the wrong-404 case depends on. The cost is small. At 1,936 keys per WRF dataset the journal blob is tens of kilobytes; at a million keys it is still cheap, though the cleared blob stays as dead bytes in the booklet file until a prune.

```
N=100000: record_write 0.01s, persist 0.02s, slot bytes=2,600,133
N=1000000: record_write 0.14s, persist 0.37s, slot bytes=26,000,133
clear+persist 0.00s, file bytes=26,072,619
```

## What I checked and found sound

**Mutants that were killed.** A1 without the reload: 8 failures. A2 without the journaling: 1 failure, the crash-route republish test only, so that one test is the sole pin. D2 without the open stamp: both `test_open_fetches_a_changed_remote_once` cases. D3 without the inference: 4 failures. C2 belt removed: `test_stale_index_belt` only, which is the arrangement round 1 accepted. D1 with `updated=True` on failures: 3 failures.

**The migration flow, with the real in-session delete.** I ran the actual `hydrate_and_delete.py --delete` under PyPI's 0.10.5 on the fixture's partial reader cache, then republished with the working copy. Route A is a plain push. Route B fails group 1, crashes after the commit, reopens, and pushes again. Both end with all ten values, the metadata, format 3, a recorded `group_bytes`, and a clean fsck. The in-session delete leaves `remote_ts` set and the sidecar emptied, which the 0.11 open handles the same way as the raw delete the tests use.

```
hydrate_and_delete rc = 0 | store keys left: []
slot1 journal: {"v":1,...,"num_groups":5,"num_groups_set":true,...}
slot2 remote_state: {"v":1,"remote_ts":1791317261345130,"manifest":{},"meta_section":null}
=== 0.11 route B
committed index: ['k0', 'k1', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9']
reopen: journal.written=['k0', ..., 'k9']
push 2: PushResult(updated=True, failures={})
reader: all 10 values correct = True | metadata = {'schema': 'legacy', 'n': 10}
fsck: claimed_but_missing=[] empty_groups=[] orphans=[]
```

**Your other questions, from reading.** A HEAD that returns 404 after the commit leaves the session uninitialised; the next push re-HEADs and either adopts the remote with the reconciliation protected by the commit-time watermark and the journal, or runs as a creating push over the committed sidecar. No loss either way, not executed. The GC phase reads only `pre_push_manifest` and `new_gens`, so the reload cannot affect it. A2 cannot disturb `loc_map`: booklet's `set_reserved` syncs then appends. A no-op push leaves the `.changelog` file and the `Change` object's cached path, but HEAD did the same, so D1 changed nothing there. The D2 stamp after a skipped reconciliation is acceptable because no concurrent writer can exist during `_init_common`. The push binds `group_bytes` once at the `update_remote` call, so a mid-session pull cannot split a push. An online reader whose remote is gone and whose sidecar is a 0.10 hash-grouped one reports per-key and raises `KeyError` for unmaterialised keys while `keys()` still lists them; that is the pre-existing remote-gone behaviour and harmless. `copy_remote` carries the metadata in both fake paths, and s3func's `copy_object` docstring says the source metadata is copied when none is given; I did not run it against real S3.

**Two out-of-scope observations.** My simulated hard crash of a flag `'n'` session that had synced once left booklet's `n_keys` header stale, and the recovery push died in booklet's `__delitem__` with `OverflowError: can't convert negative int to unsigned`. That is my crash simulation and booklet, not this change, and I did not trace it further. Separately, the pre-push adoption's forced pull short-circuits `check_local_remote_sync`, so no uuid check runs before a foreign remote's index is reconciled against the local file; that is 0.10.5 behaviour, read only.

<!-- finished: 2026-10-07T11:48:49+13:00 exit=0 -->
