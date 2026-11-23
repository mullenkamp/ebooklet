<!-- ai-review-harness
round:      edr-mongo-code-1
agent:      claude
requested:  model=claude-fable-5-2 effort=high
resolved:   model=claude-fable-5-2
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/76bdfdbe-322e-43ec-a8dc-1ca32edd5331/scratchpad/edr-mongo-code-1-brief.md
scopes:     ebooklet cfdb
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs
started:    2026-11-23T21:32:57+13:00
-->

<!-- staged cfdb: 21060 KiB of 808212 KiB (787152 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged ebooklet: 10996 KiB of 499644 KiB (488648 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- rebuilt from 7 agent turn(s); the report is whichever is substantive -->
Read through both diffs and all the new tests; now checking how cfdb actually uses the store, then running the interleaving experiments.
Confirmed one serious issue (two overlapping refreshes leave MongoDB permanently wrong with both reporting `applied`). Continuing with edge-case refreshes, the heal path, and stale-index reads.
Per-key remotes with a stale index can return numbers from neither commit with no error; now pinning down whether a refresh landing mid-request does the same for grouped storage.
Checked s3func's `HttpSession` (a stateless wrapper over a urllib3 pool, so sharing it across fetch threads is fine). Now re-running the author's mutation run and adding my own mutants.
Heal-path branches all behave as designed. Fixing a slip in my per-key experiment (I had accidentally warmed both keys) and rerunning it.
Refactor differential done (one small post-exception state difference). Last checks: packaging of the new subpackage, and which cfdb read paths work through a store-opened dataset.
# Review: MongoDB read path (ebooklet) and `open_store_dataset` (cfdb)

**Verdict: not ready to release as documented.** One documented guarantee is false (two overlapping refreshes do not converge), and a request can return numbers that belong to neither commit, with no error. Within the single-refresher model, the heal path, the 404 classification, the memo, batching, the `remote.py` refactor and the cfdb dispatch all held up under execution.

Setup: `mongod` 8.3.7 from conda-forge (not the `mongo:8.0` CI uses), pymongo 4.20.1, booklet 0.12.11, s3func 0.10.0, Python from `uv sync`. Every experiment ran the working-copy code against `fake_s3` with real pushes and a real `mongod`. My scripts lived in `/scratch/exp` and are gone, so the decisive ones are reproduced below.

## Findings, most serious first

### 1. Two overlapping refreshes leave MongoDB permanently wrong, and both report `applied`

**Class:** silent missing data and false 500s, never self-corrected.

**What happens.** Refresh A GETs commit C2 and stalls. C3 lands. Refresh B GETs C3 and does step 1. A then runs to completion inside B's step 1→2 window, and B finishes. Neither `rev` condition trips. `applied` ends equal to the remote's pair, so every later poll says `nochange`, but the entries are A's:
- C3's new key is deleted from MongoDB, so `get()` returns `None`, which cfdb reads as fill.
- Grouped entries name garbage-collected generations, so reads raise `RemoteIntegrityError`.

**Trigger.** Two `refresh()` calls for one dataset overlapping, with a commit between their GETs, and the older one writing between the newer one's steps 1 and 2. That means a broken one-worker rule: a deploy overlap, a supervisor restarting a hung worker that later resumes, or overlapping timer ticks. `docs/ops.md` says the `rev` condition "makes an accidental second one converge, but not efficiently". That is refuted.

**Confidence:** high (executed, both storage kinds).

The script, with `Gate(db, coll, method)` being a proxy that pauses the first call of `coll.method` before running it:

```python
push(store, t/'seed.blt', gb, flag='n', set_items={f'k{i}': ... for i in range(6)}, metadata={'m': 1})
refresh(db, DS, HookConnection(store, DB))                      # C1 applied
push(store, t/'w.blt', set_items={'k1': b'C2-k1'*4})
gA = Gate(db, ENTRIES, 'find')                                  # A: after HEAD+GET(C2), before _current_entries
tA = Thread(target=lambda: resA.update(refresh(gA, DS, HookConnection(store, DB)))); tA.start(); gA.reached.wait()
push(store, t/'w.blt', set_items={'k1': b'C3-k1'*4, 'knew': b'C3-new'*4})
gB = Gate(db, DATASETS, 'find_one_and_update')                  # B: after step 1, before step 2
tB = Thread(target=lambda: resB.update(refresh(gB, DS, HookConnection(store, DB)))); tB.start(); gB.reached.wait()
gA.go.set(); tA.join(); gB.go.set(); tB.join()
```

`PYTHONPATH=/scratch/work/ebooklet:/scratch/exp /opt/envs/ebooklet/bin/python e1_two_refreshers.py grouped` (then `per_key`):

```
refresh A  : {'status': 'applied', 'upserted': 6, 'removed': 1, 'unmanifested': 0, 'applied': ('01a2…258f', 1795423251873749)}
refresh B  : {'status': 'applied', 'upserted': 7, 'removed': 0, 'unmanifested': 0, 'applied': ('01a2…258f', 1795423251883287)}
remote pair: ('01a20790ad8a8f8098eb8fbdc140258f', 1795423251883287)
applied    : {'uuid': '01a20790ad8a8f8098eb8fbdc140258f', 'ts': 1795423251883287} rev 3
entries == remote index: False
  keys missing from MongoDB: ['knew']
  keys with a stale entry  : ['k0', 'k1', 'k2', 'k3', 'k4', 'k5']
poll 1     : nochange
poll 2     : nochange
poll 3     : nochange
entries == remote index after 3 more polls: False
MongoStore.get('knew') -> None | ebooklet reader -> b'C3-newC3-newC3-newC3-new' | *** DIFFERENT, no error ***
MongoStore.get('k1') raised RemoteIntegrityError: Dataset 'ds1': the remote index claims key(s) ['k1'] but their objects are missing, …
MongoStore.get('k0') raised RemoteIntegrityError: …
---- per_key
entries == remote index: False
  keys missing from MongoDB: ['knew']
  keys with a stale entry  : ['k1']
poll 1..3  : nochange
MongoStore.get('knew') -> None | ebooklet reader -> b'C3-newC3-newC3-newC3-new' | *** DIFFERENT, no error ***
```

- **(a) Executed:** the real `refresh()` twice in threads; only the scheduling was controlled, at two MongoDB calls.
- **(b) Not checked:** how likely this interleaving is in your worker (I have not seen plan 2's process model), and behaviour on a replica set.

**Fix direction.** Optimistic checks alone cannot close this, because the loser can write entries after the winner's last check. Take a per-dataset lease in the dataset document and make step 4 conditional on its token. Add a periodic forced re-diff that ignores `applied` equality, so any divergence heals in bounded time. Correct the ops.md sentence either way.

### 2. A request can return numbers from neither commit, silently

**Class:** silent wrong data.

**What happens.** The store reads the metadata once at construction but reads entries live, and `rev` is only consulted on an absent entry or a 404. If a commit changes how a variable's chunks decode while keeping its keys, the request decodes new bytes with old metadata.

**Trigger.** A commit that re-encodes a variable in place (delete and recreate with the same name, shape, chunking and encoded width, but a different scale or offset), or a replaced remote, plus one of:
- a refresh landing during the request (both storage kinds); or
- on per-key remotes, simply a stale index: every request between the writer's upload and the refresh.

In practice an absent key often masks this by accident (finding 4): when I left the attrs keys unset, the same runs raised `StaleIndexError`.

**Confidence:** high that it happens (executed); the trigger is rare.

The experiment (`e3e_attrs.py`): a grid dataset with every attrs key present; v1 uses `dtype('float32', precision=2, min_value=-100, max_value=500)` with values 0..49; a `'w'` session then does `del ds['t']`, recreates it with `precision=1, min_value=0, max_value=5000` and values 1000..1049, and pushes. Reads go through `cfdb.open_store_dataset(MongoStore(...))`.

```
per_key  control: built AFTER refresh   -> equals v2 (the new commit)
per_key  request built BEFORE refresh   -> *** equals NEITHER commit, no error *** got=[-0.9000015 -0.80000305 -0.69999695] v1=[0. 1. 2.] v2=[1000. 1001. 1002.]
per_key  refresh lands DURING request   (refresh removed 0 keys) -> *** equals NEITHER commit, no error *** got=[-0.9000015 …] v1=[0. 1. 2.] v2=[1000. 1001. 1002.]
grouped  control: built AFTER refresh   -> equals v2 (the new commit)
grouped  request built BEFORE refresh   -> raised StaleIndexError Dataset 'ds1': chunk(s) ['__.attrs'] 404'd and the remote has committed since the index …
grouped  refresh lands DURING request   (refresh removed 0 keys) -> *** equals NEITHER commit, no error *** got=[-0.9000015 …]
```

The ops.md "Known limit" says each chunk is internally correct. That holds for the bytes but not for the decoded values.

- **(a) Executed:** real cfdb working copy, real pushes, real refresh between `open_store_dataset` and `.data`.
- **(b) Not checked:** whether any production pipeline re-encodes in place, and other decode-affecting metadata changes (compression, chunk shape with equal byte count).

**Fix direction.** Give the store a `rev` re-check the API calls before sending a response. To be complete, the refresh must also mark "applying" before step 1; otherwise a request that starts and ends between steps 1 and 2 still mixes.

### 3. Per-key remotes: one response mixes cached old chunks with new, even uncommitted, bytes

**Class:** inconsistent array, each chunk valid. The docs understate the window.

**What happens.** Per-key objects are overwritten in place before the commit. A stale-index request serves cached keys from the old commit and fetches uncached keys as whatever the object holds now. This runs from the writer's first PUT until the refresh, not only "if a refresh lands during the request" as ops.md says. `test_a_per_key_overwrite_is_cached_under_the_objects_own_timestamp` pins this behaviour.

**Trigger.** Any overwrite on a per-key remote (your live flow forecast is per-key) with a partly warm cache.

**Confidence:** high (executed).

`e8_perkey_ahead.py`: k00 warmed, k01 not; a push overwrites both; a read is fired from a `before_put` hook at the commit PUT, and again after it.

```
k00 is cached, k01 is not: ['k00']
DURING the push, pre-commit  : MongoStore k00=b'old-0o' k01=b'NEW-1N' | fresh ebooklet reader k00=b'NEW-0N' k01=b'NEW-1N' | remote calls {'GET': 1}
after commit, before refresh : MongoStore k00=b'old-0o' k01=b'NEW-1N' | fresh ebooklet reader k00=b'NEW-0N' k01=b'NEW-1N' | remote calls {'GET': 1}
refresh: applied
after the refresh            : MongoStore k00=b'NEW-0N' k01=b'NEW-1N' | …
```

The output also shows a cost: until the refresh, every request refetches the overwritten key, because the cache lookup uses the entry's timestamp.

- **(a) Executed:** as above.
- **(b) Not checked:** whether a coordinate chunk and its data chunks are ever overwritten in place by your pipelines. That is the case where this mislabels data.

**Decision for you.** The store can tell (fetched timestamp ≠ entry timestamp). Raising `StaleIndexError` there makes per-key match grouped, at the cost of 503s for those chunks for the length of a push, or indefinitely after a failed one. Otherwise document the real window.

### 4. Any refresh during a request turns an absent key into `StaleIndexError`, even when nothing was deleted

**Class:** spurious 503, misleading message.

`_absent` raises whenever `rev` moved. cfdb routinely reads keys that never existed: `_<var>.attrs` for variables without attrs, `__.attrs` at open, and sparse chunks. The message says the keys "vanished".

**Trigger.** Any applied refresh, including a pure append, between store construction and the read of any absent key.

**Confidence:** high (executed). From `e3d_inplace.py` (same as finding 2 but without attrs):

```
per_key  refresh lands DURING request   (refresh removed 0 keys) -> raised StaleIndexError Dataset 'ds1' changed during this request (keys ['_t.attrs'] vanished); retry.
```

- **(b) Not checked:** how often this fires at your request and commit rates; requests are short, so probably rarely.

`test_an_appending_refresh_during_a_request_reads_correctly` only reads keys that exist, so it does not show this.

### 5. Test gaps: which paths have no test that would fail

Your 25 mutants: I re-ran all of them with the runner repointed and `-x` removed so failing tests are named. **All 25 killed**, each by a relevant test.

I added 32 mutants; 18 survived. All ran with `/opt/envs/ebooklet/bin/python mutate.py my_mutants.json` against the same three test files, each ending `64 passed`.

| Surviving mutant | What it shows |
|---|---|
| Heal refetches with the old entry (`retry[k] = entries[k]`) | The ops.md row "re-read entry changed → fetched from the new location" has no test |
| Group fetch keyed by gid only | `test_a_gid_with_two_generations_mid_apply_fetches_each_from_its_own_object` uses two separate `get()` calls, so the `(g, n)` grouping its name claims is never exercised |
| Store built on `applied=None` proceeds | The construction-time `StaleIndexError` is untested |
| Classification HEAD non-200 ignored | Untested |
| Chunk-GET error swallowed (`raise HTTPError` → `pass`) | `test_a_failed_fetch_raises_and_never_returns_none` still passes, because `RemoteIntegrityError` is an `HTTPError`; it cannot tell a transient 5xx from an integrity fault |
| `_batches`, `WRITE_BATCH`, `IN_BATCH` loops cut to the first batch | No test crosses a batch boundary |
| GET-404 branch removed; `stale_get` removed; `stale_get` advances `checked_at` | No test reaches either path |
| Legacy gate on the GET removed | Untested (the HEAD gate fires first) |
| Apply does not clear `missing` | Missing → recreated → applied is untested |
| Validators not re-applied by `collMod`; no `d` index | Untested |

The rest were benign: no not-delivered safety net, ordered insert, single-thread fetch, `load_items` refetching memoised keys, duplicate-key not tolerated (it only logs), `checked_at` taken at write time, and the redundant legacy gate on the HEAD.

Killed, for the record: group GC before the commit, emptied-group GC before the commit (the plan lists a GC mutant; your evidence files have none, so I added both), diff without `u`, stamp from the HEAD instead of the GET, `_mark_missing` clearing `applied`, heal-vanished without a `rev` check.

I then drove the untested paths with the real code (`e4_untested.py`, `e6_heal.py`) and **all behaved correctly**, so these are coverage gaps, not bugs:

```
1 batching (WRITE_BATCH=3, IN_BATCH=2, store batches=3): initial applied 23 equal True / diff applied 2 11 equal True
1 batching: cold store equal True absent None True chunks cached 13 of 13 / warm store equal True remote calls {}
2 stale_get: stale_get checked_at advanced False rev 1 -> 1 applied unchanged True
3 first-ever refresh, GET 404 -> missing ; doc: {'_id': 'ds1', 'applied': None, 'db_url': None, 'missing': True, 'rev': 0} ; next refresh -> applied
4 refresh of a deleted remote -> missing ; of the recreated remote -> applied ; missing False equal True
5 store on applied=None -> RAISED StaleIndexError
6 chunk 404 then HEAD 500 -> RAISED HTTPError ; 7 chunk GET 500 -> HTTPError (is RemoteIntegrityError: False) calls {'GET': 1}
a. refresh lands between the first 404 and the re-read -> (True, {'GET': 2})     # equals an ebooklet reader
b. entry changes on re-read, third commit GCs it -> RAISED StaleIndexError ; calls {'GET': 2, 'HEAD': 1}
c. group object lacks one member -> RAISED RemoteIntegrityError ; memo: k02, k04 set, k03 '<unset>'
d. group re-packed in another order -> equal True | calls {'GET': 2}
e. 40 concurrent fetches, 12 injected 404s, threads=8: equal True (grouped and per_key), cached 40, warm calls {}
```

- **(b) Not checked:** s3func's real `HttpSession` under load. I only read that it is a stateless wrapper over a urllib3 pool; threads were exercised against the fake session.

### 6. Performance: a fully warm `.loc` request costs 19 MongoDB round trips

cfdb reads coordinate and attrs chunks one at a time through `get()`, and each is a one-key `_fill` (entries find, then chunks find, or a `rev` re-read if absent). Only the data-variable selection is batched.

`e7_roundtrips.py`: grid, 480 time steps, `.loc` over 170 hours of one variable, commands counted with a pymongo listener:

```
cold  .loc time slice: 27 MongoDB commands {'find:datasets': 2, 'find:entries': 9, 'find:chunks': 8, 'insert:chunks': 8} | entries finds for ONE key: 8 | remote {'GET': 8} | 19.1 ms
warm  same request   : 19 MongoDB commands {'find:datasets': 2, 'find:entries': 9, 'find:chunks': 8} | entries finds for ONE key: 8 | remote {} | 16.2 ms
warm  open only      : 3 MongoDB commands {'find:datasets': 2, 'find:entries': 1}
```

Round trips grow with the number of coordinate chunks, serially. The cold run also issued 8 separate inserts. A new `ThreadPoolExecutor` is built per fetch, even for one task.

- **(b) Not checked:** latency against a remote MongoDB (the timings are loopback), and large coordinates.

Cheapest fix: have the API (or cfdb) call `load_items` on the coordinate and attrs keys right after open.

### 7. `remote.py` refactor: one behaviour difference, after an exception

I compared `git archive HEAD` against the working copy (`e9_remote_refactor.py`).

```
HEAD   : (a) warning attributed to remote.py | enclosing: def _load_db_metadata(self)
         (c) re-pull raised UnsupportedFormatError ; session afterwards: format_version 4 storage_kind legacy
working: (a) warning attributed to remote.py | enclosing: def classify_db_metadata(meta)
         (c) re-pull raised UnsupportedFormatError ; session afterwards: format_version 3 storage_kind grouped
```

- **Warning attribution:** the `group_bytes` warning still lands in `ebooklet/remote.py`, shown once under the default filter in both. It was never attributed to user code.
- **Post-exception state:** a live session whose re-pull hits a too-new remote used to be left at `format_version=4` / `legacy`; it now keeps its old values. Every consumer of `storage_kind` that I found calls `_load_db_metadata()` first, which raises again, so I found no path where this matters.
- Malformed metadata raises the same exceptions in both.

- **(b) Not checked:** the live-credential suites, which exercise more callers.

### 8. Smaller items

- **Notes count is stale:** I got 355 passed (79 tests in the five new files), not 352 (76).
- **`_entries` is written but never read** in `store.py`; the "entries memoised" line in the docstring describes dead state.
- **`get_metadata(include_timestamp=True)` with no metadata** returns `(None, None)`; an ebooklet reader returns `None` (observed in `e2_edges.py`).
- **Read preference (reasoned, not run):** the absent-key rule needs the `rev` read to be at least as fresh as the entries read. With secondary reads and no causal consistency it can return `None` silently. ops.md should require primary reads.
- **A transient HEAD 404** takes the dataset offline (`RemoteMissingError`) for a whole poll interval although the index and cache are intact. This is a design choice worth stating.
- **cfdb message wording untested:** swapping `'file'`/`'dataset'` in the two `dataset_class` calls fails only the docs-inventory tests, which fail in any copied tree in my runner. The other seven cfdb mutants were killed by real tests.
- **Outside this change:** `cfdb.open_edataset(flag='n')` over an existing remote read the old remote's chunk during `v[:] = data` (`ValueError: cannot reshape array of size 144 into shape (12,3,2)` at `support_classes.py:1618`). I did not investigate further.

## What I checked and found sound

- **Suites:** ebooklet hermetic 355 passed in 14.9 s; with the URI unset the mongo tests skip, and with `CI=1` they error as intended. cfdb 495 passed in 4.3 s.
- **Heal path and classification, single refresher:** every branch in `e6_heal.py` gave the designed answer. I found no way to get wrong bytes or a wrong `None` without finding 1's second refresher.
- **Store built between refresh steps 2 and 3:** it serves a cached chunk for a key the commit deleted, and `None` once the entry is gone. This is consistent with the new metadata, which no longer references such keys.
- **Refresh edge cases** (`e2_edges.py`, both kinds): metadata-only remote, no-metadata remote, delete-only and metadata-only pushes (the stamp changes each time), all keys deleted, keys containing `|`, unicode, 900 characters or empty, and empty values. All matched the remote index and an ebooklet reader.
- **Validators** accepted every document on first insert, the missing-upsert path, and the apply path.
- **cfdb read paths through the store** (`e10_cfdb_paths.py`): `.data`, integer slices, `.loc`, `iter_chunks` (decoded and raw), `get_chunk`, `groupby`, `rechunker`, `select`, `select_loc`, attrs and repr all equal `open_edataset`. `copy`, `prune` and `map` raise the intended errors.
- **Packaging:** the wheel contains `ebooklet/mongo/*` and declares the `mongo` extra.

## What none of this covers

- No live B2: CDN or negative-cache behaviour, real `HttpSession` retries, and URL-escaping of per-key object names.
- The live equality result and the plan-review records were not re-run or audited.
- Replica-set behaviour, failover rollback, and `ensure_schema` raced from several processes.
- `interp`, dataset-level `iter_chunks` and a working `map` call were not reached (my invocations were wrong or needed a CRS).
- The `'n'` replacement sweep order was not probed by the writer-order test or by me.

<!-- mcp offered to the arm: 0 server(s), 0 tool(s) -->
<!-- wrap-up: requested at 4800s of 5400s (turn budget 525s); not needed: the arm finished first -->
<!-- finished: 2026-11-23T22:05:05+13:00 exit=0 -->
