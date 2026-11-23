<!-- ai-review-harness
round:      edr-mongo-code-1
agent:      claude
requested:  model=claude-sonnet-5-5 effort=xhigh
resolved:   model=claude-sonnet-5-5
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/76bdfdbe-322e-43ec-a8dc-1ca32edd5331/scratchpad/edr-mongo-code-1-brief.md
scopes:     ebooklet cfdb
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs
started:    2026-11-23T21:33:01+13:00
-->

<!-- staged cfdb: 21060 KiB of 808212 KiB (787152 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged ebooklet: 10996 KiB of 499644 KiB (488648 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- rebuilt from 6 agent turn(s); the report is whichever is substantive -->
I'll start by looking at the scopes and the diff.
Reading is done for `refresh.py`, `store.py`, `schema.py` and the `remote.py` diff. Next I'll check how the fetchers behave in `utils.py`, then set up a real `mongod` and run the suites.
Mongod 8.3.7 is running and both environments are built. Next I'll run ebooklet's hermetic suite against the real server to check the 352-pass claim.
Both suites pass here: ebooklet 355 passed (the notes say 352) and cfdb 495 passed. Now the cfdb diff.
Now the interleaving harness. It drives a real push (commit plus GC) and a real `refresh()` against the fake S3 and the live mongod. A probe request runs at every push op and every refresh step.
I found two silent wrong-data bugs, one false documentation claim and one availability bug. All are reproduced by execution. I found no wrong bytes in a single-refresher run of any interleaving I could build.

**Environment.** mongod v8.3.7 from conda-forge, pymongo 4.20.1, s3func 0.10.0, booklet 0.12.11, and cfdb 0.11.0 from PyPI inside ebooklet's env. For anything that needed `open_store_dataset` I used a third env with both working copies installed editable. All scripts below are in `/scratch/exp`, which will not survive, so the key ones are inlined.

## Your claims, re-run

| Claim | Result |
|---|---|
| ebooklet hermetic suite | **355 passed in 62.04 s** here. Your notes say 352 in 8.1 s. I did not investigate the difference; the 62 s is probably this machine. |
| cfdb suite | 495 passed, as claimed |
| 25 mutants killed | Reproduced. I re-ran your `mutate.py` with `REPO` and `PY` edited, and all 21 + 4 were killed. |
| Skip/fail without a mongod | Skips locally; with `CI=1` it errors "CI is set but EBOOKLET_TEST_MONGO_URI is not". |

## Findings, most serious first

### 1. A request built before a refresh decodes the new commit's chunks with the old commit's metadata, silently
**Class: wrong data, silently.**

- **What.** `_fill` never checks that an entry belongs to the commit the store was built on. The store keeps `_applied` and `_rev` but only consults `rev` when a key is absent.
- **Why it matters.** `ops.md` says "each chunk is internally correct". At the byte level that is true. A cfdb chunk is only meaningful with its dataset metadata (dtype scale and offset, coordinates), and that metadata is frozen at construction.
- **Trigger.**
  1. The remote is replaced with the same variable names and chunk layout but different packing or coordinates. I reproduced `delete_remote` plus a new push, and a `flag='n'` replacement.
  2. A store was built before the refresh and reads chunk keys after it.
  3. After the refresh it never looks up an absent key. Datasets with attrs on every variable (normal CF) never trigger the `rev` check.
  4. The stored width matches. In an earlier variant with a different width, cfdb raised `ValueError: cannot reshape array of size 36 into shape (12,3,2)`, which is a 500, not a 503.
- **Confidence.** High on the mechanism. I forced the timing deterministically.

Command:
```
PYTHONPATH=/scratch/exp /opt/envs/both/bin/python /scratch/exp/cfdbrecreate2.py none
```
`cfdbrecreate2.py` outline:
- A0 is a grid with float32 packed as `precision=2, min=-100, max=100`, attrs on every variable, 48×5×4, data in [-50, 50].
- A1 has the same names and chunking, with `min=0, max=200` and data in [60, 90].
- The script builds A0, refreshes, then builds `R = cfdb.open_store_dataset(MongoStore(...))`.
- It deletes the remote, pushes A1, refreshes, then reads `R['t'].data`.

Output:
```
refresh after recreate: applied 24 0
request on the A0-built store returned (no error):
  x coordinate : [10.  10.5 11.  11.5]  (A0 x = [170 170.5 171 171.5], A1 x = [10.  10.5 11.  11.5] )
  t[0,0,:]     : [-10.849998 -25.580002 -11.150002 -21.300003]  | A0 truth [-41.44 -26.32  30.13   8.22] | A1 truth [89.15 74.42 88.85 78.7 ]
  t equals A0: False  equals A1: False  range -39.9900016784668 -10.050003051757812
```
The same output appears for a `flag='n'` replacement without `delete_remote` (`cfdbreplace3.py`). That path keeps the same uuid, which I checked: `uuid changed on a flag=n replacement of an existing remote: False`.

**A fix that does not work.** I patched a copy of `_fill` to raise `StaleIndexError` when `e['u'] != self._applied[0]`. It stops the `delete_remote` case: the output becomes `request raised StaleIndexError ... the remote was recreated during this request`. All 64 mongo tests still pass, so no test pins either behaviour. But the output above for the `flag='n'` replacement is identical with the patch, so a uuid pin is insufficient.

**Direction I did not prototype.**
- Tag each entry with the `rev` of the apply that wrote it, and have the store reject entries newer than its own `rev`.
- Bump `rev` before step 1 so the tag is meaningful.
- This trades your documented "appending refresh reads correctly" for 503s on any chunk the later apply touched.

**(a)** Executed against real mongod, real cfdb, a real push, and the real refresh.
**(b)** I did not check whether any production dataset is ever replaced with changed packing. I did not check large or real data. I did not check whether `flag='n'` replacement is the real-world rebuild path.

### 2. Two overlapping refreshes diverge permanently, and the docs say they converge
**Class: wrong data, silently, or a false 500 (`RemoteIntegrityError`).**

- **What.** Steps 1 and 3 are unconditional writes. `rev` only guards step 4.
- **Docs refuted.** `ops.md`: "The `rev` condition makes an accidental second one converge". Plan: "the rev condition is the guard if a second one ever runs". Both are false for these schedules.
- **Trigger.** Two `refresh()` calls on one dataset overlapping, for example a rolling deploy, a hung worker that resumes, or a manual run. Each schedule below also needs a specific interleaving, which I forced with hooks and events.
- **Confidence.** High. Both schedules are deterministic and I ran each in both storage modes.

**Schedule 1: the older refresh's blind step-3 delete.** The order is A.s1, A.s2, [remote commits A2 re-adding k05], B.s1, B.s2, B.s3, A.s3, A.s4 (fails, writes `applied=None`), B.s4 (succeeds).

Command: `PYTHONPATH=/scratch/exp /opt/envs/ebooklet/bin/python /scratch/exp/two_refreshers.py grouped`
```
Dataset 'ds1': another refresh interleaved with this one; the next poll redoes it.
A: interleaved   B: applied
applied = {'uuid': '01a2079e6b5d8fb7a942ee9a262aabee', 'ts': 1795424152460224}  remote head = ('01a2079e6b5d8fb7a942ee9a262aabee', 1795424152460224)
next poll: nochange
remote has k05: True | mongo entries has k05: False | missing in mongo: ['k05']
MongoStore.get('k05') -> None  (remote value: b'RE-ADDED-k05' )
```
`per_key` gives the same result.

**Schedule 2: the older refresh's upsert overwrites the newer one's.** The order is B.s1, A.s1 (older entries), A.s2, A.s3, A.s4 (ok), B.s2, B.s3, B.s4 (ok). Both report `applied`, so the `rev` guard never fires.

Command: `... two_refreshers3.py grouped`
```
A: applied  B: applied
next poll: nochange
mongo entry k01: {'ts': 1795424193543453, 'g': 0, 'n': '6f4aed5332854', 'o': 20, 'l': 14}
remote objects: ['db/0.b54333d7230d4', 'db/1.84da7ec90c8a4', 'db/2.436c6d3c8d754', 'db/3.95424103e0cc4']
get('k01') raised RemoteIntegrityError Dataset 'ds1': the remote index claims key(s) ['k01'] but their objects are missing, and the remote has not committed since this index (remo
after 2 more polls: get('k01') raised RemoteIntegrityError
```
The entry points at a generation the A2 push already GC'd, and `applied` equals the head. Per-key mode serves correct bytes in this schedule.

Both states persist until the remote's next commit. The mechanism is that the loser's step 1 or 3 lands after the winner's.

Two related details:
- The loser's unconditional `applied: None` can clobber a winner's valid `applied`. That gives a transient 503, not wrong data.
- Fixing this needs mutual exclusion or fencing, not a stronger step 4. I have not prototyped one. The shape I would try is claim-first: bump `rev` with a compare-and-set before step 1, tag entries with it, and make steps 1 and 3 conditional on the tag. A lease document would also work.

Harness helper, needed to read the scripts:
```python
class HookedColl:   # wraps write methods: calls hook('mongo-before'|'mongo-after', coll, method)
    def __getattr__(self, name):
        attr = getattr(self._c, name)
        if name not in ('bulk_write','find_one_and_update','delete_many','update_one','insert_many'): return attr
        def wrapped(*a, **kw):
            self._hook('mongo-before', self._c.name, name); out = attr(*a, **kw)
            self._hook('mongo-after', self._c.name, name); return out
        return wrapped
```
In schedule 1, A's hook fires after its `find_one_and_update`. It pushes A2 and starts B in a thread. B blocks on an Event before its `update_one` (step 4), and A releases it only once A has finished.

**(a)** Executed, with deterministic scheduling.
**(b)** I did not check how your worker is actually deployed, so I do not know whether overlap can happen. I did not check a replica set, where write timing differs.

### 3. Any refresh that lands mid-request fails the request if it then touches any absent key
**Class: availability, loud, not wrong data. It contradicts your plan's claim.**

Plan: "A store built before an appending refresh reads correctly". Your test passes only because it reads keys that exist.

- **What.** cfdb looks up `_{var}.attrs` lazily per variable, and most variables have none. Every absent key triggers `_rev_changed()`. `rev` is bumped by every apply, so an append makes any in-flight request that touches a not-yet-memoised absent key fail.
- **Trigger.** A commit refresh lands during a request. The request fails if it has not yet looked up an absent key.
- **Confidence.** High.

Command: `PYTHONPATH=/scratch/exp /opt/envs/both/bin/python /scratch/exp/stale_attrs.py`
```
refresh: applied
in-flight request raised: StaleIndexError - Dataset 'cf1' changed during this request (keys ['_b.attrs'] vanished); retry.
```
In that run the pushed commit only appended time steps, and variable `b`'s own chunks were intact.

**Fix direction.** Make absence distinguishable from "removed by the apply that bumped `rev`". Tombstones would do it, or per-entry apply tags as in finding 1.

**(a)** Executed. **(b)** The retry cost in a real API is not measured.

### 4. `object_reader(url, http_session=s3func.HttpSession())` fails, then hangs the process
**Class: footgun on the documented path.**

- **What.** `HttpSession`'s constructor default is `stream=True`. Its responses have `.data is None` and hold their pool connection. Your docstring and `ops.md` tell users to share one `s3func.HttpSession`, and `remote.create_s3_read_session` builds it with `stream=False`.
- **Trigger.** The user follows the docs and constructs the session with defaults.
- **Confidence.** High.

Commands: `httptest.py`, `httptest2.py`. A real local HTTP server serves the fake store with `x-amz-meta-*` headers and Range support.
```
HttpSession(stream=False): ok=True 0.00s
HttpSession() defaults  : raised TypeError: object of type 'NoneType' has no len() (0.00s)
```
With `s3func.HttpSession(max_connections=2)`:
```
0 ['TypeError']
1 ['TypeError']
2 HUNG (>8s)
```
The third request hangs forever on the `block=True` pool, and every other request on the shared session behind it hangs too.

**Fix.** `object_reader` should reject or normalise a session whose `_stream` is true.

**(a)** Executed against the real `s3func.HttpSession`.
**(b)** I did not test `s3func.S3Session`, and `object_reader` cannot use it anyway.

### 5. Test gaps: guarantees that no test pins
I applied each mutant to a fresh copy (`mut.py`) and ran the 64 mongo tests.

| Mutant | Result | Meaning |
|---|---|---|
| Heal step 1 skips the entry re-read | **SURVIVED** | "404 and re-read entry changed → fetched from the new location" (ops.md table) has no test. My harness does reach the branch: with the mutant, mid-request `ok` drops from 696 to 270 in grouped mod-only. |
| `WRITE_BATCH` slice `-1`, `IN_BATCH` slices `-1` (3 mutants) | **SURVIVED** | No test crosses a batch boundary. With both set to 3 in a copy, all 64 tests pass, so the batching is correct but unpinned. |
| `stale_get` branch removed | **SURVIVED** | New status, no test |
| `applied is None` no longer raises `StaleIndexError` | **SURVIVED** | Construction on a not-yet-applied dataset is untested |
| HEAD non-200 branch removed | **SURVIVED** | |
| Non-`Missing` fetch error ignored | **SURVIVED** | See the weak test below |
| `threads` forced to 1 | **SURVIVED** | Multi-threaded fetch is untested |
| `object_reader(url)` default path broken | **SURVIVED** | I ran it by hand and it works |
| `_mark_missing` without `$setOnInsert` | **SURVIVED** | The first-ever missing remote is untested. I ran that path by hand and it validates. |
| Grouped GC moved before the commit PUT | KILLED | `test_writer_order` is sound for grouped |

**One weak test.** `test_a_failed_fetch_raises_and_never_returns_none` uses `pytest.raises(urllib3.exceptions.HTTPError)`. `RemoteIntegrityError` is an `HTTPError` subclass. With the "non-Missing fetch error ignored" mutant, a transient 500 gets reported as "the remote contradicts its own index", and the test still passes.

**Untested guarantees from the plan and docs:**
- The row above.
- Convergence with two refreshers.
- A "known limit" that is narrower than the real one (finding 1).

**(a)** Executed. **(b)** The mutants are mine, so the set is not exhaustive.

## Your eight questions

**1. Heal path.**
- I ran a push and a refresh inside one request, before the i-th store Mongo `find` or S3 GET and the j-th, for all i ≤ j. That is 20 configs: 2 storage modes × 5 scenarios (mod-del-add, mod-only, del-only, two-commits, replace) × 2 cache states. I then re-ran 8 of them with a stricter oracle (a request built at A0 may never return `None` for a key A0 has).
- There was no wrong-bytes or `None`-for-present result. The misclassified `RemoteIntegrityError` and `RemoteMissingError` cases are `StaleIndexError`-or-correct, except where the remote genuinely does not exist.
- Example: `mode=grouped scenario=mod-del-add cache=cold store-ops=9: {'stale': 383, 'ok': 744, 'none-for-key-absent-in-one-commit': 56}`.
- Partly delivered groups behave. A shifted offset heals via full-group recovery, with all 12 values right. A member removed from the group object gives `RemoteIntegrityError` for that key only, and 11 other keys are right.
- Over real HTTP, a deleted generation classifies as `RemoteIntegrityError`.
- **Gap:** my oracle accepts bytes equal to any commit's value, so it cannot see finding 1.

**2. Refresh consistency, single refresher.**
- I wrapped every Mongo write with a hook and tested crashing before and after each write. I ran this with `WRITE_BATCH` and `IN_BATCH` set to 3, so the step-1 batches and the deletes split across several calls.
- That gave 12 crash points in each storage mode, and all 24 converged on the next poll with no wrong mid-read.
- The interleave matrix (21 to 43 variants per config) also ended with zero final-state mismatches.
- Validators accepted every path I drove: first-ever missing, first applied in grouped and per-key, an empty remote (0 entries), a remote with and without metadata, a metadata-only commit, unicode and `|` in keys, a 1500-character key, and an empty value.
- The `rev` guard fails only for two overlapping refreshes (finding 2).

**3. The memo.** I found no stale or wrong memoised value. `None` is only memoised after the `rev` check, and failures are never memoised. `self._entries` is write-only (lines 105, 174, 181, 236 never read it), so it is dead state. A store that already raised `StaleIndexError` raises it forever on that instance, so the retry needs a new store, and the docs should say so.

**4. Concurrency.**
- I ran 16 threads × 3 rounds against one shared real `HttpSession(stream=False, max_connections=4)` with a per-key dataset. Each request used its own 8-worker pool over 60 keys. No error, all bytes correct.
- 300 refresh calls leaked no file descriptors (8 → 9).
- Your fake-session tests only ever used one worker, per the survived mutant.
- Not checked: the S3-signing session.

**5. `remote.py` refactor.**
- Behaviour-preserving as far as I could run it. The invalid-`group_bytes` warning, run against `git archive HEAD` and the working tree, shows the same file and a different line: `WARN remote.py 258` before, `WARN remote.py 199` after.
- Attribution was never to user code, and still isn't.
- The call sites are unchanged, including `main.py` 283, 437 and 1127, `utils.py` 2001 and the offline op.
- Reasoned only: on a too-new remote the old code assigned `self.format_version` before raising, and the new code assigns nothing. No caller catches `UnsupportedFormatError`.

**6. cfdb dispatch.**
- Old vs new `open_dataset` gave identical output on 13 cases: all four types, `GRID`, `bogus`, `None`, a list, a stored unknown type, a missing stored key, an unhashable stored type, and the lock released after each.
- Old vs new `open_edataset` gave identical output on 7 cases.
- The `what` argument reproduces both old messages.
- Through a `MongoStore` I ran coords, attrs, slicing, `.loc`, `iter_chunks`, `rechunker`, `groupby` (variable and dataset), `interp` and `to_xarray`. All equal `open_edataset`.
- `copy`, `map` and `prune` refuse with clear errors, and writes are refused.
- Not exercised by me: `cfdb_to_netcdf4` (h5netcdf is not installed), the xarray backend, and the cfdb Sybil docs tests.
- One nit: a failure after `Dataset.__init__` registers its finalizer closes the store twice. `open_dataset` already behaves this way.

**7. Tests.** See finding 5.

**8. Request-path cost (measured with a Mongo command recorder).**

| Request | Mongo commands | Per-collection | S3 |
|---|---|---|---|
| warm, 1 variable | 13 | 3 `datasets`, 6 `entries`, 4 `chunks` | none |
| cold, same request | 17 | adds 4 inserts | 4 GETs |
| open + close only | 3 | | |

- Each cfdb `get` of a new key is its own two-round-trip `_fill`, and each absent key adds a `datasets` read for the `rev` check.
- The chunk insert is synchronous inside the request.
- The store inherits ebooklet's merged range for sparse misses in one group.
- I did not measure latency, memory for large fetches, or the cost at 156k entries.

## Verified sound
- Writer-order tests, including my grouped-GC-before-commit mutant.
- Heal classification for one refresher.
- Single-refresher crash convergence.
- The cfdb dispatch refactor.
- The `remote.py` helpers.
- The validators.
- `object_reader` with a correctly built session.
- Skip and fail behaviour of the tests.

## Smaller items
- `ensure_schema(db, True)` passes the `isinstance(…, int)` check, then fails in `collMod` with an `OperationFailure`.
- `store.py` has lines over the project's 120-column limit (9 lines, up to 251 characters).
- `ops.md` says "two consecutive commits", but a skipped poll can span more.
- Outside this change: a `flag='n'` replacement in my replacement test lost the five attrs keys from the remote. In `cfdbreplace2.py` it printed `keys only in A0: ['__.attrs', '_t.attrs', '_time.attrs', '_x.attrs', '_y.attrs'] | only in A1: []`. My guess is that cfdb's `attrs_finalizer` sees attrs equal to the old remote's and skips the write. I did not confirm the cause, and I did not check whether it also occurs on an unmodified HEAD.

## Not checked
- A real B2 or S3 endpoint or any CDN. My HTTP server is a toy.
- A replica set or sharded MongoDB, read preference and write concern. Your ordering contract needs `primary` reads, and the docs do not say so.
- Server versions other than 8.3.7. CI uses `mongo:8.0`.
- Python 3.10 and 3.11 at runtime. I only parsed the 15 changed or new files under 3.10.
- The live equality script and the mkdocs build.
- `uv.lock` contents.
- Any scale beyond the tests, and the 10k and 50k batch sizes themselves. I only ran batch size 3.

<!-- mcp offered to the arm: 0 server(s), 0 tool(s) -->
<!-- wrap-up: requested at 4800s of 5400s (turn budget 525s); not needed: the arm finished first -->
<!-- finished: 2026-11-23T22:26:32+13:00 exit=0 -->
