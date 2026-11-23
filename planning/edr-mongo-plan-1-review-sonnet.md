<!-- ai-review-harness
round:      edr-mongo-plan-1
agent:      claude
requested:  model=claude-sonnet-5-5 effort=xhigh
resolved:   model=claude-sonnet-5-5
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/76bdfdbe-322e-43ec-a8dc-1ca32edd5331/scratchpad/edr-mongo-plan-1-brief.md
scopes:     ebooklet cfdb envlib
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs
started:    2026-11-23T20:41:08+13:00
-->

<!-- staged cfdb: 21040 KiB of 694048 KiB (673008 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged ebooklet: 10440 KiB of 229984 KiB (219544 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged envlib: 10996 KiB of 766244 KiB (755248 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- rebuilt from 11 agent turn(s); the report is whichever is substantive -->
I'll start by looking at the layout, the reference material and the design record.
Now I'll build the ebooklet and cfdb environments and read the key code.
I'm reading ebooklet's commit and GC order (`update_remote`) to check the plan's "commit before GC and before per-key deletes" assumption.
Next I'm checking whether I can get a real MongoDB server, since that decides which Mongo claims I can test and which stay reasoning.
conda-forge has MongoDB 8.0.x. I'm installing a real `mongod` on disk.
I'm building the cfdb stub-store experiment: all four dataset types, with every store method and attribute probe recorded.
I'm checking the label types across the three fetch paths (Q4), starting with what `unpack_group` returns as a timestamp, since the recovery path labels with it.
The per-request store survives `del` and even `ds.close()` until a cyclic GC pass. I'm identifying what holds it, so I don't misattribute the cause.
The output for m2 and m3 cut off the pass/fail line. I'm re-running with just the summary, and without `-x`.
My `values_correct` check is wrong even for the consistent files, probably because of packed-float precision. I'm comparing against cfdb's own read of the consistent file instead.
# Review of EDR plan 1: the MongoDB read path

I found three problems that can serve wrong data or lose a deletion silently, and a fourth whose guarantee holds only if the plan pins four details. Most of the plan's other claims held up under test. Your 404 rule is exact for every case I could stage with real writer commits and GC, provided the applied stamp is read at store construction and the writer-order invariants hold. The refresh converges after a crash at each apply step.

The 404 rule, the write order and the store contract are all exercised below against the real fetchers, ebooklet's writer, cfdb and a real `mongod`. The refresh is not: no plan code exists, so the refresh tests run a model I wrote from your steps (Evidence 5). The model is my reading of the plan, so treat results that depend on it as reasoning on real data.

**What I ran against**
- `mongod` 8.0.23 (conda-forge, standalone) with pymongo 4.20.1.
- The ebooklet and cfdb working copies, on Python 3.12.16.
- The unmutated hermetic ebooklet suite: 276 passed in 5.6 s.

## Findings, most serious first

### Class A: wrong data or a lost deletion, silently

**F1. The refresh rule "skip if the GET stamp is not newer" can strand the mirror permanently.**
- **What:** The commit stamp is `time.time_ns()//1000` on the writer, with no skew guard (`booklet.utils.make_timestamp_int`, called at `utils.py:1804`). A commit with a lower stamp than the previous one is a legal ebooklet commit. ebooklet's own readers handle it because they compare with `!=` (`remote_state.remote_ts != remote.timestamp`, and `current_ts != base_ts` at the commit guard). Your Step 3 compares "newer".
- **Why it matters:** Each poll then sees HEAD ≠ applied, downloads the whole db object, skips it, and leaves Mongo stale indefinitely.
- **Trigger:** The writer's clock is behind the previous writer's by more than the gap between pushes. That means pushes from more than one host, an NTP step back, or a VM resume. A restored older db object does the same.
- **Second consequence:** HEAD ≠ applied forever, so every real integrity fault is classified 503 and never 500.
- **A narrower variant:** `uuid` is written in step 2 but `applied_ts` in step 4. A remote recreated with a lower stamp, plus a crash after step 2, leaves the doc holding the new uuid with the old stamp. "Same uuid, not newer" then skips forever and the removed entries are never deleted. In the model, six deleted keys were served indefinitely (Evidence 5).
- **Fix:**
  - Treat any different `(uuid, ts)` pair as new state, as ebooklet does.
  - Skip only on equality.
  - Store the applied stamp as one sub-document written in step 4 only, and keep the doc's `uuid` for the entries separate.
  - If you also want to guard against a stale GET, re-HEAD instead of ordering stamps.
- **Confidence:** High that the writer accepts a lower stamp and that ebooklet's readers see it (executed). Medium on your rule, which I modelled from the plan's prose.
- **(a) Executed:** a real push under a patched clock (−60 s), the real ebooklet readers on the result, and the model refresh over the real stamps (Evidence 4 and 5).
- **(b) Not checked:** how skewed your writer fleet's clocks are. Whether "newer" meant something other than `<=`. Whether production writers run ebooklet 0.12.0.

**F2. cfdb never raises on an absent entry, so any request that straddles a refresh with deletions can return a wrong coordinate axis, not just NaNs.**
- **What:** The plan returns `None` for a key with no entry, and cfdb reads `None` as blank. In the stub run, dropping data chunks gave NaN. Dropping a coordinate chunk gave wrong coordinate values with no exception. Dropping the attrs keys gave `{}`. Plain `open_dataset` does the same thing; the stub only supplies the missing entries.
- **Why your write order doesn't cover it:** It prevents metadata running ahead of entries. It does not cover a store built from doc D0 whose later entry reads come after the refresh's step 3 has deleted keys that D0 still references.
- **Trigger:** A request spans the refresh of a push that deletes chunk keys the old metadata names. Examples are a removed variable, a truncated coordinate, or a dataset rebuilt with fewer chunks. The window is the request duration. Probability is low and impact is high.
- **Your order does work for appends:** old metadata plus new entries read correctly, while new metadata plus old entries gave a wrong time axis and 720 NaN cells (Evidence 9).
- **Fix:**
  - On any `None` result, re-read the dataset doc once. If the applied stamp differs from the one read at construction, raise `StaleIndexError`.
  - Bump a `rev` field in step 2, because `applied_ts` only changes in step 4, after the deletes.
  - The extra query happens only on the `None` path.
- **Confidence:** High on cfdb's behaviour (executed). Medium on exposure, since it needs a deleting push.
- **(a) Executed:** the stub-store drops and the two real cfdb files mixed in both directions (Evidence 7 and 9).
- **(b) Not checked:** shrink or rechunk pushes. Mongo's `find` snapshot behaviour under concurrent writes.

**F3. cfdb calls `load_items(all keys)` and then `get` per chunk, and a store that re-queries on each `get` is both slow and unsafe.**
- **What:** Every variable read is one `load_items` followed by N `get` calls (Evidence 7). The plan's `get` re-reads entries and chunks and returns `None` for an absent entry.
- **Unsafe:** The refresh can land between `load_items` and `get`. `get` then finds no entry and fills silently (F2's mechanism), even though `load_items` already succeeded.
- **Slow:** On loopback, 322 chunks cost 18 ms as one `$in` per collection and 79 ms as 322 two-query `get`s. That is 4.4× with zero network latency, and 644 round trips against 2 over a real network (Evidence 10).
- **Fix:** Memoize entries and bytes in the per-request store. `load_items` fills the memo and `get` serves from it.
- **Confidence:** High. **(a)** executed. **(b)** a networked Mongo.

### Class B: false alarms and availability

**F4. The 404 classification is exact only under four conditions the plan does not pin.**
I staged real writer commit + GC, per-key delete, an out-of-band delete, replacement, and a crash before GC. Grouped and per-key both classified correctly (Evidence 2). These cases give a 503 where they should:
- Fault plus an unrelated later commit is classified 503, then 500 after the refresh. That is transient and benign.
- A writer commit with GC is 503.
- A key deleted by a writer is 503.
- A replacement is 503.
- The false-500 direction needs a stamp read late or an ebooklet writer reorder, as follows.

The conditions:
1. **Applied stamp timing.** It must be the construction-time stamp. If the store re-reads applied at HEAD time after the worker caught up, a plain GC 404 becomes a false 500 (Evidence 3).
2. **HEAD after the entry re-read**, as the plan has it.
3. **Writer order** (commit before GC, commit before per-key delete, db object deleted first by `delete_remote`), plus an uncached HEAD. ebooklet's suite pins only the first. I mutated copies:

| Mutant | Result |
|---|---|
| GC before commit | 7 existing tests fail |
| Per-key delete before commit | 276 pass |
| `delete_remote` children first | 276 pass |

   Under the per-key mutant, a probe at the commit PUT sees a false "integrity (500)" (Evidence 12).
4. **Your listed tests can't see writer order.** "A real writer commit with GC, then an uncached read" gives the same end state for either order. FakeS3 can observe it with a hook at the commit PUT (Evidence 12).

- **Blast radius:** A group repack 404s unchanged members too. In S1, `k01` kept its `ts` and bytes but moved, and 404'd.
- **Confidence:** High. **(a)** executed. **(b)** B2 and Cloudflare HEAD caching, which your plan defers to plan 2, and older ebooklet writers. I did not verify that envlib's writers all run 0.12.0-era code.

**F5. `missing` is never cleared.**
- Step 1 sets it on a HEAD 404. Step 2 ("equal → stop") never clears it. A transient 404 therefore makes the dataset unavailable until the next commit.
- ebooklet's own push path treats a HEAD 404 as possibly transient ("a transient answer, a routing hiccup", in the `push()` pre-push re-check comment).
- **Fix:** set `missing=False` on every HEAD 200.
- **Confidence:** High on the model. It rests on the plan's wording, because the plan has no code yet.

### Class C: Mongo and test-design specifics (all executed against the real server)

**F6. Mongo traps** (Evidence 8):
- A `$jsonSchema` `bsonType:'int'` on offset or gid rejects values over 2³¹ (code 121). Both fields go up to 2³²−1 in ebooklet's format. Tests with small offsets won't catch this. Use `number`, or `['int','long']`.
- A mixed `insert_many(ordered=False)` batch returned `nInserted 1` with codes `[121]` for the bad document. "Tolerate duplicate-key errors" must filter on code 11000 only.
- An oversize document in a batch raised `OperationFailure` and lost the whole batch, including the good documents. Pre-filter by size.
- TTL expires only on a BSON Date. In my run an `int` and a `str` field survived while a `datetime` and an array of dates expired. A `time.time()` field would give an unbounded cache with no error.
- `create_index` with a changed TTL raises code 85. `collMod` works.
- A test can set `ttlMonitorSleepSecs=1` at runtime and then check expiry in about a second.

**F7. Test and mutant gaps:**
- "Skip the GET-stamp check" can only be killed by a test that enshrines F1's behaviour.
- "Uuid out of the chunk id" is hard to kill. A flag='n' replacement keeps the remote's uuid (`uuid changed: False`, Evidence 2). Only `delete_remote` plus recreate changes it. Colliding on the same `(key, ts)` needs explicit timestamps.
- Crash injection stops at step 2. The mutant "applied before deletes" needs injection after step 3 or a full-order assertion.
- Add tests for:
  - the empty-list guard (`get_remote_group_values(..., [])` raises `IndexError`, Evidence 11);
  - a per-key object with no `timestamp` metadata (raises `KeyError`);
  - the unmanifested gid (stored with `n=None`, it GETs `0.None` and ends in the integrity marker);
  - TTL type and expiry, and a validator with offset > 2³¹;
  - `import ebooklet` with pymongo blocked.
- `bool(S3SessionReader)` already raises `AttributeError` in ebooklet, because `read_access` is never defined (Evidence 11). That is a pre-existing nit, relevant only if your code truth-tests a reader.

### Labels (Q4): I found no case where wrong bytes land under a wrong label
- **Group reads:** Generations are immutable and entries are atomic documents. Exp A confirmed the fetched bytes equal the source for both storage kinds.
- **Per-key reads:** The label is the object's own `ts`. During the in-place-overwrite window it differs from the entry `ts` (Evidence 2, S1 per_key: label 1795420088025047 against entry 1795420088015809). Lookups use the entry `ts`, so every request misses and refetches until the refresh. That is a performance cost, not wrong data.
- **Cold versus warm:** Cold reads can return the newer, possibly uncommitted bytes while warm reads return the entry's version. ebooklet's local cache behaves the same way.
- **Recovery path:** It labels with the embedded integer `ts`.

## Checked and sound

**Fetcher footprint.** The real fetchers touch only `reader.get_object` and `reader.read_db_key`, and only `sink.set(key, value, ts, encode_value=False)`. No HEAD is issued, for either storage kind (Evidence 1). GET and HEAD both extract user metadata through `add_metadata_from_urllib3`, so the "stamp from the GET" premise holds in s3func's code. I did not test it against real B2 or Cloudflare.

**Refresh convergence (model).**
- After a crash at steps 1, 2 or 3, one normal refresh converges.
- A commit between HEAD and GET converges, with the stamp taken from the GET.
- A crash at step 2 plus a second push converges.
- A repack with an unchanged `ts` is caught only because the diff compares whole entries (Evidence 5).
- Whole-entry diff is enough: every change to bytes bumps `ts` through ebooklet's skew rule, and a repack changes gen or offset. Dataset-level changes are rewritten on every apply.

**Store contract.** For all four dataset types, `writable`, `get_metadata`, `get`, `load_items`, `map`, `close` and `sync` suffice. No other attribute probe was recorded across repr, `.data`, `loc`, `select`, `select_loc`, `iter_chunks`, `groupby`, `load()`, `interp.to_points`, `to_grid` and `interp_na`, and `to_netcdf4` on grid (Evidence 7).
- `Variable.map` is the only caller of `_blt.map`.
- cfdb's finalizers do not leak a per-request store (refcount alone frees it), provided the store has no self-cycle.
- `to_netcdf4` fails on `ts_ortho` with or without the stub, as in your known-issues list.

**Housekeeping facts.**
- The CHANGELOG head reads "0.12.0 (unreleased)".
- PyPI shows ebooklet 0.12.0 (uploaded 2026-11-23), cfdb 0.11.0 and booklet 0.12.11.
- The `remote.py` line references match, as do `utils.py:781`, `:934` and `:975`.

**Mongo scale** (156,500 entries, 150 B per document, 23.6 MB):

| Operation | Result |
|---|---|
| `insert_many` | 0.93 s |
| Full diff-read | 0.32 s |
| Every entry changed (`bulk_write` `ReplaceOne` upsert) | 3.4 s |
| `$in` over 322 ids | 2.8 ms |
| `$in` over 5,000 ids | 24 ms |

A `$nin` over all 156,500 ids is accepted (a 4.00 MB query). Duplicate-key handling in `insert_many(ordered=False)` works as you assumed (Evidence 8).

## Your questions

| Q | Answer |
|---|---|
| 1 | Exact for grouped and per-key under F4's conditions. Per-key has no 404 source other than a delete (commit first) or a fault. |
| 2 | The refresh converges on every interruption I injected. The exceptions are F1's rule and F5's flag. |
| 3 | The write order holds for appends. F2 and F3 are the reader-side gaps. |
| 4 | No wrong label found. |
| 5 | The contract is complete. The risks are F3 and the store's own lifetime. |
| 6 | See F7 and F4. |
| 7 | See F6 and the scale numbers. |
| 8 | See below. |

**Unnecessary or simpler (Q8).**
- The `s` provenance field has no reader.
- `d`, `k` and the `entries.d` index repeat the `_id` prefix. A range scan on `_id` would avoid them. That is reasoning, not tested.
- The retry sleep of `retry_cap_s` (1.5 s) can't resolve a condition that clears only at the next poll. One immediate re-read is enough unless polling is seconds-fast.
- The per-request `threads=8` pool is worth it only if measured.
- cfdb's `open_store_dataset` duplicates the `dt` → class dispatch in `open_dataset`. Factor out a helper so a fifth dataset type can't be missed.
- The xarray backend can't take a store (it calls `open_dataset(filename)`), so leave it out of scope.
- **Step 1's refactor:** the existing tests give `storage_kind` one direct assertion (the `None` case). Add a table-driven test across format 1, format 2 with and without `num_groups`, format 3, and a too-new format before refactoring core ebooklet.
- **Alternative to the 503 window:** The window is a repack plus the poll interval. Delaying GC to `fsck`'s age gate would shrink it. This is a design idea I did not evaluate.

## Not checked, overall
- Real B2 and Cloudflare: HEAD caching, negative caching of 404s, GET metadata over `HttpSession`.
- Python 3.10 and 3.11.
- The cfdb test suite.
- A replica-set Mongo, and Mongo over a network.
- Multi-worker interleavings (reasoned only).
- `dataset_id` → `db_url` changes across dataset versions.
- ebooklet writers older than 0.12.0.
- The spike's scripts.
- Two driver ops in my cfdb run raised errors from my own code and are not store-related: `Variable.iter_chunks(chunk_shape=...)` and `Dataset.map` (an unpicklable lambda). I did not re-run them.

## Evidence

Shared helpers are in `/scratch/exp/common.py`. They are discarded with the container, so the essentials are inlined where a finding rests on them. In every run, `ebooklet` resolved to `/scratch/work/ebooklet/ebooklet` unless stated, and cfdb runs used `/opt/envs/cfdb/bin/python`.

```python
# common.py (core)
class HeadlessReader(remote.S3SessionReader):      # the plan's object_reader: skips __init__'s HEAD
    def __init__(self, read_session, read_db_key, threads=4):
        self._read_session = read_session; self.read_db_key = read_db_key; self.threads = threads
class Sink:   # records every call the fetchers make on it
    def set(self, key, value, ts, encode_value=True): self.sets.append((key, bytes(value), ts, encode_value))
    def __getattr__(self, name): self.calls.append(name); raise AttributeError(name)
```

### 1. Fetchers through a `get_object`-only reader
```
$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_a_headless.py
grouped index value_len 19 manifest {0: '9a55220c4ff64', 1: '705cdc76f1534', ...}
 fetched 13 values; bytes equal source: True
 sink methods called beyond set(): []
 reader attrs touched: ['get_object', 'read_db_key']
 session calls: ['get_object']
 encode_value kwargs seen: {False}
per_key index value_len 15 manifest {}
 fetched 3 values; bytes equal source: True
 sink methods called beyond set(): []
 reader attrs touched: ['get_object', 'read_db_key']
 session calls: ['get_object']
 encode_value kwargs seen: {False}
```

### 2. 404 classification against real writer commits and GC
The experiment seeds a real remote, takes the applied stamp from a GET's metadata, runs a real push, then reads with the real fetchers. It classifies with one HEAD compared to the applied `(uuid, ts)`.
```
===== grouped
S1 writer rewrote k00 (commit+GC); stale read of ALL keys, #404 -> 2 of 10 keys 404 [stale(503)]
S2 writer deleted k03 ... stale read k03 -> ok   (grouped lazy delete keeps group bytes: True)
S3 object deleted out-of-band, no commit -> 404 [integrity(500)]
S3b same fault, then an unrelated commit before the 404 is classified -> 404 [stale(503)]
    after refresh, same entry unchanged? True -> integrity(500)
    replacement: uuid changed: False
S4 replaced remote, stale read k00 -> 404 [stale(503)]
S4 replaced remote, stale read k07 -> 404 [stale(503)]
    writer raised: RuntimeError crash before GC
S5 commit landed, GC crashed; stale read k00 -> ok
===== per_key
S1 ... stale read of ALL keys, #404 -> 0 of 10 keys 404
    keys served OK but with bytes != original: ['k00']
    e.g. k00 entry ts 1795420088015809 label given to sink 1795420088025047 label==entry ts? False
S2 writer deleted k03 ... stale read k03 -> 404 [stale(503)]
S3 object deleted out-of-band, no commit -> 404 [integrity(500)]
S3b same fault, then an unrelated commit ... -> 404 [stale(503)]
    after refresh, same entry unchanged? True -> integrity(500)
    replacement: uuid changed: False
S4 replaced remote, stale read k00 bytes=b'REPLACED-k00' label=1795420088081014 (entry ts 1795420088070602 ... ) -> ok
S4 replaced remote, stale read k07 bytes=b'val07val07' ... -> ok
S5 commit landed, GC crashed; stale read k00 -> ok
```
I also read `delete_remote` (`remote.py:457–475`). It deletes the db object first and then the children.

### 3. False 500 from reading the applied stamp late
```
$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_h_order.py
request (entries read at T0) gets 404 for ['k00', 'k01']
worker: applied(+2 -0)
HEAD stamp == applied stamp read at CONSTRUCTION? False -> classification:  stale(503)  (correct)
HEAD stamp == applied stamp RE-READ at classification time? True -> classification:  integrity(500)  (FALSE ALARM: the 404 was the GC of an old generation)
```

### 4. Commit stamp skew
The experiment pushes commit 1 normally. A persistent reader file `r.blt` syncs at commit 1. A real push by a writer under `booklet.utils.make_timestamp_int = lambda ts=None: real(ts) - 60_000_000` carries the second commit.
```
$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_c_skew.py
commit 1 stamp 1795420120089375
push result True PushResult(updated=True, failures={})
commit 2 stamp 1795420060099483  (t2 - t1) us = -59989892  same uuid: True
ebooklet fresh reader  a = b'A2-new'  c = b'C-added'
ebooklet persistent reader (synced at commit 1) a = b'A2-new'  c = b'C-added'
plan step 2: HEAD == applied? False  -> proceeds to GET
plan step 3: "skip the apply if the GET stamp is not newer than applied_ts with the same uuid":  newer = False => apply skipped
```
The last two lines apply my reading of the plan's rule to the real stamps.

### 5. Refresh model
The model is a literal reading of Step 3 over real Mongo and real ebooklet payloads.
```python
def refresh(db, ds, store, *, crash_after=None, skip_rule='plan', between_head_get=None):
    h = sess.head_object('db'); head_time = time.time()
    if h.status == 404: set missing + $max checked_at; return
    doc = db.datasets.find_one({'_id': ds}) or {}
    head = (h.metadata['uuid'], int(h.metadata['timestamp'])); applied = (doc.get('uuid'), doc.get('applied_ts'))
    if applied == head: $max checked_at; return 'nochange'
    if between_head_get: between_head_get()
    g = sess.get_object('db'); gstamp = (g.metadata['uuid'], int(g.metadata['timestamp']))
    if skip_rule == 'plan' and doc and gstamp[0] == doc.get('uuid') and gstamp[1] <= doc['applied_ts']: return 'skipped'
    new = parse(g.data); cur = mongo_entries(); changed = {k:e for k,e in new if cur.get(k)!=e}; removed = [k for k in cur if k not in new]
    bulk upsert(changed);  [crash 1]
    datasets.update_one($set meta_section, uuid, format_version);  [crash 2]
    delete_many(removed);  [crash 3]
    datasets.update_one($set applied_ts=gstamp[1], $max checked_at=head_time)
```
```
$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_d_refresh.py
D1 ... initial load equals remote index   converged=True / after push+refresh converged=True / second refresh: nochange
D2 crash after step 1 -> applied(+0 -2)  converged=True
   crash after step 2 -> applied(+0 -2)  converged=True
   crash after step 3 -> applied(+0 -0)  converged=True
D3 racing commit between HEAD and GET -> applied(+4 -3) converged=True; next poll: nochange
D4 crash@2 then a SECOND push -> applied(+3 -1) converged=True
D5 members with UNCHANGED ts but changed (gen/offset/len): ['k01']  -> whole-entry diff converges True

$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_e_stuck.py
E1 remote deleted + re-created (new uuid) by a writer whose clock is behind; crash after step 2; then keep polling
 [skip rule = plan] new uuid: True  T_new - T_old (us) = -599687839
  next 3 polls: ['skipped(GET stamp not newer)', 'skipped(GET stamp not newer)', 'skipped(GET stamp not newer)']
  state after 3 more polls   converged=False | stale entries still served: ['old0', 'old1', 'old2', 'old3', 'old4', 'old5']
 [skip rule = neq] new uuid: True  T_new - T_old (us) = -599681493
  next 3 polls: ['applied(+0 -6)', 'nochange', 'nochange']
  state after 3 more polls   converged=True
E2 transient HEAD 404, then the same db object again
  poll during the 404 -> missing
  poll after it is back -> nochange
  datasets doc: missing = True (store construction raises RemoteMissingError while True)
```

### 6. Writer-order mutants (existing hermetic suite, mutated copies in `/scratch/mut`)
```
unmutated baseline:           276 passed
m1 GC before the commit PUT:  7 failed, 269 passed   (test_generational::test_failed_commit_leaves_readers_and_sidecar_untouched + 6 in test_lease_commit_guard)
m2 per-key delete_objects before the commit PUT:   276 passed
m3 delete_remote children first, db object last:   276 passed
```
The m2 and m3 summaries came from re-running with `-q`, and I used the pass counts, not the grep over test names that polluted the first output.

### 7. cfdb stub store (all four types; every access logged)
```
$ /opt/envs/cfdb/bin/python /scratch/exp/exp_f_cfdb.py
grid  var.data   ok  calls={'load_items': 1, 'get': 8}
grid  ds.select_loc ok calls={'load_items': 4, 'get': 7}
grid  var.map(f)  RAISED NotImplementedError: map   calls={'load_items': 1, 'map': 1}
ts_forecast var.groupby(first coord) ok calls={'load_items': 2, 'get': 3}   (same pattern for ts_ortho, grid_forecast)
 ... no ATTR-PROBE entry recorded in any row; "after ds.close(): ['close']"; ds.sync() -> {'sync': 1}
interp (CRS'd grid):  to_points  ok  store calls={'get': 14, 'load_items': 5} probes=[]
                      to_grid    ok  store calls={'load_items': 5, 'get': 11} probes=[]
                      interp_na  ok  store calls={'load_items': 5, 'get': 11} probes=[]
absent entries (grid):  drop all data-var chunks   -> NO EXCEPTION; NaN cells=1728
                        drop coord latitude chunks -> NO EXCEPTION; coords equal={'latitude': False,...}
                        drop coord time chunks     -> NO EXCEPTION; coords equal={...,'time': False}
                        drop all attrs keys        -> NO EXCEPTION
ts_ortho:  drop coord time chunks -> NO EXCEPTION; coords equal={'point': 'obj', 'time': False}
plain open_dataset(ts_ortho).to_netcdf4 -> AttributeError 'Point' object has no attribute '_factor'  (pre-existing)
```

### 8. Mongo
```
$ /opt/envs/ebooklet/bin/python /scratch/exp/exp_g_mongo.py
  insert_many(156500) ordered=False : 0.93s    full diff-read find({d}) -> dict : 0.32s
  entries size MB 23.6 ... avgObjSize 150      bulk_write ReplaceOne upsert x156500 (every entry changed): 3.39s
  $in over 322 ids: 2.8 ms | 5000: 23.9 ms | 50000: 174.7 ms | 156500: 393.5 ms (query doc ~4.00 MB); plan FETCH IXSCAN; $nin with 156500 ids OK
  BulkWriteError nInserted= 2 errors codes= [11000]; stored ids ['a','x','y']
  oversize in batch -> OperationFailure BSONObj size: 17825818 (0x110001A) is invalid. Size must be between 0 and 167936
  stored after oversize attempt: ['a', 'x', 'y']        # p and q, the good docs in that batch, were lost
  offset 3_000_000_000 REJECTED code=121   gid 4_000_000_000 REJECTED code=121   (validator bsonType int)
  mixed batch: nInserted 1 codes [121]
  create_index with a different TTL -> 85 IndexOptionsConflict
  collMod: {'expireAfterSeconds_old': 3600, 'expireAfterSeconds_new': 60, 'ok': 1.0}
  after 1s survivors: ['int', 'iso-string', 'no-t']    # BSON Date and array-of-dates rows expired; int and str never do
```
(`ttlMonitorSleepSecs` was set at runtime to 1.)

### 9. Entries ahead of metadata versus metadata ahead of entries (real cfdb files)
Two real files: 40 h, and the same dataset grown to 60 h.
```
$ /opt/envs/cfdb/bin/python /scratch/exp/exp_n_mix.py
  consistent old                               shape=(6, 6, 40) values_correct=True time_axis_correct=True NaN_cells=0
  consistent new                               shape=(6, 6, 60) values_correct=True time_axis_correct=True NaN_cells=0
  OLD meta + NEW entries (plan: entries first) shape=(6, 6, 40) values_correct=True time_axis_correct=True NaN_cells=0
  NEW meta + OLD entries (what the order prevents) shape=(6, 6, 60) values_correct=False time_axis_correct=False NaN_cells=720
```
My first version of this script compared against raw random data and reported `values_correct=False` even for the consistent files, because of packed-float rounding. I fixed the reference to cfdb's own read of the consistent file. The numbers above are from the fixed run.

### 10. Batch versus per-get Mongo cost
```
322 chunks: one $in per collection (load_items) 18.2 ms | get() per chunk, 2 queries each 78.5 ms | ratio x4.3 [loopback]
322 chunks: one $in per collection (load_items) 17.8 ms | get() per chunk, 2 queries each 79.0 ms | ratio x4.4 [loopback]
```

### 11. Fetcher edge cases and a pre-existing nit
```
empty key_infos -> IndexError list index out of range
per-key object without user metadata -> KeyError KeyError('timestamp')
gen=None -> remote object '0.None' is missing but the remote index still claims key(s) ['k0'] (integrity failure) ...
bool(real S3SessionReader) -> AttributeError 'S3SessionReader' object has no attribute 'read_access'
```

### 12. Writer-order probe at the commit PUT
A reader holding the applied stamp T0 is probed just before the db-object PUT of a real push.
```
--- unmutated ebooklet:  per_key: none (correct)        grouped: none (correct)
--- mutant m1 (GC before commit):     grouped: [('k00', 'integrity(500)'), ('k01', 'integrity(500)')]
--- mutant m2 (per-key delete before commit):  per_key: [('k03', 'integrity(500)')]
```

### 13. A retraction
My first lifetime run suggested cfdb's finalizers keep a per-request store alive until a GC pass. That was my stub's own self-cycle (`self.load_items = self._load_items`). With a clean stub, both the dataset and the store are freed by refcount, with or without `ds.close()`. Your plan is fine on this point as long as the store has no self-cycle.

<!-- mcp offered to the arm: 0 server(s), 0 tool(s) -->
<!-- wrap-up: requested at 4800s of 5400s (turn budget 525s); not needed: the arm finished first -->
<!-- finished: 2026-11-23T21:03:04+13:00 exit=0 -->
