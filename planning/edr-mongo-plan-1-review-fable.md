<!-- ai-review-harness
round:      edr-mongo-plan-1
agent:      claude
requested:  model=claude-fable-5-2 effort=high
resolved:   model=claude-fable-5-2
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/76bdfdbe-322e-43ec-a8dc-1ca32edd5331/scratchpad/edr-mongo-plan-1-brief.md
scopes:     ebooklet cfdb envlib
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs
started:    2026-11-23T20:41:04+13:00
-->

<!-- staged cfdb: 21040 KiB of 694048 KiB (673008 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged ebooklet: 10440 KiB of 229984 KiB (219544 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- staged envlib: 10996 KiB of 766244 KiB (755248 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml /.claude /.antigravitycli /site /logs) -->
<!-- rebuilt from 7 agent turn(s); the report is whichever is substantive -->
Still gathering: I've read the design record, the prototype and ebooklet's fetch/commit/GC code; now tracing cfdb's store usage before running the experiments.
Reading is done on the fetchers and the commit path; baseline hermetic suite passes (276 in 5.85 s) and a real `mongod` is running. Next: main.py's read/heal path, then the experiments.
Source reading is finished; starting the executed experiments now, beginning with the HEAD-less reader driven through ebooklet's real fetchers.
Mid-refresh mixing is confirmed on real cfdb (silent NaN, no error). Running the MongoDB-specific checks and the mutant-observability checks next, then writing up.
Mongo checks done. One last small experiment on whether the listed tests can actually kill the listed mutants, then the write-up.
Experiments are complete. Running the cfdb baseline once and shutting down the test `mongod`, then writing the findings.
# Review of EDR plan 1: the MongoDB read path

**Verdict:** the core mechanism holds up against the real code, but four things need fixing before you write it. A request can return silent NaN across a refresh that removes keys; the refresh can stall for good on a backwards commit stamp; the 404 rule can raise false integrity faults in two ways; and the `missing` flag has no rule for clearing it.

**How to weight the evidence.** No plan code exists, so I wrote a literal transcription of Steps 3–4 (`model.py`, described in the appendix) and drove it against real ebooklet pushes with commit and GC, ebooklet's real fetchers, the fake S3, real cfdb, and a real `mongod`. Results about ebooklet, cfdb or MongoDB behaviour are executed. Results about the plan's logic are executed against my transcription, so weight those as reasoning with a worked example.

**Environment observed:** Python 3.12.16, booklet 0.12.11, s3func 0.10.0, pymongo 4.20.1, `mongod` 8.3.7 from conda-forge (not the plan's 8.0), ebooklet working copy 0.12.0, cfdb 0.11.0. Baselines: ebooklet hermetic suite `276 passed in 5.85s`; cfdb `488 passed in 4.31s`.

---

## Findings, most serious first

### 1. A request built before a refresh that removes keys returns silent NaN (wrong data, no error)

- **What:** the write order (entries, metadata, deletes) only stops new metadata meeting old entries. The store reads the metadata once at construction and the entries later, so old metadata meets post-delete entries. A deleted key returns `None`, and cfdb turns that into fill values.
- **Trigger:** a push that removes chunk keys (a rolling-window `truncate`, or a deleted variable), with a request constructed before the refresh's delete step and reading after it. The window is one request's duration per such push.
- **Confidence:** high on the mechanism; frequency is low.
- **Fix:** have the refresh write an "applying" token before its first upsert and clear it together with the applied stamp. The store compares (applied stamp, token) at construction and after its reads, and raises a retryable error on any difference. That costs one extra `datasets.find` per request.

`PYTHONPATH=/scratch/work/ebooklet /opt/envs/cfdb/bin/python e4_mixing.py` (ts_ortho, 3 points × 480 h, pushed with `cfdb.open_edataset` to the fake S3):

```
===== grouped
B. refresh applied {'upserted': 11, 'removed': 5, ...}; request built before it now reads flow: raised=None, shape (3, 480) (snapshot (3, 480)), NaN cells in the first 240 h: 720 of 720 (snapshot had 0); rest equal to snapshot: True
C. variable deleted + refreshed ({'upserted': 0, 'removed': 6, ...}); the earlier-built request reads flow: all NaN = True, snapshot all NaN = False, no exception
===== per_key
B. ... NaN cells in the first 240 h: 720 of 720 (snapshot had 0); rest equal to snapshot: True
C. ... all NaN = True, snapshot all NaN = False, no exception
```

Additive pushes are fine (`e4b.py`):

```
append: ...; earlier-built request reads flow equal to its snapshot: True; shape (2, 240)
prepend: ...; earlier-built request reads flow equal to its snapshot: True; shape (2, 240)
```

- **(a) Executed:** real cfdb classes on my store transcription, real pushes, real MongoDB; the refresh ran to completion between the request's construction and its read.
- **(b) Not checked:** I forced the interleaving, so I did not measure how often it happens. I did not test the window between the metadata update and the deletes (new metadata, entries not yet deleted). I did not test a variable re-created under the same name with a different encoding. I did not implement the proposed token.

### 2. The refresh stalls permanently when a commit's stamp is lower than the applied one

- **What:** Step 3 skips the apply "if the GET stamp is not newer than `applied_ts`". ebooklet's commit stamps are the writer's wall clock (`utils.py:1806`) and are not monotonic; ebooklet itself only compares them for inequality.
- **Consequence:** every later poll skips. The mirror keeps stale entries and deleted keys, and every repacked group answers 503 indefinitely. That is the "503 forever, never loud" state your change 1 set out to remove. The plan does not say whether a skip advances `checked_at`; if it does, monitoring sees a healthy worker.
- **Trigger:** two consecutive commits where the second writer's clock is behind the first commit's stamp: host skew larger than the commit gap, or a clock that ran ahead and was corrected. The stall lasts until a commit exceeds the applied stamp.
- **Confidence:** high that it stalls; the trigger is rare.
- **Fix, either of:**
  - make stamps monotonic at the source (`max(now, base_ts + 1)` at the commit, where `base_ts` is already in hand at `utils.py:1401`);
  - treat a GET that agrees with this poll's HEAD as authoritative whatever its order.
  
  In both cases a skip must not advance `checked_at` and should go loud after N polls.

`/opt/envs/ebooklet/bin/python e2_order.py` (second push with `booklet.utils.make_timestamp_int` shifted back one hour):

```
--- commit stamps: first 1795420237763601 second 1795416637810119 second-first (us): -3599953482 | push ok: True
plain reader sees b after the backwards-stamped commit: b'2'
```

`e5_stall.py` (a), with the second commit 10 minutes behind:

```
(a) rule=newer   ... (delta -599948148 us) | 3 polls: ['skipped', 'skipped', 'skipped'] | mirror==remote: False | get(k1): StaleIndexError
(a) rule=differs ... | 3 polls: [{'upserted': 3, 'removed': 0, ...}, 'nochange', 'nochange'] | mirror==remote: True | get(k1): b'CHANGED'
```

The guard does earn something. With a replayed db object two commits old and a live HEAD (`e5_stall.py` (d)), "newer" skips and "differs" regresses the mirror for one poll; both then converge, and the regressed store answered `StaleIndexError`, not wrong bytes.

- **(a) Executed:** a real push with a shifted clock; my refresh transcription under both rules.
- **(b) Not checked:** whether your writer hosts can skew this way. "Differs" would briefly revive lazily-deleted grouped keys under a stale GET; I reasoned that and did not run it. The stale GET in (d) is hand-replayed, not a real CDN.

### 3. The 404 rule can call transient conditions integrity faults (breaks a stated guarantee)

Your base rule is right, and I derived why. If the applied stamp is read before the entry and the later HEAD equals it, the remote sat at one commit throughout, the entry belongs to that commit, and writers never delete before committing. So a 404 there is a true fault. Three things break it in practice.

- **(i) The plan does not say which applied stamp is compared.** "What MongoDB says was applied" reads naturally as "read it now". If it is read after the back-off and the refresh completed during the back-off, HEAD equals applied while the entry in hand is stale, and the store raises a false `RemoteIntegrityError`. The trigger is the most likely moment: a request whose ≤1.5 s back-off straddles the refresh that would have healed it. Pin the construction-time stamp (or re-read the stamp, then the entry, in that order) and add a mutant for it.
- **(ii) Nothing re-issues the fetch before declaring a fault.** One spurious 404 with a current mirror becomes a loud 500. ebooklet's own code treats a transient 404 as real (`main.py:268–278`).
- **(iii) Per-key remotes behind a CDN (reasoning only).** A key deleted and later re-created can be answered from a negatively cached 404 after the refresh, giving a false 500. The "uncached endpoint" requirement should cover chunk 404s on per-key remotes, not only db-object HEADs. Grouped generations are never re-created, so they are immune.

`PYTHONPATH=/scratch/work/ebooklet /opt/envs/ebooklet/bin/python e2_classify.py` (identical lines for per-key):

```
===== grouped
S1 writer committed (+GC / per-key delete), mirror not refreshed:
    get(k1) -> StaleIndexError | get(k7, deleted) -> b'value-7' | get(k0) -> StaleIndexError
    after refresh: get(k1) -> b'CHANGED' | get(k7) -> None
S2 object db/0.9a32db04f58d4 deleted, mirror current -> ['RemoteIntegrityError']
S3 ... refresh lands during the back-off; applied stamp = construction -> StaleIndexError | a fresh store then reads: b'CHANGED'
S3 ... refresh lands during the back-off; applied stamp = reread       -> RemoteIntegrityError | a fresh store then reads: b'CHANGED'
S8 delete committed AND refreshed between entry read and fetch -> get(k7) = None
S6 delete_remote() after refresh -> get(k1) = RemoteMissingError
S4 one injected 404 on a chunk GET, mirror current, refetch_before_fault=False -> RemoteIntegrityError
S4 one injected 404 on a chunk GET, mirror current, refetch_before_fault=True -> b'value-1'
```

Per-key S1 differs as expected: the overwritten key returns the new bytes, and the deleted key gives `StaleIndexError`.

The ordering your rule depends on, from real pushes (`e2_order.py`):

```
--- grouped: ops of an update(k1)+delete(k5) push
    ('PUT', 'db/0.f7a121a8d1b14', None)
    ('PUT', 'db', '1795420237646084')
    ('DELETE*', ['db/0.d61db9cc6e7a4'])
--- per_key: ops of an update(k1)+delete(k5) push
    ('PUT', 'db/k1', '1795420237686111')
    ('PUT', 'db', '1795420237728357')
    ('DELETE*', ['db/k5'])
```

`delete_remote` deletes the db object first (`remote.py:474–475`), which is why S6 gives `RemoteMissingError` and not a false 500.

- **(a) Executed:** real commits, GC and teardown; the classification in my transcription under both stamp readings; S4 with a hand-injected 404.
- **(b) Not checked:** real B2 or Cloudflare behaviour. That every production writer runs the commit-before-delete protocol; a pre-0.10 writer wiped before committing, per the comment at `utils.py:1793`. The exactness argument also assumes unique commit stamps and a strongly consistent, uncached HEAD.

### 4. `missing` has no rule for being cleared

- **What:** Step 1 sets `missing` on a HEAD 404. Step 2 stops on "no change" without touching it. Read literally, one transient HEAD 404 leaves the dataset raising `RemoteMissingError` until the next commit.
- **Trigger:** one wrong 404 on a poll.
- **Confidence:** high for the literal reading; you may have intended otherwise.
- **Fix:** clear it on the no-change path and at the final step, and test it.

`e5_stall.py` (b):

```
(b) clear `missing` on later polls=False: poll with injected HEAD 404 -> missing; next poll -> nochange; store construction -> RemoteMissingError
(b) clear `missing` on later polls=True : poll with injected HEAD 404 -> missing; next poll -> nochange; store construction -> <model.MongoStore object ...
```

- **(a) Executed:** my transcription with an injected 404.
- **(b) Not checked:** how often B2 or your endpoint returns a spurious HEAD 404.

### 5. "One worker replica" is load-bearing and unguarded (reasoning, not run)

- **What:** two overlapping refreshes holding different GETs can interleave so that the older one's entry writes land after the newer one writes `applied_ts`. The next poll then sees "no change" and the stale entries stay. That is a lost deletion, or a false 500 by the HEAD rule.
- **Trigger:** a start-first rolling deploy, or a manual run beside the worker.
- **Fix:** the token from finding 1 covers it. Finish with a conditional update on the token, and null `applied_ts` on a mismatch so the next poll redoes the diff.
- **Related:** `uuid` is written with the metadata and `applied_ts` at the end, yet Step 3 compares "with the same uuid". Write the applied pair (uuid, ts) together, last.

### 6. The prototype's warm numbers do not carry over to a per-request store

- **What:** cfdb calls `load_items` and then `get` once per chunk, and reads each variable's attrs with a plain `get`. A store with no in-process memory pays two MongoDB round trips for every `get`.
- **Trigger:** every request; it grows with chunk count (a 322-chunk map is about 650 extra round trips).
- **Fix:** specify a request-scoped memo of what `load_items` returned. It also pins each key to one entry for the request and avoids fetching an oversized or uninsertable chunk twice.

`e4_mixing.py`, a 3-variable dataset read in full:

```
A. cfdb-through-store == cfdb.open_edataset for every variable and attrs: True | mongo round trips for that request: {'datasets.find': 1, 'entries.find': 19, 'chunks.find': 18, 'chunks.insert_many': 6}
   warm repeat: remote GETs: 0 | mongo round trips: {'datasets.find': 1, 'entries.find': 19, 'chunks.find': 18}
   warm repeat with a request-scoped memo of load_items results: mongo round trips: {'datasets.find': 1, 'entries.find': 7, 'chunks.find': 6}
```

`e7_mongo.py`: `322 single find_one round trips (localhost): 37.1 ms` against `$in over 322 ids: 322 docs in 2.9 ms`.

- **(a) Executed:** round-trip counts on my transcription; timings on a localhost `mongod`.
- **(b) Not checked:** a networked MongoDB, or the real WRF and ECan shapes.

### 7. Tests and mutants that cannot observe what they claim

- **"Skip the GET-stamp check" survives the listed race test.** Both rules give the same outcome (`e8_mutants.py`): `plan rule: (True, True, 'nochange') | mutant (no GET-stamp comparison): (True, True, 'nochange')`. Killing it needs a GET older than applied, which the fake S3 only produces by replay.
- **"Leave the uuid out of the chunk id" needs the same key and timestamp with different bytes under two uuids.** A `flag='n'` replacement keeps the uuid and bumps the timestamp; only `delete_remote()` plus a new file changes the uuid (`e8b.py`):
  ```
  flag='n' over the live remote -> ... | uuids differ: False | same ts: False
  delete_remote() then a new file -> ... | uuids differ: True | same ts: True
  ```
  So the uuid defence covers teardown-and-recreate; in-place replacement relies on ebooklet's timestamp bump.
- **"Diff on ts only" is observable.** Unchanged members of a repacked group change in `gen` alone (`group 0 member k0: changed fields = ['gen']`). Add the finer mutant "diff without gen".
- **"Label a per-key chunk with the index ts" is observable as a label.** The harm only appears when the object is older than the index (a stale edge), which the fake S3 cannot produce by itself.
- **Guarantees with no test or mutant:**
  - `missing` clearing;
  - which applied stamp is compared;
  - a re-fetch before a fault;
  - entry gone on re-read (my S8);
  - grouping by `(g, n)` when one gid carries two generations mid-apply;
  - never passing an empty list (the fetcher raises `IndexError list index out of range`, observed);
  - `checked_at` unchanged on each kind of failed or skipped refresh;
  - `load_items` given a generator;
  - HEAD 404 mapping to `RemoteMissingError`.
- **The write-order test asserts the implementation's order, not its consequence.** A behavioural test builds a store between steps, as in finding 1.
- **(b) Not checked:** I did not write the tests; "observable" means I staged the state with real pushes.

### 8. Store contract: complete for reads, with four loose ends

`/opt/envs/cfdb/bin/python e6_contract.py` wraps a real booklet file in a stub exposing only your listed methods and records every attribute cfdb asks for. For all four types, data, index selection, `.loc`, `select_loc`, `iter_chunks`, rechunked iteration, `items()`, attrs, repr and groupby read equal to `open_dataset`. Grid output:

```
   map()                -> NotImplementedError: map is not supported on this store
   copy()               -> AttributeError: stub store has no '_n_buckets'
   to_netcdf4()         -> None
   prune()              -> AttributeError: stub store has no 'prune'
   store attrs touched: {'get_metadata': 2, 'writable': 1, 'load_items': 34, 'get': 92, 'map': 1, '_local_file': 1, '_n_buckets': 1, 'prune': 1, 'sync': 1}
   load_items argument types: {'generator': 33}
   close() twice -> store.close calls: 1 | finalizer alive before/after: True False
dropped without close(): store.close calls after del + gc: 1
xarray CfdbDataStore.__init__ opens via: ["self._ds = open_dataset(filename, flag='r')"]
```

- **`copy()`** fails on `blt._n_buckets` (`main.py:482–483`). The store cannot give a "clear error" unless it fakes booklet privates or `Dataset.copy` is edited, which is a second edit to existing cfdb code.
- **`prune()`** is not guarded by `writable` and raises `AttributeError`.
- **The xarray backend** opens by file name only, so it cannot reach a store dataset. Drop it from the claim or add a hook.
- **`map()`** runs a full `load()` before it raises, so the store fetches everything first.
- **Finalizers:** close is idempotent, and a dataset dropped without `close()` still closes its store. A shared reader survives only because your `close` does nothing.
- **(b) Not checked:** `interp` stopped at "Dataset must have a CRS defined" in my fixtures, so it was not exercised (it is source-read as going through the same paths). `to_netcdf4` on the two `ts_` types fails inside cfdb (`'Point' object has no attribute '_factor'`), unrelated to the store. `combine` and `merge` were not run.

### 9. MongoDB specifics (server 8.3.7, not 8.0)

```
read all 156500 entries of one dataset for the diff: 0.32s
bulk upsert 10,000 changed entries: 0.35s
bulk upsert all 156500: 4.31s
$in over 156500 ids (query BSON ~10.52 MB): 156500 docs in 708.3 ms
duplicates: codes [11000] nInserted 3 | c1 kept its first bytes: b'x'
oversized doc inside the batch -> BulkWriteError | batch members stored: ['d0', 'd2']
validator: offset 3_000_000_000 (>2^31): REJECTED code 121
validator: ts=5 (small int under bsonType long): REJECTED code 121
create_index again with a new TTL -> 85 IndexOptionsConflict
collMod -> {'expireAfterSeconds_old': 3600, 'expireAfterSeconds_new': 1}
expired doc removed after 46s (monitor period); a doc with NO `t` field still present: True
transactions on this standalone: OperationFailure Transaction numbers are only allowed on a replica set member or mongos
```

- **Diff and `$in` at WRF scale work.** The full-dataset `$in` is 10.5 MB against a 16 MB limit, so batch it.
- **An oversized chunk in an unordered batch** arrives as a write error with code 10334 (from `e8_mutants.py`), not 11000. "Tolerating duplicate-key errors" must filter by size first or also accept 10334 and 121.
- **pymongo picks int32 or int64 by value.** A validator that names one width rejects the other, and offsets can exceed 2³¹. Use `['int','long']`.
- **A changed TTL does need `collMod`**, as you planned. A chunk without a date in `t` never expires, so the validator should require it.
- **(b) Not checked:** MongoDB 8.0 itself, a networked server, concurrent fills, or TTL under load.

### 10. Per-key: uncommitted bytes are served and are not cacheable until the refresh (low)

With a per-key push interrupted between its uploads and its commit, the mirror is current and the store serves the uncommitted value, exactly as plain ebooklet does. The lookup by entry timestamp never hits, so every read goes to the remote. "No remote calls on warm reads" is false for such keys, in every per-key push window.

`e5_stall.py` (c):

```
(c) per-key push with a failed commit -> HTTPError
    remote stamp == applied stamp: True
    store.get(k1) twice -> b'UNCOMMITTED' b'UNCOMMITTED' | labels [('label', 'k1', 'entry_ts', 1795420410476254, 'label_ts', 1795420410541409)] | remote GETs for 2x k1 + 2x k0: 3
    plain open_ebooklet reader get(k1) -> b'UNCOMMITTED'
```

The object-timestamp label kept the cache honest here.

### 11. Unnecessary or movable

- **The live Step 8 need not be the only cfdb-to-store integration check.** A hermetic one runs (line A in finding 6). `fake_s3` is excluded from the wheel (`pyproject.toml:217`), so it must live in ebooklet's suite with cfdb as a dev dependency.
- **`$max` on `checked_at`** does nothing with one writer.
- **The `d` field and its index** could be an `_id` prefix range.

---

## What I checked and found sound

- **Fetcher surface.** Through a reader built without the HEAD, ebooklet's three fetchers touched only `get_object` and `set`, and the bytes and timestamps equalled an `open_ebooklet` reader's for every key in both modes. `e1_reader.py`: `reader attrs touched: {'get_object': 9} | sink attrs touched: {'set': 14} | session calls: {'GET': 9}`. The index parses from `io.BytesIO`.
- **Refresh convergence.** Interrupting the apply at half the upserts and after steps 1, 2 and 3, in both modes, converged on the next poll, and the poll after reported no change. A commit landing between HEAD and GET was applied with the GET's stamp. `e3_converge.py`, one line of eight alike: `grouped  crash_after=2 ... next refresh -> {'upserted': 0, 'removed': 2, ...} | converged: True | following poll: nochange`.
- **Cache labels.** I found no path that stores bytes under a wrong label with strongly consistent storage (source-read plus S1 and (c) above).
- **Cited facts.** Your file:line citations match the trees. PyPI shows ebooklet 0.12.0 (uploaded 2026-11-23T06:46:50) and cfdb 0.11.0.

## Appendix: the harness

- **Lost with the container.** All scripts lived in `/scratch/exp`; the outputs above are the record.
- **Refresh transcription.** `model.py` follows Step 3 line by line: HEAD, equality stop, GET, stamp from the GET's metadata, `parse_db_payload`, whole-entry diff against `entries.find({'d': d})`, then `bulk_write` upserts, dataset `update_one`, `delete_many`, and `applied_ts` last. Its knobs are `crash_after`, `stamp_rule` (newer or differs) and `clear_missing`.
- **Store transcription.** It follows Step 4: entries by `$in`, chunks by `$in`, misses through `eu.get_remote_group_values` and `eu.get_remote_value` grouped by `(g, n)`, unordered `insert_many` labelled with the sink's timestamp, then re-read, back-off, one HEAD. Its knobs are `applied_stamp` (construction or reread), `refetch_before_fault`, `memo`, and hooks to stage interleavings.
- **Remote and writers.** The remote is `ebooklet/tests/fake_s3.py` with a counting session. Writers are real `open_ebooklet` and `cfdb.open_edataset` sessions.
- **Out of scope.** I did not open either `s3_config.toml`.

<!-- mcp offered to the arm: 0 server(s), 0 tool(s) -->
<!-- wrap-up: requested at 4800s of 5400s (turn budget 525s); not needed: the arm finished first -->
<!-- finished: 2026-11-23T21:00:37+13:00 exit=0 -->
