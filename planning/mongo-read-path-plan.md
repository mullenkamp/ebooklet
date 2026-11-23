# EDR plan 1: the MongoDB read path (revised after plan review `edr-mongo-plan-1`)

## Core

**What is being decided.** The library layer of the EDR API:
- **ebooklet** gets an optional `ebooklet[mongo]` extra with two parts. The **refresh** copies one
  dataset's remote index and metadata from B2 into MongoDB. The **store** lets cfdb read a dataset
  from MongoDB and fills missing chunks from B2.
- **cfdb** gets one public function to open a dataset on such a store.

The worker service and the API are later plans. This plan delivers tested, released libraries.

**The mechanism, in five statements.**

```python
entries = {k: (ts, gid, gen, off, len) for k, v in index}        # refresh: parse the GET'd db object
upsert(changed); update(doc: meta, rev+1); delete(removed); set(applied=(uuid, ts)) if rev unchanged   # in order
ents = entries.find({'_id': {'$in': ids}}); memo it               # store: one batch per load_items
chunks.find({'_id': {'$in': [f'{d}|{u}|{k}|{ts}' ...]}}); fetch misses with ebooklet's fetchers; insert_many
cfdb.open_store_dataset(store)                                    # cfdb reads through the store unchanged
```

**What the review changed.** Two arms ran it, Fable 5.2 and Sonnet 5.5. Both executed their
experiments against real pushes with GC, real cfdb and a real `mongod`. They independently found the
same five defects, and all five are fixed below:
1. A deleting refresh could give a request silent NaN or wrong coordinates. Fixed by a `rev`
   counter.
2. A commit stamp lower than the applied one could stall the refresh permanently. The refresh now
   skips only when the stamp is equal, never on "newer".
3. The 404 rule now compares with the applied stamp read when the store was built. It also refetches
   once before declaring a fault.
4. Each request now keeps a memo of what `load_items` returned.
5. `missing` is now cleared.

The rest of the mechanism was confirmed sound: the fetchers through a reader that skips the HEAD,
refresh convergence after a crash at any step, the cache labels and the store contract.

**Top risks remaining.**
- **Writer-order invariants.** The 404 rule depends on writers committing before they delete. Two
  writer-order mutants passed ebooklet's whole suite, so this plan adds tests that pin the order.
- **A CDN-cached HEAD or a negatively cached 404.** Either would mislead both the refresh and the 404
  rule. The endpoint requirement is carried to plan 2.

---

## Context

The design is `envlib/plans/edr_api_assessment.md` §11, and the tracker item is "EDR plan 1: the
MongoDB read path" in `envlib/OPEN_WORK.md`. The prototype is `envlib/plans/spikes/edr-mongo-store/`;
its README lists how it differs from the decided design. The review arms' reports are in
`ai-review-harness/results/edr-mongo-plan-1/`.

Confirmed in the code (exploration, a design agent and both review arms, which executed it):
- **ebooklet's fetchers** touch only `reader.get_object` / `read_db_key` and
  `sink.set(key, value, ts, encode_value=False)`. These are `utils.py:781` `get_remote_value`,
  `:934` `recover_group_members` and `:975` `get_remote_group_values`. Through a reader that skips
  the HEAD, the bytes and timestamps equal an `open_ebooklet` reader's.
- **Building a reader.** `S3SessionReader.__init__` sends a HEAD (`remote.py:173–181`), whereas
  `create_s3_read_session` makes no network call.
- **GET metadata.** GET responses carry the object's user metadata (s3func
  `add_metadata_from_urllib3`).
- **Commit stamps.** A commit stamp is the writer's wall clock (`utils.py:1804`), so stamps are not
  monotonic. ebooklet compares them only for inequality.
- **Write order.** Writers PUT new groups, then commit, then GC and delete per-key objects.
  `delete_remote` deletes the db object first (`remote.py:457–475`).
- **What cfdb uses.** Its read paths use only `writable`, `get_metadata`, `load_items`, `get`, `map`,
  `close` and `sync`. This covers all four dataset types, `.loc`, `select_loc`, iteration, groupby
  and interpolation. An absent chunk (`get` → `None`) reads as fill, and an absent coordinate chunk
  gives wrong coordinate values, with no error. The xarray backend opens by file name only.
- **Releases.** On PyPI: ebooklet 0.12.0 (uploaded 2026-11-23; the CHANGELOG still says
  "unreleased") and cfdb 0.11.0.
- **Test baselines.** ebooklet's hermetic suite runs 276 tests in about 5.7 s, and cfdb's runs 488 in
  4.3 s.

## Steps

**0. Housekeeping (ebooklet).**
- Date the 0.12.0 CHANGELOG heading (2026-11-23), and open "0.13.0 (unreleased)".
- In `pyproject.toml`, add `mongo = ["pymongo>=4.9"]` under optional dependencies. Add `pymongo` and
  `cfdb` to the `dev` group, cfdb for change 9's integration test. Refresh `uv.lock`.
- In `test.yml`, add a `mongo:8.0` service and `EBOOKLET_TEST_MONGO_URI`.
- New `ebooklet/tests/mongo_support.py`:
  - a `mongo_db` fixture that creates a unique database per test and drops it afterwards. It skips
    with a visible reason when the URI is unset, and fails if `CI` is set;
  - `ttlMonitorSleepSecs=1` set for the TTL tests;
  - a counting FakeS3 session;
  - a hook that fires just before a push's commit PUT.
- Measure the suite's runtime before adding any test.

**1. Errors, the shared remote-metadata rule, and writer-order tests (ebooklet).**
- Add `StaleIndexError(Error)` in `errors.py`, carrying the stamp the request was built on, and export
  it.
- **First,** add a table-driven `storage_kind` test covering format 1, format 2 with and without
  `num_groups`, format 3, and a too-new format.
- **Then** factor the format gate and the storage-kind rule out of `S3SessionReader` into a pure
  helper, so the refresh can apply it to GET metadata.
- **New writer-order tests**, using the commit-PUT hook: no group GC, per-key delete or
  `delete_remote` child delete may happen before the db-object PUT, and `delete_remote` must remove
  the db object first. Each must fail under its mutant.

These are the only edits to existing ebooklet code. None is on a hot path.

**2. `ebooklet/mongo/schema.py`** (about 70 lines). Collection names, plus `$jsonSchema` validators
in which every integer is `['int','long']` and `t` is a required `date`. `ensure_schema(db,
chunk_ttl_seconds)` is idempotent: it indexes `entries.d`, creates a TTL index on `chunks.t`, and
applies a TTL change through `collMod`.
- `datasets`: `{_id: dataset_id, db_url, applied: {uuid, ts} | null, rev, uuid, storage_kind,
  format_version, meta_section (raw bytes), checked_at, missing}`
- `entries`: `{_id: "d|key", d, k, ts, g, n (gen), o, l, u (uuid)}`
- `chunks`: `{_id: "d|u|key|ts", v, n, t}`

The `u` in a chunk id comes from the **entry**, so a recreated remote can never mislabel bytes.

**3. `ebooklet/mongo/refresh.py`, `refresh(db, dataset_id, remote_conn) -> RefreshResult`** (about
200 lines).
1. HEAD.
   - A 404 sets `missing` and updates `checked_at`.
   - A 200 clears `missing`.
   - If the HEAD's (uuid, ts) **equals** `applied`, update `checked_at` and stop.
2. GET the db object. Take (uuid, ts) and the format from the GET's own metadata, and gate them with
   the shared helper. **Skip only if the GET's pair equals `applied`.** Ordering is never used.
3. Parse the payload. Read the index with `booklet.FixedLengthValue(io.BytesIO(...))`, skipping the
   metadata and reserved keys. Resolve `gen` from the manifest; an unmanifested gid is stored with
   `n=None` and logged loudly.
4. Diff **whole entries** against MongoDB's current entries for the dataset.
5. Apply, in this order:
   1. upsert the changed and new entries;
   2. update the dataset document (meta section, uuid, kind, format) and **increment `rev`**, keeping
      the new value;
   3. delete the removed entries;
   4. set `applied = (uuid, ts)` and `checked_at = HEAD time`, **conditional on `rev` still equalling
      the value kept**. On a mismatch, another refresh interleaved: set `applied = null`, and the
      next poll redoes the diff.

A failed refresh never updates `checked_at`. The worker runs as one replica (plan 2); the `rev`
condition is the guard if a second one ever runs.

**4. `ebooklet/mongo/store.py`, `MongoStore(db, dataset_id, reader, *, threads=8)`** (about 260
lines). It is built per request. `object_reader(db_url, http_session)` is a `get_object`-only
subclass of `S3SessionReader` that skips the HEAD; the API can share one HTTP session per process.
- **Construction** reads the dataset document once. A missing or unknown dataset, or one with
  `missing` set, raises `RemoteMissingError`. The store keeps that document's `applied` and `rev`,
  and decodes the meta section with `parse_meta_section`.
- **The request memo.** `load_items(keys)` reads the entries with one `$in` (batched above about 50k
  ids), then the cached chunks with one `$in`. It fetches the misses with ebooklet's fetchers,
  grouped by `(g, n)` and never with an empty list. Everything goes into the memo, and `get(key)`
  serves from the memo, running its own one-key batch only for a key it hasn't seen.
- **Inserting fetched bytes.** Chunks are pre-filtered by size, then inserted with
  `insert_many(ordered=False)` and labelled with the timestamp the sink received. Duplicate-key
  errors (11000) are tolerated. Any other insert failure, or an oversized chunk, logs a warning and is
  still served.
- **An absent entry.** If any key in a batch has no entry, the store re-reads the dataset document
  once. If `rev` differs from the one it kept, it raises `StaleIndexError` (retryable). Otherwise the
  key is genuinely absent and `get` returns `None`.
- **Any fetch failure raises, never returns `None`.** A 404 or a missing-member marker is handled
  like this:
  1. Re-read the whole entry. If it changed, fetch again. If it's gone, apply the absent-entry rule.
  2. If it's unchanged, refetch once.
  3. If that also fails, send one HEAD and compare it with the `applied` kept **at construction**:
     - equal: `RemoteIntegrityError`;
     - different: `StaleIndexError`;
     - HEAD 404: `RemoteMissingError`.

  An unmanifested group ends in `RemoteIntegrityError`. Other HTTP failures raise `HTTPError`, as
  `_failure_exception` does.
- **The rest.** `writable=False`; `close` and `sync` do nothing. `map`, `prune` and `copy()` each
  raise a clear error: `NotImplementedError` for `map` and for the `_n_buckets` property, and
  `ReadOnlyError` for `prune`. The docstring says `map()` loads the data before it raises. The store
  holds no self-references, so it is freed by refcount.

**5. cfdb `open_store_dataset(store, name='store')`** (about 40 lines in `cfdb/main.py`, exported).
It can run in parallel with steps 2–4.
- First factor the `dataset_type` → class dispatch into one helper, used by `open_dataset`,
  `open_edataset` and `open_store_dataset`.
- The new function uses `create=False`, raises no `PartialDataWarning`, gives a clear error when the
  store has no metadata, and closes the store on any error.
- The xarray backend is out of scope. No docs example is executed (Sybil has no MongoDB), so the
  docstring documents it.

**6. Mutation runs** (on copies, with `PYTHONDONTWRITEBYTECODE=1`; purge `__pycache__`). Each mutant
must fail a named test:
- return `None` on a fetch failure;
- skip the `rev` check on an absent entry;
- don't increment `rev` in step 2;
- write the metadata before the entries;
- write `applied` before the deletes;
- skip the conditional on `rev` in step 4;
- skip on "newer" instead of equality;
- diff on `ts` only;
- diff without `gen`;
- label a per-key chunk with the index `ts`;
- leave the uuid out of the chunk id (test via `delete_remote` and recreate);
- compare against a freshly read `applied`;
- no refetch before a fault;
- invert the HEAD classification;
- don't clear `missing`;
- update `checked_at` on a failed refresh;
- the writer-order mutants of step 1.

After fixing any mutant that survives, mutate again.

**7. Docs and releases.**
- ebooklet `docs/ops.md`: a "MongoDB read path" section covering the collections, the order of
  writes, the `rev` rule, the 404 table and the endpoint requirements.
- CHANGELOG entries in both repos.
- The review record goes in ebooklet `planning/`: `edr-mongo-plan-1-review-synthesis.md` and the
  verdicts.
- Mike releases ebooklet 0.13.0 and cfdb 0.12.0, verified on PyPI. Then refresh the locks with
  `uv lock --upgrade-package`.

**8. The live close-check.** Adapt the spike's `bench.py` into
`ebooklet/planning/mongo-read-path-evidence/live_equality.py`. It refreshes the live ECan, WRF and
flow-forecast remotes into a throwaway `mongod` and compares cfdb reads through the store with
`open_edataset` exactly.

## Verification

- **Hermetic ebooklet tests, refresh:**
  - The initial load equals the remote index, and meta equals a reader's `get_metadata()`.
  - A push followed by a refresh upserts only the changed entries, rewrites repacked members (with
    the gen-only change) and deletes the removed ones.
  - Crash injection after each of steps 1, 2 and 3 converges. A commit landing between the HEAD and
    the GET is handled.
  - A **backwards-stamped commit is applied**.
  - A no-change poll touches only `checked_at`. A transient HEAD 404 sets `missing`, and the next
    poll clears it.
  - A recreated remote (`delete_remote` then a new file), a too-new or legacy format, and a remote
    404.
  - Two interleaved refreshes trip the `rev` condition.
  - The writer-order tests from step 1.
- **Hermetic ebooklet tests, store:**
  - The bytes equal an `open_ebooklet` reader's for every key.
  - The store makes no remote calls on construction or on warm reads.
  - The memo serves `get` after `load_items`.
  - **Behavioural:** a store built before a deleting refresh and read after it raises
    `StaleIndexError`, never fill values. A store built before an appending refresh reads correctly.
  - A real writer commit with GC, then an uncached read → `StaleIndexError`; after a refresh the read
    succeeds.
  - A refresh landing during a request still classifies as stale.
  - One injected 404 heals by refetch.
  - A group object deleted after a refresh → `RemoteIntegrityError`.
  - An entry gone on re-read.
  - A per-key overwrite is labelled with the object's timestamp.
- **Edge cases:** an empty fetch list; a per-key object with no `timestamp`; an unmanifested gid; one
  gid with two generations mid-apply; `load_items` given a generator; an oversized chunk; an insert
  failure other than 11000; a validator given an offset above 2³¹; TTL expiry by Date (and no expiry
  for a non-Date `t`); `import ebooklet` with pymongo absent.
- **Hermetic cfdb-through-store** (in ebooklet's suite): all four dataset types read equal to
  `open_edataset`.
- **cfdb's own tests:** the shared dispatch helper keeps the existing tests passing. A stub store over
  all four types reads the same as `open_dataset`. Missing metadata gives a clear error.
- **Mutation:** every mutant in step 6 fails a named test.
- **Speed:** report the suite's runtime before and after. Name and justify any test over about 1 s.
- **Live:** step 8 equality (ran), recorded under ebooklet `planning/`.
- **Close-check (tracker):** live equality passes, every mutant is caught, and both releases are on
  PyPI.

## Carried to other plans and the backlog

- **Plan 2 (the worker):**
  - The refresh and the store's HEAD must use an endpoint that caches neither db-object HEADs nor
    404s, which matters for per-key chunk 404s too.
  - Every production writer must commit before it deletes, which means ebooklet 0.10 or later.
  - One worker replica.
- **Backlog (pre-existing nits):**
  - `bool(S3SessionReader)` raises `AttributeError` (`read_access`).
  - cfdb `Dataset.prune()` is not guarded by `writable`.
  - ebooklet's floors in `requirements.txt` and `conda/meta.yaml` have drifted.

## Review

- **Plan review `edr-mongo-plan-1`:** done (Fable 5.2 and Sonnet 5.5). All 13 proposed changes were
  adopted.
- **Code review after implementation:** a correctness gate is part of the deliverable, and one author
  wrote both the check and the checked code. I'll propose arms then and launch on your go.
- The source stays staged for your review, with a walkthrough of the diff.

## Implementation notes

_Progress is recorded here step by step, so the state survives a context compaction._

**2026-11-23: Steps 0–4 and 6 done (ebooklet). Source is uncommitted, for Mike's review.**
- **Baseline:** the hermetic suite was 276 passed in 5.06 s. It is now **352 passed in 8.09 s**, run
  with `EBOOKLET_TEST_MONGO_URI` against a local `mongo:8.0`. The 76 new tests take 3.3 s. Only one
  is over 0.2 s: the TTL-monitor test (1.0 s), because it waits for MongoDB's monitor.
- **Step 1.** The `storage_kind` table test was written first and passed 10/10 on the old code. The
  refactor into `remote.classify_db_metadata` / `remote.storage_kind_of` then passed the whole suite.
  The writer-order tests (`test_writer_order.py`) probe at the commit PUT and judge only objects that
  the previous commit referenced. ebooklet's own writability-probe scratch object is excluded,
  because it is PUT and deleted before the commit by design.
- **Deviation from the plan:** `refresh()` returns a plain dict, not a `RefreshResult` class, per
  the stack's plain-data API convention.
- **A refresh status the plan did not name:** `stale_get`. The HEAD shows a new commit but the GET
  returns the applied one, i.e. a cache is in front of the remote. It applies nothing, logs a
  warning, and does not advance `checked_at`.
- **The cfdb integration test** (`test_mongo_cfdb.py`) opens datasets with the cfdb classes directly
  until cfdb ships `open_store_dataset`. Switch it to the public function after the cfdb 0.12.0
  release.
- **Step 6 mutation runs:** 25 mutants, all killed. The results are in
  `mongo-read-path-evidence/` (`mut_*_final.txt`; the runner is `mutate.py`, which applies each
  mutant to copies only, with bytecode off).
  - The survivor was "diff without gen". A generation-only move happens in a real repack only when a
    member keeps its position, and member order varies from run to run (measured). The test now
    stages that state directly.
  - On re-mutation, the first "kill" of it was my new test failing its own premise check on the
    unmutated code too. That was caught, and the test was rewritten; the kill now comes from a
    genuine assertion.
- **Step 5 (cfdb):**
  - `open_store_dataset` was added, together with one dispatch table (`DATASET_CLASSES` /
    `E_DATASET_CLASSES` via `dataset_class()`).
  - The new tests (`cfdb/tests/test_store_dataset.py`, 7 of them) use a stub over a real booklet
    file. The stub records any attribute beyond the contract, and cfdb asked for none.
  - cfdb's unit suite: 495 passed (488 + 7) in 3.7 s. The docs tests: 217 passed, 2 skipped.
    `mkdocs build --strict` passes, with the new reference page.
  - The cfdb working tree's `uv.lock` change predates this work.
- **Step 7 (docs):**
  - ebooklet: `docs/ops.md` has a new section, "The MongoDB read path".
  - Both changelogs have an entry: ebooklet `CHANGELOG.md` and cfdb `docs/changelog.md` (cfdb keeps
    its changelog there).
  - **Releases are still open:** Mike releases ebooklet 0.13.0 and cfdb 0.12.0.
  - **Pre-existing:** ebooklet's docs do not build with current mkdocstrings-python. Its
    `mkdocs.yml` uses the removed `import:` option, and there is a `Highlight.__init__` error with
    current mkdocs-material, because the dependencies are unpinned. The new ops section was checked by
    rendering the page alone with mkdocs-material (strict): both tables rendered.
- **Step 8 (live), 2026-11-23:** `mongo-read-path-evidence/live_equality.py` ran against live ECan
  streamflow (grouped, 10,660 entries), WRF precipitation (grouped, 156,500 entries, refreshed in
  6.1 s) and the flow forecast (per-key). It used editable ebooklet and cfdb working trees.
  - **Every case was equal:** 7 selections, including a `.loc` time selection, cold and warm, with
    601 chunks cached. The result is in `live_equality_result.json`.
  - Warm reads took 3–15 ms, and 166 ms for a full WRF map.
  - The second poll of each dataset reported `nochange`.
  - **Planted-defect run:** with `get()` made to return None for every third data key, the check
    exited 1, and 12 of the 14 comparisons were unequal.
**2026-11-24: code review `edr-mongo-code-1` (Fable 5.2 and Sonnet 5.5), fixes applied.** The
synthesis and verdicts are in `edr-mongo-code-1-review-synthesis.md`; the arm reports are in
`edr-mongo-code-1-review-*.md`.
- **Mike's rulings:**
  - **Concurrent refreshers:** not planned. Document "exactly one" as a hard requirement, with what
    concurrent refreshers would need, and build no lease.
  - **Any refresh overlapping a request → retry**, through a claim-first `rev` and `applying`,
    construction refusal and `verify()`. Accepted at the estimated cost: one extra round trip per
    request, and about 1 in 50,000 requests retried.
  - **Per-key timestamp mismatch:** raise.
  - **Request round trips** (13–19 when warm): deferred to plan 3.
  - **The forced re-diff safety net:** documented in `docs/ops.md` and not built. Build it if
    divergence is ever seen.
- **Built:**
  - the claim (step 0), the `applying` lifecycle, `MongoStore.verify()`, the per-key mismatch raise,
    and the `object_reader` refusal of streaming sessions;
  - `_batches` and the delete loop now read `schema.IN_BATCH` at call time, so tests can cross batch
    boundaries;
  - cleanups: `_entries` removed, the `get_metadata` edge case, `ensure_schema` rejects bool, line
    lengths;
  - `ops.md` and the CHANGELOG rewritten to match.
- **Tests:** 379 passed in 8.9 s (24 new). They cover every path either arm found untested, plus
  regressions built from the arms' experiments: the mid-push per-key mix, and re-encoding under a
  replaced remote (verify refuses it).
- **Mutants:** 53, all killed (`mut_mongo_final.txt`, `mut_writer_final.txt`, `mut_review_final.txt`;
  the review-round mutants are in `build_mutants_review.py`).
  - The arms' survivors are all now killed.
  - New mutant "`_changed` ignores applying" survived because the term was redundant: the claim bumps
    `rev` in the same write. The term was removed rather than tested.
  - Four of the original mutants were retargeted at the changed code.
- **Live check re-run, with `verify()` in the loop:** every case equal, cold and warm, and the second
  polls reported `nochange` (`live_equality_result.json`).
- **Block compressors (Mike, 2026-11-24):**
  - **The measurement:** live WRF and ECan indexes (167,160 entries) and 620 real chunks, refreshed
    into `mongo:8.0` and checked after an `fsync`. `entries` came to 55 B per document on disk with
    snappy, 41 B with zstd and 215 B uncompressed, indexes included. `chunks` came to about 40 KB per
    document with every setting, because cfdb already compresses them.
  - **The change:** `ensure_schema` creates `datasets` and `entries` with zstd, and `chunks` with
    none, and warns about an existing collection with another compressor. Plan 2 therefore needs no
    `mongod` compressor flag.
  - **Tests:** 2 new; 3 new mutants, all killed. The suite has 381 tests, and 56 mutants are all
    killed.
- **A/B of the paths the `remote.py` refactor touches (Mike asked, 2026-11-24):**
  - **How:** `mongo-read-path-evidence/bench_open.py` ran on scratch copies of HEAD and the working
    tree, each put first on `PYTHONPATH`, and each run confirms which copy it imported. The runs
    alternated head/work 5 times, each the median of 3. The first attempt was invalid (both sides
    imported the editable install); that confirmation caught it and the run was redone.
  - **Results** (per operation; `bench_open_ab_head_vs_work.jsonl`):
    - `S3Connection.open('r')` (the HEAD plus the metadata parse): 13.40 → 13.67 µs, ×1.019, against a
      run-to-run spread of 1.044;
    - `open_ebooklet(..., 'r')` plus close on a warm cache: 164.5 → 164.5 µs, ×1.000 (spread 1.056);
    - the `storage_kind` property: 0.167 → 0.211 µs, ×1.26 (spread 1.02).
  - **Ruling on the property:** the slowdown is real but is an extra function call paid only on open,
    pull, push and fsck, never per read (its callers are `main.py:449`, `1135` and `1757`, and
    `fsck.py:121`). It is about 0.03% of an open, so it is kept rather than inlining a second copy of
    the rule.
- **The ebooklet type (Mike, 2026-11-24):**
  - `refresh()` records the remote's `type` as `ebooklet_type`, and the store exposes it.
  - Non-`EVariableLengthValue` types are not refused: the store's contract is type-agnostic, and a cfdb
    open of an RCG already fails clearly.
  - 1 new test; 2 new mutants, both killed. The suite has 382 tests, and 58 mutants are all killed.
- **Benchmarks (2026-11-24, Mike asked):**
  - **The suite:** `benchmarks/bench_mongo.py`, in booklet's `bench.py` style: a Timer, the median of
    3 repeats, ops/sec, and JSON output. Each run includes booklet and ebooklet baselines on the same
    keys and values. The remote is the in-memory fake S3, against a local `mongo:8.0`. The suite takes
    45 s. Run-to-run spread across the 3 repeats was under 10% for every operation.
  - **Results** (in `mongo-read-path-evidence/bench_mongo_*.json`): the default run (200 B values,
    grouped storage, n = 1k and 10k), a 64 KB-value run (n = 500) and a per-key run (n = 1k).
  - **Findings, as a ratio of booklet's local `random_read` at 424–447K keys/s:**
    - batched warm reads (`load_items`, then `get`): 173–186K keys/s, about 0.4×. That is faster than
      an ebooklet reader's warm local reads, 49–54K keys/s (about 0.12×).
    - per-key warm `get`: about 2K keys/s, roughly 0.5 ms each for 2 loopback round trips;
    - absent-key `get`: about 2.3K keys/s;
    - cold batched fill from the in-memory remote: 80K keys/s grouped and 42K keys/s per-key at
      200 B; 6K keys/s at 64 KB (about 390 MB/s);
    - store construction: 0.2 ms;
    - initial refresh: 36–44K entries/s;
    - a poll that finds no change: 0.4–0.5 ms, plus one real HEAD in production.
  - **What follows:** per-key round trips dominate the warm cost. This quantifies the item deferred to
    plan 3 (prefetching attrs and coordinate chunks in one batch).
  - **Not comparable:** booklet's own stored `results.json` (str serializer, an older booklet, perhaps
    another machine), which is why the baseline is measured inline.
- **Closed 2026-11-24.**
  - **Release and CI:** Mike reviewed and released ebooklet 0.13.0 and cfdb 0.12.0, both verified on
    PyPI with a wheel and an sdist. ebooklet's CI passed on `e153553` on 3.10–3.12, with the MongoDB
    service.
  - **Post-release (uncommitted):** the dev floor is now `cfdb>=0.12.0`, and `test_mongo_cfdb.py`
    uses `cfdb.open_store_dataset` (382 passed). The cfdb, envlib and envlib-ingest-base locks were
    refreshed. The four other repos' locks wait on Mike (the envlib `OPEN_WORK.md` item "Refresh locks
    to booklet 0.12.11 and ebooklet 0.11.2").
  - **The tracker item** "EDR plan 1: the MongoDB read path" is in envlib's `OPEN_WORK_DONE.md`.
  - **Carried to later plans:** plan 2 (one worker replica, an uncached endpoint, the option of
    credentials) and plan 3 (`verify()`, retry with a new store, fewer round trips).
