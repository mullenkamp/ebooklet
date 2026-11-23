# Plan review — EDR plan 1: a MongoDB read path for ebooklet remotes, read through cfdb

## Framing

I am the author of the work below and I am reviewing my own plan. I want an independent technical
critique before writing any code. Be direct; if the reasoning is wrong, say so and show why.

The stack: **booklet** (a local key-value file) → **ebooklet** (booklet synced to an S3/B2 remote in a
"grouped" storage format) → **cfdb** (a CF-conventions array database whose chunks are values in an
ebooklet) → **envlib** (a catalogue of cfdb datasets). A future read-only web API (OGC API – EDR)
will serve envlib datasets. The design decided for it: one "worker" process copies each dataset's
remote index and metadata into MongoDB; stateless API processes read everything from MongoDB, and
on a cache miss fetch the chunk from the remote and insert it into an insert-only chunk cache in
MongoDB. This plan builds only the library layer: an optional `ebooklet[mongo]` extra (a "refresh"
function and a read-only "store") and a small cfdb function that opens a dataset on such a store.
The full plan is at the end of this brief, verbatim. The design record it implements is
`plans/edr_api_assessment.md` §11 in the envlib scope, and a throwaway prototype that proved the
approach against live data is in `plans/spikes/edr-mongo-store/` there (its README lists how the
prototype differs from the decided design).

Scopes: `ebooklet` (the ebooklet repo), `cfdb` (the cfdb repo) and `envlib` (the envlib repo, for the
design record and the prototype).

## How I would like you to work

- **Check the claims, do not take them.** The plan cites file:line facts about ebooklet and cfdb, and
  several of its guarantees rest on them. Re-check anything a conclusion of yours depends on.
- **Execution means running the code under review.** This is a plan, so there is no new code yet,
  but every mechanism it relies on exists and can be driven: ebooklet's real remote fetchers
  (`ebooklet/utils.py`), its in-memory fake S3 (`ebooklet/tests/fake_s3.py`, with real pushes that
  garbage-collect replaced group objects — see `ebooklet/tests/test_generational.py`), and cfdb's real
  read path. The decisive experiments are the ones that exercise the plan's guarantees against that
  real code, for example:
  - drive ebooklet's fetchers through a `get_object`-only reader built without the HEAD request, and
    see whether anything else is touched;
  - stage a real writer commit + GC in the fake S3 between a "refresh" and a read, and check whether
    the plan's 404 classification (HEAD compared with the applied stamp) gives the right answer in
    every case, for grouped AND per-key storage;
  - interrupt the plan's refresh order at each step (and race a HEAD against a later commit) and check
    whether the next refresh converges;
  - open cfdb datasets of all four types on a stub store and see which store methods its read paths
    really call (including label selection, iteration, the xarray backend).
  A re-implementation of the logic in a script of your own is a model of your reading, not a test of
  the code; claims from one are weighted as reasoning. Mutating a copy to see whether a guard fires is
  encouraged.
- **Evidence for every experiment.** For each experiment a finding rests on, paste its command and raw
  output into your reply, whoever ran it.
- **Look for what I did not think to check.** The questions below are where *I* suspect I am weak,
  which is exactly the wrong place to stop.
- **Separate severity.** "Wrong, will serve wrong data or lose a deletion silently" is a different
  class from "I would design this differently".
- For each finding, state **what has to happen for it to bite** (its trigger).
- Do not open git-ignored local machine configuration — out of scope.

## Reproduction notes

- ebooklet: `uv sync`, then the hermetic tests:
  `uv run pytest ebooklet/tests --ignore=ebooklet/tests/test_ebooklet.py --ignore=ebooklet/tests/test_map.py --ignore=ebooklet/tests/test_404_integrity.py --ignore=ebooklet/tests/test_delete_safety_live.py --ignore=ebooklet/tests/test_push_integrity.py --ignore=ebooklet/tests/test_rcg_entries.py --ignore=ebooklet/tests/test_scale_push.py`
  The ignored files need live B2 credentials, which are not in scope; without them they fail at
  collection.
- cfdb: `uv sync`, then `uv run pytest cfdb/tests --ignore=cfdb/tests/test_edataset.py` (the ignored
  file needs live credentials). cfdb's `dev` group includes ebooklet.
- MongoDB: nothing in either repo depends on pymongo yet. A MongoDB server and `pymongo` may be
  obtainable through the package tools the preamble describes; if you cannot get a server, say so and
  mark any claim about MongoDB behaviour as reasoning.
- The prototype's scripts (`plans/spikes/edr-mongo-store/` in the envlib scope) read live public
  remotes over HTTP and expect a local `mongod`; they are evidence of what was measured, not
  something this review needs to rerun.

## Questions I am least confident about

1. **The 404 classification (plan Core, change 1; Step 4).** After a short retry, the store sends one
   HEAD of the db object and compares (uuid, timestamp) with the applied stamp: equal → integrity
   fault (500), different → index behind (503). Is that exact? I rely on writers committing before
   they garbage-collect group generations and before per-key deletes. Check per-key storage
   especially, where uploads overwrite live objects in place before the commit. If the
   classification can call a transient condition an integrity fault, or a permanent fault
   transient, it breaks a stated guarantee.
2. **Refresh convergence (Step 3).** Writing `applied_ts` last, taking the stamp from the GET
   response, skipping a GET whose stamp is not newer, and running one worker replica: does every
   interruption or race converge on the next refresh without losing a deletion or reviving a removed
   key? Is diffing whole entries `(ts, gid, gen, offset, len, uuid)` enough to detect every change a
   push can make?
3. **"The metadata never runs ahead of the entries."** The write order is entries, then the dataset
   document, then deletions. Does that actually prevent a cfdb read from producing silent fill
   values mid-refresh (grown coordinates, attrs keys, a variable added or removed)? Is there a read
   that still mixes two pushes in a harmful way?
4. **Cache labels (Step 4).** Each cached chunk is labelled with the timestamp the fetcher passed to
   `.set()`: the object's own timestamp for per-key storage, the index timestamp for a ranged group
   read, the embedded entry timestamp for group recovery. Can bytes ever be stored under a label
   that does not describe them, so that a later request reads wrong bytes from the cache?
5. **The store contract (Steps 4–5).** Is the listed set of store methods complete for every cfdb
   read path, including selection by label, iteration, the xarray backend, groupby and interpolation?
   What happens with per-request stores and cfdb's finalizers?
6. **Tests and mutants (Verification, Step 6).** Can the listed tests actually observe the failures
   they claim to guard, using the fake S3? Which plan guarantee has no test that would fail if it
   were broken?
7. **MongoDB specifics.** `$in` queries over hundreds of ids; diffing against all of a dataset's
   entries (~156k for one WRF variable); `insert_many(ordered=False)` with duplicate keys; TTL and
   `collMod`; `$jsonSchema` validators over Binary fields; standalone MongoDB (no transactions).
8. Anything in the plan that is unnecessary for its goal.

## Deliverable

A ranked list of findings, most serious first: what it is, why it matters, its trigger, your
confidence, and the evidence — the file and line, or the command and its actual output. For every
finding you mark verified, state **(a)** what you executed or derived, and **(b)** what it depends on
that you did *not* check — assumptions inherited from me, baselines taken on trust, alternatives not
tried. Clause (b) is the one I need most.

Do not report a version, a timing or an output you did not observe; "I reasoned this from the source"
is a good answer, invented provenance is worse than none. Refuting one of my claims with evidence is a
valuable outcome. If part of the plan is sound, say what you checked to conclude that, not that it
looks reasonable.

---

## The plan under review (frozen, verbatim)

# EDR plan 1: the MongoDB read path

## Core

**What is being decided.** How to build the library layer of the EDR API. ebooklet gets an optional
`ebooklet[mongo]` extra with two parts. The **refresh** copies one dataset's remote index and
metadata from B2 into MongoDB. The **store** lets cfdb read a dataset from MongoDB and fills missing
chunks from B2. cfdb gets one public function to open a dataset on such a store. The worker service
and the API are later plans; this plan delivers tested, released libraries.

**The mechanism, in five statements.**

```python
entries = {k: (ts, gid, gen, off, len) for k, v in index}        # refresh: parse the GET'd db object
upsert(changed); update(dataset_doc.meta); delete(removed); set(applied_ts, checked_at)   # in that order
ents  = entries.find({'_id': {'$in': ids}})                       # store: index entries for this request
chunks.find({'_id': {'$in': [f'{d}|{u}|{k}|{ts}' ...]}}); fetch misses with ebooklet's fetchers; insert_many
cfdb.open_store_dataset(store)                                    # cfdb reads through the store unchanged
```

**Two changes to what we agreed in discussion.** Both came out of checking the design against the code.

1. **The 404 rule needs one HEAD request.** As decided, the rule compared `checked_at` with the time
   of the 404. But the 404 is always "now", so the comparison almost always says "stale". A
   permanent integrity fault would then answer 503 forever and never log loudly. The fix: after its
   short retry, the store sends **one HEAD of the db object, on that rare path only**. It compares
   the remote's (uuid, timestamp) with what MongoDB says was applied.
   - If they are equal, the index is current, so the 404 is an integrity fault: a loud 500.
   - If they differ, the index is behind: a 503.

   The store still never writes the index and never talks to the worker. It does read db-object
   metadata from B2, which we had said only the worker would do. **This needs your OK.**
2. **`applied_ts` is written last.** If the refresh crashed after updating the metadata but before
   deleting removed entries, the next poll would see "no change" and the stale entries would stay
   for good. Writing the applied stamp as the final step makes the next poll redo the diff. The redo
   is idempotent.

**Top risks.**
- A subtle consistency error, such as wrong bytes under a valid key or a lost deletion, would not
  raise an error. That is why this plan gets a dual-blind review and the mutation runs.
- A CDN-cached HEAD would hide new commits from both the refresh and the 404 check. Both must use an
  endpoint that does not cache db-object HEADs. Checking `b2.envlib.xyz` for that falls to plan 2.

---

## Context

The design is `envlib/plans/edr_api_assessment.md` §11, and the tracker item is "EDR plan 1: the
MongoDB read path" in `envlib/OPEN_WORK.md`. The prototype is `envlib/plans/spikes/edr-mongo-store/`.
Its README lists the ways the prototype differs from the decided design, and those differences must
not be carried over.

Facts confirmed in the code (exploration plus a design agent; file:line refers to the dev trees):
- ebooklet's fetchers (`utils.py:781` `get_remote_value`, `:934` `recover_group_members`, `:975`
  `get_remote_group_values`) call only `local_file.set(key, value, ts, encode_value=False)` and
  `remote_session.get_object(...)`.
- `S3SessionReader.__init__` sends a HEAD (`remote.py:173–181`). `create_s3_read_session`
  (`remote.py:74–102`) makes no network call, and `get_object` needs only the session and
  `read_db_key` (`:319–329`).
- GET responses carry the object's user metadata (`utils.py` `_TRANSPORT_META_KEYS` note; FakeS3
  `fake_s3.py:151`).
- ebooklet's tests are hermetic through `ebooklet/tests/fake_s3.py` and `groups.py`. These include
  real pushes that garbage-collect old group objects (GC).
- cfdb's read path uses only `writable`, `get_metadata`, `load_items` (gated by `hasattr`; a
  non-empty return value raises), `get` (`None` means a blank chunk), `map`, `close` and `sync`
  (`main.py:727–798`, `support_classes.py:231–296, 860, 981, 1773`).
- `file_path` is used only for the dataset's repr, and `pathlib.Path(None)` raises (`main.py:728`).
- On PyPI the current releases are ebooklet 0.12.0 and cfdb 0.11.0. ebooklet's CHANGELOG still
  heads 0.12.0 "unreleased". ebooklet has no `benchmarks/`.

## Steps

**0. Housekeeping (ebooklet).**
- Date the 0.12.0 CHANGELOG heading from its PyPI upload, and open "0.13.0 (unreleased)".
- In `pyproject.toml`, add `[project.optional-dependencies] mongo = ["pymongo>=4.9"]`. Also add
  `pymongo` to the `dev` group, because CI's `uv sync` installs no extras. Refresh `uv.lock`.
- In `.github/workflows/test.yml`, add a `mongo:8.0` service and `EBOOKLET_TEST_MONGO_URI`.
- New `ebooklet/tests/mongo_support.py`. Its `mongo_db` fixture creates a unique database per test
  and drops it afterwards. It skips with a visible reason when the URI is unset, and fails if `CI`
  is set. The file also has a counting FakeS3 session.
- Measure the hermetic suite's runtime before any new test.

**1. Errors and the shared remote-metadata rule (ebooklet).**
- Add `StaleIndexError(Error)` in `errors.py`, carrying the applied stamp and `checked_at`, and
  export it.
- Factor the format gate and the storage-kind rule out of `S3SessionReader._load_db_metadata` and
  `storage_kind` (`remote.py:230–268, 200–215`) into a pure helper. The refresh uses it on GET
  metadata, so the rule has one definition.
- Existing tests cover the refactor. This is the only edit to existing ebooklet code, and it runs
  once per open, not on a hot path.

**2. `ebooklet/mongo/schema.py`** (about 70 lines). Collection names and `$jsonSchema` validators
(kept, by your preference). `ensure_schema(db, chunk_ttl_seconds)` is idempotent. It indexes
`entries.d`, and `chunks.t` with a TTL; a changed TTL is applied through `collMod`.
- `datasets`: `{_id: dataset_id, db_url, uuid, applied_ts, storage_kind, format_version,
  meta_section (raw bytes, for exact parity), checked_at, missing}`
- `entries`: `{_id: "d|key", d, k, ts, g, n (gen), o, l, u (uuid), s (provenance stamp)}`
- `chunks`: `{_id: "d|u|key|ts", v, n, t}`

The `u` in the chunk id comes from the **entry**, so a recreated remote can never mislabel bytes.

**3. `ebooklet/mongo/refresh.py`, `refresh(db, dataset_id, remote_conn) -> RefreshResult`** (about
200 lines).
1. HEAD. On a 404, set `missing` and advance `checked_at`.
2. If the uuid and timestamp equal `applied_ts`, advance `checked_at` (`$max`) and stop.
3. GET the db object. Take the stamp, uuid and format from the GET's own metadata, which is atomic
   with the body, and gate it with the shared helper. Skip the apply if the GET stamp is not newer
   than `applied_ts` with the same uuid.
4. Parse the payload. Read the index with `booklet.FixedLengthValue(io.BytesIO(...))`, skipping the
   metadata and reserved keys. Resolve `gen` from the manifest; an unmanifested gid is stored with
   `n=None` and logged loudly.
5. Diff **whole entries** against MongoDB's current entries. The worker therefore holds no state, and
   a repack's moved members are rewritten even when their `ts` is unchanged.
6. Apply in order:
   1. upsert the changed and new entries;
   2. update the dataset document (meta section, uuid, kind, format);
   3. delete the removed entries;
   4. set `applied_ts`, and `checked_at = HEAD time` (`$max`).

A failed refresh never advances `checked_at`. The worker runs as **one replica** (plan 2), so there
are no per-entry conditional guards; `s` is provenance only.

**4. `ebooklet/mongo/store.py`, `MongoStore(db, dataset_id, reader, *, threads=8, retry_cap_s=1.5)`**
(about 260 lines). It is built per request. `object_reader(db_url, http_session)` is a
`get_object`-only subclass of `S3SessionReader` that skips the HEAD; the API can share one HTTP
session per process.
- Construction reads the dataset document once: a missing or unknown dataset raises
  `RemoteMissingError`. The meta section is decoded with `parse_meta_section`.
- `get` and `load_items` read the entries (a key with no entry returns `None`), then the cached
  chunks, then fetch misses with ebooklet's fetchers. They group by `(g, n)` and never pass an empty
  list. Fetched bytes are inserted with `insert_many(ordered=False)`, tolerating duplicate-key
  errors, and labelled with the timestamp the sink received. A chunk over the size limit, or a
  failed insert, logs a warning and is still served.
- **Any failure raises, never returns `None`.** A 404 or a missing-member marker is handled like
  this:
  1. Re-read the whole entry: if it changed, fetch again; if it is gone, return `None`.
  2. If it is unchanged, back off up to `retry_cap_s`, then send one HEAD:
     - equal to the applied stamp: `RemoteIntegrityError`;
     - different: `StaleIndexError`;
     - HEAD 404: `RemoteMissingError`.
  3. An unmanifested group ends in `RemoteIntegrityError`. Other HTTP failures raise `HTTPError`, as
     `_failure_exception` does.
- `writable=False`; `close` and `sync` do nothing; `map` raises `NotImplementedError`; a clear error
  is given for `copy()`.

**5. cfdb `open_store_dataset(store, name='store')`** (about 40 lines in `cfdb/main.py`, exported).
It can be built in parallel with steps 2–4.
- It dispatches the four plain dataset classes on the stored `dataset_type`, with `create=False` and
  no `PartialDataWarning`.
- A store with no metadata gets a clear error, and the store is closed on any error.
- Tests use an in-memory stub store wrapping a real booklet file.
- No docs example is executed, because Sybil has no MongoDB. The docstring is enough.

**6. Mutation runs** (on copies, with `PYTHONDONTWRITEBYTECODE=1`; purge `__pycache__`). Each mutant
must fail a named test:
- return `None` on a fetch failure;
- write the metadata before the entries;
- write `applied_ts` before the deletes;
- diff on `ts` only;
- label a per-key chunk with the index `ts`;
- leave the uuid out of the chunk id;
- invert the HEAD classification;
- skip the GET-stamp check;
- advance `checked_at` on a failed refresh.

After fixing any mutant that survives, mutate again.

**7. Docs and releases.**
- ebooklet `docs/ops.md`: a "MongoDB read path" section covering the collections, the order of
  writes, the 404 table and the requirement for an uncached endpoint.
- CHANGELOG entries in both repos.
- Mike releases ebooklet 0.13.0 and cfdb 0.12.0, verified on PyPI. Then refresh the locks with
  `uv lock --upgrade-package`.

**8. The live close-check.** Adapt the spike's `bench.py` into
`ebooklet/planning/mongo-read-path-evidence/live_equality.py`. It refreshes the live ECan, WRF and
flow-forecast remotes into a throwaway `mongod`, then compares cfdb reads through the store with
`open_edataset` exactly. This is also the only integration check between cfdb and the store.

## Verification

- **Hermetic tests (ebooklet).**
  - Refresh: the initial load equals the remote index, and meta equals a reader's `get_metadata()`.
    A push followed by a refresh upserts only the changed entries, rewrites repacked members and
    deletes the removed ones.
  - Write order, asserted with a pymongo `CommandListener`.
  - Crashes injected after apply steps 1 and 2: the next poll must converge.
  - A commit landing between the HEAD and the GET.
  - No-change polls advance only `checked_at`.
  - A recreated remote, a too-new or legacy format, and a remote 404.
  - The store's bytes equal an `open_ebooklet` reader's bytes for every key. The store makes no remote
    calls on construction or on warm reads.
  - A **real** writer commit with GC, then an uncached read, gives `StaleIndexError`. After a refresh
    the read succeeds.
  - A group object deleted after a refresh gives `RemoteIntegrityError`.
  - A per-key overwrite is labelled with the object's own timestamp.
  - An absent key gives `None`, while a failed fetch raises.
  - Concurrent fills, an oversized chunk, and an unmanifested group.
- **cfdb.** A stub store over all four dataset types reads the same as `open_dataset`. `map()`,
  `copy()` and missing metadata give clear errors.
- **Mutation.** Every mutant in step 6 fails a named test.
- **Speed.** Report the suite's runtime before and after, and name and justify any test over about 1 s.
- **Live.** Step 8 equality on live datasets (ran), recorded under ebooklet `planning/`.
- **Close-check (tracker).** The live equality passes, every mutant is caught, and both releases are
  on PyPI.

## Review (to propose after you have seen this plan; nothing launches without your go)

- **Plan review.** A dual-blind round, `edr-mongo-plan-1`: Fable 5.2 and GLM-5.3 (via OpenRouter),
  with the same brief and the same scope. The scope I would propose is ebooklet `ebooklet/`, cfdb
  `cfdb/` and envlib `plans/` (the §11 design and the spike).
- **Code review** after implementation. Two triggers fire: the correctness gate is part of the
  deliverable, and one author writes the check and the code. Arms to be named then.
- The source stays staged for your review, with a walkthrough of the diff.
