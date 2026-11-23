# Code review — the MongoDB read path for ebooklet remotes, and cfdb's `open_store_dataset`

## Framing

I am the author of the code below and I am reviewing my own work before it is committed and
released. I want an independent technical critique. Be direct; if something is wrong, say so and show
why.

The stack: **booklet** (a local key-value file) → **ebooklet** (booklet synced to an S3/B2 remote in
a "grouped" or "per-key" storage format) → **cfdb** (a CF-conventions array database whose chunks are
values in an ebooklet). This change adds the library layer of a future read-only web API:

- in **ebooklet**, an optional extra `ebooklet[mongo]` (the new package `ebooklet/mongo/`):
  - `refresh()` copies one dataset's remote index and metadata into MongoDB, run by one worker
    process;
  - `MongoStore` is a read-only, per-request store that serves cfdb from MongoDB and fills an
    insert-only chunk cache from the remote on a miss;
  - `ensure_schema()` sets up the collections;
- a small refactor in `ebooklet/remote.py` (`classify_db_metadata`, `storage_kind_of`) and a new
  `StaleIndexError` in `ebooklet/errors.py`;
- new writer-order tests (`ebooklet/tests/test_writer_order.py`);
- in **cfdb**, `open_store_dataset()` and one shared dataset-type dispatch table (`cfdb/main.py`,
  `cfdb/edataset.py`).

**What is under review is the uncommitted working-tree change in both repos.** Use `git status` and
`git diff` in each; the new files are untracked. The approved plan, as revised after its own review
round, is `planning/mongo-read-path-plan.md` in the ebooklet scope. Its "Implementation notes" say
what was built and measured. `docs/ops.md`, section "The MongoDB read path", states the operating
rules. The plan review's records are the `planning/edr-mongo-plan-1-*` files.

Scopes: `ebooklet` and `cfdb`.

## How I would like you to work

- **Check the claims, do not take them.** The implementation notes report test counts, a mutation run
  and a live equality check. Re-run anything a conclusion of yours depends on.
- **Execution means running the code under review.** Run its test suites. Drive `refresh()` and
  `MongoStore` against ebooklet's fake S3 (`ebooklet/tests/fake_s3.py`, real pushes with commit and
  GC) and a real `mongod`. Mutate copies to see whether a test actually fails. The decisive experiments
  are the ones that try to make a read return wrong data without an error, or make the refresh leave
  MongoDB inconsistent with the remote, for example:
  - interleave real pushes and refreshes with a request at every point you can reach;
  - drive the heal path in `MongoStore._fetch_and_heal` through each branch;
  - concurrent fetches inside one request;
  - per-key and grouped storage;
  - recreated remotes.

  A re-implementation in a script of your own is a model of your reading, not a test of the code;
  claims from one are weighted as reasoning.
- **Evidence for every experiment.** For each experiment a finding rests on, paste its command and raw
  output into your reply, whoever ran it.
- **Look for what I did not think to check.** The questions below are where *I* suspect I am weak,
  which is exactly the wrong place to stop.
- **Separate severity.** "Wrong data or a lost deletion, silently" is a different class from "I would
  write this differently".
- For each finding, state **what has to happen for it to bite** (its trigger).
- Do not open git-ignored local machine configuration — out of scope.

## Reproduction notes

- **ebooklet.** Run `uv sync`; it installs pymongo and cfdb from the dev group. The MongoDB tests need
  a real server: set `EBOOKLET_TEST_MONGO_URI` (for example `mongodb://127.0.0.1:27017`). Without it
  they skip; with `CI` set they fail instead. A MongoDB server may be obtainable through the package
  tools the preamble describes (conda-forge has `mongodb`). Then run the hermetic suite:
  `uv run pytest ebooklet/tests --ignore=ebooklet/tests/test_ebooklet.py --ignore=ebooklet/tests/test_map.py --ignore=ebooklet/tests/test_404_integrity.py --ignore=ebooklet/tests/test_delete_safety_live.py --ignore=ebooklet/tests/test_push_integrity.py --ignore=ebooklet/tests/test_rcg_entries.py --ignore=ebooklet/tests/test_scale_push.py`
  The ignored files need live B2 credentials, which are not in scope.
- **cfdb.** Run `uv sync`, then `uv run pytest cfdb/tests --ignore=cfdb/tests/test_edataset.py` (the
  ignored file needs live credentials).
- **Mutation runner.** It is `planning/mongo-read-path-evidence/mutate.py` (mutants in
  `mutants_*.json`; results in `mut_*_final.txt`). Its `REPO` path is the author's machine and needs
  editing for your copy.
- **Which cfdb ebooklet's tests use.** ebooklet's dev environment installs cfdb 0.11.0 from PyPI,
  which predates `open_store_dataset`. That is why `ebooklet/tests/test_mongo_cfdb.py` opens datasets
  with cfdb's classes directly. The cfdb working copy in the `cfdb` scope has the new function.
- **The live check** (`planning/mongo-read-path-evidence/live_equality.py`, with its result JSON) reads
  live public remotes. It is evidence of what was measured; this review does not need to rerun it.

## Questions I am least confident about

1. **The heal path** (`MongoStore._fetch_and_heal`, `_absent`, `_fill`). Can any sequence of refreshes
   and pushes during one request make `get()` return wrong bytes, or `None` where data exists, without
   raising? Can a heal misclassify an integrity fault as stale, or the reverse? Look especially at keys
   whose entry changes between the first fetch and the re-read, keys partly delivered by a group read,
   and keys that fail inside the second fetch.
2. **The refresh's consistency** (`ebooklet/mongo/refresh.py`). Look at the write order, the `rev`
   condition, clearing `applied` on interleave, the `stale_get` path, the `_mark_missing` upsert, and
   batching (`WRITE_BATCH`, `IN_BATCH`). Does every interruption or race converge on the next poll? Do
   the validators accept every document the code writes, in every path including first-time inserts?
3. **The memo.** `_values` and `_entries` live for one request. Is there a path where a memoised value
   is stale or wrong within the request, or where a failure is memoised?
4. **Concurrency inside one request.** `_fetch` runs ebooklet's fetchers in a thread pool and shares
   one `_Sink` and one reader across threads. Is that safe with s3func's `HttpSession` and the fake
   session?
5. **The `remote.py` refactor.** Is it behaviour-preserving, including warning attribution (the
   `group_bytes` warning's stacklevel) and every caller of `_load_db_metadata`?
6. **cfdb's dispatch refactor** and `open_store_dataset`. Is every behaviour of the old if-chains
   preserved, including the class passed the literal type, error messages, and closing on error? Is
   there a read path through a store-opened dataset that the tests do not reach?
7. **The tests themselves.** Which test passes without exercising what its name claims? Which
   guarantee in the plan or in `docs/ops.md` has no test that would fail if it were broken? Mutate to
   find out.
8. **Performance on the request path.** Count the MongoDB round trips per request, and the work done
   for a request that is fully warm.

## Deliverable

A ranked list of findings, most serious first: what it is, why it matters, its trigger, your
confidence, and the evidence — the file and line, or the command and its actual output. For every
finding you mark verified, state **(a)** what you executed or derived, and **(b)** what it depends on
that you did *not* check — assumptions inherited from me, baselines taken on trust, alternatives not
tried. Clause (b) is the one I need most.

Do not report a version, a timing or an output you did not observe; "I reasoned this from the source"
is a good answer, invented provenance is worse than none. Refuting one of my claims with evidence is a
valuable outcome. If part of the code is sound, say what you checked to conclude that, not that it
looks reasonable.
