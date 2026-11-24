# Operations guide

Recovery recipes and operational conventions for running ebooklet databases,
collected from the changelogs into one place. Everything here assumes 0.11+
(storage format 3 grouped / format 2 per-key, the persistent journal, and the
typed exception taxonomy).

## The two failure channels of `push()`

`push()` reports failures through two distinct channels:

1. **Returned**: `push()` returns a `PushResult`. `result.failures` maps
   failed keys/groups to `'ExceptionClassName: message'` strings — these are
   per-object *upload* failures. Nothing is lost: the pending changes for the
   failed entries stay journaled, and the successfully-committed entries are
   already live. Fix the cause and `push()` again — only the failed work is
   redone. `bool(result)` is `True` only for a fully-successful push that
   changed the remote (a no-op push and any push with failures are falsy —
   note this is a deliberate change from pre-0.10, where the partial-failure
   dict was accidentally truthy).
2. **Raised**: failures of the *commit* itself raise —
   `urllib3.exceptions.HTTPError` when the db-object PUT fails, and
   `ebooklet.LockLostError` when the write lock was broken by another client
   (verified at push start and again immediately before the commit PUT).
   Everything stays journaled in both cases.

   Since **s3func 0.9.6** (required from ebooklet 0.10.4) there is a third
   raise site on this channel: in per-key storage mode, the post-commit pass
   that removes deleted keys from the remote raises `HTTPError` if it cannot
   resolve or delete them. It previously degraded silently instead — on a
   versioned bucket that left every version stored and billed while reporting
   success. The raise lands *before* the journal is cleared, so the pending
   deletes are retained and re-running `push()` completes them; the data
   already committed by that push is live and correct either way.

### Retrying a partial upload failure

```python
result = eb.changes().push()
if result.failures:
    # transient (network, 5xx): just push again - only failed groups re-upload
    result = eb.changes().push()
```

Non-retryable failure classes are visible in the failure string —
`GroupTooLargeError` means a group's packed size exceeds 4 GiB - its members
grew in place: re-create the database (`flag='n'`, which re-allocates every
group) instead of retrying.

### `force_push=True` after a failed commit

If the commit PUT itself failed (raised `HTTPError`), the remote's db object
may be stale or torn. Re-run with `eb.changes().push(force_push=True)` — it
re-uploads the db object unconditionally. Do this promptly: the new-generation
objects the failed commit referenced are protected from `fsck` sweeps only by
the age gate.

## Monitoring a push

`push()` narrates its progress on the **`ebooklet.push`** logger (all INFO;
failures at WARNING). Nothing is emitted unless you opt in:

```python
import logging
logging.basicConfig()   # or your own handler setup
logging.getLogger('ebooklet.push').setLevel(logging.INFO)
```

Sample records from a grouped push:

```
Pulling 3 group member value(s) (~5241 bytes) from 2 group(s) so the groups can be repacked in full.
push upload starting: 149 group(s), 128,441 key(s), 20,017,332,205 bytes
group 1/149 (23.5ac516ef3b3f4): 134,297,102 B (pack 8.2s, put 41.9s) - 134.3/20017.3 MB, 2.67 MB/s, ETA 2:04:11
...
push upload finished: 149/149 group(s), 20017.3 MB in 2:01:40, mean 2.74 MB/s, 0 failure(s)
commit succeeded (13,271,081 B db object)
```

Notes:

- The start record's byte total is exact (computed from the captured value
  lengths plus the pack-format overhead), so the per-group cumulative MB, the
  rate, and the ETA are mutually consistent. Rates are cumulative means.
- The per-group `pack`/`put` seconds are the tuning evidence for
  `push_packers` (below): if `pack` dominates, the disk is the bottleneck; if
  `put` dominates, the uplink is.

### The `push_packers` read gate

Packing a group means reading its member values from the local file; the
`push_packers` kwarg on `open_ebooklet`/`open_rcg` (default **1**) bounds how
many pack workers read the disk at once. Packing always overlaps uploading
(PUTs run outside the gate, up to `S3Connection(threads=...)`, default 10),
so the default costs nothing while giving a spinning disk the optimal
single-sweep read pattern. On storage where parallel readers scale (SSD,
RAID), raise it — `push_packers=threads` removes the gate entirely.

RAM: each in-flight group holds its full packed payload in memory (SigV4
needs the payload hash before the first byte), so peak usage is up to
`threads` × the largest group size — about 320 MB at the default 10 threads
and 32 MiB `group_bytes`. Lower `group_bytes` (or `threads`) on a small machine.

### Never prune mid-push

`prune()`/`clear()` raise `PushInProgressError` while a push is running: the
push reads value bytes at physical offsets captured up front, and a
compaction moves/destroys them. If an out-of-band compaction happens anyway
(e.g. a direct booklet-level `prune()`), the push detects it and aborts with
`ConcurrentCompactionError` **before its commit** — nothing is committed or
journal-cleared, any uploaded group objects are invisible orphans (`fsck`
sweeps them), and re-running the push converges.

## Lost or stuck write locks

- A crashed writer leaves its lock tickets behind. Opening with
  `force_lock=True` breaks tickets **older than 2 hours only**, so a live
  writer's tickets survive. But ticket age is session *start*, so a legitimately
  long session is breakable too. Treat it as a recovery tool, not a routine
  setting.
- To break *younger* tickets (you are certain the writer is dead), call
  `S3SessionWriter.break_other_locks(timestamp=<now>)` directly.
- **Leases (0.12, s3func ≥ 0.10).** `open_ebooklet(..., lock_lease=seconds)` (or
  `open_rcg`) declares the session's maximum hold:
  - Any other writer that waits on an expired leased ticket breaks it. That is
    `lease + 60 s` old, on the storage server's clock. A dead or stalled writer
    then blocks others for at most its lease, with no manual step.
  - The holder itself stops trusting the lock 60 s before the lease ends: its
    push aborts with `LockLostError` naming the lease.
  - Its commit PUT is presigned to expire at that deadline, so a commit delayed
    past it is refused by the server and nothing is committed.
  - Declare a lease well above the longest healthy session, which includes the
    lock wait, and keep `lock_timeout` well under it.
  - The lease runs from the lock ticket's PUT, so the time spent waiting for the
    lock counts against it.
  - Leases are refused for per-key storage, `flag='n'`, a local file that carries
    an unpushed `flag='n'` replacement, and `flag='r'`. The first three write or
    delete live objects outside the guarded commit. The refusals are checked at
    open and again at push, after the push has adopted the remote's current mode.
  - Tickets without a lease are never broken automatically. That includes
    every pre-0.12 writer's.
- A writer whose ticket was broken, or whose lease ran out, discovers it at its
  next push boundary. It aborts with `LockLostError` **before** committing. Its
  pending changes stay journaled: re-open the file (re-acquiring the lock) and
  push again.
- **The commit guard (0.12, grouped writers).**
  - Immediately before the commit PUT, the push HEADs the db object through
    the write session. It compares the stamp with the base its manifest and
    index were built from, which is captured at push start.
  - If another writer committed in between, the push aborts with
    `ConcurrentCommitError` (a `LockLostError`), and nothing is committed. That
    can only happen once mutual exclusion was lost.
  - Without the guard, the late commit republished a stale manifest. That left
    an unreadable group, or silently dropped the other writer's update, and
    both pushes reported success.
  - **It narrows the window rather than closing it.**
    - A commit that lands between the guard's HEAD and the commit PUT still
      causes those failures. B2 offers no conditional write to close it.
    - For a leased session, the presigned commit bounds the window at the lease
      deadline.
    - An unleased session, or one whose ticket was broken by hand before its
      deadline, has no bound.
  - **Per-key writers are not protected.** Their uploads overwrite live values
    before the guard runs, so refusing the commit comes too late.
  - **A commit that landed while the client saw an error** (a lost response, a
    5xx after the store) is detected by one HEAD and treated as committed. The
    guard also accepts the remote carrying this session's own earlier attempted
    commit, so the retry works.
  - The cleanup of replaced generations after a commit re-verifies the lock
    first. If the check itself fails, the cleanup is skipped (leftovers are
    orphans for fsck), and a committed push never raises over it.
  - Cost: one HEAD and one lock check (a LIST) per push.
  - Requirement: a writer's base stamp is read through its `db_url` when it has
    one. If that url served cached HEADs, the session's second and later pushes
    would be refused. On 2026-11-23, the B2-backed urls behind Cloudflare
    answered `cf-cache-status: DYNAMIC` (uncached).
- After a `LockLostError`, re-open the file. Since s3func 0.10, a failed
  `verify()` releases the ticket, so the same session cannot push again.

## `fsck` — integrity checking and housekeeping

```python
report = ebooklet.fsck(remote_conn)                      # report-only, lock-free
report = ebooklet.fsck(remote_conn, delete_orphans=True) # sweep, takes the write lock
```

- **Orphans** (objects nothing references: abandoned generations from crashed
  pushes, failed-GC leftovers, aged probe keys) are invisible to readers —
  sweeping them is housekeeping, not repair. The sweep age-gates every
  deletion (default 24 h) so an in-flight or promptly-retried push is never
  robbed of its fresh uploads.
- **Referenced-but-missing** objects are real integrity faults: the database
  references data that is gone. Readers of the affected keys raise
  `RemoteIntegrityError`. If a recent push partially failed, retry it (the
  self-heal path re-uploads); otherwise restore from a copy.

## `RemoteIntegrityError` triage

Raised when the remote contradicts its own index — a value fetch 404'd and a
fresh index re-pull (plus one fetch retry against the refreshed manifest)
confirmed the claim. In practice:

1. **A writer session with unpushed state**: push — the push's lost-keys
   self-heal re-uploads locally-held values.
2. **A recent partial/failed push elsewhere**: re-run that push
   (`force_push=True` if the commit failed).
3. **Neither**: run `ebooklet.fsck(conn)` to scope the damage; restore the
   affected remote from a `copy_remote` backup if one exists. The error is
   deliberately distinct from connectivity failures (it means "the store is
   inconsistent", not "the store is unreachable") — do not blanket-catch it
   with network errors.

## `flag='n'` — replacement semantics

**Use `'c'` unless you mean to replace the whole remote database.** The `'n'`
flag's contract (dbm-style) IS replacement: the next successful push replaces
the remote's entire content with this session's writes.

- Nothing is destroyed until a replacement push **fully commits**: the new
  content uploads first, one PUT atomically flips readers, and only then is
  the old content swept. A partially-failed replacement push commits nothing —
  the old remote stays fully readable, and the retry redoes the replacement.
- An unpushed `'n'` session's intent survives closing (journaled): reopening
  the local file warns loudly that the next push will replace the remote. To
  cancel a pending replacement, delete the local file and re-open from the
  remote. That recovery still works if the remote was deleted in between: the
  reopened session keeps its sidecar and the push replaces.

## `delete_remote()` — what survives locally

`delete_remote()` removes the remote database and its objects but keeps the
local file, and **the local file becomes the source of the next push**: every
value it holds — including ones it transparently read from the old remote — is
uploaded, and nothing else is. A cold cache re-creates a near-empty database
(this bites `RemoteConnGroup` catalogues hardest, where the entries *are* the
database). Delete the local file first if that is not what you want.

Since 0.10.5 the local caches that described the deleted remote — the
`.remote_index` sidecar and the remote-state slot — are forgotten automatically:
on the live session that called `delete_remote()`, and on the next writer open
(`'w'`/`'c'`/`'n'`) of a local file whose remote turns out to be gone (a `'w'`
recovering an unpushed `'n'` replacement is the one exception: it keeps its
sidecar, and the replacement push discards everything not written anyway).
"Gone" is judged from one HEAD 404, so only state the remote can re-derive is
dropped, and two guards cover a *wrong* 404: the open resets the file's
freshness stamp, so the next open against the live remote re-fetches its index
and reconciles against the file's watermark; and a push from a session that
believed the remote absent re-checks it first and, if it exists after all,
adopts it and merges rather than replacing its index. Pending local writes and
the local metadata are kept and pushed; pending deletes are kept (against a
genuinely-gone remote there is nothing left to delete). Readers (`'r'`,
including offline) keep their cache: their values heal per key, but
`keys()`/`in`/`len` on a warm reader keep listing the dead claims until that
local file is replaced.

Changing the storage mode (grouped vs per-key) needs `delete_remote()` followed
by a push: an existing remote always decides the mode. (`group_bytes`, the
grouped packing target, can change at any open: the remote records the value
its last push packed with, writers that pass none inherit it, and passing
another value packs new data to it from then on.)
If you rebuild a remote from a
*different* local file, other caches of the old incarnation raise
`UUIDMismatchError` on their next online open — delete those local files and
re-open from the remote.

## Offline read mode

`open_ebooklet(conn, path, flag='r', offline=...)` (same for `open_rcg`):

- `offline=True` — never touch the remote. Serves the existing local file
  as-is; reads of values not materialized locally raise `OfflineError`
  (the key exists — its value needs the remote).
- `offline='auto'` — normal online open, falling back to offline (with a
  `UserWarning`) **only** when the remote is unreachable at the transport
  level (DNS/connect/timeout). Integrity, format, uuid, and HTTP-status
  errors (e.g. bad credentials) still raise — a broken remote is never
  silently masked by stale local data. Check the session's `.offline`
  property to know which mode it ended up in.
- Offline data may be stale (no remote sync check runs), and offline covers
  the opened database only: opening an RCG offline lets you browse the
  catalogue, but opening a *member* remote still requires connectivity.
- To pre-populate a cache for offline use, open online and call
  `eb.load_items()` (everything) or read the keys you need.

## The MongoDB read path (`ebooklet[mongo]`, 0.13)

`ebooklet.mongo` lets any number of stateless readers serve a remote dataset from MongoDB. Install it
with `pip install 'ebooklet[mongo]'`. It has two halves:

- **`refresh(db, dataset_id, remote_conn)`** copies one dataset's remote index and metadata into
  MongoDB. Run it from **one** process (a worker) on a timer.
- **`MongoStore(db, dataset_id, reader)`** is a read-only store for one request. Open it with
  `cfdb.open_store_dataset(store)`. It reads everything from MongoDB, and on a cache miss fetches the
  chunk from the remote with ebooklet's own fetchers and inserts it into the cache. Build the reader
  with `object_reader(db_url, http_session)`; it makes no request until a miss, and one HTTP session
  can be shared per process.

Call `ensure_schema(db, chunk_ttl_seconds)` once before first use. Calling it again is safe, and a
worker typically calls it at every start. On existing collections it re-applies the validators with
`collMod`, which MongoDB's `readWrite` role does not grant. A user that runs it needs `readWrite`
plus `collMod` on the database (plan review `edr-worker-plan-2`, reproduced on 8.0, 8.3 and 9.0).

Tested on MongoDB 8.0 and 9.0, with pymongo 4.9 or later.

**The three collections:**

| Collection | One document per | Block compressor | Notes |
|---|---|---|---|
| `datasets` | dataset | zstd | The metadata section, the storage kind and format, the remote's ebooklet type (`ebooklet_type`, e.g. `EVariableLengthValue` or `RemoteConnGroup`; the store serves the stored bytes whatever the type), `applied` (the remote commit's `(uuid, timestamp)` the entries reflect), `rev` (bumped when an apply starts), `inc` (a random incarnation token, set when the document is first inserted), `applying` (an apply is in progress), `checked_at`, `missing` |
| `entries` | remote-index key | zstd | `ts`, group id `g`, generation `n`, offset `o`, length `l`, remote uuid `u` |
| `chunks` | (dataset, uuid, key, timestamp) | none | Insert-only cache of the compressed chunk bytes. A TTL index on `t` (a BSON date) expires it. |

`ensure_schema` sets the compressors when it creates a collection, because MongoDB fixes them then.
They were measured on 167k real entries and 620 real chunks (2026-11-24):
- **`entries`:** about 41 bytes per entry on disk with zstd, against 55 with the default snappy and
  215 uncompressed, indexes included.
- **`chunks`:** compression gains nothing, because cfdb already zstd-compresses the bytes. Snappy saved
  only 1%.
- **Scale:** today's catalogue is about 2 M entries, so roughly 80 MB.

An existing collection keeps the compressor it was created with, and `ensure_schema` logs a warning
on a mismatch. To change it, drop the collection: `chunks` is only a cache, and `entries` is rebuilt
by the next refresh of each dataset.

**The refresh writes in a fixed order** (after its GET, parse and diff):
0. claim: increment `rev` and set `applying`;
1. upsert the changed entries;
2. update the dataset document;
3. delete the removed entries;
4. set `applied` and `checked_at`, and clear `applying`, but only if `rev` is still the claimed
   value. If it changed, another refresh interleaved: `applied` is cleared and the next poll redoes
   the diff.

The metadata never runs ahead of the entries. A crash at any step is redone on the next poll,
because `applied` is written last and a left-over `applying` forces the redo.

**A request is answered only if no commit was applied while it ran.** A store refuses to build while
`applying` is set, and `store.verify()` raises `StaleIndexError` if `rev` has moved since the store
was built, or if the dataset document was deleted or recreated. **Call `verify()` before using a
request's results.** Without it, a request that overlaps
an apply can decode the new commit's chunks with the old commit's metadata: values, and even
coordinates, from neither commit, with no error (plan review `edr-mongo-code-1`, both arms). The
window is the apply's MongoDB writes only, about tens of milliseconds for an hourly push, so about
one request in 50,000 retries.

`StaleIndexError` is retryable **with a new store**: a store that raised it stays stale.

**A recreated dataset document (0.13.1).** If a dataset document is deleted and a later refresh
recreates it, its `rev` starts again from the beginning. It can then equal the `rev` a running
request kept, for example 1 for a dataset that has been applied only once.
- **Before 0.13.1**, such a request read a key whose entry had not been re-upserted yet as absent
  (fill values), and `verify()` passed (plan review `edr-worker-plan-2`, both arms).
- **Since 0.13.1**, each new dataset document gets a random incarnation token `inc`. The store
  compares it along with `rev`, so the request raises `StaleIndexError` instead.
- **Documents written by 0.13.0** have no `inc`. They keep the old behaviour until they are
  recreated.
- **A caller that deletes dataset documents** (for example to retire a dataset) should still prefer
  a tombstone (`rev` bumped, `applied` cleared, `missing` set), and keep the document.

**Commit timestamps are wall-clock and can go backwards.** A commit counts as new when its
`(uuid, timestamp)` *differs* from `applied`, never when it is "newer".

**What a read answers:**

| Situation | Result |
|---|---|
| The key has no entry, and `rev` and `inc` are unchanged since the store was built | `None` (the key is genuinely absent) |
| The key has no entry, and `rev` or `inc` changed during the request | `StaleIndexError` (a commit was applied, or the document recreated, during the request); retry |
| A per-key object's timestamp differs from its entry's (overwritten in place since the index was copied, perhaps not yet committed) | `StaleIndexError`; the bytes are cached under their own timestamp, so the retry after the next refresh hits the cache |
| A chunk 404s, and its re-read entry changed | Fetched again from the new location |
| A chunk 404s again after one refetch, and a HEAD of the db object **differs** from the `applied` pair read when the store was built | `StaleIndexError`: the index is behind; retry after the next refresh (an API answers 503 with `Retry-After`) |
| The same, but the HEAD **equals** that pair | `RemoteIntegrityError`: the remote contradicts its own index (an API answers 500, loudly) |
| The HEAD 404s | `RemoteMissingError` |
| Any other fetch failure | It raises. It never returns `None`, which cfdb would read as fill values. |

**Requirements for the operator:**
- The endpoint the refresh and the store use **must not cache** db-object HEADs or GETs, nor 404s.
  That includes per-key chunk 404s. A cache would show the refresh a stale commit, and would let a
  read misclassify a fault.
- The 404 rule depends on writers committing **before** they delete anything the previous commit
  referenced: grouped GC, per-key deletes, and `delete_remote()` deleting the db object first.
  ebooklet's writers do this, and `tests/test_writer_order.py` pins it.
- Run **exactly one** refresh process per dataset. This is a hard requirement. Two overlapping
  refreshes can leave MongoDB permanently inconsistent with the remote while `applied` matches it,
  so every later poll reports `nochange`. A new key then reads `None` (fill) and moved entries name
  GC'd generations (plan review `edr-mongo-code-1`; both arms reproduced it with different
  schedules). The cause is that the older refresh's unconditional entry writes and deletes can land
  around the newer refresh's checks. On Docker Swarm, use `replicas: 1` and the default stop-first
  update order, and never run a manual refresh beside the worker.
  - Stop-first is not a strict guarantee. Swarm starts the replacement task after the old task
    stops, after its node goes DOWN, or after 60 s, whichever comes first (swarmkit
    `defaultOldTaskTimeout`). So the worker must exit well within 60 s of SIGTERM: abandon the
    refresh rather than finish it, which is safe because a crash at any step is redone on the next
    poll. Set `stop_grace_period` under 60 s, and bound the HTTP timeouts, because s3func's read
    timeout is an idle timeout. To close the node-DOWN path, pin the worker to MongoDB's node
    (plan review `edr-worker-plan-2`).
  - *If concurrent refreshers are ever wanted:* every entry write and delete would have to be
    conditional. One way is to tag each entry with the claiming apply's `rev`, claim **before** the
    GET (so a higher tag always means a later commit), write only over entries with a lower tag, and
    turn deletes into tombstones. The other is a fenced lease that every write checks. Neither is
    built.
- Read with **primary** read preference (the default). The ordering above assumes a reader sees
  MongoDB's writes in order.
- **The store reads public remotes only.** `refresh()` takes any `S3Connection`, a public `db_url` or
  one with S3 credentials, and only *records* the `db_url`. But `object_reader()`, the helper that
  builds the store's reader, takes only a `db_url`: there is no public way to build a store for a
  private bucket. That is enough for envlib, whose member buckets are public and whose catalogue
  entries all carry a `db_url`.
  - To serve private datasets, extend `object_reader` to accept an `S3Connection`. ebooklet's
    `create_s3_read_session` already builds a signed `S3Session` from keys.
  - A worker may refresh through B2's S3 API with read-only keys, for example to avoid a caching CDN
    in front of the `db_url`, while the store keeps reading chunks through the public URL. That works
    today.
- Build the reader's shared session with `s3func.HttpSession(stream=False)`. `object_reader` refuses
  a streaming session (s3func's default): its responses carry no body and hold their pool
  connection, so fetches fail and the pool then blocks.
- A transient HEAD 404 at a poll marks the dataset `missing`, which makes reads raise
  `RemoteMissingError` until the next poll clears it. That is deliberate: it is better to refuse
  than to serve a dataset that may be gone.
- A dataset id must not contain `|`.

**A safety net we have not built: a forced re-diff** (designed in code review `edr-mongo-code-1`;
Mike, 2026-11-24: document it, and build it if divergence is ever seen).
- **The mechanism:** `refresh(..., force=True)` would skip the "applied pair unchanged → nothing to
  do" shortcut. It would download the db object, diff every entry against MongoDB, and repair any
  difference.
- **Why it is not needed often:** every normal refresh already diffs whole entries, so a dataset that
  commits regularly heals at its next commit anyway.
- **What it would add:** for rarely-committed datasets (the WRF hindcast, ESA SST, static
  quality-controlled data) a divergence would heal within the forcing interval instead of never. The
  divergence could come from a second refresher, a bug, a manual edit, or a MongoDB restore. More
  importantly, it would detect drift: in normal operation a forced pass finds nothing.
- **How to build it, if ever:**
  - Claim (bump `rev`) only when the diff is non-empty, so a clean pass writes nothing but
    `checked_at` and makes no in-flight request retry.
  - Log loudly, with the count, whenever a forced pass repairs anything.
  - Run it daily from the worker.
- **The estimated cost** at today's 18 datasets: about 300 MB of B2 downloads per day (mostly the
  twelve WRF indexes at about 21 MB each) and about 10 s of parse and MongoDB time.
- **Signs that it is needed:** a dataset whose MongoDB entries differ from its remote while `applied`
  matches it. Typical symptoms are a `RemoteIntegrityError` on a dataset nobody is writing to, or a
  key the remote has that reads `None`.

## Moving a hash-grouped remote (0.10) to 0.11

0.11 replaces hash grouping (`num_groups`) with write-order groups (format 3)
and has no read path for hash-grouped remotes (format 2 WITH `num_groups` in
its metadata): they refuse `r`/`w`/`c` with `UnsupportedFormatError`. Per-key
format-2 remotes are unaffected - 0.11 reads and writes them as they are, and
they stay readable by 0.10 clients. Format-3 remotes refuse 0.10 clients
("upgrade ebooklet").

Per hash-grouped remote, once:

1. **Hydrate, with the OLD ebooklet** (`ebooklet<0.11`): open the remote `'w'`
   against the local file that will be re-pushed and call `load_items()`. The
   local file then holds every value. Check that nothing failed and that the
   local key count equals the remote index's - after the delete, the local
   file is the only copy. Push or discard any pending local changes first.
2. `delete_remote()` (still with the old ebooklet, or via a 0.11 session's
   `S3SessionWriter.delete_remote()`, which works on any format).
3. **Republish with 0.11**: open the same local file `'w'` and push. A file
   whose journal recorded `num_groups` becomes grouped (`group_bytes` omitted:
   `DEFAULT_GROUP_BYTES`); the uuid and the metadata are kept, so existing
   reader caches of the remote keep working (their 15-byte index sidecar is
   discarded and re-fetched on their next online open; offline, it is kept).
   Pushing to a NEW key instead leaves the old remote untouched until you
   delete it.

## Format-1 remotes (pre-0.10)

Refused like hash-grouped ones. With 0.9.x push any pending changes, then
re-push the database with `flag='n'` from a local file that holds the full
content using 0.11. A crashed `flag='n'` push is recoverable: the journaled
replacement intent lets a `'w'` reopen of the same local file finish the
replacement.
