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
  `force_lock=True` breaks tickets **older than 2 hours only** — a live
  writer's tickets survive, so this is safe to use routinely.
- To break *younger* tickets (you are certain the writer is dead), call
  `S3SessionWriter.break_other_locks(timestamp=<now>)` directly.
- A writer whose ticket was broken discovers it at its next push boundary and
  aborts with `LockLostError` **before** writing anything. Its pending changes
  stay journaled: re-open the file (re-acquiring the lock) and push again.

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
