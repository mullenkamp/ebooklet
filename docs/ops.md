# Operations guide

Recovery recipes and operational conventions for running ebooklet databases,
collected from the changelogs into one place. Everything here assumes 0.10+
(storage format 2, the persistent journal, and the typed exception taxonomy).

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

### Retrying a partial upload failure

```python
result = eb.changes().push()
if result.failures:
    # transient (network, 5xx): just push again - only failed groups re-upload
    result = eb.changes().push()
```

Non-retryable failure classes are visible in the failure string —
`GroupTooLargeError` means a group's packed size exceeds 4 GiB: re-shard the
database (`flag='n'` re-creation with a larger `num_groups`) instead of
retrying.

### `force_push=True` after a failed commit

If the commit PUT itself failed (raised `HTTPError`), the remote's db object
may be stale or torn. Re-run with `eb.changes().push(force_push=True)` — it
re-uploads the db object unconditionally. Do this promptly: the new-generation
objects the failed commit referenced are protected from `fsck` sweeps only by
the age gate.

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
  remote.

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

## Upgrading format-1 remotes (pre-0.10) to format 2

There is no format-1 read path in 0.10 (deliberate): 0.10 refuses format-1
remotes for `r`/`w`/`c` with `UnsupportedFormatError`, and pre-0.10 clients
refuse format-2 remotes.

Per remote, once:

1. With the OLD ebooklet (0.9.x), push any pending local changes.
2. Upgrade ebooklet.
3. Re-push the database with `flag='n'` from a local file that holds the full
   content (re-pass `num_groups` — it is not inherited from the old remote).
4. RemoteConnGroup catalogues: re-add members after the member remotes are
   upgraded.

A crashed upgrade push is recoverable: the journaled replacement intent lets a
`'w'` reopen of the same local file finish the replacement.
