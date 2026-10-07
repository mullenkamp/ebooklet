# EBooklet

EBooklet is a Python key-value database that syncs with S3 (AWS or any S3-compatible service). It builds on the [Booklet](https://github.com/mullenkamp/booklet) package, providing a [MutableMapping](https://docs.python.org/3/library/collections.abc.html#collections-abstract-base-classes) (dict-like) interface backed by local files and remote S3 storage.

- **S3 sync** — push/pull changes between a local database and an S3 bucket
- **Dict-like API** — standard `MutableMapping` plus `dbm`-style methods
- **Grouped storage** (0.11: write-order groups) — values packed into group objects in the order they were written, so appending to a growing database uploads only the new data; byte-range reads
- **Concurrency** — thread-safe writes (thread locks), multiprocessing-safe (file locks), and S3 object locking for remote writes
- **Push progress** (0.10.1) — opt into per-group progress records (exact totals, rate, ETA) via `logging.getLogger('ebooklet.push').setLevel(logging.INFO)`; see the ops guide's "Monitoring a push"

Keys must be strings (S3 object name requirement). Values can use any serializer supported by Booklet.

Changes between releases are tracked in [CHANGELOG.md](CHANGELOG.md).

## Installation

```
pip install ebooklet
```

## Booklet vs EBooklet

[Booklet](https://github.com/mullenkamp/booklet) is a single-file key/value database used as the foundation for EBooklet. Booklet manages local data, while EBooklet manages the interaction between local and remote data. It is best to familiarize yourself with Booklet before using EBooklet.

EBooklet is designed so you can primarily work with Booklet locally, then push to S3 later via EBooklet. If you're actively collaborating with others, open the data using EBooklet to prevent conflicts.

Unlike Booklet which uses fast threading and OS-level file locks, EBooklet uses S3 object locking when opened for writing. This ensures only one process has write access to a remote database at a time, but is slower than local file locks.

## Quick Start

### Connection setup

Create an `S3Connection` with your credentials and bucket info:

```python
import ebooklet

remote_conn = ebooklet.S3Connection(
    access_key_id='my_key_id',
    access_key='my_secret_key',
    db_key='big_data.blt',
    bucket='my-bucket',
    endpoint_url='https://s3.us-west-001.backblazeb2.com',  # optional, for non-AWS
    db_url='https://my-bucket.org/big_data.blt',            # optional, public URL
)
```

Use an `https` `db_url` for anything public — readers fetch the database over that URL, and a plain-`http` one is served unencrypted (a `UserWarning` is emitted). `http` is fine for local testing (e.g. MinIO); silence the warning with `warnings.filterwarnings`. The same consideration applies to a plain-`http` `endpoint_url`, which additionally carries signed requests.

### Read-only shortcut

If you only need to read and have a public URL, pass it directly — no `S3Connection` needed:

```python
db = ebooklet.open_ebooklet('https://my-bucket.org/big_data.blt', '/tmp/big_data.blt', flag='r')
```

### Open, read, write

```python
with ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', flag='c', value_serializer='pickle') as db:
    db['key1'] = ['one', 2, 'three', 4]
    value = db['key1']
```

Be careful with flags — using `'n'` will delete the remote database in addition to the local one.

## Grouped Storage

A new database stores its values in **group objects** (storage format 3, since 0.11). At each push, keys that are new to the remote are packed in the order they were written to the local file: first into the remote's last group until it reaches `group_bytes` (default `ebooklet.DEFAULT_GROUP_BYTES`, 32 MiB), then into fresh groups. Each key's group id is recorded in its remote-index entry.

```python
db = ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', flag='n',
                            value_serializer='pickle')                       # grouped, 32 MiB groups
db = ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', flag='n',
                            value_serializer='pickle', group_bytes=8 * 2**20)  # grouped, 8 MiB groups
db = ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', flag='n',
                            value_serializer='pickle', group_bytes=None)       # per-key: one object per key
```

- **Appends upload only new data** (plus at most the partly filled last group, which is topped up and re-uploaded). Keys written together share groups, so a growing dataset appended in time order keeps each new slice in new groups.
- **An update repacks only its key's group** (a key keeps its group for life).
- **Deletes are lazy**: they remove the key's index entry only. The deleted value's bytes stay in its group object until that group is repacked for another reason, and stay downloadable until then; a group left with no live member is dropped. `fsck` reports each group's dead fraction.
- Reads use S3 byte-range GETs: multi-key reads from the same group use one merged range (from the first to the last wanted member).
- A grouped remote records the `group_bytes` its last push packed with. Omitted, an existing remote keeps its storage mode and its recorded `group_bytes`; a new database is grouped at 32 MiB. Passing another value changes the target from that push on: new data packs to it, existing groups keep their size, and the remote records the new value (no history is kept). `db.group_bytes` reports the value in effect. An explicit value for the other mode is ignored with a warning (with `flag='n'` it raises — call `delete_remote()` first to change the mode).
- A value larger than `group_bytes` gets a group of its own. A group can still exceed the 4 GiB packing ceiling if its members grow in place (`GroupTooLargeError`).

**Per-key storage** (`group_bytes=None`, format 2) puts each value in its own S3 object. It suits databases pushed very often in small increments (each push uploads exactly the changed values, with no group to top up).

**Hash-grouped remotes (pre-0.11, `num_groups`) are not readable by 0.11.** To move one to the current format: with `ebooklet<0.11`, open it `'w'` and call `load_items()` so the local file holds every value; `delete_remote()`; then push the local file again with 0.11. The `num_groups` argument is gone: for one release an explicit `num_groups=None` still means per-key (as it always did); an int raises.

## Syncing with S3

The `changes()` method returns a `Change` object for inspecting and pushing differences between local and remote:

```python
with ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', 'w') as db:
    db['key1'] = 'new value'

    changes = db.changes()

    for change in changes.iter_changes():
        print(change)

    changes.push()     # upload local changes to S3
```

Use `changes.discard()` to remove local changes without pushing, or pass specific keys to discard selectively:

```python
    changes.discard()          # discard all local changes
    changes.discard(['key1'])  # discard only key1
```

## Other Methods

| Method | Description |
|--------|-------------|
| `delete_remote()` | Delete the entire remote database. The local file is kept and becomes the content of the next push; its cached index/manifest of the deleted remote are forgotten |
| `copy_remote(remote_conn)` | Copy the remote to another S3 location. Efficient S3-to-S3 copy when credentials match, otherwise downloads then uploads |
| `load_items(keys=None)` | Download keys/values to the local file without returning them. Pass `None` to load everything |
| `get_items(keys)` | Load then return an iterator of `(key, value)` pairs |
| `map(func, keys=None, n_workers=None)` | Apply a function to items in parallel using multiprocessing. `func(key, value)` should return `(new_key, new_value)` or `None` to skip |

## Remote Connection Groups

Remote connection groups organize and store collections of `S3Connection` objects. All data from an `S3Connection` is stored except the `access_key` and `access_key_id`. Useful for grouping related or versioned databases together.

They work like a normal EBooklet except they use `add` instead of `set`, keys are database UUIDs, and values are dicts of `S3Connection` parameters plus metadata.

The entry schema (version 1, documented on `RemoteConnGroup.add`) is **frozen**: consumers can rely on its fields indefinitely, and any future change will come as a new `entry_version` alongside it. Entries never contain credentials.

The remote connection must already exist to be added to a group.

```python
remote_conn_rcg = ebooklet.S3Connection(
    access_key_id_rcg, access_key_rcg, db_key_rcg, bucket_rcg,
    endpoint_url=endpoint_url_rcg,
)

with ebooklet.open_rcg(remote_conn_rcg, '/tmp/rcg.blt', 'n') as rcg:
    rcg.add(remote_conn)

    changes = rcg.changes()
    changes.push()
```

## Data Formats and Stability

What EBooklet stores in a remote: storage **format 3** (grouped, since 0.11) or **format 2** (per-key; also the 0.10 hash-grouped layout, which 0.11 refuses). For a database at S3 key `D`:

| Object | Key | Contents |
|--------|-----|----------|
| db object | `D` | Body: the db-object payload (below). S3 metadata: `timestamp`, `uuid`, `type`, `init_bytes`, `format_version` (3 grouped, 2 per-key; a format-2 remote that also carries `num_groups` is a legacy hash-grouped one) |
| group generations | `D/<gid>.<gen13>` | Immutable group objects: `gid` is the decimal group id, `gen13` a 13-hex generation token minted per push. Never overwritten — a repack creates a NEW generation and the commit un-references the old one before it is deleted. A gid freed by an emptied group may be reused later, always with a fresh generation |
| per-key values | `D/<key>` | Per-key mode only (overwritten in place; each PUT is object-atomic, but there is no cross-key snapshot isolation) |
| lock tickets | `D.lock.<id>-<seq>` | Transient S3 lock objects for the active writer |

**db-object payload** — everything that must change together rides ONE object, whose single PUT is the push's atomic commit point:

```
magic b'ebooklet-db\x00' (12) | payload_version >H (2) | reserved (2)
| manifest_len >Q (8) | meta_len >Q (8) | index_len >Q (8)
| manifest: JSON {group_id: generation}      (empty in per-key mode)
| meta:     JSON {"timestamp": µs, "data": ...}   (length 0 = no metadata)
| index:    the serialized remote-index booklet
```

- **`format_version`** stamps the remote storage format: 3 for grouped, 2 for per-key (a per-key remote stays readable by 0.10 clients). The stamp follows the storage mode; `SUPPORTED_FORMAT_VERSION` is only the highest format this client reads. Opening a remote with a NEWER stamp refuses with `UnsupportedFormatError` (upgrade ebooklet). Legacy remotes — format 1, and hash-grouped format 2 (with `num_groups`) — refuse too; see "Hash-grouped remotes" above for the move to the current format.
- **User metadata** is embedded in the payload's `meta` section (no separate `_metadata` object): it commits atomically with the data.
- **Remote-index entry**: per-key, 15 bytes: `timestamp` (7) + `offset` (4) + `length` (4), with `offset` and `length` always 0. Grouped, 19 bytes: `timestamp` (7) + `gid` (4) + `offset` (4) + `length` (4) — the gid names the member's group (its live generation via the manifest), offset/length locate the value inside it, and `length` **may be 0 for an empty value**. The index's fixed value length is the layout discriminator.
- **Group object layout**: `[entry_count: >I]` then per entry `[key_len: >H][key][timestamp: 7 bytes][value_len: >I][value]`. Self-describing: recovery paths trust the embedded keys/timestamps over the index. A group's packed size is capped at 4 GiB (`GroupTooLargeError` at pack time — reachable only by in-place growth; a `flag='n'` re-creation re-allocates every group).
- **RCG entry schema v1**: frozen (see Remote Connection Groups above).

**Integrity checking** — `ebooklet.fsck(remote_conn)` reports orphans (objects nothing references: abandoned generations from crashed pushes, failed GC leftovers), referenced-but-missing objects, and torn teardowns; `fsck(conn, delete_orphans=True)` sweeps aged orphans under the write lock (orphans are invisible to readers, so this is housekeeping, not repair).

**Local state** — pending (unpushed) writes and deletions are journaled inside the local booklet file and survive sessions: reads always see your own unpushed changes, deletions cannot resurrect, and the next `push()` applies everything pending. `force_lock=True` on open breaks only lock tickets older than 2 hours (a live writer is protected; it would otherwise abort at its next push's lock re-verification).

**Recovery recipes** — partial-failure retry, `force_push` after a failed commit, lock triage, `RemoteIntegrityError` triage, offline mode, and the format upgrade recipe live in the operations guide: [`docs/ops.md`](docs/ops.md).

## Open Flags

| Flag | Meaning |
|------|---------|
| `'r'` | Open existing database for reading only (default) |
| `'w'` | Open existing database for reading and writing |
| `'c'` | Open database for reading and writing, creating it if it doesn't exist |
| `'n'` | Always create a new, empty database, open for reading and writing |
