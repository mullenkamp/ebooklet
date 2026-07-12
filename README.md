# EBooklet

EBooklet is a Python key-value database that syncs with S3 (AWS or any S3-compatible service). It builds on the [Booklet](https://github.com/mullenkamp/booklet) package, providing a [MutableMapping](https://docs.python.org/3/library/collections.abc.html#collections-abstract-base-classes) (dict-like) interface backed by local files and remote S3 storage.

- **S3 sync** — push/pull changes between a local database and an S3 bucket
- **Dict-like API** — standard `MutableMapping` plus `dbm`-style methods
- **Grouped storage** — hash keys into N groups stored as single S3 objects, with automatic byte-range reads
- **Concurrency** — thread-safe writes (thread locks), multiprocessing-safe (file locks), and S3 object locking for remote writes

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

By default, each key/value pair is stored as a separate S3 object. When `num_groups` is set, keys are hashed into N groups, each stored as a single S3 object containing all key/value pairs for that bucket. If the provided `num_groups` is not prime, it is automatically rounded up to the nearest prime for optimal hash distribution.

```python
db = ebooklet.open_ebooklet(remote_conn, '/tmp/big_data.blt', flag='n',
                            value_serializer='pickle', num_groups=64)
# num_groups is adjusted to 67 (nearest prime >= 64)
```

- Keys are assigned to groups via `blake2b` hash mod `num_groups`
- Single-key reads use S3 byte-range GET requests to fetch only the needed bytes
- Multi-key reads from the same group use a single merged byte-range GET
- On push, entire affected groups are re-packed and uploaded
- For databases already pushed to the remote, `num_groups` is read from S3 metadata (user-provided value is ignored)
- For a database created locally but not yet pushed, the creation-time choice is not recorded anywhere — re-pass the same `num_groups` when reopening before the first push (reopening without it emits a `UserWarning`, and the first push would fall back to per-key storage)

Use grouped storage when you have many small values — it reduces the number of S3 objects and can improve read performance through byte-range requests.

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
| `delete_remote()` | Delete the entire remote database |
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

What EBooklet stores in a remote, and how stable each piece is. For a database at S3 key `D`:

| Object | Key | Contents |
|--------|-----|----------|
| db object | `D` | Body: the serialized remote-index booklet. S3 metadata: `timestamp`, `uuid`, `type`, `init_bytes`, `format_version`, and `num_groups` (grouped mode) |
| children | `D/<name>` | Group objects (named by decimal group id), per-key values, and the user-metadata object `_metadata` |
| lock tickets | `D.lock.<id>-<seq>` | Transient S3 lock objects for the active writer |

- **`format_version`** (new in 0.9.5) stamps the remote storage format; the current version is 1, and remotes pushed by older versions (no stamp) are read as 1. An ebooklet refuses to open a remote whose `format_version` is newer than it supports (`UnsupportedFormatError`) — upgrade ebooklet instead.
- **Remote-index entry** (15 bytes per key): `timestamp` (7 bytes) + `offset` (4) + `length` (4). In per-key mode, `offset` and `length` are always 0. In grouped mode they locate the member's value inside its group object — `length` is the value's byte length and **may be 0 for an empty value**. Future layouts will change the index's fixed entry size (`value_len`, 15 today), which is the layout discriminator — not the `length` field.
- **Group object layout**: `[entry_count: >I]` then per entry `[key_len: >H][key][timestamp: 7 bytes][value_len: >I][value]`. Self-describing: recovery paths trust the embedded keys/timestamps over the index.
- **RCG entry schema v1**: frozen (see Remote Connection Groups above).

## Open Flags

| Flag | Meaning |
|------|---------|
| `'r'` | Open existing database for reading only (default) |
| `'w'` | Open existing database for reading and writing |
| `'c'` | Open database for reading and writing, creating it if it doesn't exist |
| `'n'` | Always create a new, empty database, open for reading and writing |
