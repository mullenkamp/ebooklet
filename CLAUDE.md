# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EBooklet is a Python key-value database that syncs with S3. It extends the [booklet](https://github.com/mullenkamp/booklet) library with S3 synchronization, providing a `MutableMapping` (dict-like) interface backed by local files and remote S3 storage.

## Build & Development Commands

```bash
# Install in development mode
uv sync

# Run tests
uv run test

# Run a single test file
uv run test ebooklet/tests/test_ebooklet.py

# Lint
uv run lint:style       # check style (ruff + black)
uv run lint:typing       # type check (mypy)
uv run lint:fmt          # auto-format (black + ruff --fix)
uv run lint:all          # style + typing

# Build package
uv build

```

## Code Style

- Line length: 120 (black + ruff)
- Python 3.10+ required
- String normalization skipped (black `skip-string-normalization = true`)
- Relative imports banned; use absolute imports from `ebooklet`

## Architecture

### Three-Layer Design

1. **`remote.py` — S3 Connection Layer**
   - `S3Connection` — configuration/credential holder; call `.open(flag)` to get a session
   - `S3SessionReader` — read-only S3 access (supports both `s3func.S3Session` and `s3func.HttpSession` for public URLs); `get_object()` supports `range_start`/`range_end` for byte-range requests
   - `S3SessionWriter(S3SessionReader)` — adds write operations, S3 locking, `copy_remote`, `delete_remote`

2. **`main.py` — Database Layer**
   - `EVariableLengthValue(MutableMapping)` — the main database class. Maintains a local booklet file + a `.remote_index` file tracking what's on S3
   - `RemoteConnGroup(EVariableLengthValue)` — specialized variant that stores `S3Connection` references (stored with booklet's `'orjson'` value serializer — a file-format id, unrelated to the msgspec runtime serialization)
   - `Change` — manages sync workflow: `pull()` updates remote index+manifest, `build_changelog()` creates the changelog (union of timestamp diff and the journal; renamed from `update()` in 0.10), `push()` runs the upload→commit→GC protocol and returns a `PushResult` (`updated`, `failures` as `'ClassName: message'` strings, `__bool__` = fully-successful; commit failures RAISE — HTTPError / `LockLostError`), `pending_deletes`/`discard()` expose and cancel pending changes
   - `open_ebooklet()` — factory function for `EVariableLengthValue` databases
   - `open_rcg()` — factory function for `RemoteConnGroup` databases
   - Both factories take `offline=False|'auto'|True` (flag='r' only): True never touches the remote (stub `OfflineSession` in remote.py; unmaterialized reads raise `OfflineError`), 'auto' falls back to offline ONLY on transport-level unreachability (`errors.TRANSPORT_ERRORS`); sessions expose `.offline`
   - `errors.py` — the typed taxonomy (0.10): everything derives from `ebooklet.Error`; pre-taxonomy parentage kept via dual inheritance (`ReadOnlyError`/`UUIDMismatchError`/`RemoteMissingError`/`UnsupportedFormatError`/`GroupTooLargeError` are ValueErrors; `RemoteIntegrityError` stays an HTTPError; `LockLostError` deliberately is NOT). `clear()` is a TRUE clear (journaled deletes, push-applied, discard-cancellable); cache eviction = `prune(timestamp=now)`; `del` of a missing key raises KeyError; `update()` has dict.update semantics

   Also `journal.py` — the persistent session state in booklet reserved slots: `JournalState` (slot 1: pending writes/deletes, num_groups tri-state, replace_pending, meta_pending — the source of read-your-writes and cross-session durability) and `RemoteState` (slot 2: the manifest + remote metadata section of the last in-sync db object). And `fsck.py` — `ebooklet.fsck()` orphan/integrity reporting + age-gated sweep.

3. **`utils.py` — Sync Engine**
   - Handles local file initialization, remote index download, changelog creation, and multi-threaded upload/download via `ThreadPoolExecutor`
   - `pack_group()` / `unpack_group()` — serialize/deserialize grouped key/value entries; `pack_group()` also returns byte-offset mapping and enforces the 4 GiB cap (`GroupTooLargeError`)
   - `upload_group()` — packs and uploads a group to a FRESH generation object, returns `(error, offsets_dict)`
   - `get_remote_group_value(s)()` — byte-range S3 GETs against a group's live generation (resolved via the manifest)
   - `check_local_vs_remote()` — compares timestamps to decide if data needs downloading (journaled pending writes are gated before this ever runs)
   - `build_db_payload()` / `parse_db_payload()` — the format-2 db-object payload (manifest + metadata section + index bytes)

### Grouped S3 Storage

When `num_groups` is set, keys are hashed into N groups (`blake2b` → `mod num_groups`). Each group is a single S3 object containing all key/value pairs for that bucket, packed via `pack_group()` / `unpack_group()` in `utils.py`.

- **Byte-range reads:** The `remote_index` stores byte offset and length for each key within its group. Single-key reads use S3 byte-range GET requests to fetch only the needed bytes. Multi-key reads from the same group use a single merged byte-range GET (min offset → max offset+length).
- **Grouped writes:** `push()` re-packs entire affected groups and uploads each to a NEW immutable generation object (`{gid}.{gen13}`); offsets are STAGED and applied to `remote_index` only after the commit. Old generations are deleted (exact keys) after the commit; readers on the old manifest are never affected mid-push.
- Per-key storage (no grouping) is used when `num_groups` is `None`.

### Data Flow

- **Read path (per-key):** `db[key]` → check local file → if missing/stale, download from S3 → cache locally → return
- **Read path (grouped):** `db[key]` → check local file → if missing/stale, byte-range GET from group S3 object using stored offset/length → cache locally → return
- **Bulk read (grouped):** `load_items()` → collect stale keys → group by group_id → one merged byte-range GET per group → cache locally
- **Write path:** `db[key] = val` → write to local booklet file only
- **Sync path:** `db.changes().push()` → lock verify → changelog (timestamp diff ∪ journal; skew-stamped edits normalized) → pull unmaterialized group members (read-your-writes gated) → upload new generations via thread pool → lock verify → ONE db-object PUT (manifest+metadata+index — the atomic commit) → apply staged index entries, clear journal per committed group, persist remote-state → GC replaced generations (failures = invisible orphans for fsck)

### Key Files per Database

- `{file_path}` — local booklet database
- `{file_path}.remote_index` — tracks what's stored remotely (FixedLengthValue, 15 bytes per key: 7-byte timestamp + 4-byte offset + 4-byte length). For per-key mode, offset/length are zero-filled.
- `{file_path}.changelog` — temporary diff file created during sync (14 bytes per key: 7-byte local timestamp + 7-byte remote timestamp)
- S3: `{db_key}` (the db object: format-2 payload = manifest + metadata section + index; its single PUT is the push's commit point) and `{db_key}/{gid}.{gen13}` (immutable group generations) or `{db_key}/{key}` (per-key mode values)

### Concurrency Model

- Reads and writes are thread-safe (thread locks in booklet)
- Multiprocessing-safe (portalocker file locks)
- Remote write locking via S3 object locks (`s3func.s3lock`)
- Resource cleanup via `weakref.finalize`

### Public API (`__init__.py`)

```python
from ebooklet import (open_ebooklet, open_rcg, EVariableLengthValue, RemoteConnGroup,
                      S3Connection, RemoteIntegrityError, UnsupportedFormatError,
                      GroupTooLargeError, fsck, FsckReport)
```

### Dependencies

Core: `booklet>=0.12.7` (reserved slots), `s3func>=0.9.3` (lock verify + age-gated breaking), `urllib3>=2`, `msgspec` (all ebooklet-owned serialization), `portalocker` (plus `uuid6` via booklet)

### Open Flags

- `r` — read only (default)
- `w` — read/write existing
- `c` — read/write, create if missing
- `n` — replace the remote: the intent is journaled (survives sessions; a 'w' reopen warns and completes it) and executed as upload→commit→sweep — a partially-failed replacement commits nothing and the old remote stays intact
