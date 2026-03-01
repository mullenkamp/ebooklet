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
   - `RemoteConnGroup(EVariableLengthValue)` — specialized variant that stores `S3Connection` references (always uses `orjson` serializer)
   - `Change` — manages sync workflow: `pull()` updates remote index, `update()` creates changelog, `push()` uploads changes
   - `open()` — top-level factory function; determines type from remote metadata or `remote_conn_group` flag

3. **`utils.py` — Sync Engine**
   - Handles local file initialization, remote index download, changelog creation, and multi-threaded upload/download via `ThreadPoolExecutor`
   - `pack_group()` / `unpack_group()` — serialize/deserialize grouped key/value entries; `pack_group()` also returns byte-offset mapping
   - `upload_group()` — packs and uploads a group, returns `(error, offsets_dict)`
   - `get_remote_group_value()` — single-key byte-range S3 GET
   - `get_remote_group_values()` — merged byte-range S3 GET for multiple keys in the same group
   - `check_local_vs_remote()` — compares timestamps to decide if data needs downloading

### Grouped S3 Storage

When `num_groups` is set, keys are hashed into N groups (`blake2b` → `mod num_groups`). Each group is a single S3 object containing all key/value pairs for that bucket, packed via `pack_group()` / `unpack_group()` in `utils.py`.

- **Byte-range reads:** The `remote_index` stores byte offset and length for each key within its group. Single-key reads use S3 byte-range GET requests to fetch only the needed bytes. Multi-key reads from the same group use a single merged byte-range GET (min offset → max offset+length).
- **Grouped writes:** `push()` re-packs entire affected groups and uploads them. `upload_group()` returns offset info that gets stored in `remote_index`.
- Per-key storage (no grouping) is used when `num_groups` is `None`.

### Data Flow

- **Read path (per-key):** `db[key]` → check local file → if missing/stale, download from S3 → cache locally → return
- **Read path (grouped):** `db[key]` → check local file → if missing/stale, byte-range GET from group S3 object using stored offset/length → cache locally → return
- **Bulk read (grouped):** `load_items()` → collect stale keys → group by group_id → one merged byte-range GET per group → cache locally
- **Write path:** `db[key] = val` → write to local booklet file only
- **Sync path:** `db.changes().push()` → create changelog (local vs remote timestamps) → upload changed items via thread pool → update remote index object on S3

### Key Files per Database

- `{file_path}` — local booklet database
- `{file_path}.remote_index` — tracks what's stored remotely (FixedLengthValue, 15 bytes per key: 7-byte timestamp + 4-byte offset + 4-byte length). For per-key mode, offset/length are zero-filled.
- `{file_path}.changelog` — temporary diff file created during sync (14 bytes per key: 7-byte local timestamp + 7-byte remote timestamp)
- S3: `{db_key}` (main index object) and `{db_key}/{key}` (individual values or group blobs)

### Concurrency Model

- Reads and writes are thread-safe (thread locks in booklet)
- Multiprocessing-safe (portalocker file locks)
- Remote write locking via S3 object locks (`s3func.s3lock`)
- Resource cleanup via `weakref.finalize`

### Public API (`__init__.py`)

```python
from ebooklet import open, EVariableLengthValue, RemoteConnGroup, S3Connection
```

### Dependencies

Core: `booklet>=0.12.1`, `s3func>=0.7.2`, `urllib3>=2` (plus `uuid6`, `orjson`, `portalocker`, `base64`)

### Open Flags

- `r` — read only (default)
- `w` — read/write existing
- `c` — read/write, create if missing
- `n` — always create new (deletes existing remote data)
