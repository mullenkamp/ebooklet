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
   - `S3SessionReader` — read-only S3 access (supports both `s3func.S3Session` and `s3func.HttpSession` for public URLs)
   - `S3SessionWriter(S3SessionReader)` — adds write operations, S3 locking, `copy_remote`, `delete_remote`

2. **`main.py` — Database Layer**
   - `EVariableLengthValue(MutableMapping)` — the main database class. Maintains a local booklet file + a `.remote_index` file tracking what's on S3
   - `RemoteConnGroup(EVariableLengthValue)` — specialized variant that stores `S3Connection` references (always uses `orjson` serializer)
   - `Change` — manages sync workflow: `pull()` updates remote index, `update()` creates changelog, `push()` uploads changes
   - `open()` — top-level factory function; determines type from remote metadata or `remote_conn_group` flag

3. **`utils.py` — Sync Engine**
   - Handles local file initialization, remote index download, changelog creation, and multi-threaded upload/download via `ThreadPoolExecutor`
   - Key helper: `check_local_vs_remote()` compares timestamps to decide if data needs downloading

### Data Flow

- **Read path:** `db[key]` → check local file → if missing/stale, download from S3 → cache locally → return
- **Write path:** `db[key] = val` → write to local booklet file only
- **Sync path:** `db.changes().push()` → create changelog (local vs remote timestamps) → upload changed items via thread pool → update remote index object on S3

### Key Files per Database

- `{file_path}` — local booklet database
- `{file_path}.remote_index` — tracks what's stored remotely (FixedLengthValue, 7-byte timestamp per key)
- `{file_path}.changelog` — temporary diff file created during sync
- S3: `{db_key}` (main index object) and `{db_key}/{key}` (individual values)

### Concurrency Model

- Writes are thread-safe (thread locks)
- Multiprocessing-safe (portalocker file locks)
- Remote write locking via S3 object locks (`s3func.s3lock`)
- Reads are NOT thread-safe
- Resource cleanup via `weakref.finalize`

### Public API (`__init__.py`)

```python
from ebooklet import open, EVariableLengthValue, RemoteConnGroup, S3Connection
```

### Dependencies

Core: `booklet>=0.9.2`, `s3func>=0.7.2`, `urllib3>=2` (plus `uuid6`, `orjson`, `portalocker`, `base64`)

### Open Flags

- `r` — read only (default)
- `w` — read/write existing
- `c` — read/write, create if missing
- `n` — always create new (deletes existing remote data)
