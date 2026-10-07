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
   - `Change` — manages sync workflow: `pull()` updates remote index+manifest, `build_changelog()` creates the changelog (union of timestamp diff and the journal; renamed from `update()` in 0.10) and — 0.10.1 — captures `loc_map` (`{key: (ts, value_offset, value_len)}` via booklet's header-only `locations()` sweep, zero extra IO) plus the `compaction_count` snapshot the offsets are valid for, `push()` runs the upload→commit→GC protocol and returns a `PushResult` (`updated`, `failures` as `'ClassName: message'` strings, `__bool__` = fully-successful; commit failures RAISE — HTTPError / `LockLostError` / `ConcurrentCompactionError`), `pending_deletes`/`discard()` expose and cancel pending changes
   - `open_ebooklet()` — factory function for `EVariableLengthValue` databases
   - `open_rcg()` — factory function for `RemoteConnGroup` databases
   - Both factories take `offline=False|'auto'|True` (flag='r' only): True never touches the remote (stub `OfflineSession` in remote.py; unmaterialized reads raise `OfflineError`), 'auto' falls back to offline ONLY on transport-level unreachability (`errors.TRANSPORT_ERRORS`); sessions expose `.offline`
   - `errors.py` — the typed taxonomy (0.10): everything derives from `ebooklet.Error`; pre-taxonomy parentage kept via dual inheritance (`ReadOnlyError`/`UUIDMismatchError`/`RemoteMissingError`/`UnsupportedFormatError`/`GroupTooLargeError` are ValueErrors; `RemoteIntegrityError` stays an HTTPError; `LockLostError` deliberately is NOT). `clear()` is a TRUE clear (journaled deletes, push-applied, discard-cancellable); cache eviction = `prune(timestamp=now)`; `del` of a missing key raises KeyError; `update()` has dict.update semantics

   Also `journal.py` — the persistent session state in booklet reserved slots: `JournalState` (slot 1: pending writes/deletes, the storage mode ('per_key'|'grouped'; a pre-0.11 hash `num_groups` reads as legacy → grouped on an absent remote), replace_pending, meta_pending — the source of read-your-writes and cross-session durability; written as journal v2 ONLY for grouped files so per-key files stay readable by 0.10) and `RemoteState` (slot 2: the manifest + remote metadata section of the last in-sync db object). And `fsck.py` — `ebooklet.fsck()` orphan/integrity reporting + age-gated sweep.

3. **`utils.py` — Sync Engine**
   - Handles local file initialization, remote index download, changelog creation, and multi-threaded upload/download via `ThreadPoolExecutor`
   - `pack_group()` / `unpack_group()` — serialize/deserialize grouped key/value entries; `pack_group()` also returns byte-offset mapping and enforces the 4 GiB cap (`GroupTooLargeError`)
   - `upload_group()` — packs and uploads a group to a FRESH generation object, returns `(error, offsets_dict)`
   - `get_remote_group_value(s)()` — byte-range S3 GETs against a group's live generation (resolved via the manifest)
   - `check_local_vs_remote()` — compares timestamps to decide if data needs downloading (journaled pending writes are gated before this ever runs)
   - `build_db_payload()` / `parse_db_payload()` — the db-object payload (manifest + metadata section + index bytes)
   - `plan_groups()` — pure allocation of gids to keys new to the remote (write order, tail top-up, fresh gids `max(manifest)+1`)

### Grouped S3 Storage (format 3, 0.11: write-order groups)

The WRITER assigns groups at push time and records each key's gid in its 19-byte remote-index entry (`encode_group_entry` / `decode_index_entry`; per-key entries are 15 bytes — the sidecar's fixed value_len is the layout discriminator). Readers take the gid from the index, so ebooklet never interprets keys.

- **Allocation:** keys NEW to the remote are taken in `loc_map` order (the local file's physical order = write order) and packed into the tail group (highest gid; pulled if not local) while `size + entry <= group_bytes`, then into fresh gids. A fresh group always takes one key (oversized values get their own group). Keys already on the remote keep their gid — an update repacks only its group. **A repack's member list is the index members of that gid plus this push's allocations — local keys never join a group by themselves.**
- **Lazy deletes:** a delete only removes the index entry (already done locally before the push); no group is repacked. A manifest gid with no live index member is dropped (and GC'd). The commit gate fires for pending deletes in both modes. Dead bytes leave on the group's next repack; `fsck` reports `dead_fraction`.
- **Byte-range reads:** single-key reads GET only the value's bytes; multi-key reads from one group use a single merged range (min offset → max offset+length).
- **Grouped writes:** each repacked group goes to a NEW immutable generation object (`{gid}.{gen13}`); offsets are STAGED and applied to `remote_index` only after the commit; old generations are deleted (exact keys) after it. A freed gid number may be reused (always with a fresh generation; readers pair index + manifest from one commit).
- **Modes:** `group_bytes` (precedence: the explicit argument, then the value the grouped remote records in its db-object metadata, written by every grouped commit, then `DEFAULT_GROUP_BYTES`, 32 MiB; resolved in `main._grouped_target`, at open and on every index pull) — `None` = per-key (format 2, stamped '2', readable by 0.10). The commit stamp follows the mode; `SUPPORTED_FORMAT_VERSION` (3) is the reader cap only. `remote_session.storage_kind` classifies remotes ('per_key' | 'grouped' | 'legacy'); legacy = format 1 or hash-grouped format 2 (`num_groups` in metadata) and is refused for r/w/c.
- **Commit-path invariant (0.11, review F1):** the local file's stamp is written LAST (after the commit PUT, the sidecar apply and the remote-state persist), and an open/pull re-fetches the whole index whenever `remote_state.remote_ts != remote.timestamp`. An open or pull that ingests a fetched index also stamps the local file LAST, after the index and the remote-state cache are durable. Never refresh the manifest without the index. The grouped push's internal-error belt (a `RuntimeError`) refuses to run over a stale index (it would mistake live groups for emptied ones). After every commit the session reloads the remote's db metadata (`_load_db_metadata()`), so a later push in the same session continues from its own commit instead of force-pulling it. A replacement's `replace_pending` is cleared in the SAME journal write that clears the committed keys (code review `ebooklet-wog-code-2`, F1): `replace_pending` with no written keys makes the next push purge every local key, so no raise may sit between the two. A pull or open stamps freshness only when its reconciliation actually ran (`reconcile_local_with_index` returns None for a skipped scan).
- **Uuid rule on every push:** `Change.push` refuses a remote whose uuid differs from the local file's (except a replacement), as the open does. The pre-push adoption's forced pull would otherwise skip the check.
- **A push that creates its remote journals every key it carries** before uploading (code review `ebooklet-wog-code-1`, F1). A hydrated local file's values are otherwise unjournaled, and after a partly failed push the next fresh-index reconciliation would delete the failed groups' keys, which are the only copy.

### Data Flow

- **Read path (per-key):** `db[key]` → check local file → if missing/stale, download from S3 → cache locally → return
- **Read path (grouped):** `db[key]` → check local file → if missing/stale, byte-range GET from group S3 object using stored offset/length → cache locally → return
- **Bulk read (grouped):** `load_items()` → collect stale keys → group by group_id → one merged byte-range GET per group → cache locally
- **Write path:** `db[key] = val` → write to local booklet file only
- **Sync path (0.10.1 pipelined):** `db.changes().push()` → lock verify → changelog + loc_map capture (timestamp diff ∪ journal; skew-stamped edits normalized AND refreshed in the map) → pull unmaterialized group members (read-your-writes gated; pulled keys tracked in `pulled_keys`) → phase B: per-group workers read member values through a PRIVATE fd at the captured offsets in ascending order (no booklet lock in the hot path; a `BoundedSemaphore(push_packers)` — per-push, default 1 — gates concurrent disk readers while PUTs run outside it) → lock verify → ONE db-object PUT (manifest+metadata+index — the atomic commit) → apply staged index entries, clear journal per committed group, persist remote-state → GC replaced generations (failures = invisible orphans for fsck). Progress narrates on the `ebooklet.push` logger (INFO).
- **Pipelined-push invariants (2026-07-15 round + dual-blind reviews — do not regress these):** (1) keys in `pulled_keys` or absent from `loc_map` MUST read through booklet's locked path — their captured offset (if any) predates the pull and addresses the STALE superseded bytes (append-only means the old block still exists); (2) captured offsets die on prune/clear — `PushInProgressError` guards `prune()`/`clear()` while `_push_active`, and workers re-check `compaction_count` against the capture snapshot, aborting with `ConcurrentCompactionError` BEFORE the commit; (3) phase-B/pull/per-key workers **return errors, never raise** — every `future.result()` is also wrapped so transport raises (e.g. `MaxRetryError`) become per-group `PushResult.failures`, never a whole-push crash; (4) `pack_group` builds its buffer as a **bytearray** — `bytes +=` is quadratic (~100GB memcpy per 134MB group; the pre-0.10.1 production bottleneck; a tripwire test guards this); (5) staged index timestamps come from the capture/locked read that produced the packed bytes, never a post-upload re-read; (6) large-body upload reliability lives in s3func ≥ 0.9.4 (streamed bytes bodies + idle-timeout semantics) — never bypass `put_object` with raw urllib3 bytes bodies.

### Key Files per Database

- `{file_path}` — local booklet database
- `{file_path}.remote_index` — tracks what's stored remotely (FixedLengthValue). Grouped: 19 bytes per key (7-byte timestamp + 4-byte gid + 4-byte offset + 4-byte length). Per-key: 15 bytes (timestamp + zero-filled offset/length). A sidecar whose layout does not match the remote is discarded and re-fetched online, kept offline.
- `{file_path}.changelog` — temporary diff file created during sync (14 bytes per key: 7-byte local timestamp + 7-byte remote timestamp)
- S3: `{db_key}` (the db object: payload = manifest + metadata section + index; its single PUT is the push's commit point) and `{db_key}/{gid}.{gen13}` (immutable group generations) or `{db_key}/{key}` (per-key mode values)

### Concurrency Model

- Reads and writes are thread-safe (thread locks in booklet)
- Multiprocessing-safe (portalocker file locks)
- Remote write locking via S3 object locks (`s3func.s3lock`)
- Resource cleanup via `weakref.finalize`

### Public API (`__init__.py`)

```python
from ebooklet import (open_ebooklet, open_rcg, EVariableLengthValue, RemoteConnGroup,
                      S3Connection, PushResult, RemoteIntegrityError, UnsupportedFormatError,
                      GroupTooLargeError, PushInProgressError, ConcurrentCompactionError,
                      fsck, FsckReport)
```

### Dependencies

Core: `booklet>=0.12.7` (reserved slots), `s3func>=0.9.3` (lock verify + age-gated breaking), `urllib3>=2`, `msgspec` (all ebooklet-owned serialization), `portalocker` (plus `uuid6` via booklet)

### Open Flags

- `r` — read only (default)
- `w` — read/write existing
- `c` — read/write, create if missing
- `n` — replace the remote: the intent is journaled (survives sessions; a 'w' reopen warns and completes it) and executed as upload→commit→sweep — a partially-failed replacement commits nothing and the old remote stays intact
