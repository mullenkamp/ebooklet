# Architectural & API Assessment of EBooklet

## Overall Verdict

The architecture is **well-suited to its purpose** — a dict-like local database with lazy S3 sync. The three-layer design (remote → main → utils) is clean, and the `MutableMapping` interface is the right abstraction. That said, there are several areas where the design could be tightened. This document breaks the assessment down by category.

---

## Strengths

**1. The hybrid local/remote model is the right call.** Writes go to the local booklet file immediately, and syncing is an explicit user action via `changes().push()`. This avoids latency on every write and gives users control over when network I/O happens. It's a natural fit for batch workloads.

**2. The `MutableMapping` interface is appropriate.** A key-value store that behaves like a dict is intuitive and composable with the rest of the Python ecosystem. Users get `db[key]`, `for k in db`, `len(db)`, etc. for free.

**3. Grouped S3 storage (`num_groups`) is a smart optimization.** Packing many keys into a smaller number of S3 objects reduces API calls and costs. The Blake2b hashing for group assignment is reasonable.

**4. Multi-level locking.** Thread locks (booklet), file locks (portalocker), and S3 object locks cover the concurrency spectrum well. The `break_other_locks()` escape hatch for deadlock recovery is practical.

**5. Resource cleanup via `weakref.finalize`.** This ensures locks and file handles get released even if the user forgets to call `.close()` or use a context manager.

---

## Design Concerns

**1. The `Change` workflow is awkward for common cases.**

The three-step `pull() → update() → push()` forces users to manage intermediate state. But `push()` internally calls `update()` anyway, and many users just want "sync my changes." Consider whether a single `db.push()` convenience method would serve the 90% case better, keeping `Change` for advanced use (inspecting diffs, selective discard).

**2. Reads silently trigger network I/O — this can surprise users.**

`db[key]` may download from S3 if the local copy is stale. This is convenient but violates the principle of least surprise for a "dict-like" object. A stale read on a fast path could introduce seconds of latency with no warning. Consider:
- A `db.load_items()` pre-fetch pattern (which exists but isn't prominent)
- Logging/callback when a remote fetch occurs
- A strict-local mode that never fetches on read

**3. `push()` return type is overloaded: `bool | dict`.** Returning `True` for success, `False` for no-op, and a `dict` for partial failure is error-prone. Users who write `if db.changes().push():` will treat a partial-failure dict as truthy (success). This should either always raise on failure or return a structured result object.

**5. `num_groups` is silently immutable after creation.** If a user opens an existing database with a different `num_groups`, it's quietly ignored (the remote value wins). This should raise a warning or error — silent parameter ignoring leads to confusion.

**6. The `open()` factory does too much type inference.** It accepts `S3Connection | str | dict` for `remote_conn`, infers database type from remote metadata, and branches on `remote_conn_group`. This makes the entry point hard to reason about. Separate constructors or explicit factory methods (`open_db()`, `open_conn_group()`) would be clearer.

---

## API Surface Issues

**1. `RemoteConnGroup.set()` raises `NotImplementedError`.** This breaks the `MutableMapping` contract. If a subclass can't support `__setitem__`, it shouldn't inherit from `MutableMapping`. Consider composition over inheritance here, or at minimum document this prominently.

**2. Missing `__repr__` / `__str__`.** Debugging is harder when `print(db)` gives you `<EVariableLengthValue object at 0x...>`. A repr showing the file path, flag, key count, and remote status would be valuable.

**3. Timestamp API is low-level.** Timestamps are microsecond POSIX integers internally but `set_timestamp()` accepts `int | str | datetime`. The asymmetry (returns int, accepts multiple types) adds cognitive load. Pick one canonical type or always return `datetime`.

**4. Empty docstrings everywhere.** Nearly every class and method has `""" """`. This hurts IDE discoverability and `help()` output. The CLAUDE.md serves as documentation for AI tools, but human developers get nothing from the code itself.

**5. `map()` pre-fetches remote items before processing.** `load_items()` is called before spawning workers to ensure data is available locally. When `keys` is provided, only those items are downloaded. When `keys=None`, all remote items are fetched first, which could be a long blocking step for large datasets.

---

## Error Handling

**1. Inconsistent error messages for read-only mode.** Some methods say `"File is open for read-only."` and others say `"File is open for read only."` (no hyphen). Minor, but suggests copy-paste rather than a centralized check.

**2. No custom exception hierarchy.** Everything is `ValueError`, `KeyError`, or `urllib3.HTTPError`. A small set of domain exceptions (`EBookletError`, `SyncError`, `LockError`, `RemoteMismatchError`) would make error handling much easier for consumers.

**3. Partial upload failures are too quiet.** Returning a dict of failures from `push()` is easy to miss. Data loss is the worst-case outcome — this deserves louder signaling.

---

## Test Coverage Gaps

- No tests for concurrent access (threading or multiprocessing)
- No tests for lock timeout / deadlock recovery (`break_other_locks`)
- No tests for network failure mid-sync (partial push recovery)
- Tests depend on execution order (`test_clear` must run last), which is fragile
- No tests for reopening a database after unclean shutdown

---

## Recommendations Summary

| Priority | Issue | Suggestion |
|----------|-------|------------|
| High | `push()` return type overload | Return a result object or raise on partial failure |
| High | Silent remote fetches on read | Add logging, or a strict-local mode |
| High | No custom exceptions | Add `EBookletError` hierarchy |
| Medium | `Change` workflow verbosity | Add `db.push()` convenience method |
| Medium | `RemoteConnGroup` breaks MutableMapping | Use composition or document clearly |
| Medium | `num_groups` silently ignored | Warn or raise on mismatch |
| Medium | Empty docstrings | Add real docstrings to public API |
| Low | `open()` does too much | Consider separate factory functions |
| Low | Timestamp type asymmetry | Standardize on one return type |
| Low | Missing `__repr__` | Add informative repr |

---

The core architecture — local booklet + lazy S3 sync with explicit push — is sound and appropriate for the use case. The main areas for improvement are around API ergonomics, error signaling, and making implicit behavior (especially network I/O on reads) more visible to the user.
