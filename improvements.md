# Proposed Improvements for ebooklet

## 1. Bugs

### 1a. Broken `copy_remote` indirect path (remote.py:503-504)

When source and target remotes use **different credentials**, the final db-object copy calls `copy_object` with a function as the first argument instead of calling `indirect_copy_remote`:

```python
# Current (broken)
resp = self._write_session.copy_object(utils.indirect_copy_remote, self._read_session, writer._write_session, writer.write_db_key, source_bucket=source_bucket, dest_bucket=target_bucket)

# Fix
resp = utils.indirect_copy_remote(self._read_session, writer._write_session, self.write_db_key, writer.write_db_key, source_bucket, target_bucket)
```

The same-credentials path (line 477) is correct.

### 1b. `lstrip` misused as prefix removal (remote.py:462, 489)

`str.lstrip()` strips a **set of characters**, not a prefix. For example if `write_db_key = "abc"`, then `"abcdef/key".lstrip("abc")` strips all leading a/b/c chars and produces `"def/key"` instead of `"def/key"`. With real keys this could silently corrupt the target key path.

```python
# Current (broken)
target_key = writer.write_db_key + source_key.lstrip(self.write_db_key)

# Fix (Python 3.9+, project requires 3.10+)
target_key = writer.write_db_key + source_key.removeprefix(self.write_db_key)
```

Both lines 462 and 489 have this bug.

### 1c. `FixedValue` vs `FixedLengthValue` mismatch (main.py:87)

The changelog is created everywhere using `booklet.FixedLengthValue`, but `Change.discard()` opens it with `booklet.FixedValue`. If these are different classes, this is a bug:

```python
# Current
with booklet.FixedValue(self._changelog_path) as f:

# Should be
with booklet.FixedLengthValue(self._changelog_path) as f:
```

### 1d. `Change.push` treats upload failures as success (main.py:114-123)

`utils.update_remote` returns either a `dict` of failures (truthy) or a `bool`. In `Change.push`, `if success:` evaluates a non-empty failures dict as truthy, so it cleans up the changelog and deletes even though uploads failed:

```python
# Current
success = utils.update_remote(...)
if success:
    self._changelog_path.unlink()  # Cleans up even on partial failure!
    ...

# Fix: distinguish between bool success and dict failures
result = utils.update_remote(...)
if isinstance(result, dict):
    # Partial failure — don't clean up changelog
    return result
elif result:
    self._changelog_path.unlink()
    ...
```

### 1e. `__getitem__` raises KeyError for stored `None` values (main.py:416-422)

If a user stores `None` as a value (valid with pickle serializer), `get()` returns `None` and `__getitem__` incorrectly raises `KeyError`. Fix with a sentinel:

```python
_MISSING = object()

def __getitem__(self, key: str):
    value = self.get(key, default=_MISSING)
    if value is _MISSING:
        raise KeyError(f'{key}')
    return value
```

### 1f. Mutable default argument (remote.py:367)

`put_object(self, key, data, metadata={})` — mutable default arguments are shared across calls and can lead to subtle bugs:

```python
# Fix
def put_object(self, key: str, data: bytes, metadata=None):
    if metadata is None:
        metadata = {}
    ...
```

## 2. Unused Code

### 2a. Unused imports

| File | Import | Notes |
|------|--------|-------|
| `remote.py:8` | `import os` | Not used |
| `remote.py:12` | `Generic` from typing | Not used |
| `remote.py:12` | `Any, Iterator, List, Dict` from typing | Not used (only `Union` is used) |
| `remote.py:17` | `import copy` | Not used |
| `main.py:7` | `Any, Iterator, List, Dict` from typing | Not used (only `Union` is used) |
| `utils.py:15` | `import shutil` | Not used (leftover from previous cleanup) |

### 2b. Unused functions

- **`remote.py:52-71` — `get_db_metadata()`**: Defined but never called anywhere in the codebase.
- **`remote.py:74-87` — `get_user_metadata()` (standalone)**: The standalone function is never called. (The `S3SessionReader.get_user_metadata()` method is used — this is the module-level function that's dead.)
- **`utils.py:345-353` — `determine_file_obj_size()`**: Only referenced in test_utils.py, never used in production code. Also makes `import io` unnecessary if removed.

### 2c. Trailing blank lines

All three source files and the test file have 40-60 trailing blank lines that should be trimmed:
- `remote.py` lines 710-755
- `utils.py` lines 370-422
- `test_ebooklet.py` lines 430-502

## 3. Code Quality

### 3a. Debug print left in production code (remote.py:234)

```python
print(f"DEBUG: status={resp_obj.status}, error={resp_obj.error}")
```

This should be removed or converted to `logging.debug(...)`.

### 3b. User-facing `print()` should use logging

Several `print()` calls provide operational feedback but should use the `logging` module so callers can control verbosity:

- `remote.py:455` — "Both the source and target remotes use the same credentials..."
- `remote.py:474` — "Copy failures have occurred..."
- `remote.py:482` — "The source and target remotes use different credentials..."
- `remote.py:500` — "Copy failures have occurred..."
- `utils.py:307` — "There were N items that failed to upload..."

### 3c. Redundant condition (main.py:144)

```python
# Current
if flag != 'r' and flag == 'n' and (remote_session.uuid is not None):

# Simplified (flag == 'n' already implies flag != 'r')
if flag == 'n' and remote_session.uuid is not None:
```

### 3d. Remaining commented-out code

Leftover from the previous cleanup:

- `utils.py:94-97` — Commented read/write flag alternatives
- `utils.py:128` — `# remote_index_key = read_conn.db_key + '.remote_index'`
- `utils.py:134` — `# shutil.copyfileobj(index0.stream, f)`
- `utils.py:150-153` — Commented `print()` debug statements
- `utils.py:160` — `# return urllib3.exceptions.HttpError(...)`
- `main.py:38` — `# self.update()`

### 3e. Minor style: set literal (main.py:220)

```python
# Current
overlap = set([utils.metadata_key_str])

# Pythonic
overlap = {utils.metadata_key_str}
```

## 4. Suggested Order of Implementation

1. **Bug fixes first** (1a-1f) — these affect correctness
2. **Unused code removal** (2a-2c) — low risk, reduces noise
3. **Code quality** (3a-3e) — cosmetic but improves maintainability
