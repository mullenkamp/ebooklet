# Grouped S3 Object Storage Implementation Plan

## Context

Each key/value in ebooklet is currently stored as a separate S3 object (`{db_key}/{key}`). For databases with many thousands of small values, this causes latency and excessive S3 ListObject queries. This plan implements hash-based grouping: multiple key/values are packed into a single S3 object, reducing object count dramatically.

## Design Summary

- **Group assignment**: `blake2b(key) % num_groups` (deterministic, no state to track)
- **num_groups**: Required parameter in `open()`, stored in S3 object metadata
- **Group format**: Self-describing binary (key names + timestamps + values embedded)
- **Remote index**: Unchanged (7-byte timestamp per key)
- **Reads**: Whole group download, cache all values locally (prefetching)
- **Writes**: Re-pack and re-upload entire affected groups on push
- **Metadata key**: Separate S3 object at `{db_key}/_metadata` (not grouped)
- **Scope**: New databases only

### S3 Object Layout (new)

```
{db_key}              <- main index (body=remote_index, metadata includes num_groups)
{db_key}/_metadata    <- metadata key (separate, not grouped)
{db_key}/{group_id}   <- group objects (0, 1, ..., num_groups-1)
```

### Group Binary Format

```
[num_entries: 4 bytes, uint32 BE]
Per entry:
  [key_len:    2 bytes, uint16 BE]
  [key_bytes:  variable]
  [timestamp:  7 bytes, uint56 BE]
  [value_len:  4 bytes, uint32 BE]
  [value_bytes: variable]
```

---

## Implementation Steps

### Step 1: Add group utility functions to `utils.py`

Add three new pure functions:

**`key_to_group_id(key: str, num_groups: int) -> int`**
- `hashlib.blake2b(key.encode(), digest_size=4).digest()` -> `int.from_bytes(digest, 'big') % num_groups`

**`pack_group(entries: list[tuple[str, int, bytes]]) -> bytes`**
- Takes list of `(key, timestamp_int, value_bytes)` tuples
- Serializes to the binary format above
- Returns bytes

**`unpack_group(data: bytes) -> list[tuple[str, int, bytes]]`**
- Parses binary format, returns list of `(key, timestamp_int, value_bytes)` tuples

### Step 2: Add group upload/download functions to `utils.py`

**`upload_group(group_id: int, local_file, remote_session, keys_in_group: list[str]) -> dict | None`**
- For each key in `keys_in_group`: get `(timestamp, value_bytes)` from `local_file.get_timestamp(key, include_value=True, decode_value=False)`
- Skip keys not found in local_file (deleted)
- Call `pack_group(entries)`
- Upload via `remote_session.put_object(str(group_id), packed_bytes)`
- If group is empty after filtering, delete the group object from S3
- Return None on success, error dict on failure

**`get_remote_group(group_id: int, local_file, remote_session) -> dict | None`**
- Download via `remote_session.get_object(str(group_id))`
- Call `unpack_group(data)`
- For each entry: `local_file.set(key, value_bytes, timestamp, encode_value=False)`
- Return None on success, error on failure

### Step 3: Modify `update_remote()` in `utils.py`

Current flow: iterate changelog -> upload each key individually -> upload remote_index.

New flow:
1. If `flag == 'n'`, delete everything in remote (unchanged)
2. Iterate changelog to find changed keys, compute `key_to_group_id` for each
3. Also compute group_ids for `deletes` set (deleted keys that need group re-packing)
4. **Separate metadata key**: if metadata key is in changelog, upload it individually via `upload_value` (keep existing function for this)
5. Build `affected_groups: dict[int, list[str]]` — for each affected group_id, gather ALL current keys from `local_file` that hash to that group (not just changed keys, since we re-pack the whole group)
6. Upload affected groups in parallel via `ThreadPoolExecutor`
7. On success per group: update `remote_index[key]` timestamps for all keys in that group
8. Upload remote_index as main db object (unchanged logic, but add `num_groups` to metadata)
9. Handle empty groups from deletions: if a group ends up empty, delete the S3 object

**Key change to gathering keys for a group**: Need to iterate `local_file.keys()` (or `local_file.timestamps()`) and filter by `key_to_group_id(key, num_groups) == target_group_id`. This is a local operation (fast).

**Signature change**: `update_remote(local_file, remote_index, changelog_path, remote_session, force_push, deletes, flag, ebooklet_type, num_groups)`

### Step 4: Keep `upload_value()` in `utils.py` for metadata only

The existing `upload_value` function stays but is only used for the metadata key (which is stored as a separate S3 object at `{db_key}/_metadata`).

Modify `upload_value` to use `'_metadata'` as the S3 key when the key is the metadata key string. Actually — currently it uses the raw metadata key string. We need to map it: when `key == metadata_key_str`, upload to `_metadata` instead.

### Step 5: Modify `get_remote_value()` in `utils.py` for metadata

Rename/modify `get_remote_value` to handle the metadata key's new S3 path (`_metadata`). For metadata: `remote_session.get_object('_metadata')`. The rest of the function stays the same for metadata.

### Step 6: Modify `_load_item()` in `main.py`

Current: check remote_index -> download single key if stale.

New:
```python
def _load_item(self, key):
    remote_time_bytes = self._remote_index.get(key)
    check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
    if check:
        if key == utils.metadata_key_str:
            failure = utils.get_remote_value(self._local_file, key, self._remote_session)
        else:
            group_id = utils.key_to_group_id(key, self._num_groups)
            failure = utils.get_remote_group(group_id, self._local_file, self._remote_session)
        return failure
    return None
```

After downloading a group, all sibling keys are cached locally with their correct timestamps. Subsequent `check_local_vs_remote` calls for those keys will return False (no re-download).

### Step 7: Modify `load_items()` in `main.py`

Current: submit individual `get_remote_value` tasks per key.

New:
1. Determine which keys need downloading (check_local_vs_remote)
2. Separate metadata key (handle individually)
3. Group remaining keys by `key_to_group_id(key, num_groups)` -> deduplicate to unique group_ids
4. Submit `get_remote_group(group_id, ...)` per unique group_id via ThreadPoolExecutor
5. Collect failures

```python
def load_items(self, keys=None):
    futures = {}
    failure_dict = {}
    groups_to_download = set()

    with ThreadPoolExecutor(max_workers=self._remote_session.threads) as executor:
        if keys is None:
            items_iter = self._remote_index.items()
        else:
            items_iter = ((k, self._remote_index.get(k)) for k in keys)

        for key, remote_time_bytes in items_iter:
            check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
            if check:
                if key == utils.metadata_key_str:
                    f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                    futures[f] = key
                else:
                    group_id = utils.key_to_group_id(key, self._num_groups)
                    if group_id not in groups_to_download:
                        groups_to_download.add(group_id)
                        f = executor.submit(utils.get_remote_group, group_id, self._local_file, self._remote_session)
                        futures[f] = f'_group_{group_id}'

        for f in as_completed(futures):
            key = futures[f]
            error = f.result()
            if error is not None:
                failure_dict[key] = error

    return failure_dict
```

### Step 8: Plumb `num_groups` through `open()` and `__init__` in `main.py`

**`open()` function**: Add `num_groups: int = None` parameter. Pass to constructors.
- **Creating new DB** (`flag='n'`, or `flag='c'` with no existing remote): `num_groups` is required (raise ValueError if None)
- **Opening existing DB** (`flag='r'`, `flag='w'`, or `flag='c'` with existing remote): `num_groups` is read from S3 metadata via `remote_session.num_groups`. User-provided value is ignored if remote already exists. If S3 metadata has no `num_groups` (old-format DB), it's None and per-key fallback is used.

**`EVariableLengthValue.__init__`**: Accept `num_groups`, pass to `_init_common`.

**`_init_common`**: Resolve final `num_groups` value:
```python
if remote_session.num_groups is not None:
    self._num_groups = remote_session.num_groups  # existing DB
else:
    self._num_groups = num_groups  # new DB (validated in open())
```

**`RemoteConnGroup.__init__`**: Also accept and pass `num_groups`.

**Branching on `self._num_groups`**: Throughout `_load_item`, `load_items`, and `update_remote`, check `if num_groups is not None` to decide between grouped vs per-key code paths. Old databases (num_groups=None) use existing per-key logic unchanged.

### Step 9: Modify `remote.py` to store/read `num_groups`

**`S3SessionReader._load_db_metadata()`**: Read `num_groups` from S3 object metadata:
```python
self.num_groups = int(meta['num_groups']) if 'num_groups' in meta else None
```

**`S3SessionReader.get_user_metadata()`**: Change to read from `_metadata` key instead of the raw metadata key string:
```python
resp_obj = self.get_object('_metadata')
```

**`update_remote()` in `utils.py`**: Include `'num_groups': str(num_groups)` in the metadata dict when uploading the main db object via `put_db_object`.

### Step 10: Modify `__delitem__` in `main.py`

Current behavior is fine — it tracks deleted keys in `self._deletes` and removes from remote_index. During push, `update_remote` will compute group_ids from the deletes set and re-pack those groups.

No change needed here.

### Step 11: Modify `delete_objects()` in `remote.py`

Current: lists S3 objects by prefix, filters by key names, deletes matching objects.

With grouping, individual key objects don't exist. The `delete_objects` method is used after push to delete per-key S3 objects. Since groups are re-packed during push (excluding deleted keys), there's no need to delete individual objects. However, empty groups (all keys deleted) should be cleaned up.

Change `update_remote` to handle this: after re-packing, if a group is empty, delete the group object. Remove the `delete_objects(deletes)` call from `update_remote` since deletions are handled by group re-packing.

### Step 12: Modify `delete_remote()` in `remote.py`

This method deletes the entire remote database. The current implementation lists all objects with prefix `{db_key}` and deletes them. This works unchanged — group objects at `{db_key}/0`, `{db_key}/1`, etc. all share the same prefix. No changes needed.

### Step 13: Modify `copy_remote()` in `remote.py`

This copies all S3 objects from one remote to another. Current implementation lists objects by prefix and copies them. This works unchanged — it copies whatever objects exist under the prefix, whether they're individual keys or groups. No changes needed.

### Step 14: Update `create_changelog()` in `utils.py`

No changes needed. The changelog is still a per-key comparison of local vs remote timestamps. The grouping only affects how uploads/downloads are organized, not how changes are detected.

### Step 15: Update tests

**`test_ebooklet.py`**: Update test setup to pass `num_groups` to `ebooklet.open()` calls. Since this replaces per-key storage entirely, all existing tests need the parameter. Use a reasonable value like `num_groups=10` for the test data set (28 keys).

**`test_utils.py`**: Add unit tests for:
- `key_to_group_id` — deterministic, same key always maps to same group
- `pack_group` / `unpack_group` — round-trip test
- Edge cases: empty group, single entry group

---

## Edge Cases and Risks

1. **Empty groups after deletion**: When all keys in a group are deleted, the group S3 object should be deleted. Handle in `upload_group` — if entries list is empty, delete the object instead of uploading an empty one.

2. **Metadata key separation**: Must ensure the metadata key is never passed to `key_to_group_id`. Guard in `upload_group` and `_load_item`.

3. **Thread safety of local_file.set()**: `get_remote_group` calls `local_file.set()` for multiple keys from a single group download. The existing thread lock on writes should handle this, but verify that concurrent group downloads don't conflict when writing to the same local booklet.

4. **num_groups validation**: Must be > 0. Should be validated in `open()` or `_init_common`.

5. **Existing databases without num_groups**: When `_load_db_metadata` finds no `num_groups` in metadata, it means an old-format database. Since we're only supporting new databases, this is fine — `num_groups` will be None for old databases, and they'll work as before. The code detects `num_groups is None` from S3 metadata and falls back to per-key behavior.

6. **Force push**: The `force_push` flag should still work — it forces re-upload of the main db object and remote_index even if no per-key changes occurred.

---

## Verification

1. Run existing tests with `num_groups` added to all `open()` calls: `uv run test`
2. Verify push creates group objects (not individual key objects) on S3
3. Verify reading from a fresh local file (remote-only data) correctly downloads groups and caches all values
4. Verify deletions result in re-packed groups without the deleted keys
5. Verify metadata is stored/retrieved via `_metadata` S3 object (not grouped)
6. Verify `copy_remote` and `delete_remote` still work with group objects
