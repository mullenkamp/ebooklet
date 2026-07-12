#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import logging
import hashlib
import struct
import booklet
import urllib3
from datetime import datetime, timezone
import base64
import portalocker
from concurrent.futures import ThreadPoolExecutor, as_completed
import orjson

logger = logging.getLogger(__name__)

############################################
### Parameters

int_to_bytes = booklet.utils.int_to_bytes
bytes_to_int = booklet.utils.bytes_to_int
metadata_key_str = booklet.utils.metadata_key_bytes.decode()

## The remote storage format version this ebooklet reads and writes. Stamped
## into the db object's S3 metadata on every push; remotes whose stamp exceeds
## it are refused with UnsupportedFormatError (absence of the stamp = 1).
SUPPORTED_FORMAT_VERSION = 1


class UnsupportedFormatError(ValueError):
    """
    The remote database's storage format_version is newer than this ebooklet
    version supports - a compatibility fault, deliberately distinct from
    RemoteIntegrityError (which means the store contradicts its own index).
    Fix: upgrade ebooklet.
    """

############################################
### Group functions


def is_prime_small(n: int) -> bool:
    """Trial-division primality test suitable for small n."""
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def next_prime(n: int) -> int:
    """Return the smallest prime >= n."""
    if n <= 2:
        return 2
    candidate = n if n % 2 != 0 else n + 1
    while not is_prime_small(candidate):
        candidate += 2
    return candidate


def key_to_group_id(key: str, num_groups: int) -> int:
    digest = hashlib.blake2b(key.encode(), digest_size=4).digest()
    return int.from_bytes(digest, 'big') % num_groups


def pack_group(entries: list[tuple[str, int, bytes]]) -> tuple[bytes, dict[str, tuple[int, int]]]:
    buf = struct.pack('>I', len(entries))
    offsets = {}
    pos = 4
    for key, timestamp, value in entries:
        key_bytes = key.encode()
        buf += struct.pack('>H', len(key_bytes))
        pos += 2
        buf += key_bytes
        pos += len(key_bytes)
        buf += int_to_bytes(timestamp, 7)
        pos += 7
        buf += struct.pack('>I', len(value))
        pos += 4
        offsets[key] = (pos, len(value))
        buf += value
        pos += len(value)
    return buf, offsets


def unpack_group(data: bytes) -> list[tuple[str, int, bytes]]:
    offset = 0
    num_entries = struct.unpack_from('>I', data, offset)[0]
    offset += 4
    entries = []
    for _ in range(num_entries):
        key_len = struct.unpack_from('>H', data, offset)[0]
        offset += 2
        key = data[offset:offset + key_len].decode()
        offset += key_len
        timestamp = bytes_to_int(data[offset:offset + 7])
        offset += 7
        value_len = struct.unpack_from('>I', data, offset)[0]
        offset += 4
        value = data[offset:offset + value_len]
        offset += value_len
        entries.append((key, timestamp, value))
    return entries


############################################
### Functions


def s3session_finalizer(session):
    """
    The finalizer function for S3Remote instances.
    """
    session._session.clear()


def ebooklet_finalizer(local_file, remote_index, remote_session, lock):
    """
    The finalizer function for book instances.
    """
    local_file.close()
    remote_index.close()
    remote_session.close()
    if lock is not None:
        lock.release()


def open_remote_conn(remote_conn, flag, local_file_exists):
    """

    """
    remote_session = remote_conn.open(flag)

    if flag == 'r' and (remote_session.uuid is None) and not local_file_exists:
        raise ValueError('No file was found in the remote, but the local file was open for read without creating a new file.')

    ebooklet_type = remote_session.type

    return remote_session, ebooklet_type


def check_local_remote_sync(local_file, remote_session, flag):
    """

    """
    overwrite_remote_index = False

    remote_uuid = remote_session.uuid
    if remote_uuid and flag != 'n':
        local_uuid = local_file.uuid

        if remote_uuid != local_uuid:
            raise ValueError('The local file has a different UUID than the remote. Use a different local file path or delete the existing one.')

        ## Check timestamp to determine if the local remote_index needs to be updated
        if (remote_session.timestamp > local_file._file_timestamp):
            overwrite_remote_index = True

    return overwrite_remote_index


def init_local_file(local_file_path, flag, remote_session, value_serializer, n_buckets, buffer_size):
    """

    """
    remote_uuid = remote_session.uuid

    if local_file_path.exists():

        if flag == 'n':
            if remote_uuid:
                local_file = booklet.open(local_file_path, flag='n', init_bytes=remote_session._init_bytes)
                local_file._n_keys = 0
            else:
                local_file = booklet.open(local_file_path, flag='n', key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)

            overwrite_remote_index = True
        else:
            ## The local file will need to always be open for write since data will be loaded from the remote regardless if the user has only opened it for read-only
            local_file = booklet.open(local_file_path, 'w')

            overwrite_remote_index = check_local_remote_sync(local_file, remote_session, flag)

    else:
        if remote_uuid:
            ## Init with the remote bytes - keeps the remote uuid and timestamp
            local_file = booklet.open(local_file_path, flag='n', init_bytes=remote_session._init_bytes)
            local_file._n_keys = 0

            overwrite_remote_index = True
        else:
            ## Else create a new file
            local_file = booklet.open(local_file_path, flag='n', key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)

            overwrite_remote_index = True

    return local_file, overwrite_remote_index


def get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag):
    """

    """
    remote_index_path = local_file_path.parent.joinpath(local_file_path.name + '.remote_index')

    if not remote_index_path.exists() or overwrite_remote_index:
        fetch_remote_index(remote_index_path, remote_session)

    return remote_index_path


def fetch_remote_index(dest_path, remote_session):
    """
    Download the remote db object (the remote index) to dest_path. Returns True on
    success, False when there is nothing to fetch (no remote uuid, or a 404);
    raises on other HTTP errors.
    """
    if not remote_session.uuid:
        return False

    index0 = remote_session.get_object()
    if index0.status == 200:
        with portalocker.Lock(dest_path, 'wb', timeout=120) as f:
            f.write(index0.data)
        return True
    elif index0.status == 404:
        return False
    else:
        raise urllib3.exceptions.HTTPError(index0.error)


def open_remote_index(remote_index_path, flag, n_buckets, buffer_size):
    """
    Open the local remote-index booklet with the standard flag mapping (read-only
    for 'r' sessions, writable otherwise, fresh file when none exists).

    Index entry layout (fixed value_len=15): timestamp(7) + offset(4) + length(4).
    Per-key mode: offset and length are always 0. Grouped mode: offset/length
    locate the member value inside its group object; length is the value's byte
    length and MAY be 0 (an empty value) - it is NOT a mode discriminator. Future
    layouts change the fixed value_len (the layout discriminator), gated by the
    db object's format_version metadata.
    """
    if remote_index_path.exists():
        if flag == 'r':
            return booklet.FixedLengthValue(remote_index_path, 'r')
        else:
            return booklet.FixedLengthValue(remote_index_path, 'w')
    else:
        return booklet.FixedLengthValue(remote_index_path, 'n', key_serializer='str', value_len=15, n_buckets=n_buckets, buffer_size=buffer_size)


class MissingRemoteObject:
    """
    Truthy failure marker: the remote index claims key(s) whose backing S3
    object returned 404 - or, for grouped members, whose group object exists
    (200) but no longer contains them (object_exists=True). Every value fetch
    is gated on the index claiming the key (check_local_vs_remote), so either
    state means the store contradicts its own index - a legitimately-deleted
    key seen through a stale reader index, or a real integrity fault. main.py's
    re-check protocol (_resolve_missing) disambiguates the two by re-pulling
    the index.

    Deliberately truthy (no __bool__/__len__) and non-None so it passes both
    failure gates: the point-read `if failure:` and load_items' `is not None`.
    """
    __slots__ = ('s3_key', 'keys', 'object_exists')

    def __init__(self, s3_key: str, keys: list, object_exists: bool = False):
        self.s3_key = s3_key   # the S3 object key (group id str, per-key key, or '_metadata')
        self.keys = keys       # the ebooklet keys the index claims live in that object
        self.object_exists = object_exists  # True: object present but lacks these members

    def __repr__(self):
        if self.object_exists:
            return (f"remote group object '{self.s3_key}' no longer contains member key(s) "
                    f"{self.keys} but the remote index still claims them (integrity failure); "
                    f"delete these keys and re-push, or restore the members")
        return (f"remote object '{self.s3_key}' is missing but the remote index still claims "
                f"key(s) {self.keys} (integrity failure); delete these keys and re-push, "
                f"or restore the object")


def get_remote_value(local_file, key, remote_session):
    """

    """
    s3_key = '_metadata' if key == metadata_key_str else key
    resp = remote_session.get_object(s3_key)

    if resp.status == 200:
        timestamp = int(resp.metadata['timestamp'])

        if key == metadata_key_str:
            local_file.set_metadata(orjson.loads(resp.data), timestamp=timestamp)
        else:
            local_file.set(key, resp.data, timestamp, encode_value=False)
    elif resp.status == 404:
        ## This fetch only runs when the remote index claims the key, so a 404
        ## is never legitimate absence - surface it for the re-check protocol.
        return MissingRemoteObject(s3_key, [key])
    else:
        return resp.error

    return None


def upload_group(group_id, local_file, remote_session, keys_in_group):
    entries = []
    for key in keys_in_group:
        result = local_file.get_timestamp(key, include_value=True, decode_value=False)
        if result is None:
            continue
        time_int_us, valb = result
        entries.append((key, time_int_us, valb))

    if entries:
        packed, offsets = pack_group(entries)
        resp = remote_session.put_object(str(group_id), packed)
        if resp.status // 100 != 2:
            return resp.error, None
        return None, offsets
    else:
        ## A failed delete leaves the index still referencing the still-existing
        ## group object - a consistent, retryable state. Reporting it as a
        ## failure keeps the group's deletes retained so the next push retries.
        error = remote_session.delete_object(str(group_id))
        if error is not None:
            return error, None
        return None, {}


## Per-entry header layout inside a packed group object (see pack_group):
## [key_len: >H][key][timestamp: 7 bytes][value_len: >I][value]
group_entry_fixed_overhead = 2 + 7 + 4


def recover_group_members(group_id, key_infos, local_file, remote_session, report_missing_members=True):
    """
    Fallback for when ranged reads into a group object cannot be verified against the
    remote_index offsets (a corrupted or shifted group object). Downloads the whole
    self-describing group object and recovers whichever requested members it actually
    contains, trusting the object's own embedded keys/timestamps rather than the index.

    Members the index claims but the (200) object does not contain are reported as a
    MissingRemoteObject marker so read paths route them through the re-check protocol
    (_resolve_missing) - previously `key in db` said True while `db[key]` silently
    raised KeyError. The push path passes report_missing_members=False: there the
    absent-member state is deliberately self-healed (the lost-keys drop in
    update_remote), which is also the recovery mechanism for the read-side error.
    """
    resp = remote_session.get_object(str(group_id))
    if resp.status == 200:
        entries = {key: (timestamp, value) for key, timestamp, value in unpack_group(resp.data)}
        missing = []
        for key, offset, length, timestamp_int in key_infos:
            entry = entries.get(key)
            if entry is not None:
                timestamp, value = entry
                local_file.set(key, value, timestamp, encode_value=False)
            else:
                missing.append(key)
        if report_missing_members and missing:
            ## Recovered members above stay materialized - they are valid remote
            ## data. The marker carries only the still-missing subset. An empty
            ## missing list must return None (plain success), never a truthy
            ## empty marker that would trigger a needless index re-pull.
            return MissingRemoteObject(str(group_id), missing, object_exists=True)
    elif resp.status == 404:
        ## key_infos comes from remote-index entries, so the index claims these
        ## members - a missing group object is never legitimate absence here.
        return MissingRemoteObject(str(group_id), [k for k, _o, _l, _t in key_infos])
    else:
        return resp.error
    return None


def get_remote_group_values(group_id, key_infos, local_file, remote_session, report_missing_members=True):
    """
    key_infos: list of (key, offset, length, timestamp_int)

    Each member read is verified against the group object's embedded entry header
    (the key itself and the value length precede every value in the packed layout)
    before being trusted - a stale index offset would otherwise silently deliver
    another entry's bytes. On any verification failure the whole (self-describing)
    group object is downloaded and parsed instead (recover_group_members;
    report_missing_members is passed through - see its docstring).
    """
    sorted_infos = sorted(key_infos, key=lambda x: x[1])

    ## Start the ranged read at the first member's entry header so every requested
    ## member's header is inside the fetched range.
    first_key, first_offset, _, _ = sorted_infos[0]
    range_start = first_offset - group_entry_fixed_overhead - len(first_key.encode())
    last = sorted_infos[-1]
    range_end = last[1] + last[2] - 1

    if range_start < 0:
        logger.warning(f"Group {group_id}: stored index offsets are malformed; recovering members from the full group object.")
        return recover_group_members(group_id, key_infos, local_file, remote_session, report_missing_members)

    resp = remote_session.get_object(str(group_id), range_start=range_start, range_end=range_end)
    if resp.status in (200, 206):
        data = resp.data
        verified = []
        for key, offset, length, timestamp_int in sorted_infos:
            key_bytes = key.encode()
            header_start = offset - group_entry_fixed_overhead - len(key_bytes) - range_start
            rel_offset = offset - range_start

            ok = header_start >= 0 and rel_offset + length <= len(data)
            if ok:
                key_len = struct.unpack_from('>H', data, header_start)[0]
                ok = (key_len == len(key_bytes)
                      and data[header_start + 2:header_start + 2 + key_len] == key_bytes
                      and struct.unpack_from('>I', data, header_start + 2 + key_len + 7)[0] == length)
            if not ok:
                verified = None
                break
            verified.append((key, data[rel_offset:rel_offset + length], timestamp_int))

        if verified is None:
            logger.warning(f"Group {group_id}: stored index offsets do not match the group object layout (corrupted or shifted object); recovering members from the full group object.")
            return recover_group_members(group_id, key_infos, local_file, remote_session, report_missing_members)

        for key, value, timestamp_int in verified:
            local_file.set(key, value, timestamp_int, encode_value=False)
    elif resp.status == 404:
        ## sorted_infos comes from remote-index entries, so the index claims
        ## these members - a missing group object is never legitimate absence.
        return MissingRemoteObject(str(group_id), [k for k, _o, _l, _t in key_infos])
    elif resp.status == 416:
        ## Requested range extends past the object's end - the object shrank
        ## relative to the index. Recover whatever actually exists.
        logger.warning(f"Group {group_id}: stored index offsets extend past the group object; recovering members from the full group object.")
        return recover_group_members(group_id, key_infos, local_file, remote_session, report_missing_members)
    else:
        return resp.error
    return None


def get_remote_group_value(group_id, key, offset, length, timestamp_int, local_file, remote_session):
    return get_remote_group_values(group_id, [(key, offset, length, timestamp_int)], local_file, remote_session)


def check_local_vs_remote(local_file, remote_time_bytes, key):
    """

    """
    if remote_time_bytes is None:
        return None

    remote_time_int = bytes_to_int(remote_time_bytes)
    local_time_int = local_file.get_timestamp(key)

    if local_time_int:
        if remote_time_int <= local_time_int:
            return False

    return True


#################################################
### local/remote changelog


def create_changelog(local_file_path, local_file, remote_index, remote_session):
    """
    Only check and save by the microsecond timestamp. Might need to add in the md5 hash if this is not sufficient.
    """
    changelog_path = local_file_path.parent.joinpath(local_file_path.name + '.changelog')
    if remote_index is not None:
        n_buckets = remote_index._n_buckets
    else:
        n_buckets = local_file._n_buckets

    with booklet.FixedLengthValue(changelog_path, 'n', key_serializer='str', value_len=14, n_buckets=n_buckets) as f:
        if remote_session.uuid and remote_index is not None:
            for key, local_int_us in local_file.timestamps():
                remote_val = remote_index.get(key)
                if remote_val:
                    remote_ts_bytes = remote_val[:7]
                    remote_int_us = bytes_to_int(remote_ts_bytes)
                    if local_int_us > remote_int_us:
                        local_bytes_us = int_to_bytes(local_int_us, 7)
                        f[key] = local_bytes_us + remote_ts_bytes
                else:
                    local_bytes_us = int_to_bytes(local_int_us, 7)
                    f[key] = local_bytes_us + int_to_bytes(0, 7)

            # Metadata
            key = booklet.utils.metadata_key_bytes.decode()
            local_int_us = local_file.get_timestamp(key)
            if local_int_us:
                remote_val = remote_index.get(key)
                if remote_val:
                    remote_ts_bytes = remote_val[:7]
                    remote_int_us = bytes_to_int(remote_ts_bytes)
                    local_int_us = local_file.get_timestamp(key)
                    if local_int_us > remote_int_us:
                        local_bytes_us = int_to_bytes(local_int_us, 7)
                        f[key] = local_bytes_us + remote_ts_bytes
                else:
                    local_bytes_us = int_to_bytes(local_int_us, 7)
                    f[key] = local_bytes_us + int_to_bytes(0, 7)
        else:
            for key, local_int_us in local_file.timestamps():
                local_bytes_us = int_to_bytes(local_int_us, 7)
                f[key] = local_bytes_us + int_to_bytes(0, 7)

            # Metadata
            key = booklet.utils.metadata_key_bytes.decode()
            local_int_us = local_file.get_timestamp(key)
            if local_int_us:
                local_bytes_us = int_to_bytes(local_int_us, 7)
                f[key] = local_bytes_us + int_to_bytes(0, 7)

    return changelog_path


def view_changelog(changelog_path):
    """

    """
    with booklet.FixedLengthValue(changelog_path) as f:
        for key, val in f.items():
            local_bytes_us = val[:7]
            remote_bytes_us = val[7:]
            local_int_us = bytes_to_int(local_bytes_us)
            remote_int_us = bytes_to_int(remote_bytes_us)
            if remote_int_us == 0:
                remote_ts = None
            else:
                remote_ts = datetime.fromtimestamp(remote_int_us*0.000001, tz=timezone.utc)

            dict1 = {
                'key': key,
                'remote_timestamp': remote_ts,
                'local_timestamp': datetime.fromtimestamp(local_int_us*0.000001, tz=timezone.utc)
                }

            yield dict1


##############################################
### Update remote


def upload_value(key, local_file, remote_session):
    """

    """
    time_int_us, valb = local_file.get_timestamp(key, include_value=True, decode_value=False)
    s3_key = '_metadata' if key == metadata_key_str else key
    resp = remote_session.put_object(s3_key, valb, {'timestamp': str(time_int_us)})
    if resp.status // 100 != 2:
        return resp.error
    else:
        return None


def update_remote(local_file, remote_index, changelog_path, remote_session, force_push, deletes, flag, ebooklet_type, num_groups=None):
    """

    """
    ## If file was open for replacement (n), then delete everything in the remote
    if flag == 'n':
        remote_session.delete_remote()

    ## Upload data and update the remote_index file
    updated = False
    failures = {}

    with booklet.FixedLengthValue(changelog_path) as cl:
        if num_groups is not None:
            ## Grouped upload path
            affected_group_ids = set()
            metadata_in_changelog = False

            for key in cl:
                if key == metadata_key_str:
                    metadata_in_changelog = True
                else:
                    affected_group_ids.add(key_to_group_id(key, num_groups))

            ## Also include groups affected by deletes
            for key in deletes:
                affected_group_ids.add(key_to_group_id(key, num_groups))

            ## Build FULL key lists per affected group: the union of locally-present
            ## keys and remote-index keys. A group object is completely replaced on
            ## upload, so every current member must be packed - not just the keys
            ## that happen to be materialized in the local file.
            group_key_sets = {gid: set() for gid in affected_group_ids}
            for key in local_file.keys():
                if key == metadata_key_str:
                    continue
                gid = key_to_group_id(key, num_groups)
                if gid in group_key_sets:
                    group_key_sets[gid].add(key)

            ## Members whose local value is missing or older than the remote must be
            ## pulled down before their group can be repacked (one ranged read per group).
            groups_to_download = {}
            pull_count = 0
            pull_bytes = 0
            ## NOTE: iterate items() in a single pass - one scan instead of a
            ## per-key chain lookup for every get(). (Before booklet 0.12.6 this
            ## was also mandatory: iterators held the thread lock across yields,
            ## so get() during iteration self-deadlocked. That constraint is
            ## gone, but the single pass remains the right access pattern.)
            for key, remote_val in remote_index.items():
                if key == metadata_key_str or key in deletes:
                    continue
                gid = key_to_group_id(key, num_groups)
                if gid not in group_key_sets:
                    continue
                group_key_sets[gid].add(key)
                if remote_val and check_local_vs_remote(local_file, remote_val[:7], key):
                    if local_file.get_timestamp(key) is not None:
                        logger.warning(f"Push is replacing the locally-stored value of '{key}' with the newer remote value before repacking its group - any unpushed local modification to it is discarded (its local timestamp is older than the remote's).")
                    offset = bytes_to_int(remote_val[7:11])
                    length = bytes_to_int(remote_val[11:15])
                    timestamp_int = bytes_to_int(remote_val[:7])
                    if length > 0:
                        groups_to_download.setdefault(gid, []).append((key, offset, length, timestamp_int))
                        pull_count += 1
                        pull_bytes += length
                    else:
                        ## A grouped member with length 0 is an EMPTY value, not
                        ## a per-key entry (those never reach this loop). There
                        ## is nothing to download - the value IS b''. Materialize
                        ## it directly, otherwise a locally-absent empty member
                        ## would fall into the lost-keys drop below and be
                        ## silently deleted by the repack.
                        local_file.set(key, b'', timestamp_int, encode_value=False)

            if groups_to_download:
                logger.info(f"Pulling {pull_count} group member value(s) (~{pull_bytes} bytes) from {len(groups_to_download)} group(s) so the groups can be repacked in full.")
                with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                    ## report_missing_members=False: on the push path an absent
                    ## member is deliberately self-healed by the lost-keys drop
                    ## below - a loud marker here would make the push fail
                    ## permanently instead of repairing the group.
                    pull_futures = {executor.submit(get_remote_group_values, gid, key_infos, local_file, remote_session, False): gid for gid, key_infos in groups_to_download.items()}
                    for future in as_completed(pull_futures):
                        gid = pull_futures[future]
                        error = future.result()
                        if error is not None:
                            ## Never upload a partially-materialized group - leave the
                            ## old remote group object intact and report the failure.
                            failures[gid] = error
                            group_key_sets.pop(gid, None)

            ## Any member that still has no local value lost its remote bytes at some
            ## earlier point (e.g. a partial push from a version < 0.8.4). Repack the
            ## group without it and remove the dangling index entry.
            group_keys = {}
            for gid, keys_set in group_key_sets.items():
                keys_in_group = []
                lost_keys = []
                for key in keys_set:
                    if local_file.get_timestamp(key) is None:
                        lost_keys.append(key)
                    else:
                        keys_in_group.append(key)
                if lost_keys:
                    logger.warning(f"Group {gid}: dropping {len(lost_keys)} key(s) whose remote bytes no longer exist (likely a partial push from a version < 0.8.4): {sorted(lost_keys)}")
                    for key in lost_keys:
                        if key in remote_index:
                            del remote_index[key]
                    updated = True
                group_keys[gid] = keys_in_group

            with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                futures = {}

                ## Upload metadata individually if changed
                if metadata_in_changelog:
                    f = executor.submit(upload_value, metadata_key_str, local_file, remote_session)
                    futures[f] = ('metadata', metadata_key_str)

                ## Upload affected groups
                for gid, keys_in_group in group_keys.items():
                    f = executor.submit(upload_group, gid, local_file, remote_session, keys_in_group)
                    futures[f] = ('group', gid)

                for future in as_completed(futures):
                    tag, identifier = futures[future]
                    run_result = future.result()
                    if tag == 'metadata':
                        if run_result is None:
                            remote_index[metadata_key_str] = cl[metadata_key_str][:7] + b'\x00' * 8
                            updated = True
                        else:
                            failures[identifier] = run_result
                    else:
                        error, offsets = run_result
                        if error is None:
                            gid = identifier
                            for key in group_keys[gid]:
                                if key in offsets:
                                    ts = local_file.get_timestamp(key)
                                    if ts:
                                        offset, length = offsets[key]
                                        remote_index[key] = int_to_bytes(ts, 7) + int_to_bytes(offset, 4) + int_to_bytes(length, 4)
                            updated = True
                        else:
                            failures[identifier] = error

            ## Deletions are handled by group re-packing (deleted keys excluded).
            ## Only clear deletes whose group was successfully repacked and uploaded:
            ## keeping a failed group's deletes in the set marks that group as
            ## affected again on the next push, so its object is eventually repacked
            ## (otherwise the deleted keys' bytes would stay orphaned in S3 forever).
            failed_gids = {gid for gid in failures if isinstance(gid, int)}
            retained_deletes = set()
            for key in deletes:
                if key_to_group_id(key, num_groups) in failed_gids:
                    retained_deletes.add(key)
                    continue
                if key in remote_index:
                    del remote_index[key]
            deletes.clear()
            deletes.update(retained_deletes)

        else:
            ## Per-key upload path (legacy)
            with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                futures = {}
                for key in cl:
                    f = executor.submit(upload_value, key, local_file, remote_session)
                    futures[f] = key

                for future in as_completed(futures):
                    key = futures[future]
                    run_result = future.result()
                    if run_result is None:
                        remote_index[key] = cl[key][:7] + b'\x00' * 8
                        updated = True
                    else:
                        failures[key] = run_result

    if failures:
        logger.warning(f"There were {len(failures)} items that failed to upload. Please run this again.")

    ## Upload the remote_index file
    remote_index.sync()

    ## Also write the db object when the remote does not exist yet (uuid is None) or
    ## the remote was wiped (flag 'n'), so that creating/replacing an EMPTY database
    ## still materializes the remote instead of being a silent no-op.
    if updated or force_push or (deletes and num_groups is None) or flag == 'n' or remote_session.uuid is None:
        time_int_us = booklet.utils.make_timestamp_int()

        ## Get main file init bytes. Direct _file access moves the shared file
        ## position, so hold the owning booklet's thread lock (booklet's own
        ## contract since 0.12.6: every position-mover locks).
        local_file._set_file_timestamp(time_int_us)
        with local_file._thread_lock:
            local_file._file.seek(0)
            local_init_bytes = bytearray(local_file._file.read(200))
        if local_init_bytes[:16] != booklet.utils.uuid_variable_blt:
            raise ValueError(local_init_bytes)

        n_keys_pos = booklet.utils.n_keys_pos
        local_init_bytes[n_keys_pos:n_keys_pos+4] = b'\x00\x00\x00\x00'

        with remote_index._thread_lock:
            remote_index._file.seek(0)
            remote_index_bytes = remote_index._file.read()

        metadata = {
            'timestamp': str(time_int_us),
            'uuid': local_file.uuid.hex,
            'type': ebooklet_type,
            'init_bytes': base64.urlsafe_b64encode(local_init_bytes).decode(),
            'format_version': str(SUPPORTED_FORMAT_VERSION),
        }
        if num_groups is not None:
            metadata['num_groups'] = str(num_groups)

        resp = remote_session.put_db_object(remote_index_bytes, metadata=metadata)

        if resp.status // 100 != 2:
            raise urllib3.exceptions.HTTPError("The db object failed to upload. You need to rerun the push with force_push=True or the remote will be corrupted.")

        ## remove deletes in remote (only for legacy per-key mode)
        if deletes and num_groups is None:
            remote_session.delete_objects(deletes)
            deletes.clear()

        updated = True

    if failures:
        return failures
    else:
        return updated


def indirect_copy_remote(source_session, target_session, source_key, target_key, source_bucket, dest_bucket):
    """

    """
    source_resp = source_session.get_object(source_key)

    if source_resp.status // 100 != 2:
        return source_resp

    target_resp = target_session.put_object(target_key, source_resp.stream, source_resp.metadata)

    return target_resp




















































