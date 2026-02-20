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

############################################
### Group functions


def key_to_group_id(key: str, num_groups: int) -> int:
    digest = hashlib.blake2b(key.encode(), digest_size=4).digest()
    return int.from_bytes(digest, 'big') % num_groups


def pack_group(entries: list[tuple[str, int, bytes]]) -> bytes:
    buf = struct.pack('>I', len(entries))
    for key, timestamp, value in entries:
        key_bytes = key.encode()
        buf += struct.pack('>H', len(key_bytes))
        buf += key_bytes
        buf += int_to_bytes(timestamp, 7)
        buf += struct.pack('>I', len(value))
        buf += value
    return buf


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

    if (not remote_index_path.exists() or overwrite_remote_index) and (flag != 'n'):
        if remote_session.uuid:
            index0 = remote_session.get_object()
            if index0.status == 200:
                with portalocker.Lock(remote_index_path, 'wb', timeout=120) as f:
                    f.write(index0.data)
            elif index0.status != 404:
                raise urllib3.exceptions.HTTPError(index0.error)

    return remote_index_path


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
        pass
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
        packed = pack_group(entries)
        resp = remote_session.put_object(str(group_id), packed)
        if resp.status // 100 != 2:
            return resp.error
    else:
        resp = remote_session.delete_object(str(group_id))

    return None


def get_remote_group(group_id, local_file, remote_session):
    resp = remote_session.get_object(str(group_id))
    if resp.status == 200:
        entries = unpack_group(resp.data)
        for key, timestamp, value in entries:
            local_file.set(key, value, timestamp, encode_value=False)
    elif resp.status == 404:
        pass
    else:
        return resp.error

    return None


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
                remote_bytes_us = remote_index.get(key)
                if remote_bytes_us:
                    remote_int_us = bytes_to_int(remote_bytes_us)
                    if local_int_us > remote_int_us:
                        local_bytes_us = int_to_bytes(local_int_us, 7)
                        f[key] = local_bytes_us + remote_bytes_us
                else:
                    local_bytes_us = int_to_bytes(local_int_us, 7)
                    f[key] = local_bytes_us + int_to_bytes(0, 7)

            # Metadata
            key = booklet.utils.metadata_key_bytes.decode()
            local_int_us = local_file.get_timestamp(key)
            if local_int_us:
                remote_bytes_us = remote_index.get(key)
                if remote_bytes_us:
                    remote_int_us = bytes_to_int(remote_bytes_us)
                    local_int_us = local_file.get_timestamp(key)
                    if local_int_us > remote_int_us:
                        local_bytes_us = int_to_bytes(local_int_us, 7)
                        f[key] = local_bytes_us + remote_bytes_us
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

            ## Build full key lists per affected group from local_file
            group_keys = {gid: [] for gid in affected_group_ids}
            for key in local_file.keys():
                gid = key_to_group_id(key, num_groups)
                if gid in group_keys:
                    group_keys[gid].append(key)

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
                    if run_result is None:
                        if tag == 'metadata':
                            remote_index[metadata_key_str] = cl[metadata_key_str][:7]
                        else:
                            ## Update remote_index for all keys in this group
                            gid = identifier
                            for key in group_keys[gid]:
                                cl_val = cl.get(key)
                                if cl_val:
                                    remote_index[key] = cl_val[:7]
                                else:
                                    ts = local_file.get_timestamp(key)
                                    if ts:
                                        remote_index[key] = int_to_bytes(ts, 7)
                        updated = True
                    else:
                        failures[identifier] = run_result

            ## Deletions are handled by group re-packing (deleted keys excluded)
            ## Remove deleted keys from remote_index
            for key in deletes:
                if key in remote_index:
                    del remote_index[key]
            deletes.clear()

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
                        remote_index[key] = cl[key][:7]
                        updated = True
                    else:
                        failures[key] = run_result

    if failures:
        logger.warning(f"There were {len(failures)} items that failed to upload. Please run this again.")

    ## Upload the remote_index file
    remote_index.sync()

    if updated or force_push or (deletes and num_groups is None):
        time_int_us = booklet.utils.make_timestamp_int()

        ## Get main file init bytes
        local_file._set_file_timestamp(time_int_us)
        local_file._file.seek(0)
        local_init_bytes = bytearray(local_file._file.read(200))
        if local_init_bytes[:16] != booklet.utils.uuid_variable_blt:
            raise ValueError(local_init_bytes)

        n_keys_pos = booklet.utils.n_keys_pos
        local_init_bytes[n_keys_pos:n_keys_pos+4] = b'\x00\x00\x00\x00'

        remote_index._file.seek(0)

        metadata = {
            'timestamp': str(time_int_us),
            'uuid': local_file.uuid.hex,
            'type': ebooklet_type,
            'init_bytes': base64.urlsafe_b64encode(local_init_bytes).decode(),
        }
        if num_groups is not None:
            metadata['num_groups'] = str(num_groups)

        resp = remote_session.put_db_object(remote_index._file.read(), metadata=metadata)

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




















































