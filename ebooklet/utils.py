#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import io
import booklet
import urllib3
from datetime import datetime, timezone
import base64
import portalocker
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import orjson

############################################
### Parameters

# default_n_buckets = 100003

# blt_files = ('.local_data', '.remote_index')

# local_storage_options = ('write_buffer_size', 'n_bytes_file', 'n_bytes_key', 'n_bytes_value', 'n_buckets')

int_to_bytes = booklet.utils.int_to_bytes
bytes_to_int = booklet.utils.bytes_to_int
metadata_key_str = booklet.utils.metadata_key_bytes.decode()

############################################
### Exception classes


# class BaseError(Exception):
#     def __init__(self, message, objs=[], temp_path=None, *args):
#         self.message = message # without this you may get DeprecationWarning
#         # Special attribute you desire with your Error,
#         # for file in blt_files:
#         #     f = getattr(obj, file)
#         #     if f is not None:
#         #         f.close()
#         for obj in objs:
#             if obj:
#                 obj.close()
#         if temp_path:
#             temp_path.cleanup()
#         # allow users initialize misc. arguments as any other builtin Error
#         super(BaseError, self).__init__(message, *args)


# class S3dbmValueError(BaseError):
#     pass

# class S3dbmTypeError(BaseError):
#     pass

# class S3dbmKeyError(BaseError):
#     pass

# class S3dbmHttpError(BaseError):
#     pass

# class S3dbmSerializeError(BaseError):
#     pass


############################################
### Functions


def fake_finalizer():
    """
    The finalizer function for S3Remote instances.
    """


def s3session_finalizer(session):
    """
    The finalizer function for S3Remote instances.
    """
    session.client.close()
    # if lock is not None:
    #     lock.release()


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
    # if flag in ('r', 'w') and (remote_session.uuid is None) and not local_file_exists:
    #     raise ValueError('No file was found in the remote, but the local file was open for read and write without creating a new file.')
    # elif flag != 'r' and remote_session is None and not local_file_exists:
    #     raise ValueError('If open for write, then an S3Remote object must be passed.')

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
            # if flag == 'r':
            #     local_file = booklet.open(local_file_path, 'r')
            # else:
            #     local_file = booklet.open(local_file_path, 'w')

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
            # remote_index_key = read_conn.db_key + '.remote_index'

            index0 = remote_session.get_object()
            if index0.status == 200:
                with portalocker.Lock(remote_index_path, 'wb', timeout=120) as f:
                    f.write(index0.data)
                    # shutil.copyfileobj(index0.stream, f)
            elif index0.status != 404:
                raise urllib3.exceptions.HTTPError(index0.error)

    return remote_index_path


def get_remote_value(local_file, key, remote_session):
    """

    """
    resp = remote_session.get_object(key)

    if resp.status == 200:
        timestamp = int(resp.metadata['timestamp'])

        # print(timestamp)
        # print(resp.data)

        # val_bytes = resp.data
        if key == metadata_key_str:
            local_file.set_metadata(orjson.loads(resp.data), timestamp=timestamp)
        else:
            local_file.set(key, resp.data, timestamp, encode_value=False)
    # elif resp.status == 404:
    #     raise KeyError(f'{key} not found in remote.')
    else:
        return resp.error
        # return urllib3.exceptions.HttpError(f'{key} returned the http error {resp.status}.')

    return None


def check_local_vs_remote(local_file, remote_time_bytes, key):
    """

    """
    # remote_time_bytes = remote_index.get(key)

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


def update_remote(local_file, remote_index, changelog_path, remote_session, force_push, deletes, flag, ebooklet_type):
    """

    """
    ## Make sure the files are synced
    # local_file.sync()
    # remote_index.sync()

    ## If file was open for replacement (n), then delete everything in the remote
    if flag == 'n':
        remote_session.delete_remote()

    ## Upload data and update the remote_index file
    # remote_index.reopen('w')

    with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
        futures = {}
        with booklet.FixedLengthValue(changelog_path) as cl:
            for key in cl:
                time_int_us, valb = local_file.get_timestamp(key, include_value=True, decode_value=False)
                f = executor.submit(remote_session.put_object, key, valb, {'timestamp': str(time_int_us)})
                futures[f] = key
    
            ## Check the uploads to see if any fail
            updated = False
            failures = []
            for future in as_completed(futures):
                key = futures[future]
                run_result = future.result()
                if run_result.status // 100 == 2:
                    remote_index[key] = cl[key][:7]
                    updated = True
                else:
                    failures.append(key)

        if failures:
            print(f"There were {len(failures)} items that failed to upload. Please run this again.")

    ## Upload the remote_index file
    remote_index.sync()

    if updated or force_push or deletes:
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

        resp = remote_session.put_db_object(remote_index._file.read(), metadata={'timestamp': str(time_int_us), 'uuid': local_file.uuid.hex, 'type': ebooklet_type, 'init_bytes': base64.urlsafe_b64encode(local_init_bytes).decode()})

        if resp.status // 100 != 2:
            urllib3.exceptions.HTTPError("The db object failed to upload. You need to rerun the push with force_push=True or the remote will be corrupted.")

        ## remove deletes in remote
        if deletes:
            remote_session.delete_objects(deletes)
            deletes.clear()

        updated = True

    if failures:
        return failures
    else:
        return updated


def determine_file_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    size = file_obj.seek(0, io.SEEK_END)
    file_obj.seek(pos)

    return size


def indirect_copy_remote(source_session, target_session, source_key, target_key, source_bucket, dest_bucket):
    """

    """
    source_resp = source_session.get_object(source_key)

    if source_resp.status // 100 != 2:
        return source_resp

    target_resp = target_session.put_object(target_key, source_resp.stream, source_resp.metadata)

    return target_resp
























































