#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import io
# from pydantic import BaseModel, HttpUrl
import pathlib
import copy
# from time import sleep
# import hashlib
# from hashlib import blake2b
import booklet
import orjson
# from s3func import S3Session, HttpSession
import urllib3
import shutil
from datetime import datetime, timezone
import base64
# import zstandard as zstd
# from glob import glob
import portalocker
import concurrent.futures
# from collections.abc import Mapping, MutableMapping
# from __init__ import __version__ as version

# import remote
from . import remote

############################################
### Parameters

# version = '0.1.0'

default_n_buckets = 100003

# blt_files = ('.local_data', '.remote_index')

# local_storage_options = ('write_buffer_size', 'n_bytes_file', 'n_bytes_key', 'n_bytes_value', 'n_buckets')

int_to_bytes = booklet.utils.int_to_bytes
bytes_to_int = booklet.utils.bytes_to_int

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


def s3session_finalizer(session, lock):
    """
    The finalizer function for S3Remote instances.
    """
    session.client.close()
    if lock is not None:
        lock.release()


# def bookcase_finalizer(temp_path, lock):
#     """
#     The finalizer function for bookcase instances.
#     """
#     if temp_path:
#         shutil.rmtree(temp_path, True)
#     if lock:
#         lock.release()


def ebooklet_finalizer(local_file, remote_index, remote_session):
    """
    The finalizer function for book instances.
    """
    local_file.close()
    # if remote_index is not None:
    remote_index.close()
    remote_session.close()
    # if read_conn is not None:
    #     read_conn.close()
    # if write_conn is not None:
    #     write_conn.close()
    # if lock is not None:
    #     lock.release()


# def write_metadata(local_meta_path, meta):
#     """

#     """
#     meta_bytes = orjson.dumps(meta, option=orjson.OPT_SERIALIZE_NUMPY)
#     with io.open(local_meta_path, 'wb') as f:
#         f.write(meta_bytes)


# def get_save_remote_file(local_path, remote_url, remote_db_key, http_session, s3_session, remote_s3_access, remote_http_access):
#     """

#     """
#     if remote_http_access:
#         func = http_session.get_object
#         key = remote_url
#     else:
#         func = s3_session.get_object
#         key = remote_db_key

#     resp = func(key)
#     if resp.status == 200:
#         with open(local_path, 'wb') as f:
#             f.write(resp.data)
#     else:
#         raise urllib3.exceptions.HTTPError(resp.error)


# def get_remote_file(remote_url, remote_db_key, http_session, s3_session, remote_s3_access, remote_http_access):
#     """

#     """
#     if remote_http_access:
#         func = http_session.get_object
#         key = remote_url
#     else:
#         func = s3_session.get_object
#         key = remote_db_key

#     resp = func(key)
#     if resp.status == 200:
#         return resp.data
#     else:
#         raise urllib3.exceptions.HTTPError(resp.error)


def check_parse_conn(remote_conn, flag, object_lock, break_other_locks, lock_timeout, local_file_exists):
    """

    """
    if object_lock and (flag != 'r'):
        remote_session = remote_conn.open(flag, object_lock, break_other_locks, lock_timeout)
    else:
        remote_session = remote_conn.open(flag)

    # if isinstance(remote_conn, str):
    #     if flag != 'r':
    #         raise ValueError('If remote_conn is a url string, then flag must be r.')
    #     remote_conn = remote.S3Connection(db_url=remote_conn)
    #     remote_session = remote_conn.open('r')

    # elif isinstance(remote_conn, remote.S3Connection):
    #     if object_lock and (flag != 'r'):
    #         remote_session = remote_conn.open(flag, object_lock, break_other_locks, lock_timeout)
    #     else:
    #         remote_session = remote_conn.open(flag)

    # else:
    #     raise TypeError('The remote_conn must be either an S3Connection object or a url string.')

    if flag in ('r', 'w') and (remote_session.uuid is None) and not local_file_exists:
        raise ValueError('No file was found in the remote, but the local file was open for read and write without creating a new file.')
    # elif flag != 'r' and remote_session is None and not local_file_exists:
    #     raise ValueError('If open for write, then an S3Remote object must be passed.')

    return remote_session


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
            if flag == 'r':
                local_file = booklet.open(local_file_path, 'r')
            else:
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

            index0 = remote_session.get_db_object()
            if index0.status == 200:
                with portalocker.Lock(remote_index_path, 'wb', timeout=120) as f:
                    f.write(index0.data)
                    # shutil.copyfileobj(index0.data, f)
            elif index0.status != 404:
                raise urllib3.exceptions.HTTPError(index0.error)

    return remote_index_path


# def init_remote_config(bucket, connection_config, remote_url, threads, read_timeout, retries):
#     """

#     """
#     http_session = None
#     s3_session = None
#     remote_s3_access = False
#     remote_http_access = False
#     remote_base_url = None
#     host_url = None

#     if remote_url is not None:
#         url_grp = urllib3.util.parse_url(remote_url)
#         if url_grp.scheme is not None:
#             http_session = HttpSession(threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
#             url_path = pathlib.Path(url_grp.path)
#             remote_base_url = url_path.parent
#             host_url = url_grp.scheme + '://' + url_grp.host
#             remote_http_access = True
#         else:
#             print(f'{remote_url} is not a proper url.')
#     if (bucket is not None) and (connection_config is not None):
#         s3_session = S3Session(connection_config, bucket, threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
#         remote_s3_access = True

#     # if (not remote_s3_access) and (flag != 'r'):
#     #     raise ValueError("If flag != 'r', then the appropriate remote write access parameters must be passed.")

#     return http_session, s3_session, remote_s3_access, remote_http_access, host_url, remote_base_url


# def init_metadata(local_meta_path, remote_keys_path, write, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs):
#     """

#     """
#     meta_in_remote = False
#     # get_remote_keys = False
#     remote_meta = None

#     if remote_s3_access:
#         int_us = make_timestamp()
#     else:
#         int_us = 0

#     new_meta = {
#         'package_version': version,
#         'local_storage_kwargs': local_storage_kwargs,
#         'value_serializer': value_serializer,
#         'last_modified': int_us,
#         'user_metadata': {},
#         'default_book': None
#         }

#     glob_str = str(local_meta_path) + '.*'
#     extra_files = glob(glob_str)

#     if local_meta_path.exists():
#         # if write:
#         #     portalocker.lock(local_meta_path, portalocker.LOCK_EX)
#         # else:
#         #     portalocker.lock(local_meta_path, portalocker.LOCK_SH)
#         with io.open(local_meta_path, 'rb') as f:
#             local_meta = orjson.loads(f.read())
#     elif not write:
#         raise FileExistsError('Bookcase was open for read-only, but nothing exists to open.')
#     else:
#         for file in extra_files:
#             os.remove(file)
#         local_meta = None

#     if remote_http_access or remote_s3_access:
#         if remote_http_access:
#             func = http_session.get_object
#             key = remote_url
#         else:
#             func = s3_session.get_object
#             key = remote_db_key

#         meta0 = func(key)
#         if meta0.status == 200:
#             if meta0.metadata['file_type'] != 'bookcase':
#                 raise TypeError('The remote db file is not a bookcase file.')
#             remote_meta = orjson.loads(meta0.data)
#             meta_in_remote = True

#             ## Remote meta is the only meta needed now
#             meta = remote_meta

#             ## Determine if the local remote index files needs to be removed
#             if local_meta is not None:
#                 remote_ts = remote_meta['last_modified']
#                 local_ts = local_meta['last_modified']
#                 if remote_ts > local_ts:
#                     # get_remote_keys_file(local_meta_path, remote_db_key, remote_url, http_session, s3_session, remote_http_access)

#                     # with open(local_meta_path, 'wb') as f:
#                     #     f.write(meta0.data)

#                     local_books = local_meta['books'].copy()
#                     remote_books = remote_meta['books']

#                     # local_remote_index_ts = {}
#                     # for file in extra_files:
#                     #     if '.remote_index' in file:
#                     #         book_hash = file.split('.')[-2]
#                     #         local_book_ts = books[book_hash]['last_modified']
#                     #         local_remote_index_ts[book_hash] = local_book_ts

#                     for file in extra_files:
#                         if '.remote_index' in file:
#                             book_hash = file.split('.')[-2]
#                             local_book_ts = local_books[book_hash]['last_modified']
#                             remote_book = remote_books.get(book_hash)
#                             if remote_book:
#                                 remote_book_ts = remote_book['last_modified']
#                                 if remote_book_ts > local_book_ts:
#                                     os.remove(file)

#                     ## Combining the local and remote books ensures a complete list if local was opened and changed offline
#                     local_books.update(remote_books)
#                     meta['books'] = local_books

#         elif meta0.status == 404:
#             if local_meta is None:
#                 meta = new_meta
#             else:
#                 meta = local_meta
#         else:
#             raise urllib3.exceptions.HTTPError(meta0.error)

#     elif local_meta:
#         meta = local_meta
#     else:
#         meta = new_meta

#     return meta, meta_in_remote


# def create_book(local_meta_path, meta, book_name, book_hash, remote_s3_access):
#     """

#     """
#     if remote_s3_access:
#         int_us = make_timestamp()
#     else:
#         int_us = 0

#     meta['books'][book_hash] = {'last_modified': int_us, 'name': book_name, 'user_metadata': {}}
#     if meta['default_book'] is None:
#         meta['default_book'] = book_hash

#     meta['last_modified'] = int_us

#     # write_metadata(local_meta_path, meta)
#     return meta








# def get_remote_index_file(book_base_path, book_hash, remote_db_key, remote_url, http_session, s3_session, remote_http_access, remote_s3_access, overwrite=False):
#     """

#     """
#     remote_index_path = book_base_path.parent.joinpath(book_base_path.name + 'remote_index')

#     if not remote_index_path.exists() or overwrite:
#         if remote_http_access:
#             remote_index_key = remote_url + f'{book_hash}.remote_index'
#             func = http_session.get_object
#         elif remote_s3_access:
#             remote_index_key = remote_db_key + f'{book_hash}.remote_index'
#             func = s3_session.get_object
#         else:
#             return remote_index_path

#         index0 = func(remote_index_key)
#         if index0.status == 200:
#             with open(remote_index_path, 'wb') as f:
#                 shutil.copyfileobj(index0.data, f)
#         elif index0.status != 404:
#             raise urllib3.exceptions.HTTPError(index0.error)

#     return remote_index_path


def get_remote_value(local_file, key, remote_session):
    """

    """
    resp = remote_session.get_object(key)

    if resp.status == 200:
        timestamp = int(resp.metadata['timestamp'])

        # print(timestamp)
        # print(resp.data)

        # val_bytes = resp.data

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


# def check_local_vs_remote_iter(local_file, remote_index, keys):
#     """

#     """
#     for key in keys:
#         yield check_local_vs_remote(local_file, remote_index, key)

# def get_value(local_file, remote_index, key, read_conn):
#     """

#     """
#     if remote_index is not None:
#         # if key not in remote_index:
#         #     return False

#         remote_time_bytes = remote_index.get(key)

#         if remote_time_bytes is None:
#             return local_file.get_timestamp(key, include_value=True)

#         remote_time_int = bytes_to_int(remote_time_bytes)
#         local_time_int_val_bytes = local_file.get_timestamp(key, include_value=True, decode_value=False)

#         if local_time_int_val_bytes:
#             timestamp, val_bytes = local_time_int_val_bytes
#             if remote_time_int <= timestamp:
#                 return timestamp, local_file._post_value(val_bytes)

#         timestamp, val_bytes = get_remote_value(local_file, key, read_conn)

#         return timestamp, local_file._post_value(val_bytes)

#     else:
#         return local_file.get_timestamp(key, include_value=True)


# def load_value(local_file, remote_index, key, read_conn):
#     """

#     """
#     if check_local_vs_remote(local_file, remote_index, key):
#         get_remote_value(local_file, key, read_conn)

#     else:
#         return key in local_file


# def get_value(local_data, local_index, remote_index, key, bucket=None, s3_client=None, session=None, host_url=None, remote_base_url=None):
#     """

#     """
#     if key in local_data:
#         if local_index:
#             local_time_bytes = local_index[key]
#         else:
#             return local_data[key]
#     else:
#         local_time_bytes = None

#     if remote_index:
#         if key not in remote_index:
#             return None

#         remote_time_bytes = remote_index[key]

#         if local_time_bytes:
#             remote_mod_time_int = bytes_to_int(remote_time_bytes)
#             local_mod_time_int = bytes_to_int(local_time_bytes)
#             if remote_mod_time_int < local_mod_time_int:
#                 return local_data[key]

#         value_bytes = get_remote_value(local_data, local_index, remote_index, key, bucket, s3_client, session, host_url, remote_base_url)

#     else:
#         value_bytes = None

#     return value_bytes


#################################################
### local/remote changelog


# def create_changelog(local_data_path, remote_index_path, local_meta_path, n_buckets, meta_in_remote):
#     """
#     Only check and save by the microsecond timestamp. Might need to add in the md5 hash if this is not sufficient.
#     """
#     changelog_path = local_meta_path.parent.joinpath(local_meta_path.name + '.changelog')
#     with booklet.FixedValue(changelog_path, 'n', key_serializer='str', value_len=14, n_buckets=n_buckets) as f:
#         with booklet.VariableValue(local_data_path) as local_data:
#             if meta_in_remote:
#                 # shutil.copyfile(remote_keys_path, temp_remote_keys_path)
#                 # f = booklet.FixedValue(temp_remote_keys_path, 'w')
#                 with booklet.VariableValue(remote_index_path) as remote_index:
#                     for key, local_bytes_us in local_data.items():
#                         remote_val = remote_index.get(key)
#                         if remote_val:
#                             local_int_us = bytes_to_int(local_bytes_us)
#                             remote_bytes_us = remote_val[:7]
#                             remote_int_us = bytes_to_int(remote_bytes_us)
#                             if local_int_us > remote_int_us:
#                                 f[key] = local_bytes_us + remote_bytes_us
#                         else:
#                             f[key] = local_bytes_us + int_to_bytes(0, 7)
#             else:
#                 # f = booklet.FixedValue(temp_remote_keys_path, 'n', key_serializer='str', value_len=26)
#                 for key, local_bytes_us in local_data.items():
#                     f[key] = local_bytes_us + int_to_bytes(0, 7)

#     return changelog_path


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
        else:
            for key, local_int_us in local_file.timestamps():
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


# def check_changelog_keys(


##############################################
### Update remote


def update_remote(local_file, remote_index, changelog_path, remote_session, executor, force_push, deletes, flag, ebooklet_type):
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

    futures = {}
    with booklet.FixedLengthValue(changelog_path) as cl:
        for key in cl:
            # remote_key = base_remote_key + '/' + key
            time_int_us, valb = local_file.get_timestamp(key, include_value=True, decode_value=False)
            f = executor.submit(remote_session.put_object, key, valb, {'timestamp': str(time_int_us)})
            futures[f] = key

    ## Check the uploads to see if any fail
        updated = False
        failures = []
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            run_result = future.result()
            if run_result.status // 100 == 2:
                remote_index[key] = cl[key][:7]
                updated = True
            else:
                failures.append(key)

        if failures:
            print(f"These items failed to upload: {', '.join(failures)}")

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

        # remote_index_key = write_conn_open.db_key + '.remote_index'
        remote_index._file.seek(0)

        # futures = {}
        # f = executor.submit(write_conn_open.put_db_object, remote_index._file.read(), {'timestamp': str(time_int_us), 'uuid': remote_index.uuid.hex, 'ebooklet_type': ebooklet_type, 'init_bytes': base64.b64encode(local_init_bytes)})
        # futures[f] = write_conn_open.db_key

        resp = remote_session.put_db_object(remote_index._file.read(), {'timestamp': str(time_int_us), 'uuid': remote_index.uuid.hex, 'ebooklet_type': ebooklet_type, 'init_bytes': base64.urlsafe_b64encode(local_init_bytes).decode()})

        # remote_index.reopen('r')

        if resp.status // 100 != 2:
            urllib3.exceptions.HTTPError("The db object failed to upload. You need to rerun the push with force_push=True or the remote will be corrupted.")


        # ## remove deletes in remote
        # if deletes:
        #     write_conn_open.delete_objects(deletes)
        #     for key in deletes:
        #         del remote_index[key]
        #     remote_index.sync()

        ## Save main file init bytes
        # local_file._set_file_timestamp(time_int_us)
        # local_file._file.seek(0)
        # local_init_bytes = bytearray(local_file._file.read(200))
        # if local_init_bytes[:16] != booklet.utils.uuid_variable_blt:
        #     raise ValueError(local_init_bytes)

        # n_keys_pos = booklet.utils.n_keys_pos
        # local_init_bytes[n_keys_pos:n_keys_pos+4] = b'\x00\x00\x00\x00'

        # ## Upload the init bytes
        # f = executor.submit(write_conn_open.put_db_object, local_init_bytes, {'timestamp': str(time_int_us), 'uuid': local_file.uuid.hex})
        # futures[f] = write_conn_open.db_key

        # failures = []
        # for future in concurrent.futures.as_completed(futures):
        #     key = futures[future]
        #     run_result = future.result()
        #     if run_result.status // 100 == 2:
        #         failures.append(key)

        # if failures:
        #     urllib3.exceptions.HTTPError(f"These items failed to upload: {', '.join(failures)}. You need to rerun the push with force_push=True or the remote will be corrupted.")

        ## remove deletes in remote
        if deletes:
            remote_session.delete_objects(deletes)
            deletes.clear()

        return True
    else:
        # remote_index.reopen('r')
        return False















































# def attach_prefix(prefix, key):
#     """

#     """
#     if key == '':
#         new_key = prefix
#     elif not prefix.startswith('/'):
#         new_key = prefix + '/' + prefix


# def test_path(path: pathlib.Path):
#     """

#     """
#     return path


def determine_file_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    size = file_obj.seek(0, io.SEEK_END)
    file_obj.seek(pos)

    return size


# def check_local_storage_kwargs(local_storage, local_storage_kwargs, local_file_path):
#     """

#     """
#     if local_storage == 'blt':
#         if 'flag' in local_storage_kwargs:
#             if local_storage_kwargs['flag'] not in ('w', 'c', 'n'):
#                 local_storage_kwargs['flag'] = 'c'
#         else:
#             local_storage_kwargs['flag'] = 'c'

#         local_storage_kwargs['file_path'] = local_file_path

#     return local_storage_kwargs



























































