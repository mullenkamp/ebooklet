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
import hashlib
from hashlib import blake2b
import booklet
import orjson
from s3func import S3Session, HttpSession
import urllib3
import shutil
from datetime import datetime, timezone
import zstandard as zstd
from glob import glob
import portalocker
import concurrent.futures
# from collections.abc import Mapping, MutableMapping
from __init__ import __version__ as version

############################################
### Parameters

# version = '0.1.0'

default_n_buckets = 100003

blt_files = ('.local_data', '.remote_index')

local_storage_options = ('write_buffer_size', 'n_bytes_file', 'n_bytes_key', 'n_bytes_value', 'n_buckets')

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


def s3remote_finalizer(lock):
    """
    The finalizer function for S3Remote instances.
    """
    if lock:
        lock.release()


def bookcase_finalizer(temp_path, lock):
    """
    The finalizer function for bookcase instances.
    """
    if temp_path:
        shutil.rmtree(temp_path, True)
    if lock:
        lock.release()


def book_finalizer(local_data, local_index, remote_index):
    """
    The finalizer function for book instances.
    """
    local_data.close()
    if local_index:
        local_index.close()
    if remote_index:
        remote_index.close()


def write_metadata(local_meta_path, meta):
    """

    """
    meta_bytes = orjson.dumps(meta, option=orjson.OPT_SERIALIZE_NUMPY)
    with io.open(local_meta_path, 'wb') as f:
        f.write(meta_bytes)


def get_save_remote_file(local_path, remote_url, remote_db_key, http_session, s3_session, remote_s3_access, remote_http_access):
    """

    """
    if remote_http_access:
        func = http_session.get_object
        key = remote_url
    else:
        func = s3_session.get_object
        key = remote_db_key

    resp = func(key)
    if resp.status == 200:
        with open(local_path, 'wb') as f:
            f.write(resp.data)
    else:
        raise urllib3.exceptions.HTTPError(resp.error)


def get_remote_file(remote_url, remote_db_key, http_session, s3_session, remote_s3_access, remote_http_access):
    """

    """
    if remote_http_access:
        func = http_session.get_object
        key = remote_url
    else:
        func = s3_session.get_object
        key = remote_db_key

    resp = func(key)
    if resp.status == 200:
        return resp.data
    else:
        raise urllib3.exceptions.HTTPError(resp.error)


def get_book_files(local_meta_path, remote_url, remote_db_key, http_session, s3_session, remote_s3_access, remote_http_access):
    """
    Function to get the book_index and book_metadata files from the remote.
    """




def init_remote_config(bucket, connection_config, remote_url, threads, read_timeout, retries):
    """

    """
    http_session = None
    s3_session = None
    remote_s3_access = False
    remote_http_access = False
    remote_base_url = None
    host_url = None

    if remote_url is not None:
        url_grp = urllib3.util.parse_url(remote_url)
        if url_grp.scheme is not None:
            http_session = HttpSession(threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
            url_path = pathlib.Path(url_grp.path)
            remote_base_url = url_path.parent
            host_url = url_grp.scheme + '://' + url_grp.host
            remote_http_access = True
        else:
            print(f'{remote_url} is not a proper url.')
    if (bucket is not None) and (connection_config is not None):
        s3_session = S3Session(connection_config, bucket, threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
        remote_s3_access = True

    # if (not remote_s3_access) and (flag != 'r'):
    #     raise ValueError("If flag != 'r', then the appropriate remote write access parameters must be passed.")

    return http_session, s3_session, remote_s3_access, remote_http_access, host_url, remote_base_url


def init_metadata(local_meta_path, remote_keys_path, write, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs):
    """

    """
    meta_in_remote = False
    # get_remote_keys = False
    remote_meta = None

    if remote_s3_access:
        int_us = make_timestamp()
    else:
        int_us = 0

    new_meta = {
        'package_version': version,
        'local_storage_kwargs': local_storage_kwargs,
        'value_serializer': value_serializer,
        'last_modified': int_us,
        'user_metadata': {},
        'default_book': None
        }

    glob_str = str(local_meta_path) + '.*'
    extra_files = glob(glob_str)

    if local_meta_path.exists():
        # if write:
        #     portalocker.lock(local_meta_path, portalocker.LOCK_EX)
        # else:
        #     portalocker.lock(local_meta_path, portalocker.LOCK_SH)
        with io.open(local_meta_path, 'rb') as f:
            local_meta = orjson.loads(f.read())
    elif not write:
        raise FileExistsError('Bookcase was open for read-only, but nothing exists to open.')
    else:
        for file in extra_files:
            os.remove(file)
        local_meta = None

    if remote_http_access or remote_s3_access:
        if remote_http_access:
            func = http_session.get_object
            key = remote_url
        else:
            func = s3_session.get_object
            key = remote_db_key

        meta0 = func(key)
        if meta0.status == 200:
            if meta0.metadata['file_type'] != 'bookcase':
                raise TypeError('The remote db file is not a bookcase file.')
            remote_meta = orjson.loads(meta0.data)
            meta_in_remote = True

            ## Remote meta is the only meta needed now
            meta = remote_meta

            ## Determine if the local remote index files needs to be removed
            if local_meta is not None:
                remote_ts = remote_meta['last_modified']
                local_ts = local_meta['last_modified']
                if remote_ts > local_ts:
                    # get_remote_keys_file(local_meta_path, remote_db_key, remote_url, http_session, s3_session, remote_http_access)

                    # with open(local_meta_path, 'wb') as f:
                    #     f.write(meta0.data)

                    local_books = local_meta['books'].copy()
                    remote_books = remote_meta['books']

                    # local_remote_index_ts = {}
                    # for file in extra_files:
                    #     if '.remote_index' in file:
                    #         book_hash = file.split('.')[-2]
                    #         local_book_ts = books[book_hash]['last_modified']
                    #         local_remote_index_ts[book_hash] = local_book_ts

                    for file in extra_files:
                        if '.remote_index' in file:
                            book_hash = file.split('.')[-2]
                            local_book_ts = local_books[book_hash]['last_modified']
                            remote_book = remote_books.get(book_hash)
                            if remote_book:
                                remote_book_ts = remote_book['last_modified']
                                if remote_book_ts > local_book_ts:
                                    os.remove(file)

                    ## Combining the local and remote books ensures a complete list if local was opened and changed offline
                    local_books.update(remote_books)
                    meta['books'] = local_books

        elif meta0.status == 404:
            if local_meta is None:
                meta = new_meta
            else:
                meta = local_meta
        else:
            raise urllib3.exceptions.HTTPError(meta0.error)

    elif local_meta:
        meta = local_meta
    else:
        meta = new_meta

    return meta, meta_in_remote


def create_book(local_meta_path, meta, book_name, book_hash, remote_s3_access):
    """

    """
    if remote_s3_access:
        int_us = make_timestamp()
    else:
        int_us = 0

    meta['books'][book_hash] = {'last_modified': int_us, 'name': book_name, 'user_metadata': {}}
    if meta['default_book'] is None:
        meta['default_book'] = book_hash

    meta['last_modified'] = int_us

    # write_metadata(local_meta_path, meta)
    return meta


def init_local_file(local_file_path, flag, read_remote, value_serializer, n_buckets, buffer_size):
    """

    """
    overwrite_remote_index = False
    if local_file_path.exists():

        if read_remote.readable:
            ## Check the uuid
            remote_uuid = read_remote.uuid

            with booklet.open(local_file_path) as f:
                local_uuid = f.uuid
                local_timestamp = f._file_timestamp

            if remote_uuid != local_uuid:
                raise ValueError('The local file has a different UUID than the remote. Use a different local file path or delete the existing one.')

            ## Check timestamp to determine if the local remote_index needs to be updated
            if read_remote.timestamp > local_timestamp:
                overwrite_remote_index = True

    else:
        if read_remote.readable:
            ## Init with the remote bytes - keeps the remote uuid and timestamp
            with booklet.open(local_file_path, flag='n', init_bytes=read_remote._init_bytes):
                pass

            overwrite_remote_index = True
        else:
            ## Else create a new file
            with booklet.open(local_file_path, flag='n', key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size):
                pass

    return local_file_path, overwrite_remote_index


def get_remote_index_file(local_file_path, overwrite_remote_index, read_remote):
    """

    """
    remote_index_path = local_file_path.parent.joinpath(local_file_path.name + '.remote_index')

    if not remote_index_path.exists() or overwrite_remote_index:
        if read_remote.readable:
            remote_index_key = read_remote.db_key + '.remote_index'
            func = read_remote.get_object

            index0 = func(remote_index_key)
            if index0.status == 200:
                with open(remote_index_path, 'wb') as f:
                    shutil.copyfileobj(index0.data, f)
            elif index0.status != 404:
                raise urllib3.exceptions.HTTPError(index0.error)
        else:
            remote_index_path = None

    return remote_index_path


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


def get_remote_value(local_data, local_index, remote_index, key, remote_s3_access, remote_http_access, s3_session=None, http_session=None, host_url=None, remote_base_url=None):
    """

    """
    if remote_http_access:
        remote_key = host_url + str(remote_base_url.joinpath(key))
        func = http_session.get_object
    else:
        remote_key = key
        func = s3_session.get_object

    ## While loop due to issue of an incomplete read by urllib3
    counter = 0
    while True:
        resp = func(remote_key)

        if resp.status == 200:
            try:
                valb = resp.stream.read()
                break
            except urllib3.exceptions.ProtocolError as error:
                print(error)
                counter += 1
                if counter == 5:
                    # close_files(local_data, remote_index)
                    raise error
        elif resp.status == 404:
            raise KeyError(f'{key} not found in remote.')
            break
        else:
            return urllib3.exceptions.HttpError(f'{key} returned the http error {resp.status}.')

    # mod_time_int = make_timestamp(resp.metadata['upload_timestamp'])
    # mod_time_bytes = int_to_bytes(mod_time_int, 6)

    # val_md5 = hashlib.md5(valb).digest()
    # obj_size_bytes = int_to_bytes(len(valb), 4)

    local_data[key] = valb
    local_index[key] = remote_index[key]

    return key


def load_value(local_data, local_index, remote_index, key, remote_s3_access, remote_http_access, s3_session, http_session, host_url=None, remote_base_url=None, overwrite=False):
    """

    """
    if remote_index:
        if key not in remote_index:
            return False

        remote_time_bytes = remote_index[key]

        local_time_bytes = None
        if not overwrite:
            if key in local_data:
                if local_index:
                    local_time_bytes = local_index[key]

        if local_time_bytes:
            remote_mod_time_int = bytes_to_int(remote_time_bytes)
            local_mod_time_int = bytes_to_int(local_time_bytes)
            if remote_mod_time_int < local_mod_time_int:
                return True

        get_remote_value(local_data, local_index, remote_index, key, remote_s3_access, remote_http_access, s3_session, http_session, host_url, remote_base_url)

        return True

    elif key in local_data:
        return True
    else:
        return False


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


def create_changelog(local_index, remote_index, book_base_path, n_buckets, meta_in_remote):
    """
    Only check and save by the microsecond timestamp. Might need to add in the md5 hash if this is not sufficient.
    """
    changelog_path = book_base_path.parent.joinpath(book_base_path.name + 'changelog')
    with booklet.FixedValue(changelog_path, 'n', key_serializer='str', value_len=14, n_buckets=n_buckets) as f:
        if meta_in_remote and remote_index:
            for key, local_bytes_us in local_index.items():
                remote_bytes_us = remote_index.get(key)
                if remote_bytes_us:
                    local_int_us = bytes_to_int(local_bytes_us)
                    remote_int_us = bytes_to_int(remote_bytes_us)
                    if local_int_us > remote_int_us:
                        f[key] = local_bytes_us + remote_bytes_us
                else:
                    f[key] = local_bytes_us + int_to_bytes(0, 7)
        else:
            for key, local_bytes_us in local_index.items():
                f[key] = local_bytes_us + int_to_bytes(0, 7)

    return changelog_path


def view_changelog(changelog_path):
    """

    """
    with booklet.FixedValue(changelog_path) as f:
        for key, val in f.items():
            local_bytes_us = val[:7]
            remote_bytes_us = val[7:]
            local_int_us = bytes_to_int(local_bytes_us)
            remote_int_us = bytes_to_int(remote_bytes_us)
            if remote_int_us == 0:
                remote_ts = None
            else:
                remote_ts = datetime.fromtimestamp(remote_int_us*0.000001)

            dict1 = {
                'key': key,
                'remote_timestamp': remote_ts,
                'local_timestamp': datetime.fromtimestamp(local_int_us*0.000001)
                }

            yield dict1



##############################################
### Update remote


def update_remote(local_meta_path, meta, local_data, remote_index_path, remote_index, book_hash, changelog_path, n_buckets, s3_session, remote_db_key, executor):
    """

    """
    ## Prep remote data
    base_remote_key = f'{remote_db_key}.{book_hash}'

    ## Upload data and update the remote_index file
    futures = {}
    with booklet.FixedValue(changelog_path) as f:
        for key in f:
            remote_key = base_remote_key + '/' + key
            f = executor.submit(s3_session.put_object(remote_key, local_data[key]))
            futures[f] = key

    ## Check the uploads to see if any fail
        updated = False
        failures = []
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            run_result = future.result()
            if run_result.status == 200:
                remote_index[key] = f[key][:7]
                updated = True
            else:
                failures.append(key)

    ## Upload the remote_index file
    if updated:
        futures = {}
        remote_index.sync()
        remote_index_key = base_remote_key + '.remote_index'
        with open(remote_index_path, 'rb') as ri:
            f = executor.submit(s3_session.put_object(remote_index_key, ri.read()))
            futures[f] = remote_index_key

        ## Save metadata file
        mod_time = make_timestamp()
        meta['last_modified'] = mod_time
        meta['books'][book_hash]['last_modified'] = mod_time
        write_metadata(local_meta_path, meta)

        ## Upload the metadata file
        with open(local_meta_path, 'rb') as ri:
            f = executor.submit(s3_session.put_object(remote_db_key, ri.read()))
            futures[f] = remote_db_key

        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            run_result = future.result()
            if run_result.status != 200:
                failures.append(key)

        if failures:
            print(f"These uploads failed: {', '.split(failures)}")

        return True
    else:
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



























































