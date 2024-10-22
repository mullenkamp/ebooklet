#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import io
import os
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import pathlib
import concurrent.futures
import multiprocessing
import threading
import booklet
import s3func
# import zstandard as zstd
import orjson
import pprint
import tempfile
import weakref
import shutil
import uuid6 as uuid
import urllib3
# import portalocker

import utils
# from . import utils

import remotes
# from . import remotes

# uuid_s3dbm = b'K=d:\xa89F(\xbc\xf5 \xd7$\xbd;\xf2'
# version = 1
# version_bytes = version.to_bytes(2, 'little', signed=False)

lock_remote = False
break_other_locks = False

#######################################################
### Classes


class Change:
    """

    """
    def __init__(self, ebooklet):
        """

        """
        self._ebooklet = ebooklet

        self.update()


    def pull(self):
        """

        """
        self._ebooklet.sync()
        self._ebooklet._read_remote._parse_db_object()
        overwrite_remote_index = utils.check_local_remote_sync(self._ebooklet._local_file, self._ebooklet._read_remote, self._ebooklet._flag)
        if overwrite_remote_index:
            utils.get_remote_index_file(self._ebooklet._local_file_path, overwrite_remote_index, self._ebooklet._read_remote)


    def update(self):
        """

        """
        self._ebooklet.sync()
        changelog_path = utils.create_changelog(self._ebooklet._local_file_path, self._ebooklet._local_file, self._ebooklet._remote_index, self._ebooklet._read_remote)

        self._changelog_path = changelog_path


    def iter_changes(self):
        if not self._changelog_path:
            self.update()
        return utils.view_changelog(self._changelog_path)


    def discard(self, keys=None):
        """
        Removes changed keys in the local file. If keys is None, then removes all changed keys.
        """
        with booklet.FixedValue(self._changelog_path) as f:
            if keys is None:
                rm_keys = f.keys()
            else:
                rm_keys = [key for key in keys if key in f]

            for key in rm_keys:
                # print(key)
                del self._ebooklet._local_file[key]

        self._changelog_path.unlink()
        self._changelog_path = None


    def push(self, force_push=False):
        """
        Updates the remote. It will regenerate the changelog to ensure the changelog is up-to-date. Returns True if the remote has been updated and False if no updates were made (due to nothing needing updating).
        Force_push will push the main file and the remote_index to the remote regardless of changes. Only necessary if upload failures occurred during a previous push.
        """
        if not self._ebooklet._write_remote.writable:
            raise ValueError('Remote is not writable.')

        self.update()

        # if self._remote_index is None:
        #     remote_index = booklet.FixedValue(self._remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=self._local_file._n_buckets, buffer_size=self._local_file._write_buffer_size)

        #     self._remote_index = remote_index
        #     self._finalizer.detach()
        #     self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, self._remote_index)

        success = utils.update_remote(self._ebooklet._local_file_path, self._ebooklet._remote_index_path, self._ebooklet._local_file, self._ebooklet._remote_index, self._changelog_path, self._ebooklet._write_remote, self._ebooklet._executor, force_push, self._ebooklet._deletes, self._ebooklet._flag)

        if success:
            self._changelog_path.unlink()
            self._changelog_path = None # Force a reset of the changelog
            self._ebooklet._deletes.clear()

            if self._ebooklet._read_remote.uuid is None:
                self._ebooklet._read_remote._parse_db_object()
                self._ebooklet._write_remote._parse_db_object()

        return success


# class UserMetadata(MutableMapping):
#     """

#     """
#     def __init__(self, bookcase, book_hash: str=None):
#         """

#         """
#         if isinstance(book_hash, str):
#             user_meta = bookcase._meta['books'][book_hash]['user_metadata']
#         else:
#             user_meta = bookcase._meta['user_metadata']

#         self._bookcase = bookcase
#         self._user_meta = user_meta
#         self._book_hash = book_hash
#         self._modified = False
#         # self._local_meta_path = local_meta_path
#         # self._remote_s3_access = remote_s3_access

#     def __repr__(self):
#         """

#         """
#         return pprint.pformat(self._user_meta)

#     def __setitem__(self, key, value):
#         """

#         """
#         self._user_meta[key] = value
#         self._modified = True


#     def __getitem__(self, key: str):
#         """

#         """
#         return self._user_meta[key]

#     def __delitem__(self, key):
#         """

#         """
#         del self._user_meta[key]
#         self._modified = True

#     def clear(self):
#         """

#         """
#         self._user_meta.clear()
#         self._modified = True


#     def keys(self):
#         """

#         """
#         return self._user_meta.keys()


#     def items(self):
#         """

#         """
#         return self._user_meta.items()


#     def values(self, keys: List[str]=None):
#         return self._user_meta.values()


#     def __iter__(self):
#         return self._user_meta.keys()

#     def __len__(self):
#         """
#         """
#         return len(self._user_meta)


#     def __contains__(self, key):
#         return key in self._user_meta


#     def get(self, key, default=None):
#         return self._user_meta.get(key)


#     def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
#         """

#         """
#         self._user_meta.update(key_value_dict)
#         self._modified = True

#     def __enter__(self):
#         return self

#     def __exit__(self, *args):
#         self.close()

#     def close(self):
#         self.sync()

#     def sync(self):
#         """

#         """
#         if self._modified:
#             int_us = utils.make_timestamp()

#             if isinstance(self._book_hash, str):
#                 if not self._bookcase.remote_s3_access:
#                     self._bookcase._meta['books'][self._book_hash]['last_modified'] += 1
#                 else:
#                     self._bookcase._meta['books'][self._book_hash]['last_modified'] = int_us
#                 # self._bookcase._meta['books'][self._book_hash]['last_modified'] = int_us
#                 self._bookcase._meta['books'][self._book_hash]['user_metadata'] = self._user_meta
#             else:
#                 if not self._bookcase.remote_s3_access:
#                     self._bookcase._meta['last_modified'] += 1
#                 else:
#                     self._bookcase._meta['last_modified'] = int_us

#                 self._bookcase._meta['user_metadata'] = self._user_meta

#             utils.write_metadata(self._bookcase._local_meta_path, self._bookcase._meta)




    # def sync(self):
    #     """

    #     """
    #     if self._modified:
    #         int_us = utils.make_timestamp()
    #         self._metadata['last_modified'] = int_us
    #         if self._version_date:
    #             self._metadata['versions'][self._version_position] = {'versions_date': self._version_date, 'user_metadata': self._user_meta}
    #         else:
    #             self._metadata['user_metadata'] = self._user_meta

    #         with io.open(self._local_meta_path, 'wb') as f:
    #             f.write(zstd.compress(orjson.dumps(self._metadata, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY)))





# class Bookcase:
#     """

#     """
#     def __init__(self,
#                  file_path: Union[str, pathlib.Path],
#                  flag: str = "r",
#                  value_serializer: str = None,
#                  n_buckets: int=12007,
#                  buffer_size: int = 2**22,
#                  remote: Union[remotes.BaseRemote, str]=None
#                  ):
#         """

#         """
#         ## Pre-processing
#         if file_path is None:
#             temp_path = pathlib.Path(tempfile.TemporaryDirectory().name)
#             local_meta_path = temp_path.joinpath('temp.bcs')
#             self._finalizer = weakref.finalize(self, shutil.rmtree, temp_path, True)
#         else:
#             local_meta_path = pathlib.Path(file_path)
#             temp_path = None

#         # local_meta_path = pathlib.Path(local_db_path)
#         remote_keys_name = local_meta_path.name + '.remote_keys'
#         remote_keys_path = local_meta_path.parent.joinpath(remote_keys_name)

#         for key, value in local_storage_kwargs.items():
#             if key not in utils.local_storage_options:
#                 raise ValueError(f'{key} in local_storage_kwargs, but it must only contain {utils.local_storage_options}.')
#         if 'n_buckets' not in local_storage_kwargs:
#             n_buckets = utils.default_n_buckets
#             local_storage_kwargs['n_buckets'] = n_buckets
#         else:
#             n_buckets = int(local_storage_kwargs['n_buckets'])
#         local_storage_kwargs.update({'key_serializer': 'str', 'value_serializer': 'bytes'})
#         if value_serializer in booklet.serializers.serial_name_dict:
#             value_serializer_code = booklet.serializers.serial_name_dict[value_serializer]
#         else:
#             raise ValueError(f'value_serializer must be one of {booklet.available_serializers}.')

#         ## Create S3 lock for writes
#         if write and remote_s3_access:
#             lock = s3func.s3.S3Lock(connection_config, bucket, remote_db_key, read_timeout=read_timeout)
#             if break_other_locks:
#                 lock.break_other_locks()
#             if not lock.aquire(timeout=lock_timeout):
#                 raise TimeoutError('S3Lock timed out')
#         else:
#             lock = None

#         ## Finalizer
#         self._finalizer = weakref.finalize(self, utils.bookcase_finalizer, temp_path, lock)

#         ## Init metadata
#         meta, meta_in_remote = utils.init_metadata(local_meta_path, remote_keys_path, write, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs)

#         ## Init local storage
#         # local_data_path = utils.init_local_storage(local_meta_path, flag, meta)

#         ## Assign properties
#         # self._temp_path = temp_path
#         self._meta_in_remote = meta_in_remote
#         self._remote_db_key = remote_db_key
#         self._n_buckets = n_buckets
#         self._write = write
#         self._buffer_size = buffer_size
#         self._connection_config = connection_config
#         self._read_timeout = read_timeout
#         self._lock = lock
#         self.remote_s3_access = remote_s3_access
#         self.remote_http_access = remote_http_access
#         self._bucket = bucket
#         self._meta = meta
#         self._threads = threads
#         self._local_meta_path = local_meta_path
#         self._remote_keys_path = remote_keys_path
#         self._value_serializer = value_serializer
#         self._value_serializer_code = value_serializer_code
#         self._local_storage_kwargs = local_storage_kwargs
#         self._host_url = host_url
#         self._remote_base_url = remote_base_url
#         self._remote_url = remote_url
#         self._s3_session = s3_session
#         self._http_session = http_session

#         ## Assign the metadata object for global
#         self.metadata = UserMetadata(self)


#     @property
#     def default_book_name(self):
#         """

#         """
#         if self._meta['default_book']:
#             return self._meta['books'][self._meta['default_book']]['name']

#     def list_book_names(self):
#         """

#         """
#         for key, val in self._meta['books'].items():
#             yield val['name']


#     def set_default_book_name(self, book_name):
#         """

#         """
#         book_hash = utils.hash_book_name(book_name)
#         if book_hash in self._meta['books']:
#             self._meta['default_book'] = book_hash
#             # meta_bytes = zstd.compress(orjson.dumps(self._meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY), level=1)
#             # with io.open(self._local_meta_path, 'wb') as f:
#             #     f.write(meta_bytes)
#         else:
#             raise KeyError(book_name)


#     # def create_book(self, book_name):
#     #     """
#     #     Remove
#     #     """
#     #     meta = utils.create_book(self._local_meta_path, self._meta, book_name, self.remote_s3_access)
#     #     self._meta = meta

#     #     return True


#     def open_book(self, book_name: str=None, flag: str='r'):
#         """
#         Remove the create_book method and include a flag parameter.
#         """
#         if book_name is None:
#             if flag == 'r':
#                 book_hash = self._meta['default_book']
#                 if book_hash is None:
#                     raise KeyError('No books exist. Open with write permissions to create a book.')
#             else:
#                 raise KeyError('book_name must be specified when open for writing.')
#         else:
#             book_hash = utils.hash_book_name(book_name)

#         if flag in ('n', 'c'):
#             if flag == 'c':
#                 if book_hash in self._meta['books']:
#                     raise KeyError(f'{book_name} already exists as a book.')
#             meta = utils.create_book(self._local_meta_path, self._meta, book_name, book_hash, self.remote_s3_access)
#             self._meta = meta

#         book = Book(self, book_hash)

#         return book


#     def close(self):
#         """

#         """
#         if self._flag != 'r':
#             self.metadata.close()

#         self._finalizer()

#     def __enter__(self):
#         return self

#     def __exit__(self, *args):
#         self.close()

#     def pull_remote_index(self, book_name):
#         """

#         """
#         ## Get base path for the book
#         if book_name is None:
#             book_hash = self._meta['default_book']
#         else:
#             book_hash = utils.hash_book_name(book_name)
#             if book_hash not in self._meta['books']:
#                 raise KeyError(book_name)

#         book_file_name = self._local_meta_path.name + f'.{book_hash}.'
#         book_base_path = self._local_meta_path.parent.joinpath(book_file_name)

#         remote_index_path = utils.get_remote_index_file(book_base_path, book_hash, self._remote_db_key, self._remote_url, self._http_session, self._s3_session, self.remote_http_access, self.remote_s3_access, True)

#         return remote_index_path


#     def pull_metadata(self):
#         """

#         """
#         if self._meta_in_remote:
#             self.metadata.sync()
#             meta, meta_in_remote = utils.init_metadata(self._local_meta_path, self._remote_keys_path, self._flag, self._write, self._http_session, self._s3_session, self._remote_s3_access, self._remote_http_access, self._remote_url, self._remote_db_key, self._value_serializer, self._local_storage_kwargs)
#             return True
#         else:
#             return False


class EBooklet(MutableMapping):
    """

    """
    def __init__(
            self,
            remote: Union[remotes.BaseRemote, str, list],
            file_path: Union[str, pathlib.Path],
            flag: str = "r",
            value_serializer: str = None,
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            # lock_remote: bool=True,
            # break_other_locks=False,
            # inherit_remote: Union[remotes.BaseRemote, str]=None,
            # inherit_data: bool=False,
            ):
        """

        """
        ## Inherit another remote
        # if (inherit_remote is not None) and (flag in ('c', 'n')):
        #     if isinstance(inherit_remote, str):
        #         inherit_remote = remotes.HttpRemote(inherit_remote)
        #     elif not isinstance(inherit_remote, remotes.BaseRemote):
        #         raise TypeError('inherit_remote must be either a Remote or a url string.')

            # TODO: Pull down the remote ebooklet and assign it to this new object

        ## Determine the remotes that read and write
        read_remote, write_remote, lock = utils.check_parse_remotes(remote, flag, lock_remote, break_other_locks)

        ## Init the local file
        local_file_path = pathlib.Path(file_path)

        local_file, overwrite_remote_index = utils.init_local_file(local_file_path, flag, read_remote, value_serializer, n_buckets, buffer_size)

        remote_index_path = utils.get_remote_index_file(local_file_path, overwrite_remote_index, read_remote, flag)

        # Open remote index file
        if remote_index_path.exists() and (flag != 'n'):
            remote_index = booklet.FixedValue(remote_index_path, 'w')
        else:
            remote_index = booklet.FixedValue(remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=n_buckets, buffer_size=buffer_size)

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, read_remote, write_remote, None)

        ## Assign properties
        if flag == 'r':
            self.writable = False
        else:
            self.writable = True

        self._flag = flag
        self._remote_index_path = remote_index_path
        self._local_file_path = local_file_path
        self._local_file = local_file
        self._remote_index_path = remote_index_path
        self._remote_index = remote_index
        self._deletes = set()
        self._read_remote = read_remote
        self._write_remote = write_remote
        # self._changelog_path = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=read_remote._threads)


    # def _pre_value(self, value) -> bytes:

    #     ## Serialize to bytes
    #     try:
    #         value = self._value_serializer.dumps(value)
    #     except Exception as error:
    #         raise error

    #     return value

    # def _post_value(self, value: bytes):

    #     ## Serialize from bytes
    #     value = self._value_serializer.loads(value)

    #     return value

    def set_metadata(self, data, timestamp=None):
        """
        Sets the metadata for the booklet. The data input must be a json serializable object. Optionally assign a timestamp.
        """
        if self.writable:
            self._local_file.set_metadata(data, timestamp)
        else:
            raise ValueError('File is open for read only.')


    def get_metadata(self, include_timestamp=False):
        """
        Get the metadata. Optionally include the timestamp in the output.
        Will return None if no metadata has been assigned.
        """
        _ = self._load_item(booklet.utils.metadata_key_bytes.decode())
        # if failure:
        #     return failure

        return self._local_file.get_metadata(include_timestamp=include_timestamp)


    def keys(self):
        """

        """
        if self._remote_index is not None:
            return self._remote_index.keys()
        else:
            return self._local_file.keys()


    def items(self):
        """

        """
        _ = self.load_items()

        return self._local_file.items()

    def values(self):
        _ = self.load_items()

        return self._local_file.values()

    def timestamps(self, include_value=False):
        """
        Return an iterator for timestamps for all keys. Optionally add values to the iterator.
        """
        _ = self.load_items()

        return self._local_file.timestamps(include_value=include_value)


    def get_timestamp(self, key, include_value=False, default=None):
        """
        Get a timestamp associated with a key. Optionally include the value.
        """
        failure = self._load_item(key)
        if failure:
            return failure

        return self._local_file.get_timestamp(key, include_value=include_value, default=default)

    def set_timestamp(self, key, timestamp):
        """
        Set a timestamp for a specific key. The timestamp must be either an int of the number of microseconds in POSIX UTC time, an ISO 8601 datetime string with timezone, or a datetime object with timezone.
        """
        if self.writable:
            self._local_file.set_timestamp(key, timestamp)
        else:
            raise ValueError('File is open for read only.')


    def set(self, key, value, timestamp=None):
        """

        """
        if self.writable:
            self._local_file.set(key, value, timestamp)

            # if self._read_remote.uuid:
            #     int_us = utils.make_timestamp()
            # else:
            #     old_val = self._local_index.get(key)
            #     if old_val:
            #         int_us = utils.bytes_to_int(old_val) + 1
            #     else:
            #         int_us = 0
            # val_bytes = self._pre_value(value)
            # self._local_data[key] = val_bytes
            # self._local_index[key] = utils.int_to_bytes(int_us, 7)
        else:
            raise ValueError('File is open for read only.')



    def __iter__(self):
        return self.keys()

    def __len__(self):
        """

        """
        if self._remote_index is not None:
            return len(self._remote_index)
        else:
            return len(self._local_file)


    def __contains__(self, key):
        if self._remote_index is not None:
            return key in self._remote_index
        else:
            return key in self._local_file


    def get(self, key, default=None):
        failure = self._load_item(key)
        if failure:
            return failure

        return self._local_file.get(key, default=default)


    def update(self, key_value_dict: dict):
        """

        """
        if self.writable:
            for key, value in key_value_dict.items():
                self[key] = value
        else:
            raise ValueError('File is open for read only.')


    def prune(self, timestamp=None, reindex=False):
        """
        Prunes the old keys and associated values. Returns the number of removed items. The method can also prune remove keys/values older than the timestamp. The user can also reindex the booklet file. False does no reindexing, True increases the n_buckets to a preassigned value, or an int of the n_buckets. True can only be used if the default n_buckets were used at original initialisation.
        """
        if self._write:
                return self._local_file.prune(timestamp=timestamp, reindex=reindex)
        else:
            raise ValueError('File is open for read only.')


    def get_items(self, keys, default=None):
        """

        """
        _ = self.load_items(keys)

        for key in keys:
            output = self._local_file.get(key, default=default)
            if output is None:
                yield key, None
            else:
                # ts, value = output
                yield key, output


    def load_items(self, keys=None):
        """
        Loads items into the local file without returning the values. If keys is None, then it loads all of the values in the remote. Returns a dict of keys with the errors trying to access the remote.
        """
        futures = {}
        failure_dict = {}

        if keys is None:
            for key, remote_time_bytes in self._remote_index.items():
                check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                if check:
                    f = self._executor.submit(utils.get_remote_value, self._local_file, key, self._read_remote)
                    futures[f] = key
        else:
            for key in keys:
                remote_time_bytes = self._remote_index.get(key)
                check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                if check:
                    f = self._executor.submit(utils.get_remote_value, self._local_file, key, self._read_remote)
                    futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            key = futures[f]
            error = f.result()
            if error is not None:
                failure_dict[key] = error

        return failure_dict


    def _load_item(self, key):
        """

        """
        remote_time_bytes = self._remote_index.get(key)
        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)

        if check:
            failure = utils.get_remote_value(self._local_file, key, self._read_remote)
            return failure
        else:
            return None


    def __getitem__(self, key: str):
        value = self.get(key)

        if value is None:
            raise KeyError(f'{key}')
        else:
            return value


    def __setitem__(self, key: str, value):
        if self.writable:
            self.set(key, value)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self.writable:
            if self._remote_index is not None:
                del self._remote_index[key]
                self._deletes.add(key)

            if key in self._local_file:
                del self._local_file[key]
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self, local_only=True):
        if self.writable:
            self._local_file.clear()

            if not local_only:
                if self._remote_index is not None:
                    self._remote_index.clear()
        else:
            raise ValueError('File is open for read only.')

    def close(self, force_close=False):
        self.sync()
        self._executor.shutdown(cancel_futures=force_close)
        # self._manager.shutdown()
        self._finalizer()


    # def __del__(self):
    #     self.close()

    def sync(self):
        self._executor.shutdown()
        del self._executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._read_remote._threads)
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        return Change(self)


    # def pull(self):
    #     """

    #     """
    #     self.sync()
    #     self._read_remote._parse_db_object()
    #     overwrite_remote_index = utils.check_local_remote_sync(self._local_file, self._read_remote)
    #     if overwrite_remote_index:
    #         utils.get_remote_index_file(self._local_file_path, overwrite_remote_index, self._read_remote)


    # def update_changelog(self):
    #     """

    #     """
    #     self.sync()
    #     changelog_path = utils.create_changelog(self._local_file_path, self._local_file, self._remote_index, self._read_remote)

    #     self._changelog_path = changelog_path


    # def changelog(self):
    #     if not self._changelog_path:
    #         self.update_changelog()
    #     return utils.view_changelog(self._changelog_path)


    # def push(self, force_push=False):
    #     """
    #     Updates the remote. It will regenerate the changelog to ensure the changelog is up-to-date. Returns True if the remote has been updated and False if no updates were made (due to nothing needing updating).
    #     Force_push will push the main file and the remote_index to the remote regardless of changes. Only necessary if upload failures occurred during a previous push.
    #     """
    #     if not self._write_remote.writable:
    #         raise ValueError('Remote is not writable.')

    #     self.update_changelog()

    #     # if self._remote_index is None:
    #     #     remote_index = booklet.FixedValue(self._remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=self._local_file._n_buckets, buffer_size=self._local_file._write_buffer_size)

    #     #     self._remote_index = remote_index
    #     #     self._finalizer.detach()
    #     #     self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, self._remote_index)

    #     success = utils.update_remote(self._local_file_path, self._remote_index_path, self._local_file, self._remote_index, self._changelog_path, self._write_remote, self._executor, force_push, self._deletes, self._flag)

    #     if success:
    #         self._changelog_path.unlink()
    #         self._changelog_path = None # Force a reset of the changelog

    #         if self._read_remote.uuid is None:
    #             self._read_remote._parse_db_object()
    #             self._write_remote._parse_db_object()

    #     return success


def open(
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    remote: Union[remotes.BaseRemote, str, list]=None,
    ):
    """
    Open an S3 dbm-style database. This allows the user to interact with an S3 bucket like a MutableMapping (python dict) object. Lots of options including read caching.

    Parameters
    -----------
    bucket : str
        The S3 bucket with the objects.

    client : botocore.client.BaseClient or None
        The boto3 S3 client object that can be directly passed. This allows the user to include whatever client parameters they wish. It's recommended to use the s3_client function supplied with this package. If None, then connection_config must be passed.

    connection_config: dict or None
        If client is not passed to open, then the connection_config must be supplied. If both are passed, then client takes priority. connection_config should be a dict of service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key.

    public_url : HttpUrl or None
        If the S3 bucket is publicly accessible, then supplying the public_url will download objects via normal http. The provider parameter is associated with public_url to specify the provider's public url style.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    buffer_size : int
        The buffer memory size used for reading and writing. Defaults to 512000.

    retries : int
        The number of http retries for reads and writes. Defaults to 3.

    read_timeout : int
        The http read timeout in seconds. Defaults to 120.

    provider : str or None
        Associated with public_url. If provider is None, then it will try to figure out the provider (in a very rough way). Options include, b2, r2, and contabo.

    threads : int
        The max number of threads to use when using several methods. Defaults to 30.

    Returns
    -------
    S3dbm

    The optional *flag* argument can be:

    +---------+-------------------------------------------+
    | Value   | Meaning                                   |
    +=========+===========================================+
    | ``'r'`` | Open existing database for reading only   |
    |         | (default)                                 |
    +---------+-------------------------------------------+
    | ``'w'`` | Open existing database for reading and    |
    |         | writing                                   |
    +---------+-------------------------------------------+
    | ``'c'`` | Open database for reading and writing,    |
    |         | creating it if it doesn't exist           |
    +---------+-------------------------------------------+
    | ``'n'`` | Always create a new, empty database, open |
    |         | for reading and writing                   |
    +---------+-------------------------------------------+

    """
    if remote is None:
        return booklet.VariableValue(file_path, flag=flag, key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
    else:
        return EBooklet(remote=remote, file_path=file_path, flag=flag, key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)