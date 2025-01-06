#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import pathlib
import concurrent.futures
import booklet
import weakref
from itertools import count
from collections import deque

# import utils
from . import utils

# import remote
from . import remote

# uuid_s3dbm = b'K=d:\xa89F(\xbc\xf5 \xd7$\xbd;\xf2'
# version = 1
# version_bytes = version.to_bytes(2, 'little', signed=False)

# lock_remote = False
# break_other_locks = False

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

        ## update the remote timestamp
        self._ebooklet._remote_session.get_timestamp()

        ## Determine if a change has occurred
        overwrite_remote_index = utils.check_local_remote_sync(self._ebooklet._local_file, self._ebooklet._remote_session, self._ebooklet._flag)

        ## Pull down the remote index
        if overwrite_remote_index:
            utils.get_remote_index_file(self._ebooklet._local_file_path, overwrite_remote_index, self._ebooklet._remote_session, self._ebooklet._flag)


    def update(self):
        """

        """
        self._ebooklet.sync()
        changelog_path = utils.create_changelog(self._ebooklet._local_file_path, self._ebooklet._local_file, self._ebooklet._remote_index, self._ebooklet._remote_session)

        self._changelog_path = changelog_path


    def iter_changes(self):
        if not self._changelog_path:
            self.update()
        return utils.view_changelog(self._changelog_path)


    def discard(self, keys=None):
        """
        Removes changed keys in the local file. If keys is None, then removes all changed keys.
        """
        if not self._ebooklet.writable:
            raise ValueError('File is open for read-only.')

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
        if not self._ebooklet._remote_session.writable:
            raise ValueError('Remote is not writable.')

        if not self._ebooklet.writable:
            raise ValueError('File is open for read-only.')

        self.update()

        # if self._remote_index is None:
        #     remote_index = booklet.FixedValue(self._remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=self._local_file._n_buckets, buffer_size=self._local_file._write_buffer_size)

        #     self._remote_index = remote_index
        #     self._finalizer.detach()
        #     self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, self._remote_index)

        success = utils.update_remote(self._ebooklet._local_file, self._ebooklet._remote_index, self._changelog_path, self._ebooklet._remote_session, self._ebooklet._executor, force_push, self._ebooklet._deletes, self._ebooklet._flag, self._ebooklet._subtype)

        if success:
            self._changelog_path.unlink()
            self._changelog_path = None # Force a reset of the changelog
            self._ebooklet._deletes.clear()

            if self._ebooklet._remote_session.uuid is None:
                self._ebooklet._remote_session.load_db_metadata()

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


class EVariableLengthValue(MutableMapping):
    """

    """
    def __init__(
            self,
            remote_conn: remote.S3Connection,
            file_path: Union[str, pathlib.Path],
            flag: str = "r",
            value_serializer: str = None,
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            object_lock: bool=False,
            break_other_locks: bool=False,
            lock_timeout: int=-1,
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

        # check_timestamp = init_check_remote

        local_file_path = pathlib.Path(file_path)

        local_file_exists = local_file_path.exists()

        ## Determine the remotes that read and write
        remote_session = utils.check_parse_conn(remote_conn, flag, object_lock, break_other_locks, lock_timeout, local_file_exists)

        ## Init the local file
        local_file, overwrite_remote_index = utils.init_local_file(local_file_path, flag, remote_session, value_serializer, n_buckets, buffer_size)

        remote_index_path = utils.get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag)

        # Open remote index file
        if remote_index_path.exists():
            # remote_index = booklet.FixedValue(remote_index_path, 'r')
            if flag == 'r':
                remote_index = booklet.FixedLengthValue(remote_index_path, 'r')
            else:
                remote_index = booklet.FixedLengthValue(remote_index_path, 'w')
        else:
            remote_index = booklet.FixedLengthValue(remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=n_buckets, buffer_size=buffer_size)

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session)

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
        # self._read_conn_open = read_conn_open
        # self._write_conn_open = write_conn_open
        self._remote_session = remote_session
        # self._changelog_path = None
        self._n_buckets = local_file._n_buckets
        # self._clear = False
        # self._lock = lock
        self._subtype = 'EVariableLengthValue'
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


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
        overlap = set()
        for key in self._local_file.keys():
            if key in self._remote_index:
                overlap.add(key)
            yield key

        for key in self._remote_index.keys():
            if key not in overlap:
                yield key


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

            # if self._read_conn.uuid:
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
        counter = count()
        deque(zip(self.keys(), counter), maxlen=0)

        return next(counter)


    def __contains__(self, key):
        if (key in self._remote_index) or (key in self._local_file):
            return True
        else:
            return False

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
        if self.writable:
            removed = self._local_file.prune(timestamp=timestamp, reindex=reindex)
            self._n_buckets = self._local_file._n_buckets

            _ = self._remote_index.prune(reindex)

            return removed
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

        writable = self._local_file.writable

        if not writable:
            self._local_file.reopen('w')

        if keys is None:
            for key, remote_time_bytes in self._remote_index.items():
                check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                if check:
                    f = self._executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                    futures[f] = key
        else:
            for key in keys:
                remote_time_bytes = self._remote_index.get(key)
                check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                if check:
                    f = self._executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                    futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            key = futures[f]
            error = f.result()
            if error is not None:
                failure_dict[key] = error

        if not writable:
            self._local_file.reopen('r')

        return failure_dict


    def _load_item(self, key):
        """

        """
        # if key in self._deletes:
        #     raise KeyError(key)

        remote_time_bytes = self._remote_index.get(key)
        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)

        if check:
            if not self._local_file.writable:
                self._local_file.reopen('w')
                failure = utils.get_remote_value(self._local_file, key, self._remote_session)
                self._local_file.reopen('r')
            else:
                failure = utils.get_remote_value(self._local_file, key, self._remote_session)
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
            if key in self._remote_index:
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

            # if not local_only:
            #     if self._remote_index is not None:
            #         self._remote_index.clear()
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
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._remote_session.threads)
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        return Change(self)


    def reopen(self, flag):
        """

        """
        self.close()
        self._local_file.reopen(flag)
        self._remote_index.reopen(flag)

        # if self._lock is not None:
        #     self._lock.aquire()

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._remote_session.threads)

        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, self._remote_index, self._remote_session)


    # def pull(self):
    #     """

    #     """
    #     self.sync()
    #     self._read_conn._parse_db_object()
    #     overwrite_remote_index = utils.check_local_remote_sync(self._local_file, self._read_conn)
    #     if overwrite_remote_index:
    #         utils.get_remote_index_file(self._local_file_path, overwrite_remote_index, self._read_conn)


    # def update_changelog(self):
    #     """

    #     """
    #     self.sync()
    #     changelog_path = utils.create_changelog(self._local_file_path, self._local_file, self._remote_index, self._read_conn)

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
    #     if not self._write_conn.writable:
    #         raise ValueError('Remote is not writable.')

    #     self.update_changelog()

    #     # if self._remote_index is None:
    #     #     remote_index = booklet.FixedValue(self._remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=self._local_file._n_buckets, buffer_size=self._local_file._write_buffer_size)

    #     #     self._remote_index = remote_index
    #     #     self._finalizer.detach()
    #     #     self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, self._remote_index)

    #     success = utils.update_remote(self._local_file_path, self._remote_index_path, self._local_file, self._remote_index, self._changelog_path, self._write_conn, self._executor, force_push, self._deletes, self._flag)

    #     if success:
    #         self._changelog_path.unlink()
    #         self._changelog_path = None # Force a reset of the changelog

    #         if self._read_conn.uuid is None:
    #             self._read_conn._parse_db_object()
    #             self._write_conn._parse_db_object()

    #     return success


class RemoteConnGroup(EVariableLengthValue):
    """

    """
    def __init__(
            self,
            remote_conn: remote.S3Connection,
            file_path: Union[str, pathlib.Path],
            flag: str = "r",
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            object_lock: bool=False,
            break_other_locks: bool=False,
            lock_timeout: int=-1,
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

        # check_timestamp = init_check_remote

        local_file_path = pathlib.Path(file_path)

        local_file_exists = local_file_path.exists()

        ## Determine the remotes that read and write
        remote_session = utils.check_parse_conn(remote_conn, flag, object_lock, break_other_locks, lock_timeout, local_file_exists)

        ## Init the local file
        local_file, overwrite_remote_index = utils.init_local_file(local_file_path, flag, remote_session, 'orjson', n_buckets, buffer_size)

        remote_index_path = utils.get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag)

        # Open remote index file
        if remote_index_path.exists():
            # remote_index = booklet.FixedValue(remote_index_path, 'r')
            if flag == 'r':
                remote_index = booklet.FixedLengthValue(remote_index_path, 'r')
            else:
                remote_index = booklet.FixedLengthValue(remote_index_path, 'w')
        else:
            remote_index = booklet.FixedLengthValue(remote_index_path, 'n', key_serializer='str', value_len=7, n_buckets=n_buckets, buffer_size=buffer_size)

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session)

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
        # self._read_conn_open = read_conn_open
        # self._write_conn_open = write_conn_open
        self._remote_session = remote_session
        # self._changelog_path = None
        self._n_buckets = local_file._n_buckets
        # self._clear = False
        # self._lock = lock
        self._subtype = 'RemoteConnGroup'
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


    def add(self, remote_conn: remote.S3Connection):
        """

        """
        if self.writable:

            if not isinstance(remote_conn, remote.S3Connection):
                raise TypeError('remote_conn/value must be a remote.S3Connection')

            ## Update remote_conn meta
            remote_conn.load_db_metadata()
            remote_conn.load_user_metadata()

            uuid0 = remote_conn.uuid
            if uuid0 is None:
                raise ValueError('Remote does not exist. It must exist to be added to a RemoteConnGroup.')

            uuid_hex = uuid0.hex

            value = remote_conn.to_dict()

            self._local_file.set(uuid_hex, value, remote_conn.timestamp)

        else:
            raise ValueError('File is open for read only.')


    def set(self, key, remote_conn: remote.S3Connection):
        """

        """
        if self.writable:

            if not isinstance(remote_conn, remote.S3Connection):
                raise TypeError('remote_conn/value must be a remote.S3Connection')

            ## Update remote_conn meta
            remote_conn.load_db_metadata()
            remote_conn.load_user_metadata()

            uuid0 = remote_conn.uuid
            if uuid0 is None:
                raise ValueError('Remote does not exist. It must exist to be added to a RemoteConnGroup.')

            uuid_hex = uuid0.hex

            if key != uuid_hex:
                raise KeyError('The key must be the uuid hex.')

            value = remote_conn.to_dict()

            self._local_file.set(uuid_hex, value, remote_conn.timestamp)

        else:
            raise ValueError('File is open for read only.')



def open(
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    remote_conn: Union[remote.S3Connection, str]=None,
    ebooklet_type: str='EVariableLengthValue',
    ):
    """
    Open an S3 dbm-style database. This allows the user to interact with an S3 bucket like a MutableMapping (python dict) object. If remote_conn is not passed, then it opens a normal booklet file.

    Parameters
    -----------
    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    key_serializer : str, class, or None
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input. A custom serializer class can also be used. If the objects can be serialized to json, then use orjson or msgpack. They are super fast and you won't have the pickle issues.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values.

    n_buckets : int
        The number of hash buckets to using in the indexing. Generally use the same number of buckets as you expect for the total number of keys.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    remote_conn : S3Connection, str, or None
        The object to connect to a remote. It can either be a Conn type object, an http url string, or None. If None, no remote connection is made and the file is only opened locally.
    
    ebooklet_type : str
        What type of ebooklet to create. Options are either EVariableLengthValue (default) or RemoteConnGroup.

    Returns
    -------
    Ebooklet or Booklet

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
    if remote_conn is None:
        return booklet.open(file_path, flag=flag, key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
    else:
        if isinstance(remote_conn, str):
            if flag != 'r':
                raise ValueError('If remote_conn is a url string, then flag must be r.')
            remote_conn = remote.S3Connection(db_url=remote_conn)
        elif not isinstance(remote_conn, remote.S3Connection):
            raise TypeError('remote_conn must be either a url string or aremote.S3Connection.')

        if remote_conn.uuid is not None:
            if isinstance(remote_conn.ebooklet_type, str):
                if remote_conn.ebooklet_type == 'EVariableLengthValue':
                    return EVariableLengthValue(remote_conn=remote_conn, file_path=file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
                elif remote_conn.ebooklet_type == 'RemoteConnGroup':
                    return RemoteConnGroup(remote_conn=remote_conn, file_path=file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size)
                else:
                    raise ValueError('What kind of EBooklet is this?!')
        else:
            if ebooklet_type == 'EVariableLengthValue':
                return EVariableLengthValue(remote_conn=remote_conn, file_path=file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
            elif ebooklet_type == 'RemoteConnGroup':
                return RemoteConnGroup(remote_conn=remote_conn, file_path=file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size)
            else:
                raise ValueError('ebooklet_type must be either EVariableLengthValue or RemoteConnGroup.')
