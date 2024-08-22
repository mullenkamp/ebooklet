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
import zstandard as zstd
import orjson
import pprint
import tempfile
import weakref
import shutil
import portalocker

import utils
# from . import utils

# uuid_s3dbm = b'K=d:\xa89F(\xbc\xf5 \xd7$\xbd;\xf2'
# version = 1
# version_bytes = version.to_bytes(2, 'little', signed=False)

#######################################################
### Classes


class Change:
    """

    """
    def __init__(self, book):
        """

        """
        if book._remote_index and book._local_index:
            changelog_path = utils.create_changelog(book._local_index, book._remote_index, book._book_base_path, book._bookcase._n_buckets, book._bookcase._meta_in_remote)
        else:
            changelog_path = None
            print('No changes can be made as the remote_index is not available.')

        self._changelog_path = changelog_path
        self._book = book


    def iter_changes(self):
        """

        """
        if self._changelog_path:
            return utils.view_changelog(self._changelog_path)
        else:
            return None


    def update_changelog(self):
        """

        """
        if self._book._remote_index and self._book._local_index:
            changelog_path = utils.create_changelog(self._book._local_index, self._book._remote_index, self._book._book_base_path, self._book._bookcase._n_buckets, self._book._bookcase._meta_in_remote)
        else:
            changelog_path = None
            print('No changes can be made as the remote_index is not available.')

        self._changelog_path = changelog_path


    def pull_remote_index(self):
        """

        """
        remote_index_path = self._book.pull_remote_index()

        return remote_index_path


    def push(self):
        """

        """
        if self._changelog_path and self._book._bookcase._remote_s3_access:
            return utils.update_remote(self._book._bookcase._local_meta_path, self._book._bookcase._meta, self._book._local_data, self._book._bookcase._remote_index_path, self._book._bookcase._remote_index, self._book._book_hash, self._changelog_path, self._book._bookcase._n_buckets, self._book._bookcase._s3_session, self._book._bookcase._remote_db_key, self._book._executor)
        else:
            return False


class UserMetadata(MutableMapping):
    """

    """
    def __init__(self, bookcase, book_hash: str=None):
        """

        """
        if isinstance(book_hash, str):
            user_meta = bookcase._meta['books'][book_hash]['user_metadata']
        else:
            user_meta = bookcase._meta['user_metadata']

        self._bookcase = bookcase
        self._user_meta = user_meta
        self._book_hash = book_hash
        self._modified = False
        # self._local_meta_path = local_meta_path
        # self._remote_s3_access = remote_s3_access

    def __repr__(self):
        """

        """
        return pprint.pformat(self._user_meta)

    def __setitem__(self, key, value):
        """

        """
        self._user_meta[key] = value
        self._modified = True


    def __getitem__(self, key: str):
        """

        """
        return self._user_meta[key]

    def __delitem__(self, key):
        """

        """
        del self._user_meta[key]
        self._modified = True

    def clear(self):
        """

        """
        self._user_meta.clear()
        self._modified = True


    def keys(self):
        """

        """
        return self._user_meta.keys()


    def items(self):
        """

        """
        return self._user_meta.items()


    def values(self, keys: List[str]=None):
        return self._user_meta.values()


    def __iter__(self):
        return self._user_meta.keys()

    def __len__(self):
        """
        """
        return len(self._user_meta)


    def __contains__(self, key):
        return key in self._user_meta


    def get(self, key, default=None):
        return self._user_meta.get(key)


    def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
        """

        """
        self._user_meta.update(key_value_dict)
        self._modified = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.sync()

    def sync(self):
        """

        """
        if self._modified:
            int_us = utils.make_timestamp()

            if isinstance(self._book_hash, str):
                if not self._bookcase.remote_s3_access:
                    self._bookcase._meta['books'][self._book_hash]['last_modified'] += 1
                else:
                    self._bookcase._meta['books'][self._book_hash]['last_modified'] = int_us
                # self._bookcase._meta['books'][self._book_hash]['last_modified'] = int_us
                self._bookcase._meta['books'][self._book_hash]['user_metadata'] = self._user_meta
            else:
                if not self._bookcase.remote_s3_access:
                    self._bookcase._meta['last_modified'] += 1
                else:
                    self._bookcase._meta['last_modified'] = int_us

                self._bookcase._meta['user_metadata'] = self._user_meta

            utils.write_metadata(self._bookcase._local_meta_path, self._bookcase._meta)




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


class Bookcase:
    """

    """
    def __init__(self,
                 local_db_path: Union[str, pathlib.Path],
                 remote_url: str=None,
                 write: bool = False,
                 remote_db_key: str=None,
                 bucket: str=None,
                 connection_config: Union[s3func.utils.S3ConnectionConfig, s3func.utils.B2ConnectionConfig]=None,
                 value_serializer: str = None,
                 buffer_size: int=524288,
                 read_timeout: int=120,
                 threads: int=20,
                 lock_timeout=-1,
                 break_other_locks=False,
                 **local_storage_kwargs,
                 ):
        """

        """
        ## Pre-processing
        if local_db_path is None:
            temp_path = pathlib.Path(tempfile.TemporaryDirectory().name)
            local_meta_path = temp_path.joinpath('temp.bcs')
            self._finalizer = weakref.finalize(self, shutil.rmtree, temp_path, True)
        else:
            local_meta_path = pathlib.Path(local_db_path)
            temp_path = None

        # local_meta_path = pathlib.Path(local_db_path)
        remote_keys_name = local_meta_path.name + '.remote_keys'
        remote_keys_path = local_meta_path.parent.joinpath(remote_keys_name)

        for key, value in local_storage_kwargs.items():
            if key not in utils.local_storage_options:
                raise ValueError(f'{key} in local_storage_kwargs, but it must only contain {utils.local_storage_options}.')
        if 'n_buckets' not in local_storage_kwargs:
            n_buckets = utils.default_n_buckets
            local_storage_kwargs['n_buckets'] = n_buckets
        else:
            n_buckets = int(local_storage_kwargs['n_buckets'])
        local_storage_kwargs.update({'key_serializer': 'str', 'value_serializer': 'bytes'})
        if value_serializer in booklet.serializers.serial_name_dict:
            value_serializer_code = booklet.serializers.serial_name_dict[value_serializer]
        else:
            raise ValueError(f'value_serializer must be one of {booklet.available_serializers}.')

        ## Check the remote config
        http_session, s3_session, remote_s3_access, remote_http_access, host_url, remote_base_url = utils.init_remote_config(bucket, connection_config, remote_url, threads, read_timeout)

        ## Create S3 lock for writes
        if write and remote_s3_access:
            lock = s3func.s3.S3Lock(connection_config, bucket, remote_db_key, read_timeout=read_timeout)
            if break_other_locks:
                lock.break_other_locks()
            if not lock.aquire(timeout=lock_timeout):
                raise TimeoutError('S3Lock timed out')
        else:
            lock = None

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.bookcase_finalizer, temp_path, lock)

        ## Init metadata
        meta, meta_in_remote = utils.init_metadata(local_meta_path, remote_keys_path, write, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs)

        ## Init local storage
        # local_data_path = utils.init_local_storage(local_meta_path, flag, meta)

        ## Assign properties
        # self._temp_path = temp_path
        self._meta_in_remote = meta_in_remote
        self._remote_db_key = remote_db_key
        self._n_buckets = n_buckets
        self._write = write
        self._buffer_size = buffer_size
        self._connection_config = connection_config
        self._read_timeout = read_timeout
        self._lock = lock
        self.remote_s3_access = remote_s3_access
        self.remote_http_access = remote_http_access
        self._bucket = bucket
        self._meta = meta
        self._threads = threads
        self._local_meta_path = local_meta_path
        self._remote_keys_path = remote_keys_path
        self._value_serializer = value_serializer
        self._value_serializer_code = value_serializer_code
        self._local_storage_kwargs = local_storage_kwargs
        self._host_url = host_url
        self._remote_base_url = remote_base_url
        self._remote_url = remote_url
        self._s3_session = s3_session
        self._http_session = http_session

        ## Assign the metadata object for global
        self.metadata = UserMetadata(self)


    @property
    def default_book_name(self):
        """

        """
        if self._meta['default_book']:
            return self._meta['books'][self._meta['default_book']]['name']

    def list_book_names(self):
        """

        """
        for key, val in self._meta['books'].items():
            yield val['name']


    def set_default_book_name(self, book_name):
        """

        """
        book_hash = utils.hash_book_name(book_name)
        if book_hash in self._meta['books']:
            self._meta['default_book'] = book_hash
            # meta_bytes = zstd.compress(orjson.dumps(self._meta, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY), level=1)
            # with io.open(self._local_meta_path, 'wb') as f:
            #     f.write(meta_bytes)
        else:
            raise KeyError(book_name)


    # def create_book(self, book_name):
    #     """
    #     Remove
    #     """
    #     meta = utils.create_book(self._local_meta_path, self._meta, book_name, self.remote_s3_access)
    #     self._meta = meta

    #     return True


    def open_book(self, book_name: str=None, flag: str='r'):
        """
        Remove the create_book method and include a flag parameter.
        """
        if book_name is None:
            if flag == 'r':
                book_hash = self._meta['default_book']
                if book_hash is None:
                    raise KeyError('No books exist. Open with write permissions to create a book.')
            else:
                raise KeyError('book_name must be specified when open for writing.')
        else:
            book_hash = utils.hash_book_name(book_name)

        if flag in ('n', 'c'):
            if flag == 'c':
                if book_hash in self._meta['books']:
                    raise KeyError(f'{book_name} already exists as a book.')
            meta = utils.create_book(self._local_meta_path, self._meta, book_name, book_hash, self.remote_s3_access)
            self._meta = meta

        book = Book(self, book_hash)

        return book


    def close(self):
        """

        """
        if self._flag != 'r':
            self.metadata.close()

        self._finalizer()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def pull_remote_index(self, book_name):
        """

        """
        ## Get base path for the book
        if book_name is None:
            book_hash = self._meta['default_book']
        else:
            book_hash = utils.hash_book_name(book_name)
            if book_hash not in self._meta['books']:
                raise KeyError(book_name)

        book_file_name = self._local_meta_path.name + f'.{book_hash}.'
        book_base_path = self._local_meta_path.parent.joinpath(book_file_name)

        remote_index_path = utils.get_remote_index_file(book_base_path, book_hash, self._remote_db_key, self._remote_url, self._http_session, self._s3_session, self.remote_http_access, self.remote_s3_access, True)

        return remote_index_path


    def pull_metadata(self):
        """

        """
        if self._meta_in_remote:
            self.metadata.sync()
            meta, meta_in_remote = utils.init_metadata(self._local_meta_path, self._remote_keys_path, self._flag, self._write, self._http_session, self._s3_session, self._remote_s3_access, self._remote_http_access, self._remote_url, self._remote_db_key, self._value_serializer, self._local_storage_kwargs)
            return True
        else:
            return False


class Book(MutableMapping):
    """

    """
    def __init__(
            self,
            # local_db_path: Union[str, pathlib.Path],
            # remote_url: HttpUrl=None,
            # flag: str = "r",
            # remote_db_key: str=None,
            # bucket: str=None,
            # connection_config: Union[s3func.utils.S3ConnectionConfig, s3func.utils.B2ConnectionConfig]=None,
            # value_serializer: str = None,
            # buffer_size: int=524288,
            # read_timeout: int=60,
            # threads: int=20,
            # **local_storage_kwargs,
            bookcase: Bookcase,
            book_hash: str,
            flag: str
            ):
        """

        """
        book_file_name = bookcase._local_meta_path.name + f'.{book_hash}.'
        book_base_path = bookcase._local_meta_path.parent.joinpath(book_file_name)

        ## Init local storage if necessary
        local_data_path, local_index_path = utils.init_local_storage(book_base_path, flag, bookcase._meta)

        ## Open local files
        local_data = booklet.VariableValue(local_data_path, flag='w')
        local_index = booklet.FixedValue(local_index_path, flag='w')

        ## Get/init remote keys file
        remote_index_path = remote_index_path = utils.get_remote_index_file(book_base_path, book_hash, bookcase._remote_db_key, bookcase._remote_url, bookcase._http_session, bookcase._s3_session, bookcase.remote_http_access, bookcase.remote_s3_access, False)

        ## Open remote keys file
        if remote_index_path.exists():
            remote_index = booklet.FixedValue(remote_index_path)
        else:
            remote_index = None

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.book_finalizer, local_data, local_index, remote_index)

        ## Assign properties
        # self._n_buckets = session._n_buckets
        # self._write = session._write
        # self._buffer_size = session._buffer_size
        # self._s3_session = s3_session
        # self._http_session = http_session
        # self._remote_s3_access = session._remote_s3_access
        # self._remote_http_access = session._remote_http_access
        # self._bucket = bucket
        # self._meta = meta
        self._flag = flag
        self._bookcase = bookcase
        self._write = bookcase._write
        self._threads = bookcase._threads
        # self._local_meta_path = session._local_meta_path
        # self._local_data_path = session._local_data_path
        # self._remote_keys_path = session._remote_keys_path
        self._book_base_path = book_base_path
        self._remote_index_path = remote_index_path
        self._local_data_path = local_data_path
        self._local_index_path = local_index_path
        self._local_index = local_index
        self._local_data = local_data
        self._remote_index = remote_index
        self._deletes = list()
        self._value_serializer = booklet.serializers.serial_int_dict[bookcase._value_serializer_code]
        # self._value_serializer = session._value_serializer

        # self._manager = multiprocessing.Manager()
        # self._lock = self._manager.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=bookcase._threads)

        ## Assign the metadata object for the version
        self.metadata = UserMetadata(bookcase, book_hash)


    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        try:
            value = self._value_serializer.dumps(value)
        except Exception as error:
            raise error

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        value = self._value_serializer.loads(value)

        return value


    def keys(self):
        """

        """
        if self._remote_index:
            return self._remote_index.keys()
        else:
            return self._local_data.keys()


    def items(self):
        """

        """
        if self._remote_index:
            futures = {}
            for key in self:
                f = self._executor.submit(utils.load_value, self._local_data, self._local_index, self._remote_index, key, self._bookcase.remote_s3_access, self._bookcase.remote_http_access, self._bookcase._s3_session, self._bookcase._http_session, self._bookcase._host_url, self._bookcase._remote_base_url)
                futures[f] = key

            for f in concurrent.futures.as_completed(futures):
                key = futures[f]
                check_key = f.result()
                if check_key is None:
                    raise KeyError(f'{key} not found in remote...but it should be there...')
                else:
                    yield key, self._post_value(self._local_data[key])

        else:
            return self._local_data.items()

    def values(self):
        if self._remote_index:
            futures = {}
            for key in self:
                f = self._executor.submit(utils.load_value, self._local_data, self._local_index, self._remote_index, key, self._bookcase.remote_s3_access, self._bookcase.remote_http_access, self._bookcase._s3_session, self._bookcase._http_session, self._bookcase._host_url, self._bookcase._remote_base_url)
                futures[f] = key

            for f in concurrent.futures.as_completed(futures):
                key = futures[f]
                check_key = f.result()
                if check_key is None:
                    raise KeyError(f'{key} not found in remote...but it should be there...')
                else:
                    yield self._post_value(self._local_data[key])

        else:
            return self._local_data.values()


    def __iter__(self):
        return self.keys()

    def __len__(self):
        """
        
        """
        if self._remote_index:
            return len(self._remote_index)
        else:
            return len(self._local_data)


    def __contains__(self, key):
        if self._remote_index:
            return key in self._remote_index
        else:
            return key in self._local_data


    def get(self, key, default=None):
        check_key = utils.load_value(self._local_data, self._local_index, self._remote_index, key, self._bookcase.remote_s3_access, self._bookcase.remote_http_access, self._bookcase._s3_session, self._bookcase._http_session, self._bookcase._host_url, self._bookcase._remote_base_url)
        if check_key is None:
            return default
        else:
            yield self._post_value(self._local_data[key])


    def update(self, key_value_dict: dict):
        """

        """
        if self._write:
            for key, value in key_value_dict.items():
                self[key] = value
        else:
            raise ValueError('File is open for read only.')


    # def prune(self):
    #     """
    #     Hard deletes files with delete markers.
    #     """
    #     if self._write:
    #             return self._local_data.prune()
    #     else:
    #         raise ValueError('File is open for read only.')

    def get_items(self, keys, default=None):
        """

        """
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.load_value, self._local_data, self._local_index, self._remote_index, key, self._bookcase.remote_s3_access, self._bookcase.remote_http_access, self._bookcase._s3_session, self._bookcase._http_session, self._bookcase._host_url, self._bookcase._remote_base_url)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            key = futures[f]
            check_key = f.result()
            if check_key is None:
                yield key, default
            else:
                yield key, self._post_value(self._local_data[key])


    def __getitem__(self, key: str):
        check_key = utils.load_value(self._local_data, self._local_index, self._remote_index, key, self._bookcase.remote_s3_access, self._bookcase.remote_http_access, self._bookcase._s3_session, self._bookcase._http_session, self._bookcase._host_url, self._bookcase._remote_base_url)

        if check_key is None:
            raise KeyError(f'{key}')
        else:
            return self._post_value(self._local_data[key])


    def __setitem__(self, key: str, value):
        if self._write:
            if self._bookcase.remote_s3_access:
                int_us = utils.make_timestamp()
            else:
                old_val = self._local_index.get(key)
                if old_val:
                    int_us = utils.bytes_to_int(old_val) + 1
                else:
                    int_us = 0
            val_bytes = self._pre_value(value)
            self._local_data[key] = val_bytes
            self._local_index[key] = utils.int_to_bytes(int_us, 7)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            if self._remote_index:
                del self._remote_index[key]
                # self._deletes.append(key)

            if key in self._local_data:
                del self._local_data[key]
                if self._local_index:
                    del self._local_index[key]
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self, local_only=True):
        self._local_data.clear()
        if self._local_index:
            self._local_index.clear()

        if self._write and not local_only:
            if self._remote_index:
                self._remote_index.clear()

    def close(self, force_close=False):
        self._executor.shutdown(cancel_futures=force_close)
        # self._manager.shutdown()
        self._finalizer()


    # def __del__(self):
    #     self.close()

    def sync(self):
        self._executor.shutdown()
        del self._executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._threads)
        if self._remote_index:
            self._remote_index.sync()
        self._local_data.sync()


    def pull_remote_index(self):
        """
    
        """
        remote_index_path = utils.get_remote_index_file(self._book_base_path, self._book_hash, self._remote_db_key, self._remote_url, self._http_session, self._s3_session, self.remote_http_access, self.remote_s3_access, True)
    
        return remote_index_path


    def changes(self):
        return Change(self)



def open(
    bucket: str, connection_config: Union[s3func.utils.S3ConnectionConfig, s3func.utils.B2ConnectionConfig]=None, public_url: str=None, flag: str = "r", buffer_size: int=512000, retries: int=3, read_timeout: int=120, provider: str=None, threads: int=30, compression: bool=True, cache: MutableMapping=None, return_bytes: bool=False):
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

    compression : bool
        Should automatic compression/decompression be applied given specific file name extensions. Currently, it can only handle zstandard with zstd and zst extensions. Defaults to True.

    cache : MutableMapping or None
        The read cache for S3 objects. It can be any kind of MutableMapping object including a normal Python dict.

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
    return S3DBM(bucket, connection_config, public_url, flag, buffer_size, retries, read_timeout, provider, threads, compression, cache, return_bytes)
