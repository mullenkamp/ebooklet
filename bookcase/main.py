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

import utils
# from . import utils

# uuid_s3dbm = b'K=d:\xa89F(\xbc\xf5 \xd7$\xbd;\xf2'
# version = 1
# version_bytes = version.to_bytes(2, 'little', signed=False)

#######################################################
### Classes


class UserMetadata(MutableMapping):
    """

    """
    def __init__(self, local_meta_path, metadata: dict, version_date: str=None):
        """

        """
        version_position = 0
        user_meta = None

        if isinstance(version_date, str):
            for i, v in enumerate(metadata['versions']):
                if v['version_date'] == version_date:
                    user_meta = v['user_metadata']
                    version_position = i

            if user_meta is None:
                   raise ValueError('version_date is not in the metadata.')
        else:
            user_meta = metadata['user_metadata']

        self._metadata = metadata
        self._user_meta = user_meta
        self._version_date = version_date
        self._version_position = version_position
        self._modified = False
        self._local_meta_path = local_meta_path


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
            self._metadata['last_modified'] = int_us
            if self._version_date:
                self._metadata['versions'][self._version_position] = {'versions_date': self._version_date, 'user_metadata': self._user_meta}
            else:
                self._metadata['user_metadata'] = self._user_meta

            with io.open(self._local_meta_path, 'wb') as f:
                f.write(zstd.compress(orjson.dumps(self._metadata, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY)))


class Session:
    """

    """
    def __init__(self,
                 local_db_path: Union[str, pathlib.Path],
                 remote_url: str=None,
                 flag: str = "r",
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
        if flag == "r":  # Open existing database for reading only (default)
            write = False
        elif flag == "w":  # Open existing database for reading and writing
            write = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            write = True
        else:
            raise ValueError("Invalid flag")

        ## Pre-processing
        local_meta_path = pathlib.Path(local_db_path)
        remote_keys_name = local_meta_path.name + '.remote_keys'
        remote_keys_path = local_meta_path.parent.joinpath(remote_keys_name)

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
        http_session, s3_session, remote_s3_access, remote_http_access, host_url, remote_base_url = utils.init_remote_config(flag, bucket, connection_config, remote_url, threads, read_timeout)

        ## Create S3 lock for writes
        if flag != 'r':
            lock = s3func.s3.S3Lock(connection_config, bucket, remote_db_key, read_timeout=read_timeout)
            if break_other_locks:
                lock.break_other_locks()
            lock.aquire(timeout=lock_timeout)
        else:
            lock = None

        ## Init metadata
        meta, meta_in_remote, version_date = utils.init_metadata(local_meta_path, remote_keys_path, http_session, s3_session, remote_s3_access, remote_http_access, remote_url, remote_db_key, value_serializer, local_storage_kwargs)

        ## Init local storage
        local_data_path = utils.init_local_storage(local_meta_path, flag, meta)

        ## Assign properties
        self._meta_in_remote = meta_in_remote
        self._version_date = version_date
        self._remote_db_key = remote_db_key
        self._flag = flag
        self._n_buckets = n_buckets
        self._write = write
        self._buffer_size = buffer_size
        self._connection_config = connection_config
        self._read_timeout = read_timeout
        self._lock = lock
        self._remote_s3_access = remote_s3_access
        self._remote_http_access = remote_http_access
        self._bucket = bucket
        self._meta = meta
        self._threads = threads
        self._local_meta_path = local_meta_path
        self._remote_keys_path = remote_keys_path
        self._local_data_path = local_data_path
        self._value_serializer_code = value_serializer_code
        self._local_storage_kwargs = local_storage_kwargs

        ## Assign the metadata object for global
        self.metadata = UserMetadata(local_meta_path, meta)


    def open(self):
        """

        """
        s3dbm = S3dbm(self)

        return s3dbm


    def close(self):
        """

        """
        ## Remove lock
        if self._flag != 'r':
            self.metadata.close()
            # TODO - Update remote here!
            self._lock.release()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class S3dbm(MutableMapping):
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
            session: Session
            ):
        """

        """
        ## Open local data
        local_data = booklet.VariableValue(session._local_data_path, flag='w')

        ## Open remote keys file
        if session._meta_in_remote:
            remote_keys = booklet.FixedValue(session._remote_keys_path)
        else:
            remote_keys = None

        ## Init the remote sessions
        if session._remote_http_access:
            http_session = s3func.HttpSession(session._threads, read_timeout=session._read_timeout, stream=False)
        else:
            http_session = None
        if session._remote_s3_access:
            s3_session = s3func.S3Session(session._connection_config, session._bucket, session._threads, read_timeout=session._read_timeout, stream=False)
        else:
            s3_session = None

        ## Assign properties
        # self._n_buckets = session._n_buckets
        self._changelog = None
        # self._write = session._write
        # self._buffer_size = session._buffer_size
        self._s3_session = s3_session
        self._http_session = http_session
        # self._remote_s3_access = session._remote_s3_access
        # self._remote_http_access = session._remote_http_access
        # self._bucket = bucket
        # self._meta = meta
        self._session = session
        # self._threads = session._threads
        # self._local_meta_path = session._local_meta_path
        # self._local_data_path = session._local_data_path
        # self._remote_keys_path = session._remote_keys_path
        self._local_data = local_data
        self._remote_keys = remote_keys
        self._deletes = list()
        self._value_serializer = booklet.serializers.serial_int_dict[session._value_serializer_code]
        # self._value_serializer = session._value_serializer

        # self._manager = multiprocessing.Manager()
        # self._lock = self._manager.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=session._threads)

        ## Assign the metadata object for the version
        self.metadata = UserMetadata(session._local_meta_path, session._meta, session._version_date)


    def _pre_value(self, value) -> bytes:

        ## Serialize to bytes
        try:
            value = self._session._value_serializer.dumps(value)
        except Exception as exc:
            raise utils.SerializeError(exc, self)

        return value

    def _post_value(self, value: bytes):

        ## Serialize from bytes
        value = self._session._value_serializer.loads(value)

        return value


    def keys(self):
        """

        """
        if self._remote_keys:
            return self._remote_keys.keys()
        else:
            return self._local_data.keys()


    def items(self, keys: List[str]=None):
        """

        """
        if self._remote_keys:
            pass
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache, self._return_bytes)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield futures[f], f.result()


    def values(self, keys: List[str]=None):
        if keys is None:
            keys = self.keys(prefix, start_after, delimiter)
        futures = {}
        for key in keys:
            f = self._executor.submit(utils.get_object_final, key, self._bucket, self._client, self._public_url, self._buffer_size, self._read_timeout, self._provider, self._compression, self._cache, self._return_bytes)
            futures[f] = key

        for f in concurrent.futures.as_completed(futures):
            yield f.result()


    def __iter__(self):
        return self.keys()

    def __len__(self):
        """
        There really should be a better way for this...
        """
        params = {'Bucket': self._bucket}

        count = 0

        while True:
            js1 = self._client.list_objects_v2(**params)

            if 'Contents' in js1:
                count += len(js1['Contents'])

                if 'NextContinuationToken' in js1:
                    params['ContinuationToken'] = js1['NextContinuationToken']
                else:
                    break
            else:
                break

        return count


    def __contains__(self, key):
        if self._remote_hash:
            return key in self._remote_keys
        else:
            return key in self._local_data


    def get(self, key, default=None):
        value = utils.get_value(self._local_data, self._remote_keys, key, self._bucket, self._s3, self._session, self._hiost_url, self._remote_base_url)

        if value is None:
            return default
        else:
            return self._post_value(value)


    def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
        """

        """
        if self._write:
            with self._lock:
                futures = {}
                for key, value in key_value_dict.items():
                    if isinstance(value, bytes):
                        value = io.BytesIO(value)
                    f = self._executor.submit(utils.put_object_s3, self._client, self._bucket, key, value, self._buffer_size, self._compression)
                    futures[f] = key
        else:
            raise ValueError('File is open for read only.')


    def prune(self):
        """
        Hard deletes files with delete markers.
        """
        if self._write:
            with self._lock:
                deletes_list = []
                files, dms = utils.list_object_versions_s3(self._client, self._bucket, delete_markers=True)

                d_keys = {dm['Key']: dm['VersionId'] for dm in dms}

                if d_keys:
                    for key, vid in d_keys.items():
                        deletes_list.append({'Key': key, 'VersionId': vid})

                    for file in files:
                        if file['Key'] in d_keys:
                            deletes_list.append({'Key': file['Key'], 'VersionId': file['VersionId']})

                    for i in range(0, len(deletes_list), 1000):
                        d_chunk = deletes_list[i:i + 1000]
                        _ = self._client.delete_objects(Bucket=self._bucket, Delete={'Objects': d_chunk, 'Quiet': True})

                return deletes_list
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key: str):
        value = utils.get_value(self._local_data, self._remote_keys, key, self._bucket, self._s3, self._session, self._hiost_url, self._remote_base_url)

        if value is None:
            raise utils.S3dbmKeyError(f'{key} does not exist.', self)
        else:
            return self._post_value(value)


    def __setitem__(self, key: str, value):
        if self._write:
            dt_us_int = utils.make_timestamp()
            val_bytes = self._pre_value(value)
            self._local_data[key] = utils.int_to_bytes(dt_us_int, 7) + val_bytes
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key):
        if self._write:
            if self._remote_keys:
                del self._remote_keys[key]
                self._deletes.append(key)

            if key in self._local_data:
                del self._local_data[key]
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self, are_you_sure=False):
        if self._write:
            if are_you_sure:
                with self._lock:
                    files, dms = utils.list_object_versions_s3(self._client, self._bucket, delete_markers=True)

                    d_keys = {dm['Key']: dm['VersionId'] for dm in dms}

                    if d_keys:
                        deletes_list = []
                        for key, vid in d_keys.items():
                            deletes_list.append({'Key': key, 'VersionId': vid})

                        for file in files:
                            deletes_list.append({'Key': file['Key'], 'VersionId': file['VersionId']})

                        for i in range(0, len(deletes_list), 1000):
                            d_chunk = deletes_list[i:i + 1000]
                            _ = self._client.delete_objects(Bucket=self._bucket, Delete={'Objects': d_chunk, 'Quiet': True})
            else:
                raise ValueError("I don't think you're sure...this will delete all objects in the bucket...")
        else:
            raise ValueError('File is open for read only.')

    def close(self, force_close=False):
        self._executor.shutdown(cancel_futures=force_close)
        # self._manager.shutdown()
        utils.close_files(self._local_data, self._remote_keys)


    # def __del__(self):
    #     self.close()

    def sync(self):
        self._executor.shutdown()
        del self._executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._threads)
        # if self._remote_keys:
        #     self._remote_keys.sync()
        self._local_data.sync()

    # def flush(self):
    #     self.sync()


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
