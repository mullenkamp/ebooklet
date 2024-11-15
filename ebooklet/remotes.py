#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 08:36:26 2024

@author: mike
"""
import os
import uuid6 as uuid
import urllib3
import booklet
from typing import Any, Generic, Iterator, Union, List, Dict
import s3func
import weakref

# import utils
from . import utils



###############################################
### Classes


class BaseRemote:
    """

    """
    # def __enter__(self):
    #     return self

    # def __exit__(self, *args):
    #     self.close()

    def open(self):
        """

        """
        raise NotImplementedError()


class BaseRemoteRead:
    """

    """
    def __bool__(self):
        """
        Test to see if remote read access is possible. Same as .read_access.
        """
        return self.read_access

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Close the remote connection. Should return None.
        """
        self._finalizer()

    def _parse_db_object(self):
        """

        """
        resp_obj = self._session.get_object(self.db_key)
        if resp_obj.status == 200:
            self._init_bytes = resp_obj.data

            self.timestamp = booklet.utils.bytes_to_int(self._init_bytes[booklet.utils.file_timestamp_pos:booklet.utils.file_timestamp_pos + booklet.utils.timestamp_bytes_len])

            self.uuid = uuid.UUID(bytes=bytes(self._init_bytes[49:65]))
        elif resp_obj.status == 404:
            self._init_bytes = None
            self.uuid = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

    def head_db_object(self):
        """

        """
        resp_obj = self._session.head_object(self.db_key)

        return resp_obj


    def get_timestamp_db_object(self):
        """

        """
        resp_obj = self.head_db_object()
        if resp_obj.status == 200:
            self.timestamp = int(resp_obj.metadata['timestamp'])

            # self.uuid = uuid.UUID(bytes=bytes(self._init_bytes[49:65]))
        elif resp_obj.status == 404:
            self.timestamp = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)


    # @property
    def uuid(self):
        """
        UUID of the remote object
        """
        raise NotImplementedError()

    # @property
    def timestamp(self):
        """
        Timestamp as int_us of the last modified date
        """
        raise NotImplementedError()

    # @property
    def readable(self):
        """
        Test to see if remote read access is possible. Returns a bool.
        """
        return True

    # @property
    def writable(self):
        """
        Test to see if remote write access is possible. Returns a bool.
        """
        return False

    def get_db_index_object(self):
        """

        """
        return self._session.get_object(self.db_key + '.remote_index')

    def get_db_object(self):
        """

        """
        return self._session.get_object(self.db_key)

    def get_object(self, key: str):
        """
        Get a remote object/file. The input should be a key as a str. It should return an object with a .status attribute as an int, a .data attribute in bytes, and a .error attribute as a dict.
        """
        return self._session.get_object(self.db_key + '/' + key)

    def head_object(self, key: str):
        """
        Get the header for a remote object/file. The input should be a key as a str.
        """
        return self._session.head_object(self.db_key + '/' + key)


class BaseRemoteReadWrite(BaseRemoteRead):
    """

    """
    @property
    def writable(self):
        """

        """
        if not self._writable_check:
            test_key = self.db_key + uuid.uuid6().hex[:13]
            put_resp = self._session.put_object(test_key, b'0')
            if put_resp.status // 100 == 2:
                self._writable = True
                _ = self._session.delete_object(test_key, put_resp.metadata['version_id'])

            self._writable_check = True

        return self._writable


    def put_db_object(self, data: bytes, metadata={}):
        """

        """
        if self.writable:
            return self._session.put_object(self.db_key, data, metadata=metadata)
        else:
            raise ValueError('S3Remote is not writable.')

    def put_db_index_object(self, data: bytes, metadata={}):
        """

        """
        if self.writable:
            return self._session.put_object(self.db_key + '.remote_index', data, metadata=metadata)
        else:
            raise ValueError('S3Remote is not writable.')

    def put_object(self, key: str, data: bytes, metadata={}):
        """

        """
        if self.writable:
            return self._session.put_object(self.db_key + '/' + key, data, metadata=metadata)
        else:
            raise ValueError('S3Remote is not writable.')


    def delete_objects(self, keys):
        """
        Delete objects
        """
        if self.writable:
            del_list = []
            resp = self._session.list_object_versions(prefix=self.db_key + '/')
            for obj in resp.iter_objects():
                key0 = obj['key']
                key = key0.split('/')[-1]
                if key in keys:
                    del_list.append({'Key': key0, 'VersionId': obj['version_id']})

            if del_list:
                del_resp = self._session.delete_objects(del_list)
                if del_resp.status // 100 != 2:
                    raise urllib3.exceptions.HTTPError(del_resp.error)
        else:
            raise ValueError('S3Remote is not writable.')


    def delete_remote(self):
        """

        """
        if self.writable:
            del_list = []
            resp = self._session.list_object_versions(prefix=self.db_key)
            for obj in resp.iter_objects():
                key0 = obj['key']
                del_list.append({'Key': key0, 'VersionId': obj['version_id']})

            self._session.delete_objects(del_list)
            self._init_bytes = None
            self.uuid = None
        else:
            raise ValueError('S3Remote is not writable.')


class BaseS3RemoteReadWrite(BaseRemoteReadWrite):
    """

    """
    def list_objects(self):
        """

        """
        return self._session.list_objects(prefix=self.db_key)


    def list_object_versions(self):
        """

        """
        return self._session.list_object_versions(prefix=self.db_key)

    def lock(self):
        """

        """
        if self.writable:
            lock = self._session.s3lock(self.db_key)
            return lock
        else:
            raise ValueError('S3Remote is not writable.')


class S3Remote(BaseRemote):
    """

    """
    def __init__(self,
                db_key: str,
                bucket: str,
                connection_config: Union[s3func.utils.S3ConnectionConfig, s3func.utils.B2ConnectionConfig],
                threads: int=20,
                read_timeout: int=60,
                retries: int=3,
                ):
        """

        """
        ## Assign properties
        self.db_key = db_key
        self.bucket = bucket
        self.connection_config = connection_config
        self.threads = threads
        self.read_timeout = read_timeout
        self.retries = retries
        self.type = 's3_remote'
        # self._lock_timeout = lock_timeout
        # self.lock = lock

        ## Get remote bytes
        self._parse_db_object()


    def _parse_db_object(self):
        """

        """
        session = s3func.S3Session(self.connection_config, self.bucket, self.threads, read_timeout=self.read_timeout, stream=False, max_attempts=self.retries)

        resp_obj = session.get_object(self.db_key)
        if resp_obj.status == 200:
            self._init_bytes = resp_obj.data

            # self.timestamp = booklet.utils.bytes_to_int(self._init_bytes[booklet.utils.file_timestamp_pos:booklet.utils.file_timestamp_pos + booklet.utils.timestamp_bytes_len])

            self.uuid = uuid.UUID(bytes=bytes(self._init_bytes[49:65]))
        elif resp_obj.status == 404:
            self._init_bytes = None
            self.uuid = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

    def open(self, check_timestamp=True):
        """

        """
        return S3RemoteReadWrite(self, check_timestamp)


class S3RemoteReadWrite(BaseS3RemoteReadWrite):
    """

    """
    def __init__(self,
                 s3_remote,
                 check_timestamp=True,
                ):
        """

        """
        ## Set up the session
        session = s3func.S3Session(s3_remote.connection_config, s3_remote.bucket, s3_remote.threads, read_timeout=s3_remote.read_timeout, stream=False, max_attempts=s3_remote.retries)
        self._session = session

        self.db_key = s3_remote.db_key
        self._init_bytes = s3_remote._init_bytes
        self.uuid = s3_remote.uuid

        ## Check for read and write access
        # self._readable_check = False
        self._writable_check = False
        # self._readable = False
        self._writable = False

        # resp_obj = self.get_db_object()
        # if resp_obj.status == 200:
        #     self.readable = True

        #     self._init_bytes = resp_obj.data

        #     self.timestamp = booklet.utils.bytes_to_int(self._init_bytes[booklet.utils.file_timestamp_pos:booklet.utils.file_timestamp_pos + booklet.utils.timestamp_bytes_len])

        #     self.uuid = uuid.UUID(bytes=bytes(self._init_bytes[49:65]))

            # if object_lock:
            #     lock = session.s3lock(db_key)

            #     if break_other_locks:
            #         lock.break_other_locks()

            #     lock.aquire(timeout=lock_timeout)

            # put_resp = session.put_object(test_key, b'0')
            # if put_resp.status == 200:
            #     self.writable = True
            #     _ = session.delete_object(test_key, put_resp.metadata['version_id'])

        if check_timestamp:
            self.get_timestamp_db_object()
        else:
            self.timestamp = None

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.s3remote_finalizer, self._session)

        ## Assign properties
        self._bucket = s3_remote.bucket
        self._connection_config = s3_remote.connection_config
        self._threads = s3_remote.threads
        self._read_timeout = s3_remote.read_timeout
        self._retries = s3_remote.retries
        self.type = 's3_remote_open'
        self._remote = s3_remote
        # self._lock_timeout = lock_timeout
        # self.lock = lock


class HttpRemote(BaseRemote):
    """
    Only get requests work.
    """
    def __init__(self,
                db_url: str,
                threads: int=20,
                read_timeout: int=60,
                retries: int=3,
                headers=None
                ):
        """

        """
        ## Assign properties
        self.db_key = db_url
        self.threads = threads
        self.read_timeout = read_timeout
        self.retries = retries
        self._headers = headers
        self.type = 'http_remote'

        ## Get remote bytes
        self._parse_db_object()


    def _parse_db_object(self):
        """

        """
        session = s3func.HttpSession(self.threads, read_timeout=self.read_timeout, stream=False, max_attempts=self.retries)

        resp_obj = session.get_object(self.db_key)
        if resp_obj.status == 200:
            self._init_bytes = resp_obj.data

            # self.timestamp = booklet.utils.bytes_to_int(self._init_bytes[booklet.utils.file_timestamp_pos:booklet.utils.file_timestamp_pos + booklet.utils.timestamp_bytes_len])

            self.uuid = uuid.UUID(bytes=bytes(self._init_bytes[49:65]))
        elif resp_obj.status == 404:
            self._init_bytes = None
            self.uuid = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

    def open(self, check_timestamp=True):
        """

        """
        return HttpRemoteRead(self, check_timestamp)


class HttpRemoteRead(BaseRemoteRead):
    """
    Only get requests work.
    """
    def __init__(self,
                http_remote,
                check_timestamp=True,
                ):
        """

        """
        # self._readable_check = False
        # self._writable_check = False
        # self._readable = False
        self.writable = False

        session = s3func.HttpSession(http_remote.threads, read_timeout=http_remote.read_timeout, stream=False, max_attempts=http_remote.retries)
        self._session = session
        self.db_key = http_remote.db_key
        self._init_bytes = http_remote._init_bytes
        self.uuid = http_remote.uuid

        if check_timestamp:
            self.get_timestamp_db_object()
        else:
            self.timestamp = None

        self._finalizer = weakref.finalize(self, session._session.clear)

        ## Assign properties
        self._threads = http_remote.threads
        self._read_timeout = http_remote.read_timeout
        self._retries = http_remote.retries
        self._headers = http_remote._headers
        self.type = 'http_remote_open'
































































