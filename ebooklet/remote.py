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
import orjson
import base64
import copy
import datetime
import concurrent.futures
# import msgspec

# import utils
from . import utils


###############################################
### Parameters

ebooklet_types = ('EVariableLengthValue', 'RemoteConnGroup')


###############################################
### Functions


def check_remote_conn(remote_conn, flag):
    """

    """
    if isinstance(remote_conn, str):
        if flag != 'r':
            raise ValueError('If remote_conn is a url string, then flag must be r.')
        remote_conn = S3Connection(db_url=remote_conn)
    elif isinstance(remote_conn, dict):
        if 'remote_conn' in remote_conn:
            remote_conn = S3Connection(**remote_conn['remote_conn'])
        else:
            remote_conn = S3Connection(**remote_conn)
    elif not isinstance(remote_conn, S3Connection):
        raise TypeError('remote_conn must be either a url string or a remote.S3Connection.')

    return remote_conn


def get_db_metadata(session, db_key):
    """

    """
    resp_obj = session.head_object(db_key)
    if resp_obj.status == 200:

        meta = resp_obj.metadata
    elif resp_obj.status == 404:
        meta = None
    else:
        raise urllib3.exceptions.HTTPError(resp_obj.error)

    if meta is None:
        return dict(uuid=None, timestamp=None, ebooklet_type=None)
    else:
        meta['uuid'] = uuid.UUID(hex=meta['uuid'])
        # self.ebooklet_type = meta['ebooklet_type']
        meta['timestamp'] = int(meta['timestamp'])

        return meta


def get_user_metadata(session, db_key):
    """

    """
    resp_obj = session.get_object(f'{db_key}/{booklet.utils.metadata_key_bytes.decode()}')
    if resp_obj.status == 200:

        meta = orjson.loads(resp_obj.data)
    elif resp_obj.status == 404:
        meta = None
    else:
        raise urllib3.exceptions.HTTPError(resp_obj.error)

    return meta


def check_write_config(
        access_key_id: str=None,
        access_key: str=None,
        db_key: str=None,
        bucket: str=None,
        endpoint_url: str=None,
        ):
    """

    """
    if isinstance(access_key_id, str) and isinstance(access_key, str) and isinstance(db_key, str) and isinstance(bucket, str):
        if isinstance(endpoint_url, str):
            if not s3func.utils.is_url(endpoint_url):
                raise TypeError(f'{endpoint_url} is not a proper url.')
        return True
    return False


def create_s3_read_session(
        access_key_id: str=None,
        access_key: str=None,
        db_key: str=None,
        bucket: str=None,
        endpoint_url: str=None,
        db_url: str=None,
        threads: int=20,
        read_timeout: int=60,
        retries: int=3,
        ):
    """

    """
    if isinstance(db_url, str):
        if not s3func.utils.is_url(db_url):
            raise TypeError(f'{db_url} is not a proper url.')
        read_session = s3func.HttpSession(threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
        key = db_url
    elif isinstance(access_key_id, str) and isinstance(access_key, str) and isinstance(db_key, str) and isinstance(bucket, str):
        if isinstance(endpoint_url, str):
            if not s3func.utils.is_url(endpoint_url):
                raise TypeError(f'{endpoint_url} is not a proper url.')
        read_session = s3func.S3Session(access_key_id, access_key, bucket, endpoint_url, threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
        key = db_key
    else:
        read_session = None
        # raise ValueError('Either db_url or a combo of access_key_id, access_key, db_key, and bucket (and optionally endpoint_url) must be passed.')

    return read_session, key


def create_s3_write_session(
        access_key_id: str=None,
        access_key: str=None,
        db_key: str=None,
        bucket: str=None,
        endpoint_url: str=None,
        threads: int=20,
        read_timeout: int=60,
        retries: int=3,
        ):
    """

    """
    if check_write_config(access_key_id, access_key, db_key, bucket, endpoint_url):
        write_session = s3func.S3Session(access_key_id, access_key, bucket, endpoint_url, threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
    else:
        write_session = None
        # raise ValueError('Either db_url or a combo of access_key_id, access_key, db_key, and bucket (and optionally endpoint_url) must be passed.')

    return write_session, db_key


# def create_connection(
#         access_key_id: str=None,
#         access_key: str=None,
#         db_key: str=None,
#         bucket: str=None,
#         endpoint_url: str=None,
#         db_url: str=None,
#         threads: int=20,
#         read_timeout: int=60,
#         retries: int=3,
#         meta: dict={},
#         ):
#     """

#     """
#     ## temp read session
#     read_session, key = create_s3_read_session(
#             access_key_id,
#             access_key,
#             db_key,
#             bucket,
#             endpoint_url,
#             db_url,
#             threads,
#             read_timeout,
#             retries,
#             )
#     if read_session is None:
#         raise ValueError('Either db_url or a combo of access_key_id, access_key, db_key, and bucket (and optionally endpoint_url) must be passed.')

#     ## Get metadata if necessary
#     if all([k in meta for k in ('uuid', 'ebooklet_type', 'user_meta')]):
#         uuid = meta['uuid']
#         if isinstance(uuid, (str, uuid6.UUID)):
#             if isinstance(uuid, str):
#                 meta['uuid'] = uuid6.UUID(hex=uuid)
#             else:
#                 raise TypeError('uuid in meta must be either a string or UUID')

#         ebooklet_type = meta['ebooklet_type']
#         if ebooklet_type not in ebooklet_types:
#             raise ValueError(f'ebooklet_type in meta must be one of {ebooklet_types}')
#         if not isinstance(meta['user_meta'], dict):
#             raise TypeError('user_meta in meta must be a dict')
#     else:
#         meta = get_db_metadata(read_session, key)
#     read_session.clear()










###############################################
### Classes

# class ConnParams(msgspec.Struct):
#     db_key: str
#     bucket: str
#     endpoint_url: str
#     type: str
#     uuid: uuid6.UUID


class JsonSerializer:
    def to_dict(self):
        d1 = dict(db_key=self.db_key,
                  bucket=self.bucket,
                  endpoint_url=self.endpoint_url,
                  db_url=self.db_url,
                  # ebooklet_type=self.ebooklet_type,
                  # timestamp=self.timestamp,
                   # user_meta=self.user_meta,
                  )
        # db_meta = dict(
        #     ebooklet_type=self.ebooklet_type,
        #     timestamp=self.timestamp,
        #     )
        # uuid1 = self.uuid
        # if uuid1 is not None:
        #     db_meta['uuid'] = uuid1.hex
        # else:
        #     db_meta['uuid'] = None
        # d1['db_meta'] = db_meta

        return d1

    def dumps(self):
        return orjson.dumps(self.to_dict())


class S3SessionReader:
    """

    """
    def __init__(self,
                 read_session,
                 read_db_key,
                 threads,
                 # timestamp,
                 # uuid,
                 # ebooklet_type,
                 # init_bytes,
                 ):
        self._read_session = read_session
        self.read_db_key = read_db_key
        self.threads = threads
        # self.timestamp = timestamp
        # self.uuid = uuid
        # self.ebooklet_type = ebooklet_type
        # self.init_bytes = init_bytes
        self._load_db_metadata()

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
        if hasattr(self, '_finalizer'):
            self._finalizer()

    def _load_db_metadata(self):
        """
        Load the db metadata from the remote.
        """
        resp_obj = self.head_object()
        if resp_obj.status == 200:
            meta = resp_obj.metadata
            # print(meta)
            self._init_bytes = base64.urlsafe_b64decode(meta['init_bytes'])
            self.timestamp = int(meta['timestamp'])
            self.uuid = uuid.UUID(hex=meta['uuid'])
            self.type = meta['type']
        elif resp_obj.status == 404:
            self._init_bytes = None
            self.uuid = None
            self.timestamp = None
            self.type = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)


    def get_uuid(self):
        """
        Get the UUID of the remote.
        """
        if self.uuid is None:
            self._load_db_metadata()

        return self.uuid


    def get_type(self):
        """
        Get the EBooklet type of the remote.
        """
        if self.type is None:
            self._load_db_metadata()

        return self.type


    def get_timestamp(self):
        """
        Get the metadata timestamp of the remote.
        """
        resp_obj = self.head_object()
        if resp_obj.status == 200:
            self.timestamp = int(resp_obj.metadata['timestamp'])

        elif resp_obj.status == 404:
            self.timestamp = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

        return self.timestamp

    def get_user_metadata(self):
        """
        Get the user metadata.
        """
        resp_obj = self.get_object(f'{booklet.utils.metadata_key_bytes.decode()}')
        if resp_obj.status == 200:
            meta = orjson.loads(resp_obj.data)
        elif resp_obj.status == 404:
            meta = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

        return meta


    # @property
    # def timestamp(self):
    #     """
    #     Timestamp as int_us of the last modified date
    #     """
    #     raise NotImplementedError()

    # @property
    # def readable(self):
    #     """
    #     Test to see if remote read access is possible. Returns a bool.
    #     """
    #     return True

    # @property
    # def writable(self):
    #     """
    #     Test to see if remote write access is possible. Returns a bool.
    #     """
    #     return False

    # def get_db_index_object(self):
    #     """

    #     """
    #     return self._session.get_object(self.db_key + '.remote_index')

    # def get_db_object(self):
    #     """
    #     Get the main db object from the remote and return an S3 response object.
    #     """
    #     return self._read_session.get_object(self.read_db_key)

    # def head_db_object(self):
    #     """

    #     """
    #     resp_obj = self.read_session.head_object(self.db_key)

    #     return resp_obj

    def get_object(self, key: str=None):
        """
        Get a remote object/file. The input should be a key as a str. It should return an object with a .status attribute as an int, a .data attribute in bytes, and a .error attribute as a dict.
        """
        if key is None:
            resp = self._read_session.get_object(self.read_db_key)
        else:
            resp = self._read_session.get_object(self.read_db_key + '/' + key)

        return resp

    def head_object(self, key: str=None):
        """
        Get the header for a remote object/file. The input should be a key as a str. It should return an object with a .status attribute as an int, a .data attribute in bytes, and a .error attribute as a dict.
        """
        if key is None:
            resp = self._read_session.head_object(self.read_db_key)
        else:
            resp = self._read_session.head_object(self.read_db_key + '/' + key)

        return resp

    # def head_object(self, key: str):
    #     """
    #     Get the header for a remote object/file. The input should be a key as a str.
    #     """
    #     return self.read_session.head_object(self.db_key + '/' + key)


class S3SessionWriter(S3SessionReader):
    """

    """
    def __init__(self,
                 read_session,
                 write_session,
                 read_db_key,
                 write_db_key,
                 threads,
                 # timestamp,
                 # uuid,
                 # ebooklet_type,
                 # init_bytes,
                 # object_lock=False,
                 # break_other_locks=False,
                 # lock_timeout=-1
                 ):
        self._read_session = read_session
        self._write_session = write_session
        self.read_db_key = read_db_key
        self.write_db_key = write_db_key
        self.threads = threads
        # self.timestamp = timestamp
        # self.uuid = uuid
        # self.ebooklet_type = ebooklet_type
        # self.init_bytes = init_bytes

        # if object_lock:
        #     lock = write_session.s3lock(self.write_db_key)

        #     if break_other_locks:
        #         lock.break_other_locks()

        #     if not lock.aquire(timeout=lock_timeout):
        #         print('Lock could not be aquired...')

        #     self._writable_check = True
        #     self._writable = True
        # else:
        #     lock = None

        self._writable_check = False
        self._writable = False

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.s3session_finalizer, self._write_session)

        ## Get latest metadata
        self._load_db_metadata()

    @property
    def writable(self):
        """
        Check to see if the remote is writable given the credentials.

        Should I include this? Or should I simply let the other methods fail if it's not writable? I do like having an explicit test...
        """
        if not self._writable_check:
            test_key = self.write_db_key + uuid.uuid6().hex[-13:]
            put_resp = self._write_session.put_object(test_key, b'0')
            if put_resp.status // 100 == 2:
                del_resp = self._write_session.delete_object(test_key, put_resp.metadata['version_id'])
                if del_resp.status // 100 == 2:
                    self._writable = True

            self._writable_check = True

        return self._writable


    def put_db_object(self, data: bytes, metadata):
        """
        Upload the main db object to the remote.
        """
        if self.writable:
            return self._write_session.put_object(self.write_db_key, data, metadata=metadata)
        else:
            raise ValueError('Session is not writable.')


    # def put_db_index_object(self, data: bytes, metadata={}):
    #     """

    #     """
    #     if self.writable:
    #         return self._session.put_object(self.db_key + '.remote_index', data, metadata=metadata)
    #     else:
    #         raise ValueError('Conn is not writable.')


    def put_object(self, key: str, data: bytes, metadata={}):
        """
        Upload an object to the remote.
        """
        if self.writable:
            key1 = self.write_db_key + '/' + key
            return self._write_session.put_object(key1, data, metadata=metadata)
        else:
            raise ValueError('Session is not writable.')


    def delete_objects(self, keys):
        """
        Delete specific objects.
        """
        if self.writable:
            del_list = []
            resp = self._write_session.list_object_versions(prefix=self.write_db_key + '/')
            for obj in resp.iter_objects():
                key0 = obj['key']
                key = key0.split('/')[-1]
                if key in keys:
                    del_list.append({'Key': key0, 'VersionId': obj['version_id']})

            if del_list:
                del_resp = self._write_session.delete_objects(del_list)
                if del_resp.status // 100 != 2:
                    raise urllib3.exceptions.HTTPError(del_resp.error)
        else:
            raise ValueError('Session is not writable.')


    def delete_remote(self):
        """
        Delete the entire remote.
        """
        if self.writable:
            del_list = []
            resp = self._write_session.list_object_versions(prefix=self.write_db_key)
            for obj in resp.iter_objects():
                key0 = obj['key']
                del_list.append({'Key': key0, 'VersionId': obj['version_id']})

            self._write_session.delete_objects(del_list)
            self._init_bytes = None
            self.uuid = None
        else:
            raise ValueError('Session is not writable.')

    def copy_remote(self, remote_conn):
        """
        Copy an entire remote dataset to another remote location. The new location must be empty.

        There's still a question of whether this method should be in the Reader rather than the writer. As a matter of API concept, this should be in the Reader as you only need to read something to copy it somewhere else (the target). But as a matter of implementation, the thing doing the copying would need to know all of the files to copy. This can be either known through a list_objects call (currently used and requires write permissions as defined in this API) or to parse the index file for all of the appropriate keys. Parsing the index file requires the file to be saved to disk and this API does not have that capability. Consequently, if the index file must be used, then it must be implemented in the EVariableLengthValue class instead of the session classes. Using the index file instead of the list_objects method would have the advantage of only copying over the objects that are actually used by ebooklet rather than other dangling objects. On the other hand, requiring the source session being a Writer would more likely cause the copy to be an effecient S3 to S3 copy rather than a slower routing through the user's network.
        """
        with remote_conn.open('w') as writer:

            if not writer.writable:
                raise ValueError('target remote is not writable.')

            ## Check if source exists
            source_uuid = self.get_uuid()
            if source_uuid is None:
                raise ValueError('The source remote does not exist.')

            ## Check is target exists
            target_uuid = writer.get_uuid()
            if target_uuid is not None:
                raise ValueError('The target remote already exists. Either delete_remote or use a different target.')

            ## Get a list of all source objects
            source_resp = self._write_session.list_objects(prefix=self.write_db_key + '/')
            if source_resp.status // 100 != 2:
                raise urllib3.exceptions.HTTPError(source_resp.error)

            ## Get all target objects
            target_resp = writer._write_session.list_objects(prefix=writer.write_db_key + '/')
            if target_resp.status // 100 != 2:
                raise urllib3.exceptions.HTTPError(target_resp.error)

            target_exist_keys = set(obj['key'] for obj in target_resp.iter_objects())

            ## Buckets
            source_bucket = self._write_session.bucket
            target_bucket = writer._write_session.bucket

            ## Determine if the s3 copy_object method can be used
            if (self._write_session._access_key_id == writer._write_session._access_key_id) and (self._write_session._access_key == writer._write_session._access_key):
                print('Both the source and target remotes use the same credentials, so copying objects is efficient.')

                futures = {}
                failures = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                    for obj in source_resp.iter_objects():
                        source_key = obj['key']
                        target_key = writer.write_db_key + source_key.lstrip(self.write_db_key)
                        if target_key not in target_exist_keys:
                            f = executor.submit(self._write_session.copy_object, source_key, target_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                            futures[f] = target_key

                    for f in concurrent.futures.as_completed(futures):
                        target_key = futures[f]
                        resp = f.result()
                        if resp.status // 100 != 2:
                            failures[target_key] = resp.error

                if failures:
                    print('Copy failures have occurred. Rerun copy_remote or delete_remote.')
                    return failures
                else:
                    resp = self._write_session.copy_object(self.write_db_key, writer.write_db_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                    if resp.status // 100 != 2:
                        raise urllib3.exceptions.HTTPError(resp.error)

            else:
                print('The source and target remotes use different credentials, so copying objects must be first downloaded than uploaded. Less efficient than if both remotes had the same credentials.')

                futures = {}
                failures = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                    for obj in source_resp.iter_objects():
                        source_key = obj['key']
                        target_key = writer.write_db_key + source_key.lstrip(self.write_db_key)
                        if target_key not in target_exist_keys:
                            f = executor.submit(utils.indirect_copy_remote, self._read_session, writer._write_session, source_key, target_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                            futures[f] = target_key

                    for f in concurrent.futures.as_completed(futures):
                        target_key = futures[f]
                        resp = f.result()
                        if resp.status // 100 != 2:
                            failures[target_key] = resp.error

                if failures:
                    print('Copy failures have occurred. Rerun copy_remote or delete_remote.')
                    return failures
                else:
                    resp = self._write_session.copy_object(utils.indirect_copy_remote, self._read_session, writer._write_session, writer.write_db_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                    if resp.status // 100 != 2:
                        raise urllib3.exceptions.HTTPError(resp.error)


    def list_objects(self):
        """

        """
        return self._write_session.list_objects(prefix=self.write_db_key + '/')


    def list_object_versions(self):
        """

        """
        return self._write_session.list_object_versions(prefix=self.write_db_key + '/')

    def create_lock(self):
        """
        Initialise an S3 lock object. A lock is not immediately aquired. This must be done via the lock object (as well as releasing locks).
        """
        if self.writable:
            lock = self._write_session.s3lock(self.write_db_key)
            return lock
        else:
            raise ValueError('Session is not writable.')

    def break_other_locks(self, timestamp: str | datetime.datetime=None):
        """
        Removes all locks that are on the object older than specified timestamp. This is only meant to be used in deadlock circumstances.

        Parameters
        ----------
        timestamp : str or datetime.datetime
            All locks older than the timestamp will be removed. The default is now.

        Returns
        -------
        list of dict of the removed keys/versions
        """
        if self.writable:
            lock = self._write_session.s3lock(self.write_db_key)
            other_keys = lock.break_other_locks(timestamp)

            return other_keys
        else:
            raise ValueError('Session is not writable.')


    def _head_object_writer(self, key: str=None):
        """
        Get the header for a remote object/file. The input should be a key as a str. It should return an object with a .status attribute as an int, a .data attribute in bytes, and a .error attribute as a dict.
        """
        if key is None:
            resp = self._write_session.head_object(self.write_db_key)
        else:
            resp = self._write_session.head_object(self.write_db_key + '/' + key)

        return resp

    def _get_uuid_writer(self):
        """
        Get the uuid of the remote file.
        """
        resp_obj = self._head_object_writer()
        if resp_obj.status == 200:
            uuid1 = uuid.UUID(hex=resp_obj.metadata['uuid'])
        elif resp_obj.status == 404:
            uuid1 = None
        else:
            raise urllib3.exceptions.HTTPError(resp_obj.error)

        return uuid1


class S3Connection(JsonSerializer):
    """

    """
    def __init__(self,
                access_key_id: str=None,
                access_key: str=None,
                db_key: str=None,
                bucket: str=None,
                endpoint_url: str=None,
                db_url: str=None,
                threads: int=20,
                read_timeout: int=60,
                retries: int=3,
                # db_meta: dict=None,
                # user_meta: dict=None,
                ):
        """
        Establishes an S3 client connection with an S3 account.

        Parameters
        ----------
        access_key_id : str
            The access key id also known as aws_access_key_id.
        access_key : str
            The access key also known as aws_secret_access_key.
        db_key : str
            The key name of the database.
        bucket : str
            The bucket to be used when performing S3 operations.
        endpoint_url : str
            The nedpoint http(s) url for the s3 service.
        threads : int
            The number of simultaneous connections for the S3 connection.
        read_timeout: int
            The read timeout in seconds passed to the "retries" option in the S3 config.
        retries: int
            The number of max attempts passed to the "retries" option in the S3 config.
        """
        ## temp read session
        read_session, key = create_s3_read_session(
                access_key_id,
                access_key,
                db_key,
                bucket,
                endpoint_url,
                db_url,
                threads,
                read_timeout,
                retries,
                )
        if read_session is None:
            raise ValueError('Either db_url or a combo of access_key_id, access_key, db_key, and bucket (and optionally endpoint_url) must be passed.')

        ## Get metadata if necessary
        # if db_meta is None:
        #     meta = get_db_metadata(read_session, key)
        # elif all([k in db_meta for k in ('uuid', 'ebooklet_type', 'timestamp')]):
        #     uuid = db_meta['uuid']
        #     if isinstance(uuid, (str, uuid.UUID)):
        #         if isinstance(uuid, str):
        #             db_meta['uuid'] = uuid.UUID(hex=uuid)
        #         else:
        #             raise TypeError('uuid in meta must be either a string or UUID')

        #     ebooklet_type = db_meta['ebooklet_type']
        #     if ebooklet_type not in ebooklet_types:
        #         raise ValueError(f'ebooklet_type in meta must be one of {ebooklet_types}')
        # else:
        #     meta = get_db_metadata(read_session, key)

        # if user_meta is not None:
        #     if not isinstance(user_meta, dict):
        #         raise TypeError('user_meta in meta must be a dict or None')

        # read_session.clear()

        ## Assign properties
        self.db_key = db_key
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.endpoint_url = endpoint_url
        self.db_url = db_url
        self.threads = threads
        self.read_timeout = read_timeout
        self.retries = retries

        # self.uuid = meta['uuid']
        # self.timestamp = meta['timestamp']
        # self.ebooklet_type = meta['ebooklet_type']
        # self.user_meta = user_meta


    # def _make_read_session(self):
    #     """

    #     """
    #     read_session, key = create_s3_read_session(
    #             self.access_key_id,
    #             self.access_key,
    #             self.db_key,
    #             self.bucket,
    #             self.endpoint_url,
    #             self.db_url,
    #             self.threads,
    #             self.read_timeout,
    #             self.retries,
    #             )

    #     return read_session, key


    # def load_db_metadata(self):
    #     """

    #     """
    #     read_session, key = self._make_read_session()

    #     meta = get_db_metadata(read_session, key)
    #     self.uuid = meta['uuid']
    #     self.ebooklet_type = meta['ebooklet_type']
    #     self.timestamp = meta['timestamp']


    # def load_user_metadata(self):
    #     """

    #     """
    #     read_session, key = self._make_read_session()

    #     self.user_meta = get_user_metadata(read_session, key)


    def open(self,
             flag: str='r',
             ):
        """
        Opens a connection to the S3 remote.

        Parameters
        ----------
        flag : str
            The open flag for the remote. These are the same for booklet and ebooklet.

        Returns
        -------
        S3SessionReader or S3SessionWriter
        """
        if (flag != 'r') and (self.access_key_id is None or self.access_key is None):
            raise ValueError("access_key_id and access_key must be assigned to open a connection for writing.")

        # ## Check and load uuid if it isn't assigned
        # if self.uuid is None:
        #     self.load_db_metadata()

        ## Read session
        # read_session, read_db_key = self._make_read_session()
        read_session, read_db_key = create_s3_read_session(
                self.access_key_id,
                self.access_key,
                self.db_key,
                self.bucket,
                self.endpoint_url,
                self.db_url,
                self.threads,
                self.read_timeout,
                self.retries,
                )

        if flag == 'r':
            return S3SessionReader(read_session, read_db_key, self.threads)
        else:
            write_session, write_db_key = create_s3_write_session(
                    self.access_key_id,
                    self.access_key,
                    self.db_key,
                    self.bucket,
                    self.endpoint_url,
                    self.threads,
                    self.read_timeout,
                    self.retries,
                    )

            if write_session is None:
                raise ValueError("A write session could not be created. Check that all the inputs are assigned.")

            session_writer = S3SessionWriter(
                                    read_session,
                                    write_session,
                                    read_db_key,
                                    write_db_key,
                                    self.threads,
                                    )

            # Check to make sure the uuids are the same if the read and write sessions are different
            if isinstance(read_session, s3func.HttpSession) and session_writer.uuid is not None:
                if session_writer._get_uuid_writer() != session_writer.uuid:
                    raise ValueError('The UUIDs of the http connection and the S3 connection are different. Check to make sure the they are pointing to the right file.')

            return session_writer




















































