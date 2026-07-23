#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 08:36:26 2024

@author: mike
"""
import logging
import uuid6 as uuid
import urllib3
import booklet
from typing import Union
import s3func
import warnings
import weakref
import msgspec
import io
import base64
import datetime
import concurrent.futures

logger = logging.getLogger(__name__)
from . import utils
from .errors import ReadOnlyError, RemoteMissingError, UUIDMismatchError, OfflineError


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
        read_session = s3func.HttpSession(max_connections=threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
        key = db_url
    elif isinstance(access_key_id, str) and isinstance(access_key, str) and isinstance(db_key, str) and isinstance(bucket, str):
        if isinstance(endpoint_url, str):
            if not s3func.utils.is_url(endpoint_url):
                raise TypeError(f'{endpoint_url} is not a proper url.')
        read_session = s3func.S3Session(access_key_id, access_key, bucket, endpoint_url, max_pool_connections=threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
        key = db_key
    else:
        read_session = None

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
        write_session = s3func.S3Session(access_key_id, access_key, bucket, endpoint_url, max_pool_connections=threads, read_timeout=read_timeout, stream=False, max_attempts=retries)
    else:
        write_session = None

    return write_session, db_key









###############################################
### Classes

class JsonSerializer:
    def to_dict(self):
        d1 = dict(db_key=self.db_key,
                  bucket=self.bucket,
                  endpoint_url=self.endpoint_url,
                  db_url=self.db_url,
                  )

        return d1

    def dumps(self):
        return msgspec.json.encode(self.to_dict())


class S3SessionReader:
    """

    """
    def __init__(self,
                 read_session,
                 read_db_key,
                 threads,
                 ):
        self._read_session = read_session
        self.read_db_key = read_db_key
        self.threads = threads
        self._load_db_metadata()

    def __bool__(self):
        """
        Test to see if remote read access is possible. Same as .read_access.
        """
        return self.read_access

    @property
    def initialized(self):
        """
        Whether the remote database exists (its db object has been pushed at
        least once). Maintained by _load_db_metadata: the 404 branch leaves
        uuid None. This names the session-lifecycle fact directly instead of
        scattering `uuid is None` checks (Seam-1 decomposition).
        """
        return self.uuid is not None

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
            ## Refuse too-new remotes BEFORE parsing anything else - this is the
            ## single choke point for reader init, writer init, and every index
            ## re-pull. Remotes without the stamp predate it and are v1.
            self.format_version = int(meta.get('format_version', 1))
            if self.format_version > utils.SUPPORTED_FORMAT_VERSION:
                raise utils.UnsupportedFormatError(
                    f'This remote database uses storage format_version '
                    f'{self.format_version}, but this ebooklet version only supports '
                    f'up to {utils.SUPPORTED_FORMAT_VERSION}. Upgrade ebooklet to open it.'
                )
            self._init_bytes = base64.urlsafe_b64decode(meta['init_bytes'])
            self.timestamp = int(meta['timestamp'])
            self.uuid = uuid.UUID(hex=meta['uuid'])
            self.type = meta['type']
            self.num_groups = int(meta['num_groups']) if 'num_groups' in meta else None
        elif resp_obj.status == 404:
            self._init_bytes = None
            self.uuid = None
            self.timestamp = None
            self.type = None
            self.num_groups = None
            self.format_version = None
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
        Get the user metadata. Format 2 embeds it in the db-object payload
        (killing the old separate '_metadata' object and its split-brain), so
        this is a cheap ranged read of the payload's pre-index sections.
        """
        manifest, meta_section = utils.fetch_remote_state(self)
        if manifest is None and meta_section is None:
            return None
        _ts, data = utils.parse_meta_section(meta_section)
        return data


    def get_object(self, key: str=None, range_start: int=None, range_end: int=None):
        """
        Get a remote object/file. The input should be a key as a str. It should return an object with a .status attribute as an int, a .data attribute in bytes, and a .error attribute as a dict.
        Optionally specify range_start and range_end for byte-range requests.
        """
        if key is None:
            resp = self._read_session.get_object(self.read_db_key, range_start=range_start, range_end=range_end)
        else:
            resp = self._read_session.get_object(self.read_db_key + '/' + key, range_start=range_start, range_end=range_end)

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


class S3SessionWriter(S3SessionReader):
    """

    """
    def __init__(self,
                 read_session,
                 write_session,
                 read_db_key,
                 write_db_key,
                 threads,
                 ):
        self._read_session = read_session
        self._write_session = write_session
        self.read_db_key = read_db_key
        self.write_db_key = write_db_key
        self.threads = threads

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
            ## The probe key lives INSIDE the db_key + '/' namespace so a
            ## crashed probe's orphan is prefix-distinguishable from sibling
            ## databases and sweepable by delete_remote (the old sibling-level
            ## key db_key + <hex> was neither). The delete below is exact key +
            ## version_id, so even a pathological collision with a user key is
            ## non-destructive on versioned stores.
            test_key = self.write_db_key + '/_writable_probe_' + uuid.uuid6().hex[-13:]
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
            raise ReadOnlyError('Session is not writable.')


    def put_object(self, key: str, data: bytes, metadata=None):
        """
        Upload an object to the remote.
        """
        if metadata is None:
            metadata = {}
        if self.writable:
            key1 = self.write_db_key + '/' + key
            return self._write_session.put_object(key1, data, metadata=metadata)
        else:
            raise ReadOnlyError('Session is not writable.')


    def delete_object(self, key: str):
        """
        Delete a single object by exact key (all versions).

        Returns None on success or the error on failure. The error must be
        RETURNED, never raised: the caller (upload_group) runs in a push worker
        thread, and a raise would propagate through future.result() and crash
        the whole push instead of degrading to the partial-failure retry path.
        """
        if self.writable:
            key1 = self.write_db_key + '/' + key
            try:
                ## Exact keys, not a prefix: group ids are non-zero-padded
                ## decimals, so a prefix delete of group '1' would also destroy
                ## groups '10', '11', ... purge=True (the s3func default, made
                ## explicit) removes all versions of exactly this key.
                self._write_session.delete_objects(keys=[key1], purge=True)
            except urllib3.exceptions.HTTPError as err:
                return err
            return None
        else:
            raise ReadOnlyError('Session is not writable.')

    def delete_objects(self, keys):
        """
        Delete specific objects (exact keys, all versions).
        """
        if self.writable:
            full_keys = [self.write_db_key + '/' + key for key in keys]
            self._write_session.delete_objects(keys=full_keys, purge=True)
        else:
            raise ReadOnlyError('Session is not writable.')


    def delete_remote(self):
        """
        Delete the entire remote database: the db object itself, then every
        child object under db_key + '/'. Deliberately bounded - a bare db_key
        prefix would also match sibling databases (deleting 'mydb' would wipe
        'mydb2') and the lock namespace db_key + '.lock.', which must survive:
        the flag='n' push calls this while HOLDING its own session lock, and
        other writers' live tickets are theirs to release. Crashed writers'
        stale tickets are left for force_lock (age-gated since s3func 0.9.3:
        only tickets older than 2 hours are broken, so live writers survive).
        """
        if self.writable:
            ## The db object is the existence marker - delete it first so a
            ## torn teardown leaves only invisible orphans (swept by a rerun)
            ## rather than an index claiming missing children. purge=True
            ## resolves this one key's versions via an over-broad namespace
            ## listing - acceptable for a rare teardown operation.
            self._write_session.delete_objects(keys=[self.write_db_key], purge=True)
            self._write_session.delete_objects(prefix=self.write_db_key + '/', purge=True)
            self._init_bytes = None
            self.uuid = None
        else:
            raise ReadOnlyError('Session is not writable.')

    def copy_remote(self, remote_conn):
        """
        Copy an entire remote dataset to another remote location. The new location must be empty.

        There's still a question of whether this method should be in the Reader rather than the writer. As a matter of API concept, this should be in the Reader as you only need to read something to copy it somewhere else (the target). But as a matter of implementation, the thing doing the copying would need to know all of the files to copy. This can be either known through a list_objects call (currently used and requires write permissions as defined in this API) or to parse the index file for all of the appropriate keys. Parsing the index file requires the file to be saved to disk and this API does not have that capability. Consequently, if the index file must be used, then it must be implemented in the EVariableLengthValue class instead of the session classes. Using the index file instead of the list_objects method would have the advantage of only copying over the objects that are actually used by ebooklet rather than other dangling objects. On the other hand, requiring the source session being a Writer would more likely cause the copy to be an effecient S3 to S3 copy rather than a slower routing through the user's network.
        """
        with remote_conn.open('w') as writer:

            if not writer.writable:
                raise ReadOnlyError('target remote is not writable.')

            ## Check if source exists
            source_uuid = self.get_uuid()
            if source_uuid is None:
                raise RemoteMissingError('The source remote does not exist.')

            ## Check is target exists
            ## (Deliberately a builtin ValueError, not a taxonomy class: an
            ## occupied target is argument validation, not a remote fault.)
            target_uuid = writer.get_uuid()
            if target_uuid is not None:
                raise ValueError('The target remote already exists. Either delete_remote or use a different target.')

            ## MANIFEST-DRIVEN object list (format 2): copy exactly what the
            ## source db object references - the manifest's generation objects
            ## (grouped mode) or the index's keys (per-key mode). A blind
            ## prefix listing would also replicate orphans (abandoned
            ## generations, crashed-probe leftovers).
            db_resp = self.get_object()
            if db_resp.status not in (200, 206):
                raise urllib3.exceptions.HTTPError(db_resp.error)
            src_manifest, _src_meta, src_index_bytes = utils.parse_db_payload(db_resp.data)

            if src_manifest:
                child_keys = [utils.group_obj_key(gid, gen) for gid, gen in src_manifest.items()]
            else:
                ## Per-key mode: the object names are the index's keys.
                idx = booklet.FixedLengthValue(io.BytesIO(bytes(src_index_bytes)), 'r')
                try:
                    child_keys = [k for k in idx.keys() if k != booklet.utils.metadata_key_bytes.decode()]
                finally:
                    idx.close()

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
                logger.info('Both the source and target remotes use the same credentials, so copying objects is efficient.')

                futures = {}
                failures = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                    for child in child_keys:
                        source_key = self.write_db_key + '/' + child
                        target_key = writer.write_db_key + '/' + child
                        if target_key not in target_exist_keys:
                            f = executor.submit(self._write_session.copy_object, source_key, target_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                            futures[f] = target_key

                    for f in concurrent.futures.as_completed(futures):
                        target_key = futures[f]
                        resp = f.result()
                        if resp.status // 100 != 2:
                            failures[target_key] = resp.error

                if failures:
                    logger.warning('Copy failures have occurred. Rerun copy_remote or delete_remote.')
                    return failures
                else:
                    resp = self._write_session.copy_object(self.write_db_key, writer.write_db_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                    if resp.status // 100 != 2:
                        raise urllib3.exceptions.HTTPError(resp.error)

            else:
                logger.info('The source and target remotes use different credentials, so copying objects must be first downloaded then uploaded. Less efficient than if both remotes had the same credentials.')

                futures = {}
                failures = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                    for child in child_keys:
                        source_key = self.write_db_key + '/' + child
                        target_key = writer.write_db_key + '/' + child
                        if target_key not in target_exist_keys:
                            # source GET via the (already-required) writable, key-addressed session:
                            # a source connection with a public db_url makes _read_session an
                            # HttpSession that crashes on a bare key - _write_session is always a
                            # keyed S3Session (copy_remote reads it unconditionally above).
                            f = executor.submit(utils.indirect_copy_remote, self._write_session, writer._write_session, source_key, target_key, source_bucket=source_bucket, dest_bucket=target_bucket)
                            futures[f] = target_key

                    for f in concurrent.futures.as_completed(futures):
                        target_key = futures[f]
                        resp = f.result()
                        if resp.status // 100 != 2:
                            failures[target_key] = resp.error

                if failures:
                    logger.warning('Copy failures have occurred. Rerun copy_remote or delete_remote.')
                    return failures
                else:
                    resp = utils.indirect_copy_remote(self._write_session, writer._write_session, self.write_db_key, writer.write_db_key, source_bucket, target_bucket)
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
            lock = self._write_session.lock(self.write_db_key)
            return lock
        else:
            raise ReadOnlyError('Session is not writable.')

    def break_other_locks(self, timestamp: str | datetime.datetime=None):
        """
        Removes all locks that are on the object older than specified timestamp. This is only meant to be used in deadlock circumstances.

        Parameters
        ----------
        timestamp : str or datetime.datetime
            Lock tickets uploaded at or before the timestamp are removed. The
            default is 2 hours ago (s3func's age gate): younger tickets are
            presumed to belong to a LIVE writer, whose next push would then
            abort at its lock re-verification. Pass an explicit now to break
            everything regardless of age.

        Returns
        -------
        list of dict of the removed keys/versions
        """
        if self.writable:
            lock = self._write_session.lock(self.write_db_key)
            other_keys = lock.break_other_locks(timestamp)

            return other_keys
        else:
            raise ReadOnlyError('Session is not writable.')


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


def _offline_op(name):
    def _raise(self, *args, **kwargs):
        raise OfflineError(
            f'This session is offline - the remote operation {name!r} is unavailable. '
            "Re-open without offline (or with offline='auto' and a reachable remote) "
            'to use the remote.'
        )
    _raise.__name__ = name
    return _raise


class OfflineSession:
    """
    Stands in for the remote session when a database is opened offline
    (open_ebooklet/open_rcg with offline=True, or the offline='auto'
    fallback). Reports an uninitialized remote (uuid None), which makes every
    open-time remote interaction skip itself (index fetch, sync check,
    remote-state refresh), and raises OfflineError from any method that would
    actually touch the network. Read-only by construction: the factories
    restrict offline mode to flag='r', so no lock is ever created.
    """
    writable = False
    initialized = False
    uuid = None
    timestamp = None
    num_groups = None
    format_version = None
    type = None
    _init_bytes = None
    threads = 1

    def close(self):
        pass

    _load_db_metadata = _offline_op('_load_db_metadata')
    get_object = _offline_op('get_object')
    put_object = _offline_op('put_object')
    put_db_object = _offline_op('put_db_object')
    delete_object = _offline_op('delete_object')
    delete_objects = _offline_op('delete_objects')
    delete_remote = _offline_op('delete_remote')
    copy_remote = _offline_op('copy_remote')
    list_objects = _offline_op('list_objects')
    write_db_key = _offline_op('write_db_key')
    create_lock = _offline_op('create_lock')


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
                threads: int=10,
                read_timeout: int=60,
                retries: int=3,
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

        ## Assign properties
        self.db_key = db_key
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.endpoint_url = endpoint_url
        self.db_url = db_url
        ## Single choke point: every public entry point that carries a db_url
        ## (incl. bare url strings via check_remote_conn) constructs this class.
        if isinstance(db_url, str) and db_url.lower().startswith('http://'):
            warnings.warn(
                'db_url uses plain http - readers fetch this database unencrypted. '
                'Use an https url for anything public; http is acceptable for local '
                'testing (silence with warnings.filterwarnings).',
                UserWarning,
                stacklevel=2,
            )
        self.threads = threads
        self.read_timeout = read_timeout
        self.retries = retries

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

        ## Read session
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
                    raise UUIDMismatchError('The UUIDs of the http connection and the S3 connection are different. Check to make sure the they are pointing to the right file.')

            return session_writer
















































