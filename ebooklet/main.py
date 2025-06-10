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


#######################################################
### Classes


class Change:
    """

    """
    def __init__(self, ebooklet):
        """

        """
        self._ebooklet = ebooklet

        self._changelog_path = None

        # self.update()


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

        if not self._changelog_path:
            self.update()

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
        Updates the remote. It will regenerate the changelog to ensure the changelog is up-to-date. Returns True if the remote has been updated and False if no updates were made (due to nothing needing updating). If upload failures have occurred, then it will return a list of the keys that failed.
        Force_push will push the main file and the remote_index to the remote regardless of changes. Only necessary if upload failures occurred during a previous push.
        """
        if not self._ebooklet._remote_session.writable:
            raise ValueError('Remote is not writable.')

        if not self._ebooklet.writable:
            raise ValueError('File is open for read-only.')

        self.update()

        success = utils.update_remote(self._ebooklet._local_file, self._ebooklet._remote_index, self._changelog_path, self._ebooklet._remote_session, self._ebooklet._executor, force_push, self._ebooklet._deletes, self._ebooklet._flag, self._ebooklet._subtype)

        if success:
            self._changelog_path.unlink()
            self._changelog_path = None # Force a reset of the changelog
            self._ebooklet._deletes.clear()

            if self._ebooklet._remote_session.uuid is None:
                self._ebooklet._remote_session._load_db_metadata()

        return success


class EVariableLengthValue(MutableMapping):
    """

    """
    def __init__(
            self,
            remote_conn: remote.S3Connection | str,
            file_path: Union[str, pathlib.Path],
            flag: str = "r",
            value_serializer: str = None,
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            ):
        """

        """
        local_file_path = pathlib.Path(file_path)

        local_file_exists = local_file_path.exists()

        ## Determine the remotes that read and write
        if isinstance(remote_conn, str):
            if flag != 'r':
                raise ValueError('If remote_conn is a url string, then flag must be r.')
            remote_conn = remote.S3Connection(db_url=remote_conn)
        elif not isinstance(remote_conn, remote.S3Connection):
            raise TypeError('remote_conn must be either a url string or a remote.S3Connection.')

        ## Set up the remote session
        remote_session = utils.check_parse_conn(remote_conn, flag, local_file_exists)

        ## Lock the remote if file is opened for write
        if flag != 'r':
            lock = remote_session.create_lock()
            lock.aquire()
        else:
            lock = None

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
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session, lock)

        ## Assign properties
        if flag == 'r':
            self.writable = False
        else:
            self.writable = True

        self._flag = flag
        self.lock = lock
        self._remote_index_path = remote_index_path
        self._local_file_path = local_file_path
        self._local_file = local_file
        self._remote_index_path = remote_index_path
        self._remote_index = remote_index
        self._deletes = set()
        self._remote_session = remote_session
        self._n_buckets = local_file._n_buckets
        self._subtype = 'EVariableLengthValue'
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


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
        Returns a generator of the keys.
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
        Returns an iterator of the keys, values.
        """
        _ = self.load_items()

        return self._local_file.items()

    def values(self):
        """
        Returns an iterator of the values.
        """
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
        Set a value associated with a key.
        """
        if self.writable:
            self._local_file.set(key, value, timestamp)

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
        """
        Get a value associated with a key. Will return the default if the key does not exist.
        """
        failure = self._load_item(key)
        if failure:
            return failure

        return self._local_file.get(key, default=default)


    def update(self, key_value_dict: dict):
        """
        Set many keys/values from a dict.
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
        Return an iterator of the values associated with the input keys. Missing keys will return the default value.
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

    def clear(self):
        """
        Remove all keys and values from the local file.
        """
        if self.writable:
            self._local_file.clear()

        else:
            raise ValueError('File is open for read only.')

    def close(self, force_shutdown=False):
        """
        Close all open objects. If force_shutdown is True, then it will immediately end any processes running in the background (not recommended unless there's a deadlock).
        """
        self.sync()
        self._finalizer()


    # def __del__(self):
    #     self.close()

    def sync(self, force_shutdown=False):
        """
        Syncronize all cache to disk. This ensures all data has been saved to disk properly. If force_shutdown is True, then it will immediately end any processes running in the background (not recommended unless there's a deadlock).
        """
        self._executor.shutdown(cancel_futures=force_shutdown)
        del self._executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._remote_session.threads)
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        """
        Return a Change object of the changes that have occurred during this session.
        """
        return Change(self)


    def reopen(self, flag):
        """
        Reopen a file with a different flag. The flag must be either r or w.
        """
        if flag not in ('r', 'w'):
            raise ValueError('The flag must be either r or w.')

        self.sync()
        self._local_file.reopen(flag)
        self._remote_index.reopen(flag)

        if self._flag == 'r' and flag != 'r':
            lock = self._remote_session.create_lock()
            lock.aquire()
            self.lock = lock
        elif self._flag != 'r' and flag == 'r':
            self.lock.release()
            self.lock = None

        self._flag = flag


    def delete_remote(self):
        """
        Completely delete the remote file, but keep the local file.
        """
        if self.writable:
            self._remote_session.delete_remote()
        else:
            raise ValueError('File is open for read only.')


    def copy_remote(self, remote_conn):
        """
        Copy the entire remote file to another remote location. The new location must be empty.
        """
        if self.writable:
            self._remote_session.copy_remote(remote_conn)
        else:
            raise ValueError('File is open for read only.')


class RemoteConnGroup(EVariableLengthValue):
    """

    """
    def __init__(
            self,
            remote_conn: remote.S3Connection | str,
            file_path: Union[str, pathlib.Path],
            flag: str = "r",
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            ):
        """

        """
        local_file_path = pathlib.Path(file_path)

        local_file_exists = local_file_path.exists()

        ## Determine the remotes that read and write
        if isinstance(remote_conn, str):
            if flag != 'r':
                raise ValueError('If remote_conn is a url string, then flag must be r.')
            remote_conn = remote.S3Connection(db_url=remote_conn)
        elif not isinstance(remote_conn, remote.S3Connection):
            raise TypeError('remote_conn must be either a url string or a remote.S3Connection.')

        remote_session = utils.check_parse_conn(remote_conn, flag, local_file_exists)

        ## Lock the remote if file is opened for write
        if flag != 'r':
            lock = remote_session.create_lock()
            lock.aquire()
        else:
            lock = None

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
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session, lock)

        ## Assign properties
        if flag == 'r':
            self.writable = False
        else:
            self.writable = True

        self._flag = flag
        self.lock = lock
        self._local_file_path = local_file_path
        self._local_file = local_file
        self._remote_index_path = remote_index_path
        self._remote_index = remote_index
        self._deletes = set()
        self._remote_session = remote_session
        self._n_buckets = local_file._n_buckets
        self._subtype = 'RemoteConnGroup'
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


    def add(self, remote_conn: remote.S3Connection):
        """
        Add a remote connection to the group.
        """
        if self.writable:

            if not isinstance(remote_conn, remote.S3Connection):
                raise TypeError('remote_conn/value must be a remote.S3Connection')

            ## Get remote_conn metadata
            with remote_conn.open() as rc:
                uuid0 = rc.get_uuid()
                if uuid0 is None:
                    raise ValueError('Remote does not exist. It must exist to be added to a RemoteConnGroup.')

                uuid_hex = uuid0.hex

                user_meta = rc.get_user_metadata()
                ts = rc.get_timestamp()

            conn_dict = remote_conn.to_dict()
            conn_dict['user_meta'] = user_meta

            self._local_file.set(uuid_hex, conn_dict, ts)

        else:
            raise ValueError('File is open for read only.')


    def set(self, key, remote_conn: remote.S3Connection):
        """
        Use the add method to add remote connections to the group.
        """
        raise NotImplementedError('Use the add method to add remote connections to the group.')


def open(
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    remote_conn: Union[remote.S3Connection, str]=None,
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
        return EVariableLengthValue(remote_conn=remote_conn, file_path=file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)


