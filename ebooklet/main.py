#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, List, Dict
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import booklet
import weakref
from itertools import count
from collections import deque
import urllib3

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
        Open a Change object to interact with the remote.
        """
        ebooklet.sync()

        self._ebooklet = ebooklet

        self._changelog_path = None

        # self.update()


    def pull(self):
        """
        Update the remote index file to determine if any changes have occurred.
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
        Determine if there are any changes between the local and remote databases.
        """
        self._ebooklet.sync()
        changelog_path = utils.create_changelog(self._ebooklet._local_file_path, self._ebooklet._local_file, self._ebooklet._remote_index, self._ebooklet._remote_session)

        self._changelog_path = changelog_path


    def iter_changes(self):
        """
        Create an iterator of the changes.
        """
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

        success = utils.update_remote(self._ebooklet._local_file, self._ebooklet._remote_index, self._changelog_path, self._ebooklet._remote_session, force_push, self._ebooklet._deletes, self._ebooklet._flag, self._ebooklet.type)

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
            remote_session: remote.S3SessionReader | remote.S3SessionWriter,
            local_file_path: pathlib.Path,
            flag: str = "r",
            value_serializer: str = None,
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            ):
        """

        """
        ## Remove the remote database if flag == 'n'
        ## Lock the remote if file is opened for write
        if flag != 'r':
            if flag == 'n' and (remote_session.uuid is not None):
                remote_session.delete_remote()
            lock = remote_session.create_lock()
            lock.aquire()
            self.writable = True
        else:
            lock = None
            self.writable = False

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
        self.type = 'EVariableLengthValue'
        # self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


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
        failure = self._load_item(booklet.utils.metadata_key_bytes.decode())
        if failure:
            raise urllib3.exceptions.HTTPError(failure)

        self._local_file.sync()

        return self._local_file.get_metadata(include_timestamp=include_timestamp)


    def keys(self):
        """
        Returns a generator of the keys.
        """
        overlap = set([utils.metadata_key_str])
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
        failure_dict = self.load_items()

        if failure_dict:
            raise urllib3.exceptions.HTTPError(failure_dict)

        return self._local_file.items()

        # for key in self.load_items():
        #     value = self._local_file.get(key)
        #     yield key, value


    def values(self):
        """
        Returns an iterator of the values.
        """
        failure_dict = self.load_items()

        if failure_dict:
            raise urllib3.exceptions.HTTPError(failure_dict)

        return self._local_file.values()

        # for key in self.load_items():
        #     value = self._local_file.get(key)
        #     yield value

    def timestamps(self, include_value=False):
        """
        Return an iterator for timestamps for all keys. Optionally add values to the iterator.
        """
        failure_dict = self.load_items()

        if failure_dict:
            raise urllib3.exceptions.HTTPError(failure_dict)

        return self._local_file.timestamps(include_value=include_value)

        # for key in self.load_items():
        #     value = self._local_file.timestamps(include_value=include_value)
        #     yield value


    def get_timestamp(self, key, include_value=False, decode_value=True, default=None):
        """
        Get a timestamp associated with a key. Optionally include the value.
        """
        failure = self._load_item(key)
        if failure:
            raise urllib3.exceptions.HTTPError(failure)

        return self._local_file.get_timestamp(key, include_value=include_value, decode_value=decode_value, default=default)

    def set_timestamp(self, key, timestamp):
        """
        Set a timestamp for a specific key. The timestamp must be either an int of the number of microseconds in POSIX UTC time, an ISO 8601 datetime string with timezone, or a datetime object with timezone.
        """
        if self.writable:
            self._local_file.set_timestamp(key, timestamp)
        else:
            raise ValueError('File is open for read only.')


    def set(self, key, value, timestamp=None, encode_value=True):
        """
        Set a value associated with a key.
        """
        if self.writable:
            self._local_file.set(key, value, timestamp=timestamp, encode_value=encode_value)

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
            raise urllib3.exceptions.HTTPError(failure)

        return self._local_file.get(key, default=default)


    def update(self, key_value: MutableMapping):
        """
        Set many keys/values from a dict.
        """
        if self.writable:
            for key, value in key_value.items():
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
        Return an iterator of the values associated with the input keys. Missing keys will return the default.
        """
        if not isinstance(keys, (list, tuple, set)):
            keys = tuple(keys)

        failure_dict = self.load_items(keys)

        if failure_dict:
            raise urllib3.exceptions.HTTPError(failure_dict)

        for key in keys:
            output = self._local_file.get(key, default=default)
            yield key, output


    def load_items(self, keys=None):
        """
        Loads items into the local file from the remote. If keys is None, then it loads all of the values from the remote in to the local file. Returns a dict of failed transfers.
        """
        futures = {}

        failure_dict = {}

        # writable = self._local_file.writable

        # if not writable:
        #     self._local_file.reopen('w')
        with ThreadPoolExecutor(max_workers=self._remote_session.threads) as executor:
            if keys is None:
                for key, remote_time_bytes in self._remote_index.items():
                    check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                    if check:
                        f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                        futures[f] = key
            else:
                for key in keys:
                    remote_time_bytes = self._remote_index.get(key)
                    check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                    if check:
                        f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                        futures[f] = key

            # keys = []
            for f in as_completed(futures):
                key = futures[f]
                error = f.result()
                if error is not None:
                    failure_dict[key] = error
                # else:
                #     keys.append(key)
                    # yield key

        ## It's too risky to change the flag before/after in case the load process is cancelled part way
        # if not writable:
        #     self._local_file.reopen('r')

        return failure_dict
        # return keys


    def _load_item(self, key):
        """

        """
        # if key in self._deletes:
        #     raise KeyError(key)

        remote_time_bytes = self._remote_index.get(key)
        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)

        if check:
            # if not self._local_file.writable:
            #     self._local_file.reopen('w')
            #     failure = utils.get_remote_value(self._local_file, key, self._remote_session)
            #     self._local_file.reopen('r')
            # else:
            #     failure = utils.get_remote_value(self._local_file, key, self._remote_session)
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
        # self._executor.shutdown(cancel_futures=force_shutdown)
        # # del self._executor
        # # self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self._remote_session.threads)
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        """
        Return a Change object of the changes that have occurred during this session.
        """
        return Change(self)


    ## I don't think this is necessary...could be more of a problem...
    # def reopen(self, flag):
    #     """
    #     Reopen a file with a different flag. The flag must be either r or w.
    #     """
    #     if flag not in ('r', 'w'):
    #         raise ValueError('The flag must be either r or w.')

    #     self.sync()
    #     self._local_file.reopen(flag)
    #     self._remote_index.reopen(flag)

    #     if self._flag == 'r' and flag != 'r':
    #         lock = self._remote_session.create_lock()
    #         lock.aquire()
    #         self.lock = lock
    #     elif self._flag != 'r' and flag == 'r':
    #         self.lock.release()
    #         self.lock = None

    #     self._flag = flag


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

    # TODO
    # def rebuild_index(self):
    #     """
    #     Rebuild the remote index file from all the objects in the remote.
    #     """
    #     if self.writable:
    #         resp = self._session.list_object_versions(prefix=self.db_key + '/')


class RemoteConnGroup(EVariableLengthValue):
    """

    """
    def __init__(
            self,
            remote_session: remote.S3SessionReader | remote.S3SessionWriter,
            local_file_path: pathlib.Path,
            flag: str = "r",
            n_buckets: int=12007,
            buffer_size: int = 2**22,
            ):
        """

        """
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
        self.type = 'RemoteConnGroup'
        # self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=remote_session.threads)


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
                ebooklet_type = rc.get_type()
                ts = rc.get_timestamp()

            conn_dict = {'type': ebooklet_type,
                         'timestamp': ts,
                         'user_meta': user_meta,
                         'remote_conn': remote_conn.to_dict(),
                         }

            self._local_file.set(uuid_hex, conn_dict, ts)

        else:
            raise ValueError('File is open for read only.')


    def set(self, key, remote_conn: remote.S3Connection):
        """
        Use the add method to add remote connections to the group.
        """
        raise NotImplementedError('Use the add method to add remote connections to the group.')


def open(
    remote_conn: Union[remote.S3Connection, str, dict],
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    remote_conn_group: bool=False,
    ):
    """
    Open an S3 dbm-style database. This allows the user to interact with an S3 bucket like a MutableMapping (python dict) object.

    Parameters
    -----------
    remote_conn : S3Connection, str, or dict
        The object to connect to a remote. It can be an S3Connection object, an http url string, or a dict with the parameters for initializing an S3Connection object.

    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    value_serializer : str, class, or None
        Similar to the key_serializer, except for the values. Does not apply for the remote connection group.

    n_buckets : int
        The number of hash buckets to using in the indexing. Generally use the same number of buckets as you expect for the total number of keys.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    remote_conn_group : bool
        Should the file be opened as a remote connection group? If not, then it will be opened as a normal ebooklet (EVariableLengthValue). This parameter only applies when creating a new file.

    Returns
    -------
    Ebooklet

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
    local_file_path = pathlib.Path(file_path)

    local_file_exists = local_file_path.exists()

    ## Check and open the remote session
    remote_conn = remote.check_remote_conn(remote_conn, flag)
    remote_session, ebooklet_type = utils.open_remote_conn(remote_conn, flag, local_file_exists)

    if ebooklet_type is None:
        if remote_conn_group:
            return RemoteConnGroup(remote_session=remote_session, local_file_path=local_file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size)
        else:
            return EVariableLengthValue(remote_session=remote_session, local_file_path=local_file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
    elif ebooklet_type == 'EVariableLengthValue':
        return EVariableLengthValue(remote_session=remote_session, local_file_path=local_file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)
    elif ebooklet_type == 'RemoteConnGroup':
        return RemoteConnGroup(remote_session=remote_session, local_file_path=local_file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size)
    else:
        raise TypeError('Somehow the ebooklet got saved with an erroneous ebooklet type...')


