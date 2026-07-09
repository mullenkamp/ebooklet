#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
from collections.abc import Mapping, MutableMapping
from typing import Union
import logging
import os
import pathlib
import re
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import booklet
import orjson
import weakref
from itertools import count
from collections import deque
import urllib3

from . import utils
from . import remote

logger = logging.getLogger(__name__)


#######################################################
### Classes


_MISSING = object()


class RemoteIntegrityError(urllib3.exceptions.HTTPError):
    """
    The remote store contradicts its own index: the index claims key(s) whose
    backing object is missing (404), and a fresh index re-pull confirmed the
    claim. Distinct from connectivity errors so callers can tell "the remote
    is inconsistent" apart from "the remote is unreachable"; subclasses
    urllib3's HTTPError so existing handlers keep working.
    """


def _failure_exception(failure):
    """
    Choose the exception class for a surfaced fetch failure: confirmed
    integrity failures (missing-object markers) get RemoteIntegrityError so
    callers can discriminate; everything else stays a plain HTTPError.
    """
    if isinstance(failure, utils.MissingRemoteObject):
        return RemoteIntegrityError(failure)
    if isinstance(failure, dict) and any(isinstance(v, utils.MissingRemoteObject) for v in failure.values()):
        return RemoteIntegrityError(failure)
    return urllib3.exceptions.HTTPError(failure)

## RCG entry keys become S3 object-key suffixes in per-key mode; characters outside
## this set are known to break request signing (e.g. '!' fails B2 signature validation).
rcg_key_pattern = re.compile(r'^[A-Za-z0-9._-]+$')


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


    def pull(self):
        """
        Refresh this session's view of the remote index. Values refresh lazily per
        key on the next access (the read path compares local vs index timestamps).
        The index handle swap is serialized against point reads and load_items via
        an internal lock, but iteration (keys()) concurrent with a pull on the same
        instance may still observe a closed index.
        """
        self._ebooklet._pull_remote_index()


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

        with booklet.FixedLengthValue(self._changelog_path) as f:
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

        ## flag 'n' replaces the database with THIS session's writes only: purge
        ## everything else (transparently-read/materialized old keys and old index
        ## entries) so reads can't resurrect old data into the new database and the
        ## pushed index carries no dangling references to wiped objects.
        if self._ebooklet._flag == 'n':
            written = self._ebooklet._written_keys
            stale_local = [k for k in self._ebooklet._local_file.keys()
                           if k != utils.metadata_key_str and k not in written]
            for k in stale_local:
                del self._ebooklet._local_file[k]
            stale_index = [k for k in self._ebooklet._remote_index.keys()
                           if k != utils.metadata_key_str and k not in written]
            for k in stale_index:
                del self._ebooklet._remote_index[k]

        self.update()

        result = utils.update_remote(self._ebooklet._local_file, self._ebooklet._remote_index, self._changelog_path, self._ebooklet._remote_session, force_push, self._ebooklet._deletes, self._ebooklet._flag, self._ebooklet.type, self._ebooklet._num_groups)

        if isinstance(result, dict):
            # Partial failure — don't clean up changelog so push can be retried
            return result

        if result:
            self._changelog_path.unlink()
            self._changelog_path = None
            self._ebooklet._deletes.clear()

            if self._ebooklet._remote_session.uuid is None:
                self._ebooklet._remote_session._load_db_metadata()

        return result


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
            num_groups: int = None,
            lock_timeout: int = 300,
            force_lock: bool = False,
            ):
        """

        """
        self._init_common(remote_session, local_file_path, flag, value_serializer, n_buckets, buffer_size, 'EVariableLengthValue', num_groups, lock_timeout, force_lock)

    def _init_common(self, remote_session, local_file_path, flag, value_serializer, n_buckets, buffer_size, ebooklet_type, num_groups=None, lock_timeout=300, force_lock=False):
        """
        Shared initialization logic for EVariableLengthValue and RemoteConnGroup.
        """
        ## Lock the remote if file is opened for write
        if flag != 'r':
            lock = remote_session.create_lock()
            if force_lock:
                lock.break_other_locks()
            acquired = lock.acquire(timeout=lock_timeout)
            if not acquired:
                others = lock.other_locks()
                if others:
                    import datetime
                    now = datetime.datetime.now(datetime.timezone.utc)
                    ages = []
                    for lock_id, info in others.items():
                        ts = info.get('upload_timestamp')
                        if ts:
                            age = now - ts
                            ages.append(str(age).split('.')[0])
                    age_str = ', '.join(ages) if ages else 'unknown'
                    msg = (
                        f'Could not acquire write lock within {lock_timeout}s. '
                        f'Blocked by existing lock(s) held for: {age_str}. '
                        'If you are sure no other writer is active, re-open with force_lock=True to break stale locks.'
                    )
                else:
                    msg = (
                        f'Could not acquire write lock within {lock_timeout}s. '
                        'Re-open with force_lock=True to break stale locks.'
                    )
                raise TimeoutError(msg)
            self.writable = True

            ## Re-read the remote db metadata now that the lock is held: another
            ## writer may have created or updated the remote while this writer was
            ## waiting on the lock, and the uuid/timestamp/init_bytes/num_groups
            ## cached at session creation would otherwise be stale (a writer with a
            ## stale uuid=None would skip the remote index pull and clobber the
            ## other writer's push).
            remote_session._load_db_metadata()
        else:
            lock = None
            self.writable = False

        ## Everything between lock acquisition and finalizer registration must
        ## release the lock on failure - an early raise (e.g. local/remote UUID
        ## mismatch or a failed index fetch) would otherwise leave the exclusive
        ## remote lock held until garbage collection finally releases it.
        local_file = None
        remote_index = None
        try:
            ## flag 'n' guard: the 'n' contract keeps the old remote readable until
            ## push, and those reads need the OLD grouping - a conflicting
            ## num_groups cannot be honored.
            if flag == 'n' and num_groups is not None and remote_session.num_groups is not None and num_groups != remote_session.num_groups:
                raise ValueError(
                    f"num_groups={num_groups} conflicts with the existing remote's num_groups={remote_session.num_groups}. "
                    "flag 'n' keeps the old remote readable until push, which requires the existing grouping. "
                    "Either omit num_groups to inherit it, or call delete_remote() first to change the grouping."
                )

            ## Init the local file
            local_file_existed = local_file_path.exists()
            local_file, overwrite_remote_index = utils.init_local_file(local_file_path, flag, remote_session, value_serializer, n_buckets, buffer_size)

            remote_index_path = utils.get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag)

            ## Open remote index file
            remote_index = utils.open_remote_index(remote_index_path, flag, n_buckets, buffer_size)

            ## Resolve num_groups: remote metadata > user param
            if remote_session.num_groups is not None:
                resolved_num_groups = remote_session.num_groups
            else:
                resolved_num_groups = num_groups

            ## Nothing local records the creation-time num_groups choice (a deliberate
            ## trade-off to avoid another sidecar file), so reopening a
            ## created-but-not-yet-pushed database without re-passing num_groups would
            ## silently make the first push per-key. Warn loudly instead of guessing.
            if (flag in ('w', 'c') and local_file_existed and remote_session.uuid is None
                    and num_groups is None):
                warnings.warn(
                    'This database has not been pushed to the remote yet and num_groups was not '
                    'provided - the first push will use per-key storage. If the database was '
                    'created with grouped storage, re-open it passing the same num_groups.',
                    UserWarning,
                    stacklevel=4,  # _init_common -> subclass __init__ -> open_ebooklet/open_rcg -> user code
                )
        except BaseException:
            if remote_index is not None:
                try:
                    remote_index.close()
                except Exception:
                    pass
            if local_file is not None:
                try:
                    local_file.close()
                except Exception:
                    pass
            if lock is not None:
                lock.release()
            raise

        ## Finalizer
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session, lock)

        ## Assign properties
        self._flag = flag
        self.lock = lock
        self._local_file_path = local_file_path
        self._local_file = local_file
        self._remote_index_path = remote_index_path
        self._remote_index = remote_index
        ## Serializes the remote-index handle swap (_pull_remote_index closes and
        ## reopens the index booklet) against point reads and load_items - without
        ## it, a re-check triggered inside one thread's get() would close the index
        ## under a concurrent reader. RLock: _resolve_missing holds it while
        ## calling _pull_remote_index.
        self._index_lock = threading.RLock()
        self._deletes = set()
        self._remote_session = remote_session
        self._n_buckets = local_file._n_buckets
        self._buffer_size = buffer_size
        self.type = ebooklet_type
        self._num_groups = resolved_num_groups
        ## Keys explicitly written during THIS session (set/__setitem__/update/
        ## set_timestamp). Used by the flag 'n' push to build the replacement
        ## database from true writes only - transparently-read (materialized) old
        ## keys never leak into the new database.
        self._written_keys = set()


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
            raise _failure_exception(failure)

        self._local_file.sync()

        return self._local_file.get_metadata(include_timestamp=include_timestamp)


    def keys(self):
        """
        Returns a generator of the keys.
        """
        overlap = {utils.metadata_key_str}
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
            raise _failure_exception(failure_dict)

        return self._local_file.items()


    def values(self):
        """
        Returns an iterator of the values.
        """
        failure_dict = self.load_items()

        if failure_dict:
            raise _failure_exception(failure_dict)

        return self._local_file.values()

    def timestamps(self, include_value=False):
        """
        Return an iterator for timestamps for all keys. Optionally add values to the iterator.
        """
        failure_dict = self.load_items()

        if failure_dict:
            raise _failure_exception(failure_dict)

        return self._local_file.timestamps(include_value=include_value)


    def get_timestamp(self, key, include_value=False, decode_value=True, default=None):
        """
        Get a timestamp associated with a key. Optionally include the value.
        """
        failure = self._load_item(key)
        if failure:
            raise _failure_exception(failure)

        return self._local_file.get_timestamp(key, include_value=include_value, decode_value=decode_value, default=default)

    def set_timestamp(self, key, timestamp):
        """
        Set a timestamp for a specific key. The timestamp must be either an int of the number of microseconds in POSIX UTC time, an ISO 8601 datetime string with timezone, or a datetime object with timezone.
        """
        if self.writable:
            self._local_file.set_timestamp(key, timestamp)
            self._written_keys.add(key)
        else:
            raise ValueError('File is open for read only.')


    def set(self, key, value, timestamp=None, encode_value=True):
        """
        Set a value associated with a key.
        """
        if self.writable:
            self._local_file.set(key, value, timestamp=timestamp, encode_value=encode_value)
            self._written_keys.add(key)

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
            raise _failure_exception(failure)

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


    def prune(self, timestamp=None):
        """
        Reclaim LOCAL disk space. Removes overwritten/deleted local entries and,
        when a timestamp is given, evicts local keys older than it. Returns the
        number of removed items.

        Contract: prune never touches the remote. Remote index entries survive, so
        evicted values transparently re-pull on the next access, and pushes repack
        groups from the remote's full membership. Remote deletion is exclusively
        the job of `del`/`__delitem__` (+ push).
        """
        if self.writable:
            ## Normalize to int microseconds here: booklet's prune documents
            ## int/str/datetime but its prune_file compares raw against int
            ## timestamps (upstream bug, tracked in envlib OPEN_WORK).
            if timestamp is not None:
                timestamp = booklet.utils.make_timestamp_int(timestamp)
            removed = self._local_file.prune(timestamp=timestamp)
            self._n_buckets = self._local_file._n_buckets

            _ = self._remote_index.prune()

            return removed
        else:
            raise ValueError('File is open for read only.')


    def get_items(self, keys, default=None):
        """
        Return an iterator of the values associated with the input keys. It will first load all of the keys/values into the local file. Missing keys will return the default.
        """
        if not isinstance(keys, (list, tuple, set)):
            keys = tuple(keys)

        failure_dict = self.load_items(keys)

        if failure_dict:
            raise _failure_exception(failure_dict)

        for key in keys:
            output = self._local_file.get(key, default=default)
            yield key, output


    def load_items(self, keys=None):
        """
        Loads items into the local file from the remote. If keys is None, then it loads all of the values from the remote in to the local file. Returns a dict of failed transfers.
        """
        futures = {}
        failure_dict = {}

        with ThreadPoolExecutor(max_workers=self._remote_session.threads) as executor:
            ## The index-iteration phase holds _index_lock so a re-check in a
            ## concurrent thread cannot swap the index handle mid-scan. The
            ## fetch workers never touch the index (only local_file + session).
            with self._index_lock:
                if keys is None:
                    items_iter = self._remote_index.items()
                else:
                    items_iter = ((k, self._remote_index.get(k)) for k in keys)

                if self._num_groups is not None:
                    groups_to_download = {}
                    for key, remote_val in items_iter:
                        remote_time_bytes = remote_val[:7] if remote_val else None
                        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                        if check:
                            if key == utils.metadata_key_str:
                                f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                                futures[f] = key
                            else:
                                group_id = utils.key_to_group_id(key, self._num_groups)
                                offset = utils.bytes_to_int(remote_val[7:11])
                                length = utils.bytes_to_int(remote_val[11:15])
                                timestamp_int = utils.bytes_to_int(remote_val[:7])
                                groups_to_download.setdefault(group_id, []).append((key, offset, length, timestamp_int))

                    for group_id, key_infos in groups_to_download.items():
                        f = executor.submit(utils.get_remote_group_values, group_id, key_infos, self._local_file, self._remote_session)
                        futures[f] = f'_group_{group_id}'
                else:
                    for key, remote_val in items_iter:
                        remote_time_bytes = remote_val[:7] if remote_val else None
                        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                        if check:
                            f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                            futures[f] = key

            for f in as_completed(futures):
                key = futures[f]
                error = f.result()
                if error is not None:
                    failure_dict[key] = error

        ## Resolve missing-object markers ONCE per operation, after the pool has
        ## fully drained (the re-check protocol swaps the index handle - it must
        ## never run per-future).
        missing = {fk: e for fk, e in failure_dict.items() if isinstance(e, utils.MissingRemoteObject)}
        if missing:
            for fk, resolved in self._resolve_missing(missing).items():
                if resolved is None:
                    del failure_dict[fk]
                else:
                    failure_dict[fk] = resolved

        return failure_dict


    def _pull_remote_index(self):
        """
        Refresh this session's view of the remote index (the body extracted from
        Change.pull; Change.pull delegates here). Holds _index_lock across the
        handle swap so point reads and load_items never observe a closed index.
        """
        with self._index_lock:
            self.sync()

            ## Reload the full remote db metadata - not just the timestamp. A session
            ## created before the remote existed has uuid=None cached; refreshing only
            ## the timestamp would leave check_local_remote_sync permanently skipping
            ## the index pull (a stranded reader).
            self._remote_session._load_db_metadata()

            ## The session's group-mode view may predate the remote's creation (e.g. a
            ## reader opened before the first push) - adopt the remote's num_groups so
            ## subsequent reads use the right storage layout.
            if self._remote_session.num_groups is not None:
                self._num_groups = self._remote_session.num_groups

            ## Determine if a change has occurred
            overwrite_remote_index = utils.check_local_remote_sync(self._local_file, self._remote_session, self._flag)
            if not overwrite_remote_index:
                return

            ## Fetch FIRST, to a temp path: a failed download must leave the live
            ## session untouched (a close-then-fetch order would strand the session
            ## with a closed index handle).
            tmp_path = self._remote_index_path.parent.joinpath(self._remote_index_path.name + '.tmp')
            fetched = utils.fetch_remote_index(tmp_path, self._remote_session)
            if not fetched:
                return

            ## Swap the handle: close -> atomic replace -> reopen -> re-register the
            ## finalizer with the new index object (the old one is closed).
            self._remote_index.close()
            try:
                os.replace(tmp_path, self._remote_index_path)
            finally:
                new_index = utils.open_remote_index(self._remote_index_path, self._flag, self._n_buckets, self._buffer_size)
                self._remote_index = new_index

            self._finalizer.detach()
            self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, new_index, self._remote_session, self.lock)

            ## Record the freshness of this view so an immediate second pull() is a
            ## no-op. NOTE: this stamp must stay AFTER the fetch gate above - hoisting
            ## it would let a no-op re-check mask a later real index change.
            self._local_file._set_file_timestamp(self._remote_session.timestamp)


    def _resolve_missing(self, missing):
        """
        Re-check-then-loud protocol for MissingRemoteObject markers, ONE index
        re-pull for the whole batch. missing is {failure_key: marker}; returns
        {failure_key: narrowed_marker_or_None}.

        A marker means the index claimed key(s) whose backing object 404'd -
        either a legitimately-deleted key seen through a stale index, or a real
        integrity fault. Re-pulling the index disambiguates: keys the FRESH
        index no longer claims were legitimately deleted (treated as absent,
        with local cleanup so stale materialized values cannot resurrect);
        keys it still claims are confirmed integrity failures (kept, loud).

        Writers hold the S3 lock for their whole session, so no concurrent
        deleter can exist for them: their re-pull is a no-op (remote timestamp
        unchanged) -> still-claims -> loud, which is the intended behavior.

        Accepted residual: a reader re-checking during a writer's mid-push
        window (an emptied group object already deleted, the new db object not
        yet uploaded) re-pulls the OLD index and reports a loud failure for
        what is really a transient race.
        """
        with self._index_lock:
            try:
                self._pull_remote_index()
            except BaseException:
                all_keys = sorted({k for m in missing.values() for k in m.keys})
                logger.warning(
                    f'The index re-pull while investigating missing remote object(s) claimed by '
                    f'the index for key(s) {all_keys} itself failed; re-raising the pull error.'
                )
                raise

            out = {}
            for fkey, marker in missing.items():
                still_claimed = [k for k in marker.keys if self._remote_index.get(k) is not None]
                if still_claimed:
                    marker.keys = still_claimed
                    logger.warning(repr(marker))
                    out[fkey] = marker
                else:
                    ## Legitimate deletion: clean up any stale materialized local
                    ## values so the key does not resurrect from the local file.
                    for k in marker.keys:
                        if k == utils.metadata_key_str:
                            ## booklet has no metadata unset; overwrite with null so
                            ## get_metadata() returns None (a raw skip would serve
                            ## stale metadata after a legitimate remote deletion).
                            self._local_file.set_metadata(None)
                        elif k in self._local_file:
                            del self._local_file[k]
                    logger.info(
                        f"remote object '{marker.s3_key}' was deleted remotely; treating key(s) "
                        f'{marker.keys} as absent after refreshing the index'
                    )
                    out[fkey] = None
            return out


    def _load_item(self, key):
        """

        """
        with self._index_lock:
            remote_val = self._remote_index.get(key)
        remote_time_bytes = remote_val[:7] if remote_val else None
        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)

        if check:
            if self._num_groups is not None and key != utils.metadata_key_str:
                group_id = utils.key_to_group_id(key, self._num_groups)
                offset = utils.bytes_to_int(remote_val[7:11])
                length = utils.bytes_to_int(remote_val[11:15])
                timestamp_int = utils.bytes_to_int(remote_val[:7])
                failure = utils.get_remote_group_value(group_id, key, offset, length, timestamp_int, self._local_file, self._remote_session)
            else:
                failure = utils.get_remote_value(self._local_file, key, self._remote_session)

            if isinstance(failure, utils.MissingRemoteObject):
                failure = self._resolve_missing({key: failure})[key]
            return failure
        else:
            return None


    def __getitem__(self, key: str):
        value = self.get(key, default=_MISSING)

        if value is _MISSING:
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

            self._written_keys.discard(key)
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


    def sync(self, force_shutdown=False):
        """
        Syncronize all cache to disk. This ensures all data has been saved to disk properly. If force_shutdown is True, then it will immediately end any processes running in the background (not recommended unless there's a deadlock).
        """
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        """
        Return a Change object of the changes that have occurred during this session.
        """
        return Change(self)


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

    def map(self, func, keys=None, n_workers=None):
        """
        Apply func to items in parallel using multiprocessing.

        Parameters
        ----------
        func : callable
            A picklable function: func(key, value) -> (new_key, new_value) or None.
            Return a (key, value) tuple to yield the result. The output key can
            differ from the input key. Return None to skip (item not yielded).
            Must be a top-level function (not a lambda or closure).
        keys : iterable, optional
            Specific keys to process. If None, processes all keys.
        n_workers : int, optional
            Number of worker processes. Defaults to os.cpu_count().

        Yields
        ------
        tuple
            (key, value) pairs produced by func, as they complete.
        """
        if keys is not None and not isinstance(keys, (list, tuple)):
            keys = list(keys)

        failure_dict = self.load_items(keys)
        if failure_dict:
            raise _failure_exception(failure_dict)

        yield from self._local_file.map(func, keys=keys, n_workers=n_workers)


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
            num_groups: int = None,
            lock_timeout: int = 300,
            force_lock: bool = False,
            ):
        """

        """
        self._init_common(remote_session, local_file_path, flag, 'orjson', n_buckets, buffer_size, 'RemoteConnGroup', num_groups, lock_timeout, force_lock)


    def add(self, remote_conn: remote.S3Connection, key: str = None, user_meta=None):
        """
        Add or update (upsert) a remote connection entry in the group.

        Parameters
        ----------
        remote_conn : S3Connection
            Connection to the member remote. It must already exist (have been pushed).
        key : str or None
            The entry key. Defaults to the member remote's uuid hex. Must match
            [A-Za-z0-9._-]+ (entry keys become S3 object-key suffixes; other
            characters are known to break request signing).
        user_meta : JSON-serializable or None
            Caller-supplied metadata stored verbatim in the entry (e.g. a
            catalogue's queryable metadata dict).

        Entry schema (version 1) - treat as a stable contract:
        {'entry_version': 1,
         'type': <member ebooklet type>,
         'timestamp': <member remote's timestamp>,
         'remote_meta': <snapshot of the member remote's metadata slot>,
         'user_meta': <the user_meta argument>,
         'remote_conn': <S3Connection.to_dict() - never contains credentials>}
        Pre-0.9.0 entries lack 'entry_version' and stored the metadata snapshot
        under 'user_meta'.
        """
        if not self.writable:
            raise ValueError('File is open for read only.')

        if not isinstance(remote_conn, remote.S3Connection):
            raise TypeError('remote_conn/value must be a remote.S3Connection')

        if key is not None:
            if not isinstance(key, str):
                raise TypeError('key must be a str.')
            if key == utils.metadata_key_str:
                raise ValueError('key clashes with the internal metadata key.')
            if not rcg_key_pattern.match(key):
                raise ValueError('key must match [A-Za-z0-9._-]+ (entry keys become S3 object-key suffixes; other characters are known to break request signing).')

        if user_meta is not None:
            try:
                orjson.dumps(user_meta)
            except TypeError as e:
                raise TypeError(f'user_meta must be JSON-serializable: {e}')

        ## Get remote_conn metadata
        with remote_conn.open() as rc:
            uuid0 = rc.get_uuid()
            if uuid0 is None:
                raise ValueError('Remote does not exist. It must exist to be added to a RemoteConnGroup.')

            uuid_hex = uuid0.hex

            remote_meta = rc.get_user_metadata()
            ebooklet_type = rc.get_type()
            ts = rc.get_timestamp()

        if key is None:
            key = uuid_hex

        conn_dict = {'entry_version': 1,
                     'type': ebooklet_type,
                     'timestamp': ts,
                     'remote_meta': remote_meta,
                     'user_meta': user_meta,
                     'remote_conn': remote_conn.to_dict(),
                     }

        ## The entry is stamped with the WRITE time (timestamp=None -> now), not the
        ## member's timestamp - otherwise an upsert that changes only user_meta
        ## would never enter the changelog and would silently never push.
        self._local_file.set(key, conn_dict, None)
        self._written_keys.add(key)

        ## Clean up a stale default-keyed entry for the same member (left by a
        ## pre-0.9.0-style add) when migrating to explicit keys.
        if key != uuid_hex and uuid_hex in self:
            del self[uuid_hex]


    def set(self, key, remote_conn: remote.S3Connection):
        """
        Upsert an entry via add(). `rcg[key] = conn` is equivalent.
        """
        self.add(remote_conn, key=key)


def open_ebooklet(
    remote_conn: Union[remote.S3Connection, str, dict],
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    num_groups: int = None,
    lock_timeout: int = 300,
    force_lock: bool = False,
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
        The serializer to use to convert the input value to bytes. Run the booklet.available_serializers to determine the internal serializers that are available. None will require bytes as input.

    n_buckets : int
        The number of hash buckets to using in the indexing. Generally use the same number of buckets as you expect for the total number of keys.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    num_groups : int or None
        The number of groups for grouped S3 object storage. If not already prime, this value will be rounded up to the nearest prime for better hash distribution. Required when creating a new database (flag='n'). If None for a new database, per-key storage is used.
        For databases already pushed to the remote, this value is read from S3 metadata and the user-provided value is ignored. For a database created locally but NOT yet pushed, the creation-time choice is not recorded anywhere - re-pass the same num_groups when reopening before the first push; reopening without it emits a UserWarning and the first push would use per-key storage.
        Guidance: aim for groups of 10-100MB each. A reasonable starting point is max(10, total_expected_keys // 50). Too few groups means large S3 objects and slow partial updates; too many means more API calls per push. Each group's data is limited to 4GB due to offset encoding.

    lock_timeout : int
        Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.

    force_lock : bool
        If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.

    Returns
    -------
    EVariableLengthValue

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
    if num_groups is not None and num_groups < 1:
        raise ValueError('num_groups must be a positive integer.')
    if num_groups is not None:
        num_groups = utils.next_prime(num_groups)

    local_file_path = pathlib.Path(file_path)

    local_file_exists = local_file_path.exists()

    ## Check and open the remote session
    remote_conn = remote.check_remote_conn(remote_conn, flag)
    remote_session, ebooklet_type = utils.open_remote_conn(remote_conn, flag, local_file_exists)

    if ebooklet_type is not None and ebooklet_type != 'EVariableLengthValue':
        raise TypeError(f'The remote database is of type {ebooklet_type}, not EVariableLengthValue. Use open_rcg() instead.')

    return EVariableLengthValue(remote_session=remote_session, local_file_path=local_file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size, num_groups=num_groups, lock_timeout=lock_timeout, force_lock=force_lock)


def open_rcg(
    remote_conn: Union[remote.S3Connection, str, dict],
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    num_groups: int = None,
    lock_timeout: int = 300,
    force_lock: bool = False,
    ):
    """
    Open an S3-backed remote connection group. A remote connection group stores S3Connection references as key-value pairs, using orjson serialization.

    Parameters
    -----------
    remote_conn : S3Connection, str, or dict
        The object to connect to a remote. It can be an S3Connection object, an http url string, or a dict with the parameters for initializing an S3Connection object.

    file_path : str or pathlib.Path
        It must be a path to a local file location. If you want to use a tempfile, then use the name from the NamedTemporaryFile initialized class.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    n_buckets : int
        The number of hash buckets to using in the indexing. Generally use the same number of buckets as you expect for the total number of keys.

    buffer_size : int
        The buffer memory size in bytes used for writing. Writes are first written to a block of memory, then once the buffer if filled up it writes to disk. This is to reduce the number of writes to disk and consequently the CPU write overhead.
        This is only used when the file is open for writing.

    num_groups : int or None
        The number of groups for grouped S3 object storage. If not already prime, this value will be rounded up to the nearest prime for better hash distribution. Required when creating a new database (flag='n'). If None for a new database, per-key storage is used.
        For databases already pushed to the remote, this value is read from S3 metadata and the user-provided value is ignored. For a database created locally but NOT yet pushed, the creation-time choice is not recorded anywhere - re-pass the same num_groups when reopening before the first push; reopening without it emits a UserWarning and the first push would use per-key storage.
        Guidance: aim for groups of 10-100MB each. A reasonable starting point is max(10, total_expected_keys // 50). Too few groups means large S3 objects and slow partial updates; too many means more API calls per push. Each group's data is limited to 4GB due to offset encoding.

    lock_timeout : int
        Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.

    force_lock : bool
        If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.

    Returns
    -------
    RemoteConnGroup

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
    if num_groups is not None and num_groups < 1:
        raise ValueError('num_groups must be a positive integer.')
    if num_groups is not None:
        num_groups = utils.next_prime(num_groups)

    local_file_path = pathlib.Path(file_path)

    local_file_exists = local_file_path.exists()

    ## Check and open the remote session
    remote_conn = remote.check_remote_conn(remote_conn, flag)
    remote_session, ebooklet_type = utils.open_remote_conn(remote_conn, flag, local_file_exists)

    if ebooklet_type is not None and ebooklet_type != 'RemoteConnGroup':
        raise TypeError(f'The remote database is of type {ebooklet_type}, not RemoteConnGroup. Use open_ebooklet() instead.')

    return RemoteConnGroup(remote_session=remote_session, local_file_path=local_file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size, num_groups=num_groups, lock_timeout=lock_timeout, force_lock=force_lock)


