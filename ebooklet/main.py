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
import msgspec
import weakref
from itertools import count
from collections import deque
import urllib3

from . import utils
from . import remote
from .journal import JournalState, RemoteState
from .errors import (
    Error,
    ReadOnlyError,
    RemoteMissingError,
    RemoteIntegrityError,
    LockLostError,
    OfflineError,
    PushInProgressError,
    UUIDMismatchError,
    TRANSPORT_ERRORS,
)

logger = logging.getLogger(__name__)


#######################################################
### Classes


_MISSING = object()


## RemoteIntegrityError moved to errors.py in 0.10.0 (typed taxonomy); the
## import above keeps the old main.RemoteIntegrityError attribute path working.


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


class PushResult(msgspec.Struct):
    """
    The result of a push.

    updated: the remote changed - the db-object commit happened. A partial
    REPLACEMENT push commits nothing, so its updated is False; a partial
    ordinary push commits its successful groups/keys (updated True), unless
    every upload failed and nothing else (deletes, metadata, a new remote) was
    pending - then nothing is committed and updated is False.

    failures: per-key/per-group upload failures as 'ExceptionClassName:
    message' strings; empty means no failures. The pending changes for failed
    entries stay journaled - fix the cause and push again. (Failures of the
    commit itself are the OTHER failure channel: they RAISE - HTTPError for a
    failed commit PUT, LockLostError for a lost write lock - rather than
    returning a PushResult.)

    Truthiness: bool(result) is True only for a fully-successful push that
    changed the remote - a no-op push and any push with failures are falsy.
    (Before 0.10.0 push returned True/False/dict, and a non-empty partial-
    failure dict was truthy.)
    """
    updated: bool
    failures: dict

    def __bool__(self):
        return self.updated and not self.failures


def _failure_str(value):
    """Normalize a push-failure value ('ClassName: message' for exception-like
    values, plain str otherwise - s3func error payloads are dicts)."""
    if isinstance(value, (BaseException, utils.MissingRemoteObject)):
        return f'{type(value).__name__}: {value}'
    return str(value)

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
        ## Captured by build_changelog's sweep (0.10.1): {key: (ts, value_offset,
        ## value_len)} + the compaction_count snapshot the offsets are valid for.
        self._loc_map = None
        self._comp0 = None


    def pull(self):
        """
        Refresh this session's view of the remote index. Values refresh lazily per
        key on the next access (the read path compares local vs index timestamps).
        A fresh pull also DELETES locally-cached values the remote no longer
        provides (remote deletions propagate), except pending local writes and
        local values newer than the previously ingested index - this assumes
        write-time timestamps; explicit set(..., timestamp=...) stamps are
        incompatible with the reconciliation. Note a pull against an UNCHANGED
        remote is a no-op (freshness-gated): stale residue from pre-0.10.2
        sessions clears at the next actual remote change, not on upgrade.
        The index handle swap is serialized against point reads and load_items via
        an internal lock, but iteration (keys()/items()/timestamps()) concurrent
        with a pull on the same instance may observe a closed index or raise
        RuntimeError when the reconciliation deletes mid-iteration.
        """
        self._ebooklet._pull_remote_index()


    def build_changelog(self):
        """
        Determine if there are any changes between the local and remote
        databases (renamed from update() in 0.10.0 - the old name was easily
        confused with the MutableMapping update() on the ebooklet itself).
        """
        self._ebooklet.sync()
        changelog_path, loc_map, comp0 = utils.create_changelog(self._ebooklet._local_file_path, self._ebooklet._local_file, self._ebooklet._remote_index, self._ebooklet._remote_session, self._ebooklet._journal)

        self._changelog_path = changelog_path
        self._loc_map = loc_map
        self._comp0 = comp0


    def iter_changes(self):
        """
        Create an iterator of the changed/written keys. Pending DELETIONS are
        not part of the changelog file - see pending_deletes.
        """
        if not self._changelog_path:
            self.build_changelog()
        return utils.view_changelog(self._changelog_path)


    @property
    def pending_deletes(self):
        """
        The keys whose deletion is journaled but not yet pushed (deletions are
        applied to the remote by push()).
        """
        return frozenset(self._ebooklet._journal.deletes)


    def discard(self, keys=None):
        """
        Discard pending local changes. Changed/written keys are removed from
        the local file (their values transparently re-pull from the remote on
        next access); pending DELETIONS are cancelled and their index entries
        restored from the remote. If keys is None, discards all pending
        changes.
        """
        if not self._ebooklet.writable:
            raise ReadOnlyError('File is open for read-only.')

        if not self._changelog_path:
            self.build_changelog()

        journal = self._ebooklet._journal

        with booklet.FixedLengthValue(self._changelog_path) as f:
            if keys is None:
                rm_keys = list(f.keys())
            else:
                rm_keys = [key for key in keys if key in f]

            for key in rm_keys:
                del self._ebooklet._local_file[key]
                journal.discard_written(key)

        ## A discarded write whose key is missing from the local index copy
        ## may be a delete-then-re-set: `del k` removed k's entry and
        ## journaled a delete, the re-set cancelled that delete, and dropping
        ## the write now leaves no journal trace - so the forced re-pull
        ## below would be skipped, the index copy would stay without k, and
        ## the next push would publish an index without it. (Also true of a
        ## never-pushed key; the re-pull then simply restores nothing.)
        restore_index = any(key not in self._ebooklet._remote_index for key in rm_keys)

        ## Cancel journaled deletions: forget them, then force an index
        ## re-pull to restore the entries __delitem__ removed from the local
        ## index copy (the timestamp-gated pull would skip an unchanged
        ## remote).
        if keys is None:
            discard_deletes = set(journal.deletes)
        else:
            discard_deletes = journal.deletes & set(keys)
        for key in discard_deletes:
            journal.discard_delete(key)

        ## A full discard also reverts an unpushed metadata edit: restore the
        ## local slot from the cached remote metadata section.
        if keys is None and journal.meta_pending:
            remote_ts, data = utils.parse_meta_section(self._ebooklet._remote_state.meta_section)
            self._ebooklet._local_file.set_metadata(data, timestamp=remote_ts)
            journal.set_meta_pending(False)

        journal.persist(self._ebooklet._local_file)

        if discard_deletes or restore_index:
            self._ebooklet._pull_remote_index(force=True)

        self._changelog_path.unlink()
        self._changelog_path = None


    def push(self, force_push=False):
        """
        Updates the remote. It will regenerate the changelog to ensure the
        changelog is up-to-date. Returns a PushResult: `updated` says whether
        the remote changed, `failures` maps failed keys/groups to error
        strings (the pending changes for failed entries stay journaled for a
        retry), and `bool(result)` is True only for a fully-successful push
        that changed the remote. Failures of the COMMIT itself raise instead
        of returning: HTTPError for a failed commit PUT, LockLostError when
        the write lock was lost.
        Force_push will push the main file and the remote_index to the remote
        regardless of changes. Only necessary if upload failures occurred
        during a previous push.
        """
        ## File-mode check FIRST: a flag='r' session's remote session is a
        ## reader object with no writable attribute at all.
        if not self._ebooklet.writable:
            raise ReadOnlyError('File is open for read-only.')

        if not self._ebooklet._remote_session.writable:
            raise ReadOnlyError('Remote is not writable.')

        ## Re-verify the write lock at the push boundary: a holder whose ticket
        ## was broken (another client's force_lock) no longer has mutual
        ## exclusion and must not push. (The commit PUT re-verifies again -
        ## the point of no return.)
        if self._ebooklet.lock is not None and not self._ebooklet.lock.verify():
            raise LockLostError(
                "The write lock is no longer held (this session's lock ticket was broken "
                'by another client) - aborting the push. All pending changes are retained '
                'in the journal; re-open the file to re-acquire the lock and push again.'
            )

        journal = self._ebooklet._journal

        ## THE PRE-PUSH RE-CHECK (0.10.5). A session that opened against a remote
        ## reported ABSENT forgot its cached index and manifest (a dead
        ## incarnation must not be re-committed). But "absent" was one HEAD 404,
        ## and if it was wrong - a transient answer, a routing hiccup - this
        ## push would replace a LIVE remote's index with this file's keys only,
        ## silently (fsck would call the result healthy and the lost values
        ## orphans). Re-HEAD once here, under the write lock: if the remote
        ## exists after all, adopt it - fresh index plus reconciliation against
        ## this file's watermark - and carry on as an ordinary merge push.
        ## One extra HEAD, only on the absent path. Found by code review,
        ## 2026-09-08.
        remote_session = self._ebooklet._remote_session
        appeared = False
        if not remote_session.initialized:
            remote_session._load_db_metadata()
            appeared = remote_session.initialized

        ## The open's uuid rule (check_local_remote_sync), on every push: a
        ## remote made from a different local file is not this file's database.
        ## The pre-push adoption below would skip it (its forced pull does not
        ## check), reconciling this file against a foreign index - deleting
        ## values, possibly the only copy, that it lacks - and re-stamping the
        ## remote with this file's uuid, which locks out the writer that made
        ## it. Checked on every push, not only the one that discovered the
        ## remote, so a retry in the same session is refused too. A replacement
        ## (flag 'n') replaces whatever is there.
        if (remote_session.initialized and not journal.replace_pending
                and remote_session.uuid != self._ebooklet._local_file.uuid):
            raise UUIDMismatchError(
                'The remote was created from a different local file (a different UUID) after this '
                'session opened against it absent, so this push would not merge into it. Nothing was '
                'pushed; the pending changes are retained. Open a new local file against the remote '
                'and re-apply them there, or push this file elsewhere.'
            )

        if appeared:
            logger.warning('the remote was reported absent when this session opened but exists '
                           'now; adopting its index before pushing so this push merges into it '
                           'instead of replacing it')
            self._ebooklet._pull_remote_index(force=True)

        ## The push holds captured value offsets from the changelog sweep until
        ## its commit - a compaction (prune/clear) would invalidate them all,
        ## so both raise PushInProgressError while this flag is set. Spans the
        ## WHOLE push body (capture through phase D), not just update_remote.
        self._ebooklet._push_active = True
        try:
            ## A pending replacement replaces the database with this local file's
            ## written content only: purge everything else (transparently-read/
            ## materialized old keys and old index entries) so reads can't
            ## resurrect old data into the new database and the pushed index
            ## carries no dangling references to wiped objects.
            if journal.replace_pending:
                written = journal.written
                stale_local = [k for k in self._ebooklet._local_file.keys()
                               if k not in written]
                for k in stale_local:
                    del self._ebooklet._local_file[k]
                ## The index purge includes any stale metadata entry (metadata has
                ## no index entry in format 2).
                stale_index = [k for k in self._ebooklet._remote_index.keys()
                               if k not in written]
                for k in stale_index:
                    del self._ebooklet._remote_index[k]

            self.build_changelog()

            committed, failures = utils.update_remote(self._ebooklet._local_file, self._ebooklet._remote_index, self._ebooklet._remote_index_path, self._changelog_path, self._ebooklet._remote_session, force_push, journal, self._ebooklet._remote_state, journal.replace_pending, self._ebooklet.type, self._ebooklet._group_bytes, lock=self._ebooklet.lock, loc_map=self._loc_map, comp0=self._comp0, packers=self._ebooklet._push_packers)

            if failures:
                # Partial failure — don't clean up changelog so push can be retried.
                # replace_pending is also kept so a retry redoes the full
                # wipe-and-replace (the remote must never end up half old, half new).
                # updated is whether the commit happened: a partial REPLACEMENT
                # commits nothing, and neither does a push whose every upload
                # failed with nothing else pending (a failed commit PUT raises).
                failures = {key: _failure_str(value) for key, value in failures.items()}
                return PushResult(updated=committed, failures=failures)

            ## (A replacement session stops being one at its commit:
            ## update_remote clears replace_pending in the journal write that
            ## clears the committed keys. Without that, every later push re-ran
            ## the wipe - 0.9.4's data-loss fix.)

            ## After this session's own commit, a formerly legacy remote is in
            ## the current format - index fetches are allowed again. (The
            ## commit itself refreshed the session's db metadata.)
            if self._ebooklet._index_fetch_suppressed:
                self._ebooklet._index_fetch_suppressed = False

            ## No failures: nothing is left to retry from this changelog, whether
            ## or not anything was committed (a no-op push included).
            self._changelog_path.unlink()
            self._changelog_path = None

            return PushResult(updated=committed, failures={})
        finally:
            self._ebooklet._push_active = False


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
            *,
            group_bytes=utils.UNSET,
            lock_timeout: int = 300,
            force_lock: bool = False,
            push_packers: int = 1,
            ):
        """

        """
        self._init_common(remote_session, local_file_path, flag, value_serializer, n_buckets, buffer_size, 'EVariableLengthValue', group_bytes, lock_timeout, force_lock, push_packers)

    def _init_common(self, remote_session, local_file_path, flag, value_serializer, n_buckets, buffer_size, ebooklet_type, group_bytes=utils.UNSET, lock_timeout=300, force_lock=False, push_packers=1):
        """
        Shared initialization logic for EVariableLengthValue and RemoteConnGroup.
        """
        if not isinstance(push_packers, int) or push_packers < 1:
            raise ValueError('push_packers must be an integer >= 1.')
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
                        'If you are sure no other writer is active, re-open with force_lock=True '
                        '(age-gated: it only breaks lock tickets older than 2 hours, so a live '
                        'writer is protected; to break YOUNGER tickets, call '
                        'S3SessionWriter.break_other_locks(timestamp=<now>) directly).'
                    )
                else:
                    msg = (
                        f'Could not acquire write lock within {lock_timeout}s. '
                        'Re-open with force_lock=True to break stale locks (age-gated: only '
                        'tickets older than 2 hours are broken).'
                    )
                raise TimeoutError(msg)
            self.writable = True

            ## Re-read the remote db metadata now that the lock is held: another
            ## writer may have created or updated the remote while this writer was
            ## waiting on the lock, and the uuid/timestamp/init_bytes/format
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
            remote_kind = remote_session.storage_kind
            ## Legacy remotes (format 1, or hash-grouped format 2) are never
            ## read; only the replacement paths may proceed (gate below).
            legacy_remote = remote_kind == utils.STORAGE_LEGACY
            explicit_mode = group_bytes is not utils.UNSET
            wanted_kind = None
            if explicit_mode:
                wanted_kind = utils.STORAGE_GROUPED if group_bytes is not None else utils.STORAGE_PER_KEY

            ## flag 'n' guard: the 'n' contract keeps the old remote readable until
            ## push, and those reads need the OLD storage mode's index layout - a
            ## conflicting mode cannot be honored. (A different group_bytes within
            ## grouped mode is fine: it only steers this writer's allocation.)
            if (flag == 'n' and explicit_mode and remote_kind in (utils.STORAGE_PER_KEY, utils.STORAGE_GROUPED)
                    and wanted_kind != remote_kind):
                raise ValueError(
                    f"group_bytes={group_bytes!r} asks for {wanted_kind} storage, but the existing remote "
                    f"is {remote_kind}. flag 'n' keeps the old remote readable until push, which requires "
                    "its storage mode. Either omit group_bytes to inherit it, or call delete_remote() "
                    'first to change the storage mode.'
                )

            ## Init the local file
            local_file, overwrite_remote_index = utils.init_local_file(local_file_path, flag, remote_session, value_serializer, n_buckets, buffer_size)

            ## Load the persistent journal. flag 'n' recreates the local file,
            ## which destroys any previous journal slot - exactly right, a
            ## replacement starts with no pending history.
            journal = JournalState.load(local_file)
            ## Loaded HERE, next to the journal, so the forget block below can
            ## reset it before the index fetch. (The helper persists the reset
            ## at once, so a later reload would read the reset slot - the early
            ## load is for a single obvious ordering, not a correctness gate.)
            remote_state = RemoteState.load(local_file)
            if flag == 'n':
                journal.set_replace_pending(True)
            elif journal.replace_pending:
                warnings.warn(
                    "This local file carries an unpushed remote REPLACEMENT (a previous "
                    "flag='n' session closed before pushing) - the next push will replace "
                    'the remote database with this local content. To cancel the '
                    'replacement instead, delete the local file and re-open from the remote.',
                    UserWarning,
                    stacklevel=4,
                )

            ## A remote that no longer exists (delete_remote(), or torn down out of
            ## band) leaves this local file's caches describing a DEAD incarnation:
            ## the sidecar's index entries and the slot-2 manifest name objects
            ## that are gone, and the next push would commit them verbatim - a
            ## ghost claim every reader then trips over as RemoteIntegrityError
            ## (found live, 2026-09-07). No uuid check can catch it: an
            ## uninitialised remote has no uuid, so check_local_remote_sync never
            ## runs. Forget the dead incarnation for WRITERS only:
            ##   * 'r' is left alone - readers heal per key through
            ##     _resolve_missing's remote_gone path, and offline sessions
            ##     ('r'-only) also report initialized=False and must keep their
            ##     cache;
            ##   * a journaled replacement ('w' recovering a crashed 'n') keeps
            ##     its sidecar: the replacement purge at push drops everything
            ##     not written anyway, and that path is tested as-is;
            ##   * 'n' has fresh slots already (init_local_file recreated the
            ##     booklet) but the sidecar FILE survived, so pre-push keys()
            ##     listed ghosts.
            ## Only re-derivable state is dropped - see forget_remote_incarnation.
            if (flag != 'r' and not remote_session.initialized
                    and (flag == 'n' or not journal.replace_pending)):
                stale_sidecar = utils.remote_index_sidecar_path(local_file_path)
                if stale_sidecar.exists():
                    ## Unconditional: open_remote_index recreates it empty below. This
                    ## also covers the incident's literal shape - a FRESH local file
                    ## beside an orphaned sidecar - where there is nothing else to forget.
                    stale_sidecar.unlink()
                ## The rest only when this file was once IN SYNC with a remote (a
                ## dead incarnation to forget). A database that has simply never
                ## been pushed has nothing cached and must not rewrite its header
                ## or log "no longer exists" on every open.
                if (remote_state.remote_ts is not None or remote_state.manifest
                        or remote_state.meta_section is not None):
                    logger.info(f"removed the remote-index sidecar '{stale_sidecar}': the remote it "
                                'described no longer exists')
                    utils.forget_remote_incarnation(local_file, journal, remote_state,
                                                    reason=f"open flag='{flag}'")

            ## Format gate. 0.11 reads per-key (format 2) and write-order
            ## grouped (format 3) remotes. Legacy remotes - format 1, and
            ## hash-grouped format 2 - are refused for r/w/c; the two
            ## REPLACEMENT paths (flag='n', and 'w' recovering a crashed 'n'
            ## via the journaled intent) may proceed, but run "index-fetch-
            ## suppressed": no legacy index body is ever ingested - reads serve
            ## only the local image until this session's own commit.
            index_fetch_suppressed = False
            if legacy_remote:
                if flag == 'n' or journal.replace_pending:
                    index_fetch_suppressed = True
                else:
                    if remote_session.legacy_num_groups is not None:
                        what = (f'a hash-grouped remote (format_version {remote_session.format_version}, '
                                f'num_groups={remote_session.legacy_num_groups})')
                    else:
                        what = f'a format_version {remote_session.format_version} remote'
                    raise utils.UnsupportedFormatError(
                        f'This is {what}, which ebooklet >= 0.11 does not read. Move it to the '
                        "current format: with ebooklet<0.11, open it 'w' against its remote and "
                        'call load_items() so the local file holds every value, then '
                        "delete_remote(); then push the local file again with this ebooklet "
                        '(it becomes a write-order grouped remote).'
                    )

            ## Resolve the storage mode BEFORE the index fetch - the sidecar's
            ## layout follows it. An existing remote decides (an explicit
            ## group_bytes for the other mode is ignored with a warning; under
            ## flag 'n' it raised above). A READER with no remote to ask
            ## (offline, or the remote is gone) holds an index, and its layout
            ## decides in the same way. Otherwise an explicit group_bytes, then
            ## the journal's recorded mode, then the 0.11 default: grouped. (A
            ## writer never infers from the sidecar: a 0.10 hash-grouped
            ## sidecar is 15 bytes too, and a writer's mode decides what it
            ## pushes.)
            remote_index_path = utils.remote_index_sidecar_path(local_file_path)
            sidecar_len = utils.sidecar_value_len(remote_index_path)
            if remote_kind in (utils.STORAGE_PER_KEY, utils.STORAGE_GROUPED):
                storage = remote_kind
                deciding = 'the remote'
            elif flag == 'r' and remote_kind is None and sidecar_len is not None:
                storage = (utils.STORAGE_GROUPED if sidecar_len == utils.INDEX_LEN_GROUPED
                           else utils.STORAGE_PER_KEY)
                deciding = "this reader's local index"
            else:
                storage = None
            if storage is not None:
                if explicit_mode and wanted_kind != storage:
                    warnings.warn(
                        f'group_bytes={group_bytes!r} asks for {wanted_kind} storage, but '
                        f'{deciding} is {storage}; {deciding} wins.',
                        UserWarning, stacklevel=4,
                    )
            elif explicit_mode:
                storage = wanted_kind
            elif journal.storage is not None:
                storage = (utils.STORAGE_PER_KEY if journal.storage == utils.STORAGE_PER_KEY
                           else utils.STORAGE_GROUPED)
            else:
                storage = utils.STORAGE_GROUPED
            explicit_group_bytes = group_bytes if isinstance(group_bytes, int) else None
            if storage == utils.STORAGE_GROUPED:
                resolved_group_bytes = _grouped_target(explicit_group_bytes, remote_session)
            else:
                resolved_group_bytes = None
            if flag != 'r':
                journal.set_storage(storage)
            index_value_len = (utils.INDEX_LEN_GROUPED if storage == utils.STORAGE_GROUPED
                               else utils.INDEX_LEN_PER_KEY)

            ## Sidecar layout. A sidecar whose layout does not match the mode
            ## is discarded. That happens only when this open replaces it: a
            ## live remote (re-fetched below, or a suppressed replacement over a
            ## legacy remote, which is live too), or a writer. An OFFLINE
            ## session, or a reader whose remote is gone, took its mode from
            ## the sidecar above, so it always keeps it: the sidecar is what
            ## keys() and the materialized values hang on.
            if sidecar_len is not None and sidecar_len != index_value_len:
                remote_index_path.unlink()
                overwrite_remote_index = True

            ## The cached remote state must describe the remote's CURRENT
            ## commit, or the sidecar beside it is stale: a commit that crashed
            ## after its PUT, or another writer's commit after this file's
            ## writer lost its lock (whose local stamp can then even be NEWER
            ## than the remote's). Re-fetch the whole index - never refresh the
            ## manifest alone over a stale index.
            if (not index_fetch_suppressed and remote_session.initialized
                    and remote_state.remote_ts != remote_session.timestamp):
                overwrite_remote_index = True

            if index_fetch_suppressed:
                ## Never fetch the legacy body. flag='n' starts from a fresh
                ## EMPTY index (old keys read as absent - the documented
                ## semantic narrowing of a replacement over a legacy remote);
                ## 'w'-recovery keeps the crashed session's sidecar (the
                ## replacement image) when its layout fits.
                if flag == 'n' and remote_index_path.exists():
                    remote_index_path.unlink()
                index_fetched = False
                fetched_manifest = None
                fetched_meta = None
            else:
                remote_index_path, index_fetched, fetched_manifest, fetched_meta = utils.get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag)

            ## Open remote index file
            remote_index = utils.open_remote_index(remote_index_path, flag, n_buckets, buffer_size, index_value_len)

            ## The persistent remote-state cache (manifest + metadata section
            ## of the last in-sync db object), refreshed from what the open
            ## fetched.
            ## The ingest stamp of the PREVIOUS index this local file adopted -
            ## captured BEFORE update_committed overwrites it; it is the
            ## reconciliation discriminator between remotely-sourced values
            ## (ts <= it) and post-sync local writes (ts > it).
            prev_synced_ts = remote_state.remote_ts
            if index_fetched:
                remote_state.update_committed(fetched_manifest, fetched_meta, remote_session.timestamp)
                utils.refresh_local_metadata(local_file, journal, fetched_meta)
                remote_state.persist(local_file)

            ## Replay journaled deletes onto the fresh index copy so deleted
            ## keys cannot resurrect through contains/keys/len/reads (the
            ## same replay runs after every _pull_remote_index handle swap).
            for _k in journal.deletes:
                if _k in remote_index:
                    del remote_index[_k]

            ## Reconcile the local file against the fresh index: keys it no
            ## longer claims were deleted remotely, and without this a warm
            ## materialized copy is served (and re-pushed) forever. Only when
            ## this open actually INGESTED a fresh index - a reused cached
            ## sidecar was already reconciled by the ingest that fetched it.
            ## LAST: stamp the local file with the timestamp of the commit it
            ## just ingested (the same order as _pull_remote_index: the index
            ## and the remote-state cache are durable first). Without it, every
            ## later open saw the remote as newer and re-downloaded the whole
            ## db object until a pull or push caught the stamp up. A skipped
            ## reconciliation leaves the stamp old, so the next open retries it.
            if index_fetched:
                removed = utils.reconcile_local_with_index(local_file, remote_index, journal, prev_synced_ts)
                if removed is not None:
                    local_file._set_file_timestamp(remote_session.timestamp)

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

        ## Finalizer (persists the journal before closing the local file, so a
        ## GC'd session still records its pending state)
        self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, local_file, remote_index, remote_session, lock, journal)

        ## Assign properties
        ## _flag is FROZEN after open: the session-lifecycle facts it used to
        ## proxy live in the journal (replace_pending) and the remote session
        ## (initialized) - Seam-1 decomposition.
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
        ## The persistent pending-change journal: written keys, pending deletes,
        ## the storage-mode choice, replacement intent, pending metadata. Replaces
        ## the memory-only _written_keys/_deletes sets (Seam 2).
        self._journal = journal
        ## The persistent remote-state cache: the manifest (gid -> live
        ## generation) grouped reads resolve through, and the metadata section
        ## pushes carry forward (reserved slot 2).
        self._remote_state = remote_state
        ## True while this session sits over a format-1 remote it will
        ## replace: no index-body fetch may happen until its own v2 commit.
        self._index_fetch_suppressed = index_fetch_suppressed
        self._remote_session = remote_session
        ## Offline session: reads serve the local cache only; anything that
        ## needs the remote raises OfflineError.
        self._offline = isinstance(remote_session, remote.OfflineSession)
        self._n_buckets = local_file._n_buckets
        self._buffer_size = buffer_size
        self.type = ebooklet_type
        ## Push read-gate width: how many pack workers may read the local disk
        ## at once during push() (PUT concurrency is remote_session.threads).
        self._push_packers = push_packers
        ## True while a push is running - prune()/clear() raise during it
        ## (they would invalidate the push's captured value offsets).
        self._push_active = False
        ## The storage mode pushes use: None = per-key, an int = grouped with
        ## that packing target (see the group_bytes property). An explicit int
        ## argument is kept so a re-pull never replaces it with the remote's.
        self._group_bytes = resolved_group_bytes
        self._explicit_group_bytes = explicit_group_bytes


    @property
    def group_bytes(self):
        """
        This session's storage mode: None for per-key storage (one remote
        object per key, format 2), or the packing target in bytes for
        write-order grouped storage (format 3). The mode follows the remote
        once it exists. The target is the group_bytes argument if one was
        passed; otherwise the value the grouped remote records (whatever its
        last commit packed with); otherwise ebooklet.DEFAULT_GROUP_BYTES.
        """
        return self._group_bytes

    @property
    def offline(self):
        """
        True when this session was opened offline (offline=True, or
        offline='auto' fell back because the remote was unreachable). Offline
        reads serve the local cache as-is; values not materialized locally
        raise OfflineError.
        """
        return self._offline


    def set_metadata(self, data, timestamp=None):
        """
        Sets the metadata for the booklet. The data input must be a json serializable object. Optionally assign a timestamp.
        """
        if self.writable:
            self._local_file.set_metadata(data, timestamp)
            ## meta_pending marks an unpushed local metadata edit; it protects
            ## the edit from being overwritten by remote refreshes and is
            ## cleared when a push uploads the metadata.
            self._journal.set_meta_pending(True)
        else:
            raise ReadOnlyError('File is open for read only.')


    def get_metadata(self, include_timestamp=False):
        """
        Get the metadata. Optionally include the timestamp in the output.
        Will return None if no metadata has been assigned.

        Metadata rides the db-object payload (format 2): the local slot is
        refreshed whenever the index is pulled, so this is a local read.
        """
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
            if key in utils.reserved_key_strs:
                raise ValueError(f"'{key}' is a reserved internal key.")
            self._local_file.set_timestamp(key, timestamp)
            ## record_write maintains the journal invariant written ∩ deletes = ∅
            ## (a re-written key must not stay pending-delete: the push's delete
            ## pass runs after the upload pass and would remove the fresh index
            ## entry, silently losing the key).
            self._journal.record_write(key)
        else:
            raise ReadOnlyError('File is open for read only.')


    def set(self, key, value, timestamp=None, encode_value=True):
        """
        Set a value associated with a key.
        """
        if self.writable:
            if key in utils.reserved_key_strs:
                raise ValueError(f"'{key}' is a reserved internal key.")
            self._local_file.set(key, value, timestamp=timestamp, encode_value=encode_value)
            ## record_write maintains written ∩ deletes = ∅ (see set_timestamp).
            self._journal.record_write(key)

        else:
            raise ReadOnlyError('File is open for read only.')


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


    def update(self, other=(), /, **kwargs):
        """
        Set many keys/values with dict.update semantics: accepts a mapping, an
        iterable of key/value pairs, and/or keyword arguments. Every pair
        routes through __setitem__, so the reserved-key and serializer rules
        apply per key.
        """
        if not self.writable:
            raise ReadOnlyError('File is open for read only.')
        if isinstance(other, Mapping):
            items = other.items()
        elif hasattr(other, 'keys'):
            items = ((key, other[key]) for key in other.keys())
        else:
            items = other
        for key, value in items:
            self[key] = value
        for key, value in kwargs.items():
            self[key] = value


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
        if self._push_active:
            raise PushInProgressError(
                'prune() is not allowed while a push is running - it would move the '
                'value bytes the push is reading. Wait for the push to finish.'
            )
        if self.writable:
            ## Normalize to int microseconds here: booklet's prune documents
            ## int/str/datetime but its prune_file compares raw against int
            ## timestamps (upstream bug, tracked in envlib OPEN_WORK).
            if timestamp is not None:
                timestamp = booklet.utils.make_timestamp_int(timestamp)
            ## keep_keys: a timestamp eviction must never evict a journaled
            ## PENDING write - that would silently launder unpushed data into
            ## the changelog's dropped-key warning.
            removed = self._local_file.prune(timestamp=timestamp, keep_keys=self._journal.written)
            self._n_buckets = self._local_file._n_buckets

            _ = self._remote_index.prune()

            return removed
        else:
            raise ReadOnlyError('File is open for read only.')


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
        dispatched = []

        with ThreadPoolExecutor(max_workers=self._remote_session.threads) as executor:
            ## The index-iteration phase holds _index_lock so a re-check in a
            ## concurrent thread cannot swap the index handle mid-scan. The
            ## fetch workers never touch the index (only local_file + session).
            with self._index_lock:
                if keys is None:
                    items_iter = self._remote_index.items()
                else:
                    items_iter = ((k, self._remote_index.get(k)) for k in keys)

                ## Each index entry says how its value is stored: a grouped
                ## (19-byte) entry names its group, a per-key (15-byte) entry
                ## its own object.
                groups_to_download = {}
                to_fetch = []
                for key, remote_val in items_iter:
                    ## Stale metadata entries can survive in indexes built
                    ## from pre-format-2 local files - never fetched.
                    if key == utils.metadata_key_str:
                        continue
                    ## Read-your-writes gate: a journaled pending write is
                    ## the truth for its key - never pull the remote value
                    ## over it, regardless of timestamps.
                    if key in self._journal.written:
                        continue
                    remote_time_bytes = remote_val[:7] if remote_val else None
                    check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)
                    if check:
                        timestamp_int, group_id, offset, length = utils.decode_index_entry(remote_val)
                        if group_id is None:
                            to_fetch.append(key)
                        else:
                            groups_to_download.setdefault(group_id, []).append((key, offset, length, timestamp_int))

                ## Offline: never dispatch fetch workers - raise ONE named
                ## error before any future exists (a worker raise would
                ## surface as a raw future exception, not a clean error).
                if self._offline and (groups_to_download or to_fetch):
                    needed = sorted([k for infos in groups_to_download.values()
                                     for k, _o, _l, _t in infos] + to_fetch)
                    raise OfflineError(
                        f'This session is offline and the value(s) for key(s) {needed} '
                        'are not materialized in the local cache.'
                    )

                ## Resolve generations INSIDE _index_lock: the manifest is
                ## updated atomically with the index handle, so the pairs
                ## are consistent here.
                for group_id, key_infos in groups_to_download.items():
                    gen = self._remote_state.manifest.get(group_id)
                    if gen is None:
                        ## The index claims members of a group the manifest
                        ## does not reference - route through the re-check
                        ## protocol like any missing backing object.
                        failure_dict[f'_group_{group_id}'] = utils.MissingRemoteObject(
                            f'{group_id}.<unmanifested>', [k for k, _o, _l, _t in key_infos])
                        continue
                    f = executor.submit(utils.get_remote_group_values, group_id, gen, key_infos, self._local_file, self._remote_session)
                    futures[f] = f'_group_{group_id}'
                    dispatched.extend(k for k, _o, _l, _t in key_infos)
                for key in to_fetch:
                    f = executor.submit(utils.get_remote_value, self._local_file, key, self._remote_session)
                    futures[f] = key
                dispatched.extend(to_fetch)

            for f in as_completed(futures):
                key = futures[f]
                error = f.result()
                if error is not None:
                    failure_dict[key] = error

        ## Re-materialization guard: a worker may have completed against an
        ## index entry captured before a concurrent pull swapped + reconciled
        ## the index - drop any fetched key the CURRENT index no longer
        ## claims, else a remotely-deleted key re-enters the local file and
        ## serves until the next remote change (the defect reconciliation
        ## exists to fix). Journal-pending writes are never touched.
        if dispatched:
            with self._index_lock:
                for _k in dispatched:
                    if (_k not in utils.reserved_key_strs
                            and _k not in self._remote_index and _k not in self._journal.written
                            and _k in self._local_file):
                        del self._local_file[_k]

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


    def _pull_remote_index(self, force=False):
        """
        Refresh this session's view of the remote index (the body extracted from
        Change.pull; Change.pull delegates here). Holds _index_lock across the
        handle swap so point reads and load_items never observe a closed index.
        force=True skips the timestamp freshness gate (used by discard() to
        restore index entries a journaled delete removed locally).
        """
        with self._index_lock:
            ## An index-fetch-suppressed session (replacing a legacy remote)
            ## must never ingest the old body; its view is the local image
            ## until its own commit.
            if self._index_fetch_suppressed:
                return

            self.sync()

            ## Reload the full remote db metadata - not just the timestamp. A session
            ## created before the remote existed has uuid=None cached; refreshing only
            ## the timestamp would leave check_local_remote_sync permanently skipping
            ## the index pull (a stranded reader).
            self._remote_session._load_db_metadata()

            ## The session's storage mode may predate the remote's creation (a
            ## session opened before the first push, or a writer whose remote
            ## turned out to exist at its pre-push re-check) - adopt the
            ## remote's mode so pushes write the layout the index has, and its
            ## recorded group_bytes unless this session was given one. A legacy
            ## remote is never ingested.
            remote_kind = self._remote_session.storage_kind
            if remote_kind == utils.STORAGE_LEGACY:
                raise utils.UnsupportedFormatError(
                    'The remote is a legacy (hash-grouped or format-1) database, which '
                    'ebooklet >= 0.11 does not read - see the open-time error for the move '
                    'to the current format.'
                )
            if remote_kind == utils.STORAGE_GROUPED:
                self._group_bytes = _grouped_target(self._explicit_group_bytes, self._remote_session)
            elif remote_kind == utils.STORAGE_PER_KEY:
                self._group_bytes = None
            if remote_kind is not None and self.writable:
                self._journal.set_storage(remote_kind)
                self._journal.persist(self._local_file)

            ## Determine if a change has occurred: a newer remote stamp, or a
            ## cached remote state that does not describe the remote's current
            ## commit (see the open path).
            overwrite_remote_index = (
                force
                or utils.check_local_remote_sync(self._local_file, self._remote_session, self._flag)
                or (self._remote_session.initialized
                    and self._remote_state.remote_ts != self._remote_session.timestamp)
            )
            if not overwrite_remote_index:
                return

            ## Fetch FIRST, to a temp path: a failed download must leave the live
            ## session untouched (a close-then-fetch order would strand the session
            ## with a closed index handle).
            tmp_path = self._remote_index_path.parent.joinpath(self._remote_index_path.name + '.tmp')
            fetched, manifest, meta_section = utils.fetch_remote_index(tmp_path, self._remote_session)
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

            ## Replay journaled deletes onto the fresh index copy (still inside
            ## _index_lock, atomic with the handle swap) so a re-pull cannot
            ## resurrect a pending-delete key into contains/keys/reads.
            for _k in self._journal.deletes:
                if _k in new_index:
                    del new_index[_k]

            ## Reconcile the local file against the fresh index: keys it no
            ## longer claims were deleted remotely - without this, a warm
            ## materialized copy is served (and re-pushed) forever. remote_ts
            ## is read BEFORE update_committed: it is the ingest stamp of the
            ## PREVIOUS index, the discriminator between remotely-sourced
            ## values (ts <= it) and post-sync local writes (ts > it).
            removed = utils.reconcile_local_with_index(self._local_file, new_index, self._journal, self._remote_state.remote_ts)

            ## Adopt the pulled manifest + metadata INSIDE the same critical
            ## section as the handle swap - load_items must never pair a new
            ## index with an old manifest.
            self._remote_state.update_committed(manifest, meta_section, self._remote_session.timestamp)
            utils.refresh_local_metadata(self._local_file, self._journal, meta_section)

            self._finalizer.detach()
            self._finalizer = weakref.finalize(self, utils.ebooklet_finalizer, self._local_file, new_index, self._remote_session, self.lock, self._journal)

            ## Persist the remote-state cache BEFORE stamping freshness: a
            ## crash between the two must leave the stamp OLD (forcing a
            ## harmless re-fetch) rather than claiming freshness over a stale
            ## manifest.
            self._remote_state.persist(self._local_file)

            ## Record the freshness of this view so an immediate second pull() is a
            ## no-op. NOTE: this stamp must stay AFTER the fetch gate above - hoisting
            ## it would let a no-op re-check mask a later real index change. A
            ## skipped reconciliation leaves the stamp old, so the next pull
            ## re-fetches and reconciles again.
            if removed is not None:
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

        Generational storage (format 2) makes "still claimed" the COMMON
        HEALABLE case for readers: a writer committed and GC'd the old
        generation between this reader's index pull and its value fetch. The
        re-pull refreshes both the index and the manifest, so the fetch is
        re-issued ONCE against the new generation; only a second failure goes
        loud. (Residual: a false RemoteIntegrityError needs two full writer
        commit+GC cycles inside one reader operation - an extremely hot
        writer, not corruption.)

        A re-pull that finds the remote GONE (torn down) treats every
        outstanding marker as clean absence, not an integrity fault.
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

            ## Whole-remote absence: the database itself no longer exists -
            ## every outstanding marker is clean absence (with local cleanup),
            ## never an integrity fault.
            remote_gone = not self._remote_session.initialized

            out = {}
            for fkey, marker in missing.items():
                if remote_gone:
                    still_claimed = []
                else:
                    still_claimed = [k for k in marker.keys if self._remote_index.get(k) is not None]

                ## Keys the FRESH index no longer claims were legitimately
                ## deleted: clean up stale materialized local values so they
                ## do not resurrect from the local file.
                cleanly_absent = [k for k in marker.keys if k not in still_claimed]
                for k in cleanly_absent:
                    ## Invariant: no remote-driven path may delete a
                    ## journal-pending local value. Unreachable if the read
                    ## gates are complete (a journaled key is never
                    ## requested remotely), kept as the belt.
                    if k in self._journal.written:
                        continue
                    if k == utils.metadata_key_str:
                        ## booklet has no metadata unset; overwrite with null so
                        ## get_metadata() returns None (a raw skip would serve
                        ## stale metadata after a legitimate remote deletion).
                        self._local_file.set_metadata(None)
                    elif k in self._local_file:
                        del self._local_file[k]
                if cleanly_absent:
                    logger.info(
                        f"the remote no longer provides key(s) {cleanly_absent} (object "
                        f"'{marker.s3_key}' deleted, torn down, or repacked without them); "
                        f'treating them as absent after refreshing the index'
                    )

                if not still_claimed:
                    out[fkey] = None
                    continue

                ## Still claimed: re-issue the fetch ONCE against the
                ## refreshed index + manifest (the healable generational-GC
                ## race); only a second failure is a confirmed integrity
                ## fault.
                retry_failure = self._retry_fetch(still_claimed)
                if retry_failure is None:
                    out[fkey] = None
                else:
                    if isinstance(retry_failure, utils.MissingRemoteObject):
                        logger.warning(repr(retry_failure))
                    out[fkey] = retry_failure
            return out


    def _retry_fetch(self, keys):
        """
        Re-fetch keys the refreshed index still claims, through the refreshed
        manifest. Returns None when everything materialized, or the remaining
        failure (marker or error). Caller holds _index_lock.
        """
        by_group = {}
        per_key = []
        for k in keys:
            remote_val = self._remote_index.get(k)
            if remote_val is None:
                continue
            timestamp_int, gid, offset, length = utils.decode_index_entry(remote_val)
            if gid is None:
                per_key.append(k)
            else:
                by_group.setdefault(gid, []).append((k, offset, length, timestamp_int))
        for gid, key_infos in by_group.items():
            gen = self._remote_state.manifest.get(gid)
            if gen is None:
                return utils.MissingRemoteObject(
                    f'{gid}.<unmanifested>', [k for k, _o, _l, _t in key_infos])
            failure = utils.get_remote_group_values(gid, gen, key_infos, self._local_file, self._remote_session)
            if failure is not None:
                return failure
        for k in per_key:
            failure = utils.get_remote_value(self._local_file, k, self._remote_session)
            if failure is not None:
                return failure
        return None


    def _load_item(self, key):
        """

        """
        ## Read-your-writes gate: a journaled pending write is the truth for
        ## its key - serve the local value, never pull the remote over it.
        if key in self._journal.written:
            return None
        with self._index_lock:
            remote_val = self._remote_index.get(key)
            ## Resolve the generation inside the lock (manifest and index are
            ## updated atomically). The entry itself says whether the value is
            ## grouped (and in which group) or stored per key.
            group_id = utils.decode_index_entry(remote_val)[1] if remote_val else None
            gen = self._remote_state.manifest.get(group_id) if group_id is not None else None
        remote_time_bytes = remote_val[:7] if remote_val else None
        check = utils.check_local_vs_remote(self._local_file, remote_time_bytes, key)

        if check:
            if self._offline:
                raise OfflineError(
                    f"The value for key '{key}' is not materialized in the local cache "
                    'and this session is offline. (The key exists; its value needs the '
                    'remote.)'
                )
            if group_id is not None and key != utils.metadata_key_str:
                if gen is None:
                    ## Index claims the key, manifest lacks its group - treat
                    ## like a missing backing object (re-check protocol).
                    failure = utils.MissingRemoteObject(f'{group_id}.<unmanifested>', [key])
                else:
                    timestamp_int, _gid, offset, length = utils.decode_index_entry(remote_val)
                    failure = utils.get_remote_group_value(group_id, gen, key, offset, length, timestamp_int, self._local_file, self._remote_session)
            else:
                failure = utils.get_remote_value(self._local_file, key, self._remote_session)

            if isinstance(failure, utils.MissingRemoteObject):
                failure = self._resolve_missing({key: failure})[key]

            ## Re-materialization guard (same as load_items): the fetch ran
            ## against an index entry captured above - if a concurrent pull
            ## swapped + reconciled the index meanwhile, drop the value the
            ## CURRENT index no longer claims.
            with self._index_lock:
                if (key not in utils.reserved_key_strs
                        and key not in self._remote_index and key not in self._journal.written
                        and key in self._local_file):
                    del self._local_file[key]
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
            raise ReadOnlyError('File is open for read only.')

    def __delitem__(self, key):
        if self.writable:
            if key in utils.reserved_key_strs:
                raise ValueError(f"'{key}' is a reserved internal key.")
            ## MutableMapping contract (0.10.0): deleting a missing key raises
            ## KeyError (was a silent no-op). Presence = __contains__: in the
            ## remote index OR in the local file (a freshly-written unpushed
            ## key lives only in the local file).
            if key not in self:
                raise KeyError(key)
            if key in self._remote_index:
                del self._remote_index[key]
                self._journal.record_delete(key)
            else:
                ## A never-pushed key needs no remote delete, but a pending
                ## write for it must not survive its local deletion.
                self._journal.discard_written(key)

            if key in self._local_file:
                del self._local_file[key]

            ## Deletes are rare and booklet delete-flags are already written
            ## unbuffered, so persist the journal immediately - a hard crash
            ## between here and the next sync boundary must not silently lose
            ## the deletion (the union changelog covers crash-window WRITES
            ## via the timestamp diff, but deletes have no other record).
            self._journal.persist(self._local_file)
        else:
            raise ReadOnlyError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        """
        Remove EVERY key from the database (0.10.0: a true clear, no longer
        local cache eviction). All keys are journaled as deletions and applied
        to the remote at the next push; until then the clear is cancellable
        with changes().discard(). Unpushed pending writes are dropped as part
        of the clear. For local cache eviction (keys stay, evicted values
        re-pull on demand) use prune(timestamp=<now>) instead.
        """
        if self._push_active:
            raise PushInProgressError(
                'clear() is not allowed while a push is running - it would destroy the '
                'value bytes the push is reading. Wait for the push to finish.'
            )
        if not self.writable:
            raise ReadOnlyError('File is open for read only.')

        ## Flush buffered writes to disk first: the truncate below removes
        ## on-disk entries only, and a key still sitting in booklet's write
        ## buffer would resurface into iteration after the clear.
        self.sync()

        ## Snapshot before mutating (booklet raises on mutation during
        ## iteration); reserved/metadata entries are internal, never user keys.
        index_keys = [k for k in self._remote_index.keys()
                      if k not in utils.reserved_key_strs]
        for key in index_keys:
            del self._remote_index[key]
            self._journal.record_delete(key)

        ## Pending writes are cancelled by the clear (their keys either just
        ## became journaled deletes, or - never-pushed keys - simply cease to
        ## exist when the local file truncates below).
        self._journal.clear_written()
        self._journal.set_meta_pending(False)

        ## Truncate the local file LAST-BUT-ONE: it destroys BOTH reserved
        ## slots, so the journal (now carrying every delete) and the
        ## remote-state cache MUST be re-persisted AFTER it - persisting
        ## before the truncate would silently lose the journaled deletes on
        ## an unpushed close.
        self._local_file.clear()
        self._journal.persist(self._local_file, force=True)
        self._remote_state.persist(self._local_file, force=True)
        ## The remote's metadata stays visible (metadata is cleared by
        ## set_metadata(None), not by clear()) - restore the local slot from
        ## the cached remote section.
        utils.refresh_local_metadata(self._local_file, self._journal, self._remote_state.meta_section)

    def close(self):
        """
        Close all open objects.

        Pending (unpushed) writes and deletions survive the close: they are
        recorded in the local file's persistent journal and will be included
        in the next session's push.
        """
        self.sync()
        self._finalizer()


    def sync(self):
        """
        Syncronize all cache to disk. This ensures all data has been saved to disk properly.
        """
        ## Persist the journal first (if-dirty: a clean journal writes nothing,
        ## so iterate-while-sync patterns are not invalidated by a no-op).
        self._journal.persist(self._local_file)
        self._remote_index.sync()
        self._local_file.sync()

    def changes(self):
        """
        Return a Change object of the changes that have occurred during this session.
        """
        return Change(self)


    def delete_remote(self):
        """
        Completely delete the remote database, but keep the local file.

        The local file's VALUES are kept and become the content of the next
        push; its caches of the deleted remote - the remote-index sidecar and
        the remote-state slot - are forgotten here, so a delete-then-write-
        then-push in one session cannot commit claims for objects that no
        longer exist (the same forgetting an open performs when it finds the
        remote gone; see utils.forget_remote_incarnation for what is and is not
        dropped). ⚠️ Iteration concurrent with this call (keys()/__iter__ do
        not take the index lock) may raise RuntimeError - the same caveat as
        Change.pull. Refused while a push is running (PushInProgressError),
        like prune()/clear(): the push is reading the state this mutates.
        """
        if self.writable:
            if self._push_active:
                raise PushInProgressError(
                    'delete_remote() was called while a push is running on this session; '
                    'wait for the push to finish.')
            with self._index_lock:
                self._remote_session.delete_remote()
                ## Bulk clear: O(1), keeps the sidecar's layout (value_len, buckets,
                ## key serializer), and bumps the sidecar's own mutation counter so a
                ## concurrent iterator raises rather than reading a half-cleared index.
                self._remote_index.clear()
                self._remote_index.sync()
                utils.forget_remote_incarnation(self._local_file, self._journal, self._remote_state,
                                                reason='delete_remote()')
        else:
            raise ReadOnlyError('File is open for read only.')


    def copy_remote(self, remote_conn):
        """
        Copy the entire remote file to another remote location. The new location must be empty.
        """
        if self.writable:
            self._remote_session.copy_remote(remote_conn)
        else:
            raise ReadOnlyError('File is open for read only.')

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
            *,
            group_bytes=utils.UNSET,
            lock_timeout: int = 300,
            force_lock: bool = False,
            push_packers: int = 1,
            ):
        """

        """
        self._init_common(remote_session, local_file_path, flag, 'orjson', n_buckets, buffer_size, 'RemoteConnGroup', group_bytes, lock_timeout, force_lock, push_packers)


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

        Entry schema (version 1) - FROZEN: consumers can rely on these fields
        indefinitely; any future change arrives as a new entry_version.
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
            raise ReadOnlyError('File is open for read only.')

        if not isinstance(remote_conn, remote.S3Connection):
            raise TypeError('remote_conn/value must be a remote.S3Connection')

        if key is not None:
            if not isinstance(key, str):
                raise TypeError('key must be a str.')
            if key in utils.reserved_key_strs:
                raise ValueError('key clashes with an internal reserved key.')
            if not rcg_key_pattern.match(key):
                raise ValueError('key must match [A-Za-z0-9._-]+ (entry keys become S3 object-key suffixes; other characters are known to break request signing).')

        if user_meta is not None:
            try:
                msgspec.json.encode(user_meta)
            except (TypeError, msgspec.EncodeError) as e:
                raise TypeError(f'user_meta must be JSON-serializable: {e}')

        ## Get remote_conn metadata
        with remote_conn.open() as rc:
            uuid0 = rc.get_uuid()
            if uuid0 is None:
                raise RemoteMissingError('Remote does not exist. It must exist to be added to a RemoteConnGroup.')

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
        self._journal.record_write(key)

        ## Clean up a stale default-keyed entry for the same member (left by a
        ## pre-0.9.0-style add) when migrating to explicit keys.
        if key != uuid_hex and uuid_hex in self:
            del self[uuid_hex]


    def set(self, key, remote_conn: remote.S3Connection):
        """
        Upsert an entry via add(). `rcg[key] = conn` is equivalent.
        """
        self.add(remote_conn, key=key)


def _grouped_target(explicit_group_bytes, remote_session):
    """The packing target of a grouped session: the group_bytes argument if
    one was passed; else the value the grouped remote records (what its last
    commit packed with); else ebooklet.DEFAULT_GROUP_BYTES."""
    if explicit_group_bytes is not None:
        return explicit_group_bytes
    if remote_session.storage_kind == utils.STORAGE_GROUPED and remote_session.group_bytes is not None:
        return remote_session.group_bytes
    return utils.DEFAULT_GROUP_BYTES


def _check_group_bytes_arg(group_bytes, num_groups):
    """
    Validate the storage-mode arguments; returns group_bytes (utils.UNSET,
    None, or a validated int). num_groups is the pre-0.11 hash-grouping
    argument, kept for one release: an explicit num_groups=None keeps its old
    meaning - per-key storage - so callers that pass it keep their layout; an
    int is refused (hash grouping is gone).
    """
    if num_groups is not utils.UNSET:
        if num_groups is not None:
            raise ValueError(
                'num_groups (hash grouping) was removed in ebooklet 0.11. Grouped storage now '
                'fills groups in write order up to group_bytes (default '
                f'{utils.DEFAULT_GROUP_BYTES} bytes); pass group_bytes=<int> to choose the '
                'target, group_bytes=None for per-key storage, or omit it.'
            )
        if group_bytes is not utils.UNSET:
            raise ValueError('pass group_bytes only (num_groups is the removed pre-0.11 argument).')
        group_bytes = None
    if group_bytes is utils.UNSET or group_bytes is None:
        return group_bytes
    if isinstance(group_bytes, bool) or not isinstance(group_bytes, int):
        raise TypeError('group_bytes must be an int (bytes per group) or None (per-key storage).')
    if not 1 <= group_bytes <= utils._MAX_GROUP_BYTES:
        raise ValueError(f'group_bytes must be between 1 and {utils._MAX_GROUP_BYTES}.')
    return group_bytes


def _check_offline_arg(offline, flag):
    if not (offline is False or offline is True or offline == 'auto'):
        raise ValueError("offline must be False, 'auto', or True.")
    if offline and flag != 'r':
        raise ValueError("offline mode is read-only - open with flag='r'.")


def open_ebooklet(
    remote_conn: Union[remote.S3Connection, str, dict],
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    value_serializer: str = None,
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    *,
    group_bytes=utils.UNSET,
    lock_timeout: int = 300,
    force_lock: bool = False,
    offline: Union[bool, str] = False,
    push_packers: int = 1,
    num_groups=utils.UNSET,
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

    group_bytes : int, None, or omitted
        The remote storage mode. An int means grouped storage (format 3): at each push, keys new to the remote are packed in local-file write order into the current last group, then into fresh groups, each filled up to group_bytes bytes, so an append uploads the new data plus at most one partly filled group. Keys already on the remote keep their group (an update repacks it); deletes only remove the index entry. None means per-key storage (format 2): one remote object per key.
        Omitted: an existing remote keeps its mode and the group_bytes it records (whatever its last push packed with); a new database is grouped at ebooklet.DEFAULT_GROUP_BYTES (32 MiB). An int for a grouped remote changes the target from this session's pushes on (existing groups keep their size) and is recorded by its commits. An explicit value for the other mode than the remote's is ignored with a warning (with flag='n' it raises - call delete_remote() first to change the mode).

    lock_timeout : int
        Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.

    force_lock : bool
        If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.

    num_groups : None
        Removed in 0.11 (hash grouping); kept for one release. An explicit num_groups=None keeps its old meaning, per-key storage (same as group_bytes=None), so existing callers keep their layout; an int raises ValueError - use group_bytes.

    offline : False, 'auto', or True
        Offline READ mode (requires flag='r'). True: never touch the remote -
        serve the existing local file as-is (raises OfflineError if it does
        not exist); reads of values not materialized locally raise
        OfflineError. 'auto': try a normal online open, and fall back to the
        offline behavior (with a UserWarning) ONLY when the remote is
        unreachable at the transport level (DNS/connect/timeout) - integrity,
        format, uuid, and HTTP status errors (e.g. bad credentials) still
        raise. The session's `.offline` property says which mode it ended up
        in. Note the local data may be stale (no remote sync check runs), and
        the remote type guard is skipped - use the factory matching the
        database type.

    push_packers : int
        How many of push()'s workers may READ the local file at once while
        packing groups (the read gate). Upload concurrency is separate
        (``S3Connection(threads=...)``, default 10) and pack/upload overlap
        regardless, so the default of 1 costs nothing when threads >= 2 - it
        gives a spinning disk the optimal single-sweep read pattern. Raise it
        (up to ``threads``) for storage where parallel readers scale (SSD,
        RAID). With threads=1 pack and PUT strictly alternate (no overlap).

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
    group_bytes = _check_group_bytes_arg(group_bytes, num_groups)

    _check_offline_arg(offline, flag)

    local_file_path = pathlib.Path(file_path)

    if offline is True:
        if not local_file_path.exists():
            raise OfflineError(f'offline=True requires an existing local file; nothing found at {local_file_path}.')
        return EVariableLengthValue(remote_session=remote.OfflineSession(), local_file_path=local_file_path, flag='r', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, push_packers=push_packers)

    if offline == 'auto':
        ## Wrap the WHOLE online open (both remote touches: the metadata HEAD
        ## and the index fetch) - a transport failure from either falls back.
        try:
            return open_ebooklet(remote_conn, file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, lock_timeout=lock_timeout, force_lock=force_lock, offline=False, push_packers=push_packers)
        except TRANSPORT_ERRORS as err:
            ## Typed ebooklet errors never fall back (TRANSPORT_ERRORS lists
            ## transport classes only; this is the belt to the design rule).
            if isinstance(err, Error):
                raise
            warnings.warn(
                f'The remote is unreachable ({type(err).__name__}); opening offline and '
                f'serving the local data at {local_file_path} as-is (it may be stale).',
                UserWarning, stacklevel=2,
            )
            return open_ebooklet(remote_conn, file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, offline=True, push_packers=push_packers)

    local_file_exists = local_file_path.exists()

    ## Check and open the remote session
    remote_conn = remote.check_remote_conn(remote_conn, flag)
    remote_session, ebooklet_type = utils.open_remote_conn(remote_conn, flag, local_file_exists)

    if ebooklet_type is not None and ebooklet_type != 'EVariableLengthValue':
        raise TypeError(f'The remote database is of type {ebooklet_type}, not EVariableLengthValue. Use open_rcg() instead.')

    return EVariableLengthValue(remote_session=remote_session, local_file_path=local_file_path, flag=flag, value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, lock_timeout=lock_timeout, force_lock=force_lock, push_packers=push_packers)


def open_rcg(
    remote_conn: Union[remote.S3Connection, str, dict],
    file_path: Union[str, pathlib.Path],
    flag: str = "r",
    n_buckets: int=12007,
    buffer_size: int = 2**22,
    *,
    group_bytes=utils.UNSET,
    lock_timeout: int = 300,
    force_lock: bool = False,
    offline: Union[bool, str] = False,
    push_packers: int = 1,
    num_groups=utils.UNSET,
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

    group_bytes : int, None, or omitted
        The remote storage mode - see open_ebooklet for the full semantics. Omitted: an existing remote keeps its mode and recorded group_bytes; a new group is grouped at ebooklet.DEFAULT_GROUP_BYTES (a catalogue typically fits one group, so a full read is one request).

    lock_timeout : int
        Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.

    force_lock : bool
        If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.

    num_groups : None
        Removed in 0.11 - see open_ebooklet.

    offline : False, 'auto', or True
        Offline READ mode (requires flag='r') - see open_ebooklet for the full
        semantics. Note offline covers THIS catalogue only: opening a member
        remote still requires connectivity.

    push_packers : int
        The push read-gate width - see open_ebooklet for the full semantics.

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
    group_bytes = _check_group_bytes_arg(group_bytes, num_groups)

    _check_offline_arg(offline, flag)

    local_file_path = pathlib.Path(file_path)

    if offline is True:
        if not local_file_path.exists():
            raise OfflineError(f'offline=True requires an existing local file; nothing found at {local_file_path}.')
        return RemoteConnGroup(remote_session=remote.OfflineSession(), local_file_path=local_file_path, flag='r', n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, push_packers=push_packers)

    if offline == 'auto':
        ## Wrap the WHOLE online open (both remote touches) - see open_ebooklet.
        try:
            return open_rcg(remote_conn, file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, lock_timeout=lock_timeout, force_lock=force_lock, offline=False, push_packers=push_packers)
        except TRANSPORT_ERRORS as err:
            if isinstance(err, Error):
                raise
            warnings.warn(
                f'The remote is unreachable ({type(err).__name__}); opening offline and '
                f'serving the local data at {local_file_path} as-is (it may be stale).',
                UserWarning, stacklevel=2,
            )
            return open_rcg(remote_conn, file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, offline=True, push_packers=push_packers)

    local_file_exists = local_file_path.exists()

    ## Check and open the remote session
    remote_conn = remote.check_remote_conn(remote_conn, flag)
    remote_session, ebooklet_type = utils.open_remote_conn(remote_conn, flag, local_file_exists)

    if ebooklet_type is not None and ebooklet_type != 'RemoteConnGroup':
        raise TypeError(f'The remote database is of type {ebooklet_type}, not RemoteConnGroup. Use open_ebooklet() instead.')

    return RemoteConnGroup(remote_session=remote_session, local_file_path=local_file_path, flag=flag, n_buckets=n_buckets, buffer_size=buffer_size, group_bytes=group_bytes, lock_timeout=lock_timeout, force_lock=force_lock, push_packers=push_packers)


