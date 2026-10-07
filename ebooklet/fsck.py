#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fsck for ebooklet remotes (storage formats 2 per-key and 3 grouped).

The generational design makes integrity checking trivial: the db object's
manifest (plus, in per-key mode, its index) is the COMPLETE list of objects
the database references. Everything else under the namespace is an orphan -
an abandoned generation from a crashed or partially-failed push, a
replaced/emptied generation whose GC delete failed, or an aged writability
probe. Orphans are invisible to readers and never a correctness problem;
sweeping them reclaims storage.

Report mode (`delete_orphans=False`, the default) is lock-free. The sweep
takes the write lock (so no in-flight push exists) and additionally age-gates
each deletion (`min_age`, default 24h) as belt-and-braces - protecting a
crashed-commit writer's prompt retry, whose freshly-PUT generations are
orphans only until it re-pushes.
"""
import datetime
import io
import logging

import booklet
import msgspec
import urllib3

from . import utils

logger = logging.getLogger(__name__)


class FsckReport(msgspec.Struct):
    """The outcome of an fsck pass over one remote database."""
    db_key: str
    format_version: int | None
    db_object_exists: bool
    ## Children found while the db object itself is absent (a torn teardown):
    ## the whole namespace is orphaned.
    torn_teardown: bool = False
    expected_objects: int = 0
    orphans: list = []
    swept: list = []
    skipped_young: list = []
    ## Objects the manifest/index references that the listing does not show -
    ## a real integrity fault (readers will hit RemoteIntegrityError).
    claimed_but_missing: list = []
    ## Grouped index keys whose group id is absent from the manifest - a real
    ## integrity fault.
    unmanifested_group_ids: list = []
    ## Manifest groups no index entry points at. A push drops a group once its
    ## last live member is deleted, so a non-empty list is a fault (waste, not
    ## data loss).
    empty_groups: list = []
    ## Grouped storage: {gid: fraction of the group object's bytes held by
    ## lazily-deleted (or superseded) values}, for groups with any dead bytes.
    ## Dead bytes drop out when a group is next repacked; they stay publicly
    ## downloadable until then.
    dead_fraction: dict = {}
    ## The index entries' layout does not match the remote's format (15-byte
    ## entries are per-key, 19-byte entries grouped).
    bad_entry_layout: bool = False


def fsck(remote_conn, delete_orphans: bool = False,
         min_age: datetime.timedelta = datetime.timedelta(hours=24),
         check_objects: bool = False, lock_timeout: int = 60) -> FsckReport:
    """
    Verify (and optionally clean) an ebooklet remote.

    Parameters
    ----------
    remote_conn : S3Connection
        Must carry write credentials (listing and sweeping need them).
    delete_orphans : bool
        Sweep orphans older than min_age. Acquires the remote write lock for
        the duration of the sweep.
    min_age : datetime.timedelta
        Orphans younger than this are reported but never swept (an in-flight
        or about-to-be-retried push may still adopt them). Objects whose
        listing carries no timestamp are never swept.
    check_objects : bool
        Per-key mode only: also verify every index key's object appears in
        the listing (O(n) report entries; grouped manifests are always fully
        checked - they have one entry per group object).

    Returns
    -------
    FsckReport
    """
    session = remote_conn.open('w')
    try:
        db_key = session.write_db_key
        prefix_len = len(db_key) + 1

        ## Listing first - it also answers the torn-teardown question.
        listed = {}
        listed_size = {}
        for obj in session.list_objects().iter_objects():
            child = obj['key'][prefix_len:]
            if child:
                listed[child] = obj.get('upload_timestamp')
                listed_size[child] = obj.get('content_length')

        if not session.initialized:
            if listed:
                logger.warning(
                    f"fsck '{db_key}': the db object is ABSENT but {len(listed)} child "
                    'object(s) exist - a torn teardown. The children are all orphans; '
                    'rerun delete_remote() (or fsck with delete_orphans=True) to finish it.'
                )
            report = FsckReport(db_key=db_key, format_version=None, db_object_exists=False,
                                torn_teardown=bool(listed), orphans=sorted(listed))
            if delete_orphans:
                _sweep(session, listed, sorted(listed), min_age, lock_timeout, report)
            return report

        ## Too-new remotes already refused at open (_load_db_metadata); refuse
        ## legacy ones explicitly - fsck reasons in per-key (format 2) and
        ## write-order grouped (format 3) terms only.
        kind = session.storage_kind
        if kind not in (utils.STORAGE_PER_KEY, utils.STORAGE_GROUPED):
            raise utils.UnsupportedFormatError(
                f"fsck '{db_key}': the remote is a legacy database (format_version "
                f'{session.format_version}, num_groups={session.legacy_num_groups}); fsck only '
                'understands per-key (format 2) and write-order grouped (format 3) remotes. '
                'Move it to the current format first (hydrate with ebooklet<0.11, delete, republish).'
            )
        grouped = kind == utils.STORAGE_GROUPED

        resp = session.get_object()
        if resp.status not in (200, 206):
            raise urllib3.exceptions.HTTPError(resp.error)
        manifest, _meta_section, index_bytes = utils.parse_db_payload(resp.data)

        expected = {utils.group_obj_key(gid, gen) for gid, gen in manifest.items()}
        idx = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
        try:
            entry_len = idx._value_len
            index_items = [(k, v) for k, v in idx.items() if k != utils.metadata_key_str]
        finally:
            idx.close()
        index_keys = [k for k, _v in index_items]
        bad_entry_layout = entry_len != (utils.INDEX_LEN_GROUPED if grouped else utils.INDEX_LEN_PER_KEY)

        unmanifested = []
        empty_groups = []
        dead_fraction = {}
        if not grouped:
            expected |= set(index_keys)
        elif not bad_entry_layout:
            ## Live bytes per group, from the index alone: the packed size of
            ## its live members. Whatever else the object holds is dead.
            live_bytes = {}
            for k, v in index_items:
                _ts, gid, _off, ln = utils.decode_index_entry(v)
                live_bytes[gid] = live_bytes.get(gid, utils.group_header_len) + utils.group_entry_size(k, ln)
            unmanifested = sorted(g for g in live_bytes if g not in manifest)
            empty_groups = sorted(g for g in manifest if g not in live_bytes)
            for gid, gen in manifest.items():
                size = listed_size.get(utils.group_obj_key(gid, gen))
                live = live_bytes.get(gid, 0)
                if size and live < size:
                    dead_fraction[gid] = round(1 - live / size, 4)

        orphans = sorted(set(listed) - expected)

        claimed_but_missing = sorted(utils.group_obj_key(gid, gen)
                                     for gid, gen in manifest.items()
                                     if utils.group_obj_key(gid, gen) not in listed)
        if not grouped and check_objects:
            claimed_but_missing += sorted(k for k in index_keys if k not in listed)

        report = FsckReport(
            db_key=db_key,
            format_version=session.format_version,
            db_object_exists=True,
            expected_objects=len(expected),
            orphans=orphans,
            claimed_but_missing=claimed_but_missing,
            unmanifested_group_ids=unmanifested,
            empty_groups=empty_groups,
            dead_fraction=dead_fraction,
            bad_entry_layout=bad_entry_layout,
            )

        if bad_entry_layout:
            logger.warning(
                f"fsck '{db_key}': the index has {entry_len}-byte entries, which do not match "
                f'its {kind} storage - the index cannot be interpreted.'
            )
        if empty_groups:
            logger.warning(
                f"fsck '{db_key}': manifest group(s) {empty_groups} have no live member - "
                'a push should have dropped them (wasted storage, not data loss).'
            )

        if claimed_but_missing:
            logger.warning(
                f"fsck '{db_key}': {len(claimed_but_missing)} referenced object(s) are MISSING "
                f'from the store (readers will raise RemoteIntegrityError): {claimed_but_missing}'
            )
        if unmanifested:
            logger.warning(
                f"fsck '{db_key}': the index references group id(s) {unmanifested} that the "
                'manifest does not carry - those members are unreadable.'
            )

        if delete_orphans and orphans:
            _sweep(session, listed, orphans, min_age, lock_timeout, report)

        return report
    finally:
        session.close()


def _sweep(session, listed, orphans, min_age, lock_timeout, report):
    """Delete aged orphans under the write lock; mutate the report in place."""
    lock = session.create_lock()
    acquired = lock.acquire(timeout=lock_timeout)
    if not acquired:
        raise TimeoutError(
            'fsck could not acquire the write lock for the orphan sweep - another '
            'writer is active. Re-run later (the report above is still valid).'
        )
    try:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - min_age
        for child in orphans:
            ts = listed.get(child)
            if ts is None or ts > cutoff:
                report.skipped_young.append(child)
                continue
            err = session.delete_object(child)
            if err is None:
                report.swept.append(child)
            else:
                logger.warning(f"fsck: could not delete orphan '{child}': {err}")
    finally:
        lock.release()
