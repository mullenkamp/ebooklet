"""
Hermetic tests for write-order grouped storage (format 3, 0.11): allocation in
local-file write order filled to group_bytes, tail top-up, lazy deletes, the
commit-path fix (no stale index after a crashed commit or a lost lock), the
sidecar layout rule, the discard fix, and the republish path for a local file
left by ebooklet 0.10.5 (fixtures/legacy_0105, made by make_legacy_0105.py).

Every test that relies on group placement asserts it as a precondition - group
membership is an allocation outcome, not a function of the key.
"""
import contextlib
import pathlib
import pickle
import shutil
import struct
import warnings
from unittest import mock

import pytest
import urllib3

from ebooklet import (
    open_ebooklet,
    open_rcg,
    utils,
    fsck,
    LockLostError,
    OfflineError,
    UnsupportedFormatError,
    UUIDMismatchError,
    DEFAULT_GROUP_BYTES,
)
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import gid_of, remote_index_gids, remote_manifest, group_objects

FIXTURE = pathlib.Path(__file__).parent / 'fixtures' / 'legacy_0105'

## An entry for a 1-char key and a 2-byte value packs to 2+1+7+4+2 = 16 bytes;
## with the 4-byte header, GB2 holds exactly two of them (4+16+16 = 36).
GB2 = 40


def _conn(store, db_key='d'):
    return fake_s3.FakeS3Connection(store, db_key)


def _put_mark(eb):
    return len(eb._remote_session._write_session.put_log)


def _group_puts(eb, mark, db_key='d'):
    """Group-object keys PUT since mark (the db object itself excluded)."""
    return [k for k in eb._remote_session._write_session.put_log[mark:] if k.startswith(db_key + '/')]


def _gids_of_puts(keys):
    return sorted(int(k.split('/')[1].split('.')[0]) for k in keys)


def _unpack(data):
    """Packed group object -> [key, ...] in packed order."""
    n = struct.unpack_from('>I', data, 0)[0]
    pos, keys = 4, []
    for _ in range(n):
        klen = struct.unpack_from('>H', data, pos)[0]
        pos += 2
        keys.append(data[pos:pos + klen].decode())
        pos += klen + 7
        vlen = struct.unpack_from('>I', data, pos)[0]
        pos += 4 + vlen
    return keys


def _seed(store, tmp_path, keys, group_bytes=GB2, name='w.blt', db_key='d'):
    with open_ebooklet(_conn(store, db_key), tmp_path / name, flag='n', group_bytes=group_bytes) as eb:
        for k in keys:
            eb[k] = k.encode() * 2 if len(k) == 1 else k.encode()
        assert eb.changes().push()


#################################################
### Allocation


def test_allocation_follows_write_order(tmp_path):
    """Keys pushed together fill groups in the order they were WRITTEN, not
    key order; each group object packs its members in that order too."""
    store = {}
    _seed(store, tmp_path, ['z', 'a', 'm', 'b'])
    gids = remote_index_gids(store, 'd')
    assert gids == {'z': 0, 'a': 0, 'm': 1, 'b': 1}
    objs = group_objects(store, 'd')
    assert _unpack(store[objs[0]][0]) == ['z', 'a']
    assert _unpack(store[objs[1]][0]) == ['m', 'b']


def test_append_touches_only_new_groups_when_the_tail_is_full(tmp_path):
    """An append to a full tail uploads ONLY fresh groups - every existing
    generation is untouched, and the uploaded bytes are the new data's."""
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c', 'd', 'e', 'f'])       # g0..g2, full
    old_manifest = remote_manifest(store, 'd')
    assert sorted(old_manifest) == [0, 1, 2], 'precondition'
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        mark = _put_mark(eb)
        for k in ('x', 'y', 'w'):
            eb[k] = k.encode() * 2
        assert eb.changes().push()
        puts = _group_puts(eb, mark)
    assert _gids_of_puts(puts) == [3, 4]
    new_manifest = remote_manifest(store, 'd')
    for gid in (0, 1, 2):
        assert new_manifest[gid] == old_manifest[gid], f'existing group {gid} was rewritten'
    ## Uploaded bytes = the new data (3 entries) + 2 headers, nothing else.
    assert sum(len(store[k][0]) for k in puts) == 3 * 16 + 2 * 4


def test_append_tops_up_a_partial_tail(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c', 'd', 'e'])            # g2 holds only 'e'
    assert remote_index_gids(store, 'd')['e'] == 2, 'precondition'
    old_manifest = remote_manifest(store, 'd')
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        mark = _put_mark(eb)
        eb['x'] = b'xx'
        assert eb.changes().push()
        assert _gids_of_puts(_group_puts(eb, mark)) == [2]
        assert gid_of(eb, 'x') == 2
    new_manifest = remote_manifest(store, 'd')
    assert new_manifest[0] == old_manifest[0] and new_manifest[1] == old_manifest[1]
    assert new_manifest[2] != old_manifest[2]


def test_tail_size_uses_the_updated_local_length(tmp_path):
    """The tail's projected size counts an updated member at its NEW local
    length: 'a' grows from 2 to 7 bytes (entry 16 -> 21), so the tail holds
    4 + 21 = 25 and a new 16-byte entry (41 > GB2) opens a fresh group. At
    the index's old length (4 + 16 + 16 = 36) it would have fitted."""
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        eb['a'] = b'A' * 7
        eb['b'] = b'bb'
        assert eb.changes().push()
        assert (gid_of(eb, 'a'), gid_of(eb, 'b')) == (0, 1)


def test_update_repacks_only_its_own_group(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c', 'd', 'e', 'f'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        assert gid_of(eb, 'c') == 1, 'precondition'
        mark = _put_mark(eb)
        eb['c'] = b'CC'
        assert eb.changes().push()
        assert _gids_of_puts(_group_puts(eb, mark)) == [1]
        assert gid_of(eb, 'c') == 1, 'an updated key keeps its group'


def test_tail_top_up_from_a_second_machine(tmp_path):
    """A writer that does not hold the tail's members locally pulls them and
    repacks the tail in full - nothing is lost."""
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c', 'd', 'e'])            # g2 = ['e'], not local below
    with open_ebooklet(_conn(store), tmp_path / 'other.blt', flag='w', group_bytes=GB2) as eb:
        assert 'e' not in eb._local_file, 'precondition: the tail member is not local'
        eb['x'] = b'xx'
        assert eb.changes().push()
        assert gid_of(eb, 'x') == gid_of(eb, 'e') == 2
    objs = group_objects(store, 'd')
    ## (Pack order inside a group: captured local values first, pulled ones last.)
    assert sorted(_unpack(store[objs[2]][0])) == ['e', 'x']
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r['e'] == b'ee' and r['x'] == b'xx'


def test_oversized_value_gets_its_own_group(tmp_path):
    store = {}
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='n', group_bytes=GB2) as eb:
        eb['a'] = b'aa'
        eb['big'] = b'B' * 100
        eb['c'] = b'cc'
        assert eb.changes().push()
        assert (gid_of(eb, 'a'), gid_of(eb, 'big'), gid_of(eb, 'c')) == (0, 1, 2)
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r['big'] == b'B' * 100


def test_gid_reuse_after_the_tail_is_emptied(tmp_path):
    """Emptying the tail drops its gid; the next allocation reuses the number
    with a fresh generation, and a reader holding the OLD index stays correct."""
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c'], group_bytes=1)       # g0, g1, g2
    reader = open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r')
    try:
        assert gid_of(reader, 'c') == 2, 'precondition'
        with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=1) as eb:
            del eb['c']
            assert eb.changes().push()
            assert 2 not in remote_manifest(store, 'd')
            eb['d'] = b'dd'
            assert eb.changes().push()
            assert gid_of(eb, 'd') == 2, 'precondition: the freed gid number is reused'
        ## The reader's stale index still claims c in g2 (old generation, GC'd).
        assert reader.get('c') is None
        assert reader['d'] == b'dd'
        assert reader['a'] == b'aa'
    finally:
        reader.close()


#################################################
### Lazy deletes


def test_lazy_delete_uploads_nothing_and_leaves_bytes(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a', 'b'])                            # one group
    old_manifest = remote_manifest(store, 'd')
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        mark = _put_mark(eb)
        del eb['a']
        result = eb.changes().push()
        assert result and result.updated
        assert _group_puts(eb, mark) == [], 'a lazy delete uploaded a group'
    assert remote_manifest(store, 'd') == old_manifest
    obj = group_objects(store, 'd')[0]
    assert _unpack(store[obj][0]) == ['a', 'b'], 'the dead bytes stay until a repack'
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert 'a' not in r and r.get('a') is None
        assert r['b'] == b'bb'
    assert fsck(_conn(store)).dead_fraction == {0: round(16 / 36, 4)}

    ## A later repack of the group (an update of its live member) drops them.
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        eb['b'] = b'BB'
        assert eb.changes().push()
    obj = group_objects(store, 'd')[0]
    assert _unpack(store[obj][0]) == ['b']
    assert fsck(_conn(store)).dead_fraction == {}


def test_delete_then_reset_is_a_new_key(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c', 'd'])                  # g0 = a,b  g1 = c,d (full)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        old = gid_of(eb, 'a')
        del eb['a']
        eb['a'] = b'NEW'
        assert eb.changes().push()
        assert gid_of(eb, 'a') not in (None, old), 'precondition: re-set key re-allocated'
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r['a'] == b'NEW'
        assert r['b'] == b'bb'


#################################################
### Failures


def test_failed_fresh_group_leaves_no_trace_and_retries(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a'], group_bytes=1)
    eb = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=1)
    try:
        eb['n'] = b'nn'
        eb['m'] = b'mm'
        session = eb._remote_session._write_session
        orig_put = session.put_object

        def failing_put(key, data, metadata=None):
            if key.startswith('d/2.'):
                return fake_s3.FakeResp(status=500, error={'message': 'induced'})
            return orig_put(key, data, metadata)

        session.put_object = failing_put
        result = eb.changes().push()
        assert set(result.failures) == {2} and result.updated
        assert 'm' in eb._journal.written and 'n' not in eb._journal.written
        assert gid_of(eb, 'm') is None, 'a failed fresh group staged an index entry'
        assert 2 not in remote_manifest(store, 'd')

        session.put_object = orig_put
        assert eb.changes().push()
        assert not eb._journal.written
    finally:
        eb.close()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert (r['a'], r['n'], r['m']) == (b'aa', b'nn', b'mm')
    rep = fsck(_conn(store))
    assert rep.claimed_but_missing == [] and rep.empty_groups == [] and rep.unmanifested_group_ids == []


def test_the_remote_records_group_bytes_and_later_writers_inherit_it(tmp_path):
    """A grouped remote records the group_bytes its last commit packed with. A
    writer that passes none (the same file later, a second machine) inherits
    it, a reader reports it, and a writer that passes another value overwrites
    it - one value, no lineage. (Before: an append that omitted group_bytes
    packed at the 32 MiB default, whatever the dataset was created with.)"""
    store = {}
    _seed(store, tmp_path, ['a', 'b'])                       # gid 0 is full at GB2
    assert store['d'][1]['group_bytes'] == str(GB2)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        assert eb.group_bytes == GB2
        eb['c'] = b'cc'
        eb['d'] = b'dd'
        assert eb.changes().push()
        ## At the 32 MiB default, c and d would have topped up gid 0.
        assert (gid_of(eb, 'c'), gid_of(eb, 'd')) == (1, 1)
    with open_ebooklet(_conn(store), tmp_path / 'second.blt', flag='w') as eb:
        assert eb.group_bytes == GB2, 'a fresh local file did not inherit the recorded value'
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r.group_bytes == GB2
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=60) as eb:
        eb['e'] = b'ee'
        assert eb.changes().push()
    assert store['d'][1]['group_bytes'] == '60'
    with open_ebooklet(_conn(store), tmp_path / 'second.blt', flag='w') as eb:
        assert eb.group_bytes == 60


@pytest.mark.parametrize('bad', ['abc', '0', str(utils._MAX_GROUP_BYTES + 1)])
def test_an_invalid_recorded_group_bytes_is_ignored(tmp_path, bad):
    """A recorded value that is not a valid target is treated as unrecorded,
    with a warning, instead of failing every open (readers included)."""
    store = {}
    _seed(store, tmp_path, ['a'])
    store['d'][1]['group_bytes'] = bad
    with pytest.warns(UserWarning, match='invalid group_bytes'):
        r = open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r')
    with r:
        assert r.group_bytes == DEFAULT_GROUP_BYTES
        assert r['a'] == b'aa'


def test_a_no_op_push_leaves_no_changelog(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        result = eb.changes().push()
        assert not result.updated and not result.failures, 'precondition: a no-op push'
        assert not list(tmp_path.glob('w.blt.changelog*'))


def test_per_key_remote_records_no_group_bytes(tmp_path):
    """Per-key db-object metadata stays exactly what 0.10 writes."""
    store = {}
    _seed(store, tmp_path, ['a'], group_bytes=None)
    assert 'group_bytes' not in store['d'][1]
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r.group_bytes is None


def test_push_adopts_the_recorded_group_bytes_of_a_remote_that_appeared(tmp_path):
    """A writer whose remote read as ABSENT at open resolves the default; when
    the re-check at push time finds the remote (its own, grouped at another
    target), the push adopts the recorded group_bytes, and the commit keeps
    recording it."""
    store = {}
    _seed(store, tmp_path, ['a'], name='first.blt')                      # grouped at GB2
    shutil.copyfile(tmp_path / 'first.blt', tmp_path / 'late.blt')      # same database, same uuid
    eb = _open_while_absent(store, tmp_path / 'late.blt')
    try:
        assert eb.group_bytes == DEFAULT_GROUP_BYTES and not eb._remote_session.initialized, 'precondition'
        eb['b'] = b'bb'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')                    # the 'remote exists after all' warning
            assert eb.changes().push()
        assert eb.group_bytes == GB2
    finally:
        eb.close()
    assert store['d'][1]['group_bytes'] == str(GB2)


@pytest.mark.parametrize('group_bytes', [None, GB2])
def test_push_whose_every_upload_failed_reports_not_updated(tmp_path, group_bytes):
    """Every upload failed and nothing else was pending, so nothing was
    committed: PushResult.updated is False (0.10.5 reported True)."""
    store = {}
    _seed(store, tmp_path, ['a'], group_bytes=group_bytes)
    eb = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=group_bytes)
    try:
        eb['a'] = b'A2'
        eb['b'] = b'bb'
        commit_before = store['d'][1]['timestamp']
        session = eb._remote_session._write_session
        orig_put = session.put_object

        def failing_put(key, data, metadata=None):
            if key.startswith('d/'):
                return fake_s3.FakeResp(status=500, error={'message': 'induced'})
            return orig_put(key, data, metadata)

        session.put_object = failing_put
        result = eb.changes().push()
        assert result.failures, 'precondition: the uploads failed'
        assert store['d'][1]['timestamp'] == commit_before, 'precondition: nothing was committed'
        assert result.updated is False

        session.put_object = orig_put
        result = eb.changes().push()
        assert result.updated is True and not result.failures
    finally:
        eb.close()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert (r['a'], r['b']) == (b'A2', b'bb')


class _Crash(BaseException):
    pass


def test_crash_after_the_commit_reopens_with_the_full_index(tmp_path):
    """The local file is stamped LAST, after the committed state is applied:
    a crash right after the commit PUT leaves the stamp old, so the reopen
    fetches the whole index (0.10.5 refreshed only the manifest and kept the
    stale index beside it)."""
    store = {}
    _seed(store, tmp_path, ['a', 'b'])
    eb = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    eb['a'] = b'A2'
    eb['c'] = b'cc'
    real_info = utils.push_logger.info

    def info(msg, *a, **k):
        if 'commit succeeded' in str(msg):
            raise _Crash()
        return real_info(msg, *a, **k)

    with pytest.raises(_Crash):
        with mock.patch.object(utils.push_logger, 'info', info):
            eb.changes().push()
    eb._finalizer.detach()
    eb._local_file.close()
    eb._remote_index.close()

    committed = remote_index_gids(store, 'd')
    assert set(committed) == {'a', 'b', 'c'}, 'precondition: the commit landed'
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        assert {k: gid_of(eb, k) for k in eb._remote_index.keys()} == committed
        assert eb._remote_state.remote_ts == eb._remote_session.timestamp
        assert eb.changes().push() is not None
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert (r['a'], r['b'], r['c']) == (b'A2', b'bb', b'cc')
    rep = fsck(_conn(store))
    assert rep.claimed_but_missing == [] and rep.empty_groups == []


def test_crashed_commit_leaves_the_local_stamp_old(tmp_path):
    """Mechanism 1 of the commit-path fix, isolated: the local file's stamp
    is written LAST, so a crash right after the commit PUT leaves it older than
    the remote's (the timestamp rule alone then re-fetches the index)."""
    store = {}
    _seed(store, tmp_path, ['a'])
    eb = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    stamp_before = eb._local_file._file_timestamp
    eb['b'] = b'bb'
    real_info = utils.push_logger.info

    def info(msg, *a, **k):
        if 'commit succeeded' in str(msg):
            raise _Crash()
        return real_info(msg, *a, **k)

    try:
        with pytest.raises(_Crash):
            with mock.patch.object(utils.push_logger, 'info', info):
                eb.changes().push()
        remote_ts = int(store['d'][1]['timestamp'])
        assert eb._local_file._file_timestamp == stamp_before < remote_ts
        assert eb._remote_state.remote_ts != remote_ts
    finally:
        eb._finalizer.detach()
        eb._local_file.close()
        eb._remote_index.close()


def test_lagging_remote_state_forces_a_full_index_fetch(tmp_path):
    """Mechanism 2, isolated: even with a local stamp claiming freshness, a
    cached remote state that does not describe the remote's current commit
    makes the open re-fetch the WHOLE index (the lost-lock shape: a stamp
    newer than the remote's)."""
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'other.blt', flag='w', group_bytes=GB2) as eb:
        eb['b'] = b'bb'                       # another writer's commit
        assert eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        pass                                  # in sync: knows a and b
    ## Make the stale-index state: stamp far in the future, cached state and
    ## sidecar from before b existed.
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        del eb._remote_index['b']
        eb._remote_state.remote_ts = 1
        eb._remote_state._dirty = True
        eb._remote_state.persist(eb._local_file)
        eb._local_file._set_file_timestamp(int(store['d'][1]['timestamp']) + 10**9)
        eb._finalizer.detach()
        eb._local_file.close()
        eb._remote_index.close()
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        assert 'b' in eb._remote_index, 'the open did not re-fetch the index'


@pytest.mark.parametrize('group_bytes', [None, GB2])
def test_replacement_whose_post_commit_head_fails_survives_the_retry(tmp_path, group_bytes):
    """A replacement push commits, then the HEAD that refreshes the session
    fails. The journal must not be left saying 'replacement pending' with no
    written keys: the retry would purge every local key and commit an empty
    database (review round 2, F1)."""
    store = {}
    _seed(store, tmp_path, ['old'], group_bytes=group_bytes, name='old.blt')
    keys = [f'k{i}' for i in range(5)]
    eb = open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='n', group_bytes=group_bytes)
    try:
        for k in keys:
            eb[k] = k.encode()
        sess = eb._remote_session
        real_head, real_put = sess.head_object, sess.put_db_object
        state = {}

        def put_db(data, metadata):
            state['committed'] = True
            return real_put(data, metadata)

        def head(key=None):
            if key is None and state.pop('committed', False):
                return fake_s3.FakeResp(status=503, error={'message': 'induced'})
            return real_head(key)

        sess.put_db_object, sess.head_object = put_db, head
        with pytest.raises(urllib3.exceptions.HTTPError):
            eb.changes().push()
        assert sorted(remote_index_gids(store, 'd')) == keys, 'precondition: the replacement committed'
        assert not eb._journal.replace_pending, 'the committed replacement is still pending'
        sess.put_db_object, sess.head_object = real_put, real_head
        with contextlib.suppress(RuntimeError):      # grouped: the stale-index belt says re-open
            eb.changes().push()
        assert sorted(eb._local_file.keys()) == keys
    finally:
        eb.close()
    with open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='w') as eb:
        assert sorted(eb._local_file.keys()) == keys
        eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert sorted(r.keys()) == keys and r['k3'] == b'k3'


def test_a_skipped_reconciliation_leaves_the_stamp_old(tmp_path):
    """An ingest whose reconciliation scan was skipped (aborted twice by
    concurrent writes) must not stamp the local file fresh, on the pull path
    or the open path: the next pull or open then ingests and reconciles again."""
    store = {}
    _seed(store, tmp_path, ['a'])

    def other_commit(key):
        with open_ebooklet(_conn(store), tmp_path / 'other.blt', flag='w') as o:
            o[key] = key.encode() * 2
            assert o.changes().push()
        return int(store['d'][1]['timestamp'])

    skipped = mock.patch.object(utils, 'reconcile_local_with_index', return_value=None)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        remote_ts = other_commit('b')
        with skipped:
            eb.changes().pull()
        assert 'b' in eb._remote_index, 'precondition: the pull ingested the new index'
        assert eb._local_file._file_timestamp < remote_ts, 'pull claimed freshness over a skipped scan'
        eb.changes().pull()
        assert eb._local_file._file_timestamp == remote_ts
    remote_ts = other_commit('c')
    with skipped:
        with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='r') as r:
            assert 'c' in r._remote_index, 'precondition: the open ingested the new index'
            assert r._local_file._file_timestamp < remote_ts, 'open claimed freshness over a skipped scan'
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='r') as r:
        assert r._local_file._file_timestamp == remote_ts


def test_lagging_remote_state_forces_a_full_index_fetch_on_pull(tmp_path):
    """Mechanism 2 on the pull path: a local stamp claiming freshness does not
    gate the re-fetch when the cached remote state is not the remote's
    current commit."""
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        with open_ebooklet(_conn(store), tmp_path / 'other.blt', flag='w', group_bytes=GB2) as other:
            other['b'] = b'bb'                # another writer's commit
            assert other.changes().push()
        eb._remote_state.remote_ts = 1
        eb._local_file._set_file_timestamp(int(store['d'][1]['timestamp']) + 10**9)
        assert 'b' not in eb._remote_index, 'precondition'
        eb.changes().pull()
        assert 'b' in eb._remote_index, 'the pull did not re-fetch the index'


def test_pull_refuses_a_remote_that_became_legacy(tmp_path):
    """A session opened before a legacy remote (re)appeared at its key never
    ingests the legacy index on pull."""
    work, store = _fixture(tmp_path)
    legacy = {k: v for k, v in store.items() if k == 'legacy' or k.startswith('legacy/')}
    with _conn(store, 'legacy').open('w') as s:
        s.delete_remote()
    with open_ebooklet(_conn(store, 'legacy'), tmp_path / 'late.blt', flag='w') as eb:
        store.update(legacy)
        with pytest.raises(UnsupportedFormatError, match='see the open-time error'):
            eb.changes().pull()
        assert len(eb._remote_index) == 0


@pytest.mark.parametrize('flag', ['r', 'w'])
def test_open_fetches_a_changed_remote_once(tmp_path, flag):
    """An open that ingests a newer commit stamps the local file with it, so
    later opens do not re-download the db object (0.10.5 re-fetched on every
    open after one remote change, until a pull or push caught the stamp up)."""
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'c.blt', flag=flag):
        pass                                  # the cache is in sync
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        eb['b'] = b'bb'                       # the remote changes
        assert eb.changes().push()
    real_fetch = utils.fetch_remote_index
    fetches = []

    def counting_fetch(*a, **k):
        fetches.append(1)
        return real_fetch(*a, **k)

    with mock.patch.object(utils, 'fetch_remote_index', counting_fetch):
        for _ in range(3):
            with open_ebooklet(_conn(store), tmp_path / 'c.blt', flag=flag) as eb:
                assert eb['b'] == b'bb'
    assert len(fetches) == 1, f'the db object was fetched on {len(fetches)} of 3 opens'


def test_lost_lock_does_not_lose_the_other_writers_keys(tmp_path):
    """W2 breaks W1's lock and commits; W1's push raises LockLostError. W1's
    reopen must see W2's commit (0.10.5: its early local stamp claimed
    freshness, the stale index survived, and W1's retry published an index
    without W2's keys)."""
    store = {}
    _seed(store, tmp_path, [f'k{i}' for i in range(6)], group_bytes=1, name='w1.blt')
    with open_ebooklet(_conn(store), tmp_path / 'w2.blt', flag='w', group_bytes=1):
        pass
    w1 = open_ebooklet(_conn(store), tmp_path / 'w1.blt', flag='w', group_bytes=1)
    w1['w1_new'] = b'B'
    real_upload = utils.upload_group
    state = {'done': False}

    def preempt(*a, **k):
        if not state['done']:
            state['done'] = True
            with open_ebooklet(_conn(store), tmp_path / 'w2.blt', flag='w', force_lock=True, group_bytes=1) as w2:
                for i in range(5):
                    w2[f'w2_{i}'] = b'C'
                assert w2.changes().push()
            w1.lock.broken = True
        return real_upload(*a, **k)

    with pytest.raises(LockLostError):
        with mock.patch.object(utils, 'upload_group', preempt):
            w1.changes().push()
    w1.close()
    with open_ebooklet(_conn(store), tmp_path / 'w1.blt', flag='w', group_bytes=1) as w1:
        assert 'w2_0' in w1, "the reopen did not see the other writer's commit"
        assert w1.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert sorted(r.keys()) == sorted([f'k{i}' for i in range(6)] + [f'w2_{i}' for i in range(5)] + ['w1_new'])
    rep = fsck(_conn(store))
    assert rep.claimed_but_missing == [] and rep.empty_groups == []


def test_stale_index_belt(tmp_path):
    """The push refuses before any upload when its index is not the remote's
    current commit (unreachable through the API - the open re-fetches)."""
    store = {}
    _seed(store, tmp_path, ['a', 'b'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as eb:
        eb._remote_state.remote_ts = 1
        eb['c'] = b'cc'
        mark = _put_mark(eb)
        with pytest.raises(RuntimeError, match="not the remote's current commit"):
            eb.changes().push()
        assert _group_puts(eb, mark) == []
        assert 'c' in eb._journal.written


def test_two_pushes_in_one_session(tmp_path):
    store = {}
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='n', group_bytes=GB2) as eb:
        eb['a'] = b'aa'
        assert eb.changes().push()
        eb['b'] = b'bb'
        assert eb.changes().push()
        eb['c'] = b'cc'
        assert eb.changes().push()
        assert (gid_of(eb, 'a'), gid_of(eb, 'b'), gid_of(eb, 'c')) == (0, 0, 1)


#################################################
### Modes, layouts and the discard fix


def _open_while_absent(store, path, db_key='d', **kwargs):
    """Open path 'w' while its remote reads as absent (a transient 404 at
    open), then restore the remote: the case the pre-push re-check is for."""
    hidden = {k: store.pop(k) for k in list(store) if k == db_key or k.startswith(db_key + '/')}
    try:
        return open_ebooklet(_conn(store, db_key), path, flag='w', **kwargs)
    finally:
        store.update(hidden)


def test_push_adopts_the_mode_of_a_remote_that_appeared(tmp_path):
    """A writer whose remote read as ABSENT at open resolves the mode it was
    given (grouped here); when the re-check at push time finds the remote (its
    own, per-key), the push adopts the remote's mode and index instead of
    pushing a grouped layout over a per-key remote."""
    store = {}
    _seed(store, tmp_path, ['a'], group_bytes=None, name='first.blt')
    shutil.copyfile(tmp_path / 'first.blt', tmp_path / 'late.blt')      # same database, same uuid
    eb = _open_while_absent(store, tmp_path / 'late.blt', group_bytes=GB2)
    try:
        assert eb.group_bytes == GB2 and not eb._remote_session.initialized, 'precondition'
        eb['b'] = b'bb'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')     # the 'remote exists after all' and 'remote wins' warnings
            assert eb.changes().push()
        assert eb.group_bytes is None
        assert eb._journal.storage == 'per_key', 'the adopted mode was not journaled'
    finally:
        eb.close()
    assert store['d'][1]['format_version'] == '2'
    assert 'd/a' in store and 'd/b' in store
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert (r['a'], r['b']) == (b'aa', b'bb')


@pytest.mark.parametrize('group_bytes', [None, GB2])
def test_push_refuses_a_remote_another_local_file_created(tmp_path, group_bytes):
    """A writer whose remote was absent at open, while ANOTHER local file
    created it meanwhile: the push refuses (as the open would) instead of
    adopting the foreign index, and keeps refusing on retry. (Before: it merged,
    reconciled its own file against the foreign index, and re-stamped the
    remote with its uuid, locking out the writer that created it.)"""
    store = {}
    late = open_ebooklet(_conn(store), tmp_path / 'late.blt', flag='w', group_bytes=group_bytes)
    try:
        _seed(store, tmp_path, ['a'], group_bytes=group_bytes, name='first.blt')
        late['b'] = b'bb'
        before = dict(store)
        for _ in range(2):                                               # and again on retry
            with pytest.raises(UUIDMismatchError, match='different local file'):
                late.changes().push()
        assert store == before, 'the refused push wrote to the remote'
        assert 'b' in late._journal.written
    finally:
        late.close()
    with open_ebooklet(_conn(store), tmp_path / 'first.blt', flag='w') as first:
        assert sorted(first.keys()) == ['a']


def test_a_replacement_replaces_a_remote_another_local_file_created(tmp_path):
    """The uuid rule exempts a replacement (flag 'n'), as the open does: a
    replacement opened while the remote was absent replaces whatever another
    local file created meanwhile."""
    store = {}
    eb = open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='n', group_bytes=GB2)
    try:
        _seed(store, tmp_path, ['a'], name='first.blt')
        assert store['d'][1]['uuid'] != eb._local_file.uuid.hex, 'precondition: a foreign remote'
        eb['x'] = b'xx'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')                    # the 'remote exists after all' warning
            assert eb.changes().push()
    finally:
        eb.close()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert sorted(r.keys()) == ['x']


def test_rcg_defaults_to_grouped(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a'], db_key='member')
    with open_rcg(_conn(store, 'cat'), tmp_path / 'cat.rcg', flag='n') as rcg:
        assert rcg.group_bytes == DEFAULT_GROUP_BYTES
        rcg.add(_conn(store, 'member'), key='m1')
        assert rcg.changes().push()
    assert store['cat'][1]['format_version'] == '3'
    with open_rcg(_conn(store, 'cat'), tmp_path / 'cat-r.rcg', flag='r') as rcg:
        assert list(rcg.keys()) == ['m1']


def test_replacement_over_grouped_starts_at_gid_0(tmp_path):
    store = {}
    _seed(store, tmp_path, ['a', 'b', 'c'], group_bytes=1)
    old_objects = set(group_objects(store, 'd').values())
    with open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='n', group_bytes=1) as eb:
        eb['x'] = b'xx'
        eb['y'] = b'yy'
        assert eb.changes().push()
        assert (gid_of(eb, 'x'), gid_of(eb, 'y')) == (0, 1)
    assert not (set(group_objects(store, 'd').values()) & old_objects), 'old objects survived'
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert sorted(r.keys()) == ['x', 'y']


def test_copy_remote_of_an_emptied_grouped_remote(tmp_path):
    """A grouped remote whose groups were all emptied has an empty manifest;
    copy_remote branches on the storage kind, not on the manifest."""
    store = {}
    _seed(store, tmp_path, ['a'])
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        del eb['a']
        assert eb.changes().push()
    assert remote_manifest(store, 'd') == {}, 'precondition'
    with _conn(store).open('w') as src:
        assert not src.copy_remote(_conn(store, 'copy'))
    assert store['copy'][1]['format_version'] == '3'
    assert store['copy'][1]['group_bytes'] == store['d'][1]['group_bytes'] == str(GB2)
    with open_ebooklet(_conn(store, 'copy'), tmp_path / 'c.blt', flag='r') as r:
        assert list(r.keys()) == []


@pytest.mark.parametrize('group_bytes', [None, GB2])
def test_discard_after_delete_then_reset_keeps_the_key(tmp_path, group_bytes):
    """Pre-existing 0.10.5 bug: del k; db[k] = v; changes().discard() left the
    sidecar without k and nothing journaled, so the next unrelated push
    published an index without k."""
    store = {}
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='n', group_bytes=group_bytes) as eb:
        eb['a'] = b'a1'
        eb['b'] = b'b1'
        assert eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w') as eb:
        del eb['a']
        eb['a'] = b'a2'
        eb.changes().discard()
        assert 'a' in eb, 'discard did not restore the index entry'
        eb['z'] = b'unrelated'
        assert eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert sorted(r.keys()) == ['a', 'b', 'z']
        assert r['a'] == b'a1'


#################################################
### The republish path for a 0.10.5 local file (fixture)


def _fixture(tmp_path):
    work = tmp_path / 'legacy'
    shutil.copytree(FIXTURE, work)
    with open(work / 'store.pkl', 'rb') as f:
        store = pickle.load(f)
    return work, store


def test_legacy_remote_is_refused_with_the_move_recipe(tmp_path):
    work, store = _fixture(tmp_path)
    with pytest.raises(UnsupportedFormatError, match='num_groups=5'):
        open_ebooklet(_conn(store, 'legacy'), work / 'writer.blt', flag='w')
    with pytest.raises(UnsupportedFormatError, match='load_items'):
        open_ebooklet(_conn(store, 'legacy'), work / 'reader.blt', flag='r')
    ## The session-level teardown still works on a legacy remote.
    with _conn(store, 'legacy').open('w') as s:
        s.delete_remote()
    assert not [k for k in store if k == 'legacy' or k.startswith('legacy/')]


def test_republish_in_place_after_delete(tmp_path):
    """The 0.10.5 producer file, after delete_remote(), pushes as a grouped
    remote at the same key: every key, the metadata, the same uuid."""
    work, store = _fixture(tmp_path)
    old_uuid = store['legacy'][1]['uuid']
    with _conn(store, 'legacy').open('w') as s:
        s.delete_remote()
    with open_ebooklet(_conn(store, 'legacy'), work / 'writer.blt', flag='w') as eb:
        assert eb.group_bytes == DEFAULT_GROUP_BYTES
        assert eb.changes().push()
    meta = store['legacy'][1]
    assert meta['format_version'] == '3' and 'num_groups' not in meta
    assert meta['uuid'] == old_uuid
    with open_ebooklet(_conn(store, 'legacy'), tmp_path / 'fresh.blt', flag='r') as r:
        assert {k: r[k] for k in r.keys()} == {f'k{i}': f'value-{i}'.encode() for i in range(10)}
        assert r.get_metadata() == {'schema': 'legacy', 'n': 10}
    rep = fsck(_conn(store, 'legacy'))
    assert rep.claimed_but_missing == [] and rep.orphans == [] and rep.empty_groups == []

    ## An old 0.10.5 reader cache (15-byte sidecar) of that remote heals online.
    assert utils.sidecar_value_len(work / 'reader.blt.remote_index') == utils.INDEX_LEN_PER_KEY, 'precondition'
    with open_ebooklet(_conn(store, 'legacy'), work / 'reader.blt', flag='r') as r:
        assert r._remote_index._value_len == utils.INDEX_LEN_GROUPED
        assert r['k7'] == b'value-7'
        assert r['k0'] == b'value-0'


def test_republish_to_a_new_key(tmp_path):
    work, store = _fixture(tmp_path)
    before = dict(store)
    with open_ebooklet(_conn(store, 'newkey'), work / 'writer.blt', flag='w') as eb:
        assert eb.changes().push()
    assert {k: v for k, v in store.items() if k == 'legacy' or k.startswith('legacy/')} == \
        {k: v for k, v in before.items()}, 'the old remote was touched'
    with open_ebooklet(_conn(store, 'newkey'), tmp_path / 'fresh.blt', flag='r') as r:
        assert sorted(r.keys()) == sorted(f'k{i}' for i in range(10))
        assert r['k9'] == b'value-9'


def _fail_group_1(eb, db_key='legacy'):
    """Make every PUT of group 1's objects fail; returns the restore callable."""
    session = eb._remote_session._write_session
    orig_put = session.put_object

    def failing_put(key, data, metadata=None):
        if key.startswith(f'{db_key}/1.'):
            return fake_s3.FakeResp(status=500, error={'message': 'induced'})
        return orig_put(key, data, metadata)

    session.put_object = failing_put
    return lambda: setattr(session, 'put_object', orig_put)


FIXTURE_VALUES = {f'k{i}': f'value-{i}'.encode() for i in range(10)}


def test_republish_retry_in_one_session_keeps_the_failed_groups_keys(tmp_path):
    """Review F1, same-session route: after hydrate + delete the local values
    are the only copy and unjournaled. A republish whose group 1 fails, retried
    in the same session, must keep and then publish that group's keys - the
    session knows its own commit (it no longer takes it for a remote that
    appeared and force-pulls over the local values)."""
    work, store = _fixture(tmp_path)
    with _conn(store, 'legacy').open('w') as s:
        s.delete_remote()
    eb = open_ebooklet(_conn(store, 'legacy'), work / 'writer.blt', flag='w', group_bytes=64)
    try:
        assert not eb._journal.written, 'precondition: the hydrated values are unjournaled'
        restore = _fail_group_1(eb)
        r1 = eb.changes().push()
        restore()
        assert set(r1.failures) == {1}, 'precondition: one group failed'
        assert set(remote_index_gids(store, 'legacy')) < set(FIXTURE_VALUES), \
            "precondition: the failed group's keys are not on the remote"
        assert eb._remote_session.uuid == eb._local_file.uuid
        assert eb._remote_session.timestamp == int(store['legacy'][1]['timestamp'])
        with mock.patch.object(eb, '_pull_remote_index', side_effect=AssertionError('force-pulled its own commit')):
            assert eb.changes().push()
        assert sorted(eb.keys()) == sorted(FIXTURE_VALUES)
    finally:
        eb.close()
    with open_ebooklet(_conn(store, 'legacy'), tmp_path / 'fresh.blt', flag='r') as r:
        assert {k: r[k] for k in r.keys()} == FIXTURE_VALUES


def test_republish_crash_after_a_partial_commit_keeps_the_failed_groups_keys(tmp_path):
    """Review F1, crash route: group 1 fails, the commit of the rest lands and
    the process dies before the local state is applied. The reopen ingests the
    committed index; the failed group's keys (the only copy) must survive its
    reconciliation - a push that creates the remote journals every key."""
    work, store = _fixture(tmp_path)
    with _conn(store, 'legacy').open('w') as s:
        s.delete_remote()
    eb = open_ebooklet(_conn(store, 'legacy'), work / 'writer.blt', flag='w', group_bytes=64)
    assert not eb._journal.written, 'precondition: the hydrated values are unjournaled'
    _fail_group_1(eb)
    real_info = utils.push_logger.info

    def info(msg, *a, **k):
        if 'commit succeeded' in str(msg):
            raise _Crash()
        return real_info(msg, *a, **k)

    with pytest.raises(_Crash):
        with mock.patch.object(utils.push_logger, 'info', info):
            eb.changes().push()
    eb._finalizer.detach()
    eb._local_file.close()
    eb._remote_index.close()

    assert set(remote_index_gids(store, 'legacy')) < set(FIXTURE_VALUES), \
        "precondition: the commit landed without the failed group's keys"
    with open_ebooklet(_conn(store, 'legacy'), work / 'writer.blt', flag='w', group_bytes=64) as eb:
        assert sorted(eb._local_file.keys()) == sorted(FIXTURE_VALUES)
        assert eb.changes().push()
    with open_ebooklet(_conn(store, 'legacy'), tmp_path / 'fresh.blt', flag='r') as r:
        assert {k: r[k] for k in r.keys()} == FIXTURE_VALUES
    rep = fsck(_conn(store, 'legacy'))
    assert rep.claimed_but_missing == [] and rep.empty_groups == []


@pytest.mark.parametrize('group_bytes', [None, GB2])
def test_offline_reader_reports_the_mode_of_its_index(tmp_path, group_bytes):
    """A reader with no remote to ask reports the mode of the index it holds
    (a per-key cache reported the grouped default)."""
    store = {}
    _seed(store, tmp_path, ['a'], group_bytes=group_bytes)
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as r:
        assert r['a'] == b'aa'
    expected = None if group_bytes is None else DEFAULT_GROUP_BYTES
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r', offline=True) as r:
        assert r.group_bytes == expected
        assert r['a'] == b'aa'
    ## An explicit group_bytes for the other mode does not override the index
    ## the reader holds (or discard it).
    other = GB2 if group_bytes is None else None
    with pytest.warns(UserWarning, match="local index wins"):
        r = open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r', offline=True, group_bytes=other)
    with r:
        assert r.group_bytes == expected
        assert list(r.keys()) == ['a'] and r['a'] == b'aa'


def test_offline_keeps_a_legacy_sidecar(tmp_path):
    """Offline there is nothing to re-fetch: a mismatched (15-byte) sidecar is
    KEPT, so keys() and the materialized values still serve."""
    work, store = _fixture(tmp_path)
    with open_ebooklet(_conn(store, 'legacy'), work / 'reader.blt', flag='r', offline=True) as r:
        assert sorted(r.keys()) == sorted(f'k{i}' for i in range(10))
        assert r['k1'] == b'value-1'
        with pytest.raises(OfflineError):
            r['k5']
    assert utils.sidecar_value_len(work / 'reader.blt.remote_index') == utils.INDEX_LEN_PER_KEY
