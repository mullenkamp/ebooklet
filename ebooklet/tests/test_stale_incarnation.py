#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Forgetting a dead remote incarnation (0.10.5).

Found live, 2026-09-07: an ingest writer re-opened a local file whose
`.remote_index` sidecar and remote-state slot described a remote that
`delete_remote()` had since removed. Every open-time freshness check is keyed
off the remote's uuid, and a deleted remote has none, so the open reused the
stale sidecar verbatim and the next push committed an index still claiming a
key of the dead incarnation - a ghost with no backing object. Eight days later
every reader of that key raised RemoteIntegrityError.

A writer open (or a live session's delete_remote()) now forgets the dead
incarnation. THE INVARIANT under test throughout: only state that the remote
can re-derive is dropped, because "uninitialised" is a guess from one HEAD 404.
The misfire tests (a 404 while the remote is alive) pin that: a wrong 404 must
cost one extra index fetch, never a key, a deletion intent or a metadata
version. Hermetic via fake_s3.
"""
import contextlib
import logging
import pathlib
import warnings
from unittest import mock

import pytest

import ebooklet.utils as eb_utils
from ebooklet import fsck, open_ebooklet, open_rcg
from ebooklet.journal import JournalState, RemoteState
from ebooklet.tests import fake_s3

ITEMS = {'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}


def _conn(store, db_key):
    return fake_s3.FakeS3Connection(store, db_key)


def _seed(store, db_key, tmp_path, name='w.blt', items=None, num_groups=None, metadata=None):
    """Create + push a small db from `name`."""
    with open_ebooklet(_conn(store, db_key), tmp_path / name, flag='n', num_groups=num_groups) as eb:
        for k, v in (items or ITEMS).items():
            eb[k] = v
        if metadata is not None:
            eb.set_metadata(metadata)
        assert eb.changes().push()


def _writer(store, db_key, tmp_path, name, **kw):
    return open_ebooklet(_conn(store, db_key), tmp_path / name, flag='w', **kw)


def _reader(store, db_key, tmp_path, name='fresh.blt', **kw):
    return open_ebooklet(_conn(store, db_key), tmp_path / name, flag='r', **kw)


def _delete_remote(store, db_key):
    with _conn(store, db_key).open('w') as s:
        s.delete_remote()
    assert not [k for k in store if k == db_key or k.startswith(db_key + '/')]


def _partial_cache(store, db_key, tmp_path, name='b.blt'):
    """A second local file that materialized ONE key - its sidecar claims all three."""
    with _writer(store, db_key, tmp_path, name) as eb:
        assert eb['k1'] == b'v1'
        assert sorted(eb._remote_index.keys()) == ['k1', 'k2', 'k3']
        assert eb._remote_state.remote_ts is not None


def _key_for_group(gid, num_groups, taken=()):
    """Find a short key that hashes into the wanted group id (test_delete_safety idiom)."""
    i = 0
    while True:
        k = f'g{i}'
        if eb_utils.key_to_group_id(k, num_groups) == gid and k not in taken:
            return k
        i += 1


def _manifest(store, db_key):
    return eb_utils.parse_db_payload(store[db_key][0])[0]


def _fsck_clean(store, db_key):
    rep = fsck(_conn(store, db_key), check_objects=True)
    assert rep.claimed_but_missing == [], rep.claimed_but_missing
    assert rep.unmanifested_group_ids == [], rep.unmanifested_group_ids
    return rep


@contextlib.contextmanager
def _spurious_404(db_key):
    """One window in which HEAD of the db object answers 404 while the store is intact -
    a wrong bucket, a typo'd db_key, an endpoint on the wrong account."""
    orig = fake_s3.FakeS3Session.head_object

    def fake(self, key, version_id=None):
        if key == db_key:
            return fake_s3.FakeResp(404)
        return orig(self, key, version_id)

    with mock.patch.object(fake_s3.FakeS3Session, 'head_object', new=fake):
        yield


#################################################
### The incident


@pytest.mark.parametrize('flag', ['w', 'c'])
def test_incident_partial_cache_reopen_pushes_no_ghosts(tmp_path, flag, caplog):
    """FAILS ON 0.10.4: the reopen kept the dead incarnation's sidecar and the push
    committed k2/k3 as claims with no objects (fsck reported both)."""
    store = {}
    _seed(store, 'db1', tmp_path)
    _partial_cache(store, 'db1', tmp_path)
    _delete_remote(store, 'db1')

    with caplog.at_level(logging.INFO, logger='ebooklet.utils'):
        with open_ebooklet(_conn(store, 'db1'), tmp_path / 'b.blt', flag=flag) as eb:
            assert list(eb._remote_index.keys()) == []
            assert eb._remote_state.manifest == {}
            assert eb._remote_state.remote_ts is not None   # the reconciliation watermark is KEPT
            assert sorted(eb.keys()) == ['k1']          # the one materialized value
            eb['k4'] = b'v4'
            assert eb.changes().push()
    assert any('no longer exists' in r.message for r in caplog.records)

    with _reader(store, 'db1', tmp_path) as eb:
        ## k1 re-pushes: the local file is the source of a re-created remote
        assert sorted(eb.keys()) == ['k1', 'k4']
        assert eb['k1'] == b'v1'
        assert 'k2' not in eb
        assert eb.get('k2') is None
    _fsck_clean(store, 'db1')


def test_incident_grouped_manifest_carries_no_old_generation(tmp_path):
    """FAILS ON 0.10.4 through the OTHER cache: the slot-2 manifest still named the
    dead incarnation's generations and pre_push_manifest carried them into the commit."""
    store, ng = {}, 5
    a = _key_for_group(0, ng)
    b = _key_for_group(1, ng)
    c = _key_for_group(2, ng)
    _seed(store, 'db2', tmp_path, items={a: b'a', b: b'b', c: b'c'}, num_groups=ng)
    old_manifest = _manifest(store, 'db2')
    assert len(old_manifest) == 3

    with _writer(store, 'db2', tmp_path, 'b.blt') as eb:
        assert eb[a] == b'a'                           # materialize ONE group's member
    _delete_remote(store, 'db2')

    d = _key_for_group(0, ng, taken=(a, b, c))          # new key in a's group only
    with _writer(store, 'db2', tmp_path, 'b.blt', num_groups=ng) as eb:
        assert eb._remote_state.manifest == {}
        eb[d] = b'd'
        assert eb.changes().push()

    new_manifest = _manifest(store, 'db2')
    assert not (set(new_manifest.items()) & set(old_manifest.items())), 'old generation carried forward'
    assert set(new_manifest) == {0}, 'groups 1 and 2 have no local members and must not be manifested'
    for gid, gen in new_manifest.items():
        assert f'db2/{eb_utils.group_obj_key(gid, gen)}' in store
    with _reader(store, 'db2', tmp_path) as eb:
        assert sorted(eb.keys()) == sorted([a, d])
    _fsck_clean(store, 'db2')


def test_orphaned_sidecar_after_local_rebuild(tmp_path):
    """The incident's literal shape: the local main file was recreated by a plain
    local open (which knows nothing of the sidecar) and the sidecar was left behind.
    FAILS ON 0.10.4: a fresh local file + uninitialised remote adopted the orphan."""
    store = {}
    _seed(store, 'db3', tmp_path)
    _partial_cache(store, 'db3', tmp_path)
    _delete_remote(store, 'db3')

    (tmp_path / 'b.blt').unlink()
    assert eb_utils.remote_index_sidecar_path(tmp_path / 'b.blt').exists()

    with _writer(store, 'db3', tmp_path, 'b.blt') as eb:
        assert sorted(eb.keys()) == []
        eb['k4'] = b'v4'
        assert eb.changes().push()
    with _reader(store, 'db3', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k4']
    _fsck_clean(store, 'db3')


def test_flag_n_after_delete_has_empty_prepush_view(tmp_path):
    """'n' recreates the local booklet (fresh slots) but the sidecar FILE survived, so
    pre-push keys() listed ghosts on 0.10.4 (the replacement purge protected the commit)."""
    store = {}
    _seed(store, 'db4', tmp_path)
    _partial_cache(store, 'db4', tmp_path)
    _delete_remote(store, 'db4')

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(_conn(store, 'db4'), tmp_path / 'b.blt', flag='n') as eb:
            assert list(eb._remote_index.keys()) == []
            assert sorted(eb.keys()) == []
            eb['k9'] = b'v9'
            assert eb.changes().push()
    with _reader(store, 'db4', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k9']
    _fsck_clean(store, 'db4')


def test_same_session_delete_then_push(tmp_path):
    """FAILS ON 0.10.4: a live session kept its stale index/manifest across
    delete_remote() and committed the same ghosts."""
    store = {}
    _seed(store, 'db5', tmp_path)
    with _writer(store, 'db5', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1'
        eb.delete_remote()
        assert list(eb._remote_index.keys()) == []
        assert eb._remote_state.manifest == {} and eb._remote_state.meta_section is None
        assert eb._remote_state.remote_ts is not None   # the reconciliation watermark is KEPT
        eb['k4'] = b'v4'
        assert eb.changes().push()
    with _reader(store, 'db5', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k1', 'k4']
    _fsck_clean(store, 'db5')


def test_rcg_flavor_after_delete(tmp_path):
    """The envlib catalogue class shares _init_common - one end-to-end check."""
    store = {}
    member_conn = _conn(store, 'member1')
    with open_ebooklet(member_conn, tmp_path / 'm.blt', flag='n') as eb:
        eb['x'] = b'x'
        assert eb.changes().push()
    rcg_conn = _conn(store, 'rcg1')
    key1, key2 = 'abc123def456abc123def456', 'abc123def456abc123def457'
    with open_rcg(rcg_conn, tmp_path / 'rcgw.blt', flag='n') as rcg:
        rcg.add(member_conn, key=key1, user_meta={'variable': 'streamflow'})
        assert rcg.changes().push()
    _delete_remote(store, 'rcg1')
    with open_rcg(rcg_conn, tmp_path / 'rcgw.blt', flag='w') as rcg:
        assert list(rcg._remote_index.keys()) == []
        rcg.add(member_conn, key=key2, user_meta={'variable': 'precipitation'})
        assert rcg.changes().push()
    with open_rcg(rcg_conn, tmp_path / 'rcgr.blt', flag='r') as rcg:
        assert sorted(rcg.keys()) == sorted([key1, key2])   # key1 was local; it re-pushes
    _fsck_clean(store, 'rcg1')


#################################################
### Guards that must NOT fire (these pass on 0.10.4 too - over-firing guards)


@pytest.mark.parametrize('num_groups', [None, 5])
def test_crashed_replacement_still_recovers_when_remote_deleted(tmp_path, num_groups):
    """OVER-FIRING GUARD (passes on 0.10.4): a 'w' recovering an unpushed 'n' keeps its
    sidecar even when the remote was deleted in between - the replacement purge drops
    everything not written. test_journal.py::test_unpushed_replacement_survives_reopen
    is the non-deleted baseline; the grouped case is where a stale pre_push_manifest
    actually exists and must not reach the commit."""
    store = {}
    _seed(store, 'db6', tmp_path, items={'old1': b'o1', 'old2': b'o2'}, num_groups=num_groups)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(_conn(store, 'db6'), tmp_path / 'w.blt', flag='n', num_groups=num_groups) as eb:
            eb['new1'] = b'n1'                          # closes WITHOUT pushing
    _delete_remote(store, 'db6')

    with pytest.warns(UserWarning, match='REPLACEMENT'):
        eb = open_ebooklet(_conn(store, 'db6'), tmp_path / 'w.blt', flag='w')
    try:
        assert set(eb._remote_index.keys()) == {'old1', 'old2'}, 'recovery must keep its sidecar'
        assert eb.changes().push()
    finally:
        eb.close()
    with _reader(store, 'db6', tmp_path) as eb:
        assert sorted(eb.keys()) == ['new1']
    _fsck_clean(store, 'db6')


def test_reader_against_deleted_remote_unchanged(tmp_path):
    """OVER-FIRING GUARD (passes on 0.10.4): 'r' and offline 'r' keep their cache. Values
    heal per key through _resolve_missing's remote_gone path; the key LISTING does not -
    documented, and a reader must not unlink a cache on an unconfirmed 404."""
    store = {}
    _seed(store, 'db7', tmp_path)
    with _reader(store, 'db7', tmp_path, 'r.blt') as eb:
        assert eb['k1'] == b'v1'
    _delete_remote(store, 'db7')

    with _reader(store, 'db7', tmp_path, 'r.blt') as eb:
        assert eb['k1'] == b'v1'
        assert eb.get('k2') is None
        assert sorted(eb._remote_index.keys()) == ['k1', 'k2', 'k3'], 'reader cache must be untouched'
        assert eb._remote_state.remote_ts is not None
    assert (tmp_path / 'r.blt.remote_index').exists()      # literal: this guard must run on 0.10.4 too
    with _reader(store, 'db7', tmp_path, 'r.blt', offline=True) as eb:
        assert eb.offline is True
        assert eb['k1'] == b'v1'


#################################################
### Misfire: a 404 while the remote is alive must cost one fetch, never data


@pytest.mark.parametrize('num_groups', [None, 5])
def test_spurious_404_round_trip(tmp_path, num_groups):
    """FAILS ON THE FIX AS FIRST DRAFTED (no freshness-stamp reset): the file stayed
    stranded believing the remote's index was empty, and the next push silently
    truncated the remote to the locally-held keys."""
    store = {}
    _seed(store, 'db8', tmp_path, num_groups=num_groups)
    _partial_cache(store, 'db8', tmp_path)

    with _spurious_404('db8'):
        with _writer(store, 'db8', tmp_path, 'b.blt', num_groups=num_groups) as eb:
            assert list(eb._remote_index.keys()) == []      # it did forget...
    assert 'db8' in store                                    # ...but nothing was gone

    with _writer(store, 'db8', tmp_path, 'b.blt') as eb:
        assert sorted(eb._remote_index.keys()) == ['k1', 'k2', 'k3'], 're-fetch must heal the cache'
        assert sorted(eb.keys()) == ['k1', 'k2', 'k3']
        eb['k9'] = b'v9'
        assert eb.changes().push()
    with _reader(store, 'db8', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k1', 'k2', 'k3', 'k9']
        assert eb['k2'] == b'v2'
    rep = _fsck_clean(store, 'db8')
    assert rep.orphans == [], rep.orphans


@pytest.mark.parametrize('num_groups', [None, 5])
def test_deletion_intent_survives_a_misfire(tmp_path, num_groups):
    """FAILS ON THE FIX AS FIRST DRAFTED (journaled deletes cleared): the pending
    deletion was cancelled and the key came back on the next open."""
    store = {}
    _seed(store, 'db9', tmp_path, num_groups=num_groups)
    with _writer(store, 'db9', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1'
        del eb['k2']                                         # journaled, not pushed
        assert eb._journal.deletes == {'k2'}

    with _spurious_404('db9'):
        with _writer(store, 'db9', tmp_path, 'b.blt', num_groups=num_groups) as eb:
            assert eb._journal.deletes == {'k2'}

    with _writer(store, 'db9', tmp_path, 'b.blt') as eb:
        assert eb._journal.deletes == {'k2'}
        assert 'k2' not in eb
        assert sorted(eb.keys()) == ['k1', 'k3']
        assert eb.changes().push()
    with _reader(store, 'db9', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k1', 'k3']
    _fsck_clean(store, 'db9')


def test_metadata_precedence_survives_a_misfire(tmp_path):
    """FAILS ON THE FIX AS FIRST DRAFTED (a persisted meta_pending latch): the blipped
    writer kept republishing its stale metadata over the owner's newer version."""
    store = {}
    _seed(store, 'db10', tmp_path, items={'k1': b'v1'}, metadata={'dataset_type': 'grid', 'v': 1})
    with _writer(store, 'db10', tmp_path, 'y.blt') as eb:
        assert eb.get_metadata()['v'] == 1

    with _spurious_404('db10'):
        with _writer(store, 'db10', tmp_path, 'y.blt') as eb:
            assert eb._journal.meta_pending is False

    with _writer(store, 'db10', tmp_path, 'w.blt') as owner:          # the seed file
        owner.set_metadata({'dataset_type': 'grid', 'v': 2})
        assert owner.changes().push()

    with _writer(store, 'db10', tmp_path, 'y.blt') as eb:
        assert eb.get_metadata()['v'] == 2
        assert eb._journal.meta_pending is False
        eb['k5'] = b'v5'
        assert eb.changes().push()
    with _reader(store, 'db10', tmp_path) as eb:
        assert eb.get_metadata()['v'] == 2


def test_genuine_rebuild_carries_metadata(tmp_path):
    """The reason the metadata question exists: cfdb decides create-vs-attach from
    get_metadata(), so a remote rebuilt from a local file must not be born without it.
    Achieved at push time (remote absent, nothing cached), not by a persisted latch."""
    store = {}
    _seed(store, 'db11', tmp_path, items={'k1': b'v1'}, metadata={'dataset_type': 'grid', 'v': 1})
    with _writer(store, 'db11', tmp_path, 'y.blt') as eb:
        assert eb.get_metadata()['v'] == 1
    _delete_remote(store, 'db11')
    with _writer(store, 'db11', tmp_path, 'y.blt') as eb:
        assert eb._journal.meta_pending is False
        eb['k4'] = b'v4'
        assert eb.changes().push()
    with _reader(store, 'db11', tmp_path) as eb:
        assert eb.get_metadata() == {'dataset_type': 'grid', 'v': 1}


#################################################
### Helper units


def test_forget_remote_incarnation_drops_only_rederivable_state(tmp_path):
    store = {}
    _seed(store, 'db12', tmp_path, metadata={'m': 1})
    with _writer(store, 'db12', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1'
        del eb['k2']
        eb['k5'] = b'v5'
        assert eb._remote_state.remote_ts is not None
        watermark = eb._remote_state.remote_ts
        eb_utils.forget_remote_incarnation(eb._local_file, eb._journal, eb._remote_state, reason='unit')
        ## dropped: the cached remote state (manifest, metadata section) and the freshness stamp
        assert eb._remote_state.manifest == {}
        assert eb._remote_state.meta_section is None
        assert eb._local_file._file_timestamp == 0
        assert RemoteState.load(eb._local_file).manifest == {}        # persisted
        ## kept: the reconciliation watermark, journaled deletes and writes, the metadata slot, no latch
        assert eb._remote_state.remote_ts == watermark
        assert RemoteState.load(eb._local_file).remote_ts == watermark
        assert eb._journal.deletes == {'k2'}
        assert eb._journal.written == {'k5'}
        assert eb._journal.meta_pending is False
        assert eb.get_metadata() == {'m': 1}
        assert JournalState.load(eb._local_file).deletes == {'k2'}
        ## idempotent
        eb_utils.forget_remote_incarnation(eb._local_file, eb._journal, eb._remote_state, reason='again')
        assert eb._remote_state.manifest == {} and eb._remote_state.remote_ts == watermark


def test_remote_state_reset_marks_dirty_only_on_change_and_keeps_the_watermark():
    rs = RemoteState()
    rs._dirty = False
    rs.reset()
    assert rs._dirty is False
    rs.update_committed({}, None, 5)                  # watermark only: nothing cached to forget
    rs._dirty = False
    rs.reset()
    assert rs._dirty is False and rs.remote_ts == 5
    rs.update_committed({0: 'abc'}, b'meta', 7)
    rs._dirty = False
    rs.reset()
    assert rs._dirty is True
    assert (rs.manifest, rs.meta_section, rs.remote_ts) == ({}, None, 7)


def test_remote_index_sidecar_path():
    p = pathlib.Path('/x/y.blt')
    assert eb_utils.remote_index_sidecar_path(p) == pathlib.Path('/x/y.blt.remote_index')


#################################################
### Code-review round: the watermark, the pre-push re-check, quietness


@pytest.mark.parametrize('num_groups', [None, 5])
def test_remote_deletion_during_misfire_window_is_reconciled(tmp_path, num_groups):
    """FAILS if forget drops remote_ts: the healing re-fetch then skips reconciliation
    (prev_synced_ts None), a key deleted remotely during the window is served locally
    and RE-PUSHED (resurrection). Both code-review arms found this."""
    store = {}
    _seed(store, 'db13', tmp_path, num_groups=num_groups)
    with _writer(store, 'db13', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1' and eb['k2'] == b'v2'          # both materialized
    with _spurious_404('db13'):
        with _writer(store, 'db13', tmp_path, 'b.blt', num_groups=num_groups) as eb:
            assert list(eb._remote_index.keys()) == []
    with _writer(store, 'db13', tmp_path, 'w.blt') as owner:      # another client deletes k1
        del owner['k1']
        assert owner.changes().push()
    with _writer(store, 'db13', tmp_path, 'b.blt') as eb:         # the heal
        assert 'k1' not in eb
        assert eb.get('k1') is None
        assert eb._local_file.get('k1') is None, 'stale local copy must be reconciled away'
        assert sorted(eb.keys()) == ['k2', 'k3']
        eb['k9'] = b'v9'
        assert eb.changes().push()
    with _reader(store, 'db13', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k2', 'k3', 'k9'], 'k1 resurrected'
    _fsck_clean(store, 'db13')


@pytest.mark.parametrize('num_groups', [None, 5])
def test_second_writer_in_deletion_window_does_not_resurrect(tmp_path, num_groups):
    """The same resurrection through the change's OWN headline scenario, no 404 anomaly:
    delete_remote(); a second writer opens in the window (forgets); the OWNER rebuilds
    from its own local file WITHOUT k2 (same uuid - a rebuild from a different local file
    is refused by UUIDMismatchError, by design); the second writer opens again and pushes."""
    store = {}
    _seed(store, 'db14', tmp_path, num_groups=num_groups)
    with _writer(store, 'db14', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1' and eb['k2'] == b'v2' and eb['k3'] == b'v3'
    _delete_remote(store, 'db14')
    with _writer(store, 'db14', tmp_path, 'b.blt', num_groups=num_groups) as eb:   # in the window
        assert list(eb._remote_index.keys()) == []
    with _writer(store, 'db14', tmp_path, 'w.blt', num_groups=num_groups) as owner:  # the seed file
        del owner['k2']                                                # k2 deliberately dropped
        assert owner.changes().push()
    with _writer(store, 'db14', tmp_path, 'b.blt') as eb:
        assert sorted(eb._remote_index.keys()) == ['k1', 'k3']
        assert 'k2' not in eb and eb._local_file.get('k2') is None
        eb['k9'] = b'v9'
        assert eb.changes().push()
    with _reader(store, 'db14', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k1', 'k3', 'k9'], 'k2 resurrected'
    _fsck_clean(store, 'db14')


@pytest.mark.parametrize('num_groups', [None, 5])
def test_misfired_session_push_adopts_the_live_remote(tmp_path, num_groups, caplog):
    """FAILS without the pre-push re-check: a session whose open saw a TRANSIENT 404 and
    which then pushes replaced the live remote's index with its own keys only (k1 + k9),
    orphaning everything else while fsck called the result healthy (Opus, code review)."""
    store = {}
    _seed(store, 'db15', tmp_path, items={f'k{i}': f'v{i}'.encode() for i in range(1, 6)}, num_groups=num_groups)
    with _writer(store, 'db15', tmp_path, 'b.blt') as eb:
        assert eb['k1'] == b'v1'
    orig = fake_s3.FakeS3Session.head_object

    def fake(self, key, version_id=None):
        return fake_s3.FakeResp(404) if key == 'db15' else orig(self, key, version_id)
    patch = mock.patch.object(fake_s3.FakeS3Session, 'head_object', new=fake)
    patch.start()
    try:
        eb = _writer(store, 'db15', tmp_path, 'b.blt', num_groups=num_groups)
        assert list(eb._remote_index.keys()) == []                 # it forgot
    finally:
        patch.stop()                                                # the 404 was transient
    try:
        eb['k9'] = b'v9'
        with caplog.at_level(logging.WARNING, logger='ebooklet.main'):
            assert eb.changes().push()
        assert any('exists now' in r.message for r in caplog.records)
        assert sorted(eb._remote_index.keys()) == ['k1', 'k2', 'k3', 'k4', 'k5', 'k9']
    finally:
        eb.close()
    with _reader(store, 'db15', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k1', 'k2', 'k3', 'k4', 'k5', 'k9'], 'the live remote was truncated'
        assert eb['k4'] == b'v4'
    rep = _fsck_clean(store, 'db15')
    assert rep.orphans == [], rep.orphans


def test_forget_is_quiet_on_a_never_created_remote(tmp_path, caplog):
    """A database that has simply never been pushed has nothing to forget: no header
    rewrite, no 'no longer exists' log line, on repeated plain opens."""
    store = {}
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(_conn(store, 'db16'), tmp_path / 'new.blt', flag='c') as eb:
            eb['k1'] = b'v1'
            eb._local_file._set_file_timestamp(12345)              # a sentinel a forget would zero
        with caplog.at_level(logging.INFO, logger='ebooklet.utils'):
            for _ in range(3):
                with open_ebooklet(_conn(store, 'db16'), tmp_path / 'new.blt', flag='c') as eb:
                    assert eb._local_file._file_timestamp == 12345, 'the forget ran on a never-created remote'
                    assert eb['k1'] == b'v1'
    assert not any('no longer exists' in r.message for r in caplog.records)


def test_session_delete_remote_resets_every_metadata_field(tmp_path):
    store = {}
    _seed(store, 'db17', tmp_path)
    s = fake_s3.FakeS3Connection(store, 'db17').open('w')
    assert s.initialized and s.timestamp is not None and s.format_version is not None
    s.delete_remote()
    assert (s._init_bytes, s.uuid, s.timestamp, s.type, s.num_groups, s.format_version) == (None,) * 6
    assert s.initialized is False


def test_delete_remote_is_refused_while_a_push_is_running(tmp_path):
    """Like prune()/clear(): the push is reading the index/state delete_remote() mutates."""
    from ebooklet import PushInProgressError
    store = {}
    _seed(store, 'db18', tmp_path)
    with _writer(store, 'db18', tmp_path, 'w.blt') as eb:
        eb._push_active = True
        try:
            with pytest.raises(PushInProgressError):
                eb.delete_remote()
        finally:
            eb._push_active = False
        assert 'db18' in store, 'the remote must be untouched'
