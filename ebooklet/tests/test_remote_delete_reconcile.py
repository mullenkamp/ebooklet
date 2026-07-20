#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Remote-deletion propagation to warm local caches (0.10.2).

Pre-fix, a locally-materialized key deleted remotely was served forever
(keys()/in/len/reads all treat local presence as authoritative and the
fetch-404 cleanup path never fires for a materialized key), and a warm
WRITER re-pushed such keys (create_changelog treats every index-absent
local key as a new write). Every fresh index ingest now reconciles the
local file against the fresh index. These tests pin the convergence, the
three safety guards (reserved keys / journal-pending writes / the
timestamp discriminator), the documented limitations, and the
re-materialization guard. Hermetic via fake_s3.
"""
import logging

import booklet
import pytest

import ebooklet.utils as eb_utils
from ebooklet import open_ebooklet, open_rcg
from ebooklet.tests import fake_s3


def _seed(store, db_key, tmp_path, name='writer.blt', items=None, num_groups=None):
    """Create + push a small db; returns nothing (connections are cheap)."""
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', num_groups=num_groups) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}).items():
            eb[k] = v
        assert eb.changes().push()


def _writer(store, db_key, tmp_path, name='writer.blt'):
    return open_ebooklet(fake_s3.FakeS3Connection(store, db_key), tmp_path / name, flag='w')


def _reader(store, db_key, tmp_path, name='reader.blt', **kw):
    return open_ebooklet(fake_s3.FakeS3Connection(store, db_key), tmp_path / name, flag='r', **kw)


def _delete_and_push(store, db_key, tmp_path, keys, name='writer.blt'):
    with _writer(store, db_key, tmp_path, name) as eb:
        for k in keys:
            del eb[k]
        assert eb.changes().push()


#################################################
### Convergence through pull() and re-open


@pytest.mark.parametrize('num_groups', [None, 5])
def test_pull_reconciles_warm_deleted_key(tmp_path, num_groups, caplog):
    store = {}
    _seed(store, 'db1', tmp_path, num_groups=num_groups)

    with _reader(store, 'db1', tmp_path) as eb:
        assert eb['k1'] == b'v1'  # materialize the warm copy

        _delete_and_push(store, 'db1', tmp_path, ['k1'])

        with caplog.at_level(logging.INFO, logger='ebooklet.utils'):
            eb.changes().pull()

        assert 'k1' not in eb
        assert sorted(eb.keys()) == ['k2', 'k3']
        assert len(eb) == 2
        assert eb.get('k1') is None
        assert 'k1' not in dict(eb.items())
        ## physically gone from the local file, not just masked
        assert eb._local_file.get('k1') is None
        ## the remaining keys still read fine
        assert eb['k2'] == b'v2'
        assert any('no longer provides' in r.message for r in caplog.records)


@pytest.mark.parametrize('num_groups', [None, 5])
@pytest.mark.parametrize('flag', ['r', 'w'])
def test_reopen_reconciles_warm_deleted_key(tmp_path, num_groups, flag):
    store = {}
    _seed(store, 'db2', tmp_path, num_groups=num_groups)

    local = tmp_path / ('second.blt' if flag == 'r' else 'writer.blt')
    if flag == 'r':
        with _reader(store, 'db2', tmp_path, 'second.blt') as eb:
            assert eb['k1'] == b'v1'
    ## (the 'w' case reuses writer.blt, whose creator materialized k1 itself)

    _delete_and_push(store, 'db2', tmp_path, ['k1'],
                     name='deleter.blt')

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db2'), local, flag=flag) as eb:
        assert 'k1' not in eb
        assert sorted(eb.keys()) == ['k2', 'k3']
        assert eb._local_file.get('k1') is None


def test_no_resurrection_across_pulls(tmp_path):
    store = {}
    _seed(store, 'db3', tmp_path)

    with _reader(store, 'db3', tmp_path) as eb:
        assert eb['k1'] == b'v1'
        _delete_and_push(store, 'db3', tmp_path, ['k1'])
        eb.changes().pull()
        assert 'k1' not in eb

        eb.changes().pull()  # freshness-gated no-op
        assert 'k1' not in eb

        with _writer(store, 'db3', tmp_path) as w:
            w['k4'] = b'v4'
            assert w.changes().push()
        eb.changes().pull()
        assert 'k1' not in eb
        assert eb['k4'] == b'v4'


def test_push_does_not_resurrect_deleted_key(tmp_path):
    """Pre-fix: the creator's warm k1 re-entered create_changelog as a NEW
    write after a foreign delete, silently republishing it."""
    store = {}
    _seed(store, 'db4', tmp_path)

    _delete_and_push(store, 'db4', tmp_path, ['k1'], name='deleter.blt')

    with _writer(store, 'db4', tmp_path) as eb:  # creator's warm cache
        assert 'k1' not in eb  # open-path reconcile already ran
        eb['k4'] = b'v4'
        assert eb.changes().push()

    with _reader(store, 'db4', tmp_path, 'verify.blt') as eb:
        assert 'k1' not in eb
        assert eb['k4'] == b'v4'


def test_open_path_only_reader_reconciles(tmp_path):
    """Pins prev_synced_ts = RemoteState.remote_ts: a reader whose only
    syncs were open-path fetches (the open never stamps the file timestamp)
    must still reconcile on reopen."""
    store = {}
    _seed(store, 'db5', tmp_path)

    with _reader(store, 'db5', tmp_path) as eb:
        assert eb['k1'] == b'v1'

    _delete_and_push(store, 'db5', tmp_path, ['k1'])

    with _reader(store, 'db5', tmp_path) as eb:
        assert 'k1' not in eb
        assert eb._local_file.get('k1') is None


def test_replacement_convergence(tmp_path):
    """A flag='n' re-push (same uuid) replaces the remote's contents; a warm
    reader must converge to the replacement, old keys vanishing."""
    store = {}
    _seed(store, 'db6', tmp_path)

    with _reader(store, 'db6', tmp_path) as eb:
        assert eb['k1'] == b'v1'

    conn = fake_s3.FakeS3Connection(store, 'db6')
    with open_ebooklet(conn, tmp_path / 'replacer.blt', flag='n') as eb:
        eb['k9'] = b'v9'
        assert eb.changes().push()

    with _reader(store, 'db6', tmp_path) as eb:
        assert sorted(eb.keys()) == ['k9']
        assert 'k1' not in eb
        assert eb._local_file.get('k1') is None
        assert eb['k9'] == b'v9'


#################################################
### The safety guards


def test_journal_pending_write_survives_and_pushes(tmp_path):
    store = {}
    _seed(store, 'db7', tmp_path)

    ## second writer records a pending (unpushed) write, closes
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db7'), tmp_path / 'b.blt', flag='w') as eb:
        eb['k_new'] = b'pending'

    _delete_and_push(store, 'db7', tmp_path, ['k1'])

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db7'), tmp_path / 'b.blt', flag='w') as eb:
        assert eb['k_new'] == b'pending'  # journal-pending write survived reconcile
        assert 'k1' not in eb
        assert eb.changes().push()

    with _reader(store, 'db7', tmp_path, 'verify.blt') as eb:
        assert eb['k_new'] == b'pending'
        assert 'k1' not in eb


def test_unjournaled_crash_window_write_survives(tmp_path):
    """A durable write the journal never recorded (booklet auto-flush crash
    window) carries a write-time stamp NEWER than the last ingest - the
    timestamp guard must keep it."""
    store = {}
    _seed(store, 'db8', tmp_path)

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db8'), tmp_path / 'b.blt', flag='w') as eb:
        eb._local_file.set('k_crash', b'crash')  # bypasses the journal entirely

    with _writer(store, 'db8', tmp_path, 'deleter.blt') as w:
        w['k4'] = b'v4'
        assert w.changes().push()

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db8'), tmp_path / 'b.blt', flag='w') as eb:
        assert eb.get('k_crash') == b'crash'


def test_backdated_unjournaled_write_is_deleted(tmp_path):
    """The documented residual (and the R1 limitation): an unjournaled write
    BACKDATED to before the last ingest is indistinguishable from a
    remotely-sourced value and is reconciled away."""
    store = {}
    _seed(store, 'db9', tmp_path)

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db9'), tmp_path / 'b.blt', flag='w') as eb:
        old_ts = eb._remote_state.remote_ts - 1  # strictly before the last ingest
        eb._local_file.set('k_back', b'old', timestamp=old_ts)

    with _writer(store, 'db9', tmp_path, 'deleter.blt') as w:
        w['k4'] = b'v4'
        assert w.changes().push()

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db9'), tmp_path / 'b.blt', flag='w') as eb:
        assert eb.get('k_back') is None
        assert eb._local_file.get('k_back') is None


def test_future_stamped_deleted_key_survives(tmp_path):
    """The other R1 direction, pinned: a key pushed with an explicit FUTURE
    timestamp defeats the discriminator - after remote deletion the warm
    copy survives locally (documented limitation of explicit stamps)."""
    store = {}
    _seed(store, 'db10', tmp_path)

    future_ts = booklet.utils.make_timestamp_int() + 10_000_000_000  # ~2.8h ahead
    with _writer(store, 'db10', tmp_path) as eb:
        eb.set('kf', b'vf', timestamp=future_ts)
        assert eb.changes().push()

    with _reader(store, 'db10', tmp_path) as eb:
        assert eb['kf'] == b'vf'
        _delete_and_push(store, 'db10', tmp_path, ['kf'])
        eb.changes().pull()
        assert eb._local_file.get('kf') == b'vf'  # survives: ts > prev_synced_ts


def test_metadata_survives_reconciliation(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'db11')
    with open_ebooklet(conn, tmp_path / 'writer.blt', flag='n') as eb:
        eb['k1'] = b'v1'
        eb['k2'] = b'v2'
        eb.set_metadata({'title': 'meta stays'})
        assert eb.changes().push()

    with _reader(store, 'db11', tmp_path) as eb:
        assert eb['k1'] == b'v1'
        assert eb.get_metadata() == {'title': 'meta stays'}
        _delete_and_push(store, 'db11', tmp_path, ['k1'])
        eb.changes().pull()
        assert 'k1' not in eb
        assert eb.get_metadata() == {'title': 'meta stays'}


def test_discard_force_pull_loses_nothing(tmp_path):
    store = {}
    _seed(store, 'db12', tmp_path)

    with _writer(store, 'db12', tmp_path) as eb:
        eb['w1'] = b'pending'
        eb.changes().discard()
        assert eb.get('w1') is None       # the discarded write is gone
        assert eb['k1'] == b'v1'          # materialized keys intact
        assert sorted(eb.keys()) == ['k1', 'k2', 'k3']


def test_offline_reopen_serves_stale_cache(tmp_path):
    """Offline sessions never ingest, so they never reconcile - the stale
    cache keeps serving (documented; convergence needs a connected open)."""
    store = {}
    _seed(store, 'db13', tmp_path)

    with _reader(store, 'db13', tmp_path) as eb:
        assert eb['k1'] == b'v1'

    _delete_and_push(store, 'db13', tmp_path, ['k1'])

    with _reader(store, 'db13', tmp_path, offline=True) as eb:
        assert eb.offline is True
        assert eb['k1'] == b'v1'  # stale, served without error


def test_write_vs_delete_conflict_resurrects_by_push(tmp_path):
    """Deliberate last-writer-wins toward the write: a journaled pending
    write for a key another client deleted remotely survives reconcile and
    the next push re-creates the key."""
    store = {}
    _seed(store, 'db14', tmp_path)

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db14'), tmp_path / 'b.blt', flag='w') as eb:
        eb['k1'] = b'revised'  # pending write for a key about to be foreign-deleted

    _delete_and_push(store, 'db14', tmp_path, ['k1'])

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db14'), tmp_path / 'b.blt', flag='w') as eb:
        assert eb['k1'] == b'revised'  # journal-pending write beat the foreign delete
        assert eb.changes().push()

    with _reader(store, 'db14', tmp_path, 'verify.blt') as eb:
        assert eb['k1'] == b'revised'


def test_rcg_flavor_converges(tmp_path):
    """The envlib catalogue class (RemoteConnGroup) shares _init_common -
    one end-to-end convergence check (the live incident's shape)."""
    store = {}
    member_conn = fake_s3.FakeS3Connection(store, 'member1')
    with open_ebooklet(member_conn, tmp_path / 'm.blt', flag='n') as eb:
        eb['x'] = b'x'
        assert eb.changes().push()

    rcg_conn = fake_s3.FakeS3Connection(store, 'rcg1')
    key = 'abc123def456abc123def456'
    with open_rcg(rcg_conn, tmp_path / 'rcgw.blt', flag='n') as rcg:
        rcg.add(member_conn, key=key, user_meta={'variable': 'streamflow'})
        assert rcg.changes().push()

    with open_rcg(rcg_conn, tmp_path / 'rcgr.blt', flag='r') as rcg:
        assert rcg[key]['user_meta'] == {'variable': 'streamflow'}  # materialize

    with open_rcg(rcg_conn, tmp_path / 'rcgw.blt', flag='w') as rcg:
        del rcg[key]
        assert rcg.changes().push()

    ## the warm public-style reader converges on reopen - the live defect
    with open_rcg(rcg_conn, tmp_path / 'rcgr.blt', flag='r') as rcg:
        assert key not in rcg
        assert sorted(rcg.keys()) == []


#################################################
### The re-materialization guard (R3)


def test_load_items_drops_stale_worker_completion(tmp_path, monkeypatch):
    """A worker completing against an index entry captured before a
    concurrent pull swapped + reconciled the index must not re-insert the
    deleted key. Simulated deterministically: the patched fetch performs the
    foreign delete + pull, THEN materializes its stale value."""
    store = {}
    _seed(store, 'db15', tmp_path)

    reader = open_ebooklet(fake_s3.FakeS3Connection(store, 'db15'), tmp_path / 'r.blt', flag='r')
    try:
        real_get = eb_utils.get_remote_value
        state = {'fired': False}

        def stale_worker(local_file, key, remote_session):
            if key == 'k2' and not state['fired']:
                state['fired'] = True
                _delete_and_push(store, 'db15', tmp_path, ['k2'])
                reader.changes().pull()  # swap + reconcile (k2 not local yet)
                old_ts = booklet.utils.make_timestamp_int() - 10_000_000
                local_file.set(key, b'stale', timestamp=old_ts)  # the late completion
                return None
            return real_get(local_file, key, remote_session)

        monkeypatch.setattr(eb_utils, 'get_remote_value', stale_worker)
        reader.load_items(keys=['k2'])
        assert reader._local_file.get('k2') is None  # the guard dropped it
        assert 'k2' not in reader
    finally:
        reader.close()


def test_point_read_drops_stale_worker_completion(tmp_path, monkeypatch):
    """Same guard on the point-read path (_load_item)."""
    store = {}
    _seed(store, 'db16', tmp_path)

    reader = open_ebooklet(fake_s3.FakeS3Connection(store, 'db16'), tmp_path / 'r.blt', flag='r')
    try:
        state = {'fired': False}

        def stale_worker(local_file, key, remote_session):
            state['fired'] = True
            _delete_and_push(store, 'db16', tmp_path, ['k2'])
            reader.changes().pull()
            old_ts = booklet.utils.make_timestamp_int() - 10_000_000
            local_file.set(key, b'stale', timestamp=old_ts)
            return None

        monkeypatch.setattr(eb_utils, 'get_remote_value', stale_worker)
        assert reader.get('k2') is None
        assert state['fired']
        assert reader._local_file.get('k2') is None
        assert 'k2' not in reader
    finally:
        reader.close()


#################################################
### The helper directly (edge semantics)


class _StubFile:
    def __init__(self, items, fail_times=0):
        self.d = dict(items)  # {key: ts_int}
        self.fail_times = fail_times

    def timestamps(self):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError('booklet mutated during iteration')
        return iter(list(self.d.items()))

    def __len__(self):
        return len(self.d)

    def __contains__(self, key):
        return key in self.d

    def __delitem__(self, key):
        del self.d[key]


class _StubJournal:
    written = frozenset({'pending'})


def test_helper_prev_none_is_noop():
    f = _StubFile({'a': 10, 'b': 20})
    removed = eb_utils.reconcile_local_with_index(f, {}, _StubJournal(), None)
    assert removed == []
    assert sorted(f.d) == ['a', 'b']


def test_helper_guards_and_boundaries():
    f = _StubFile({
        'gone': 50,        # index-absent, old -> deleted
        'boundary': 100,   # index-absent, ts == prev -> deleted (<=)
        'newer': 101,      # index-absent, newer than prev -> kept
        'pending': 50,     # journal-pending -> kept
        'claimed': 50,     # still in the index -> kept
    })
    removed = eb_utils.reconcile_local_with_index(f, {'claimed': b'x'}, _StubJournal(), 100)
    assert removed == ['boundary', 'gone']
    assert sorted(f.d) == ['claimed', 'newer', 'pending']


def test_helper_retry_then_skip(caplog):
    ## one abort -> retried and succeeds
    f = _StubFile({'gone': 50}, fail_times=1)
    assert eb_utils.reconcile_local_with_index(f, {}, _StubJournal(), 100) == ['gone']

    ## two aborts -> skipped with a warning, nothing deleted
    f = _StubFile({'gone': 50}, fail_times=2)
    with caplog.at_level(logging.WARNING, logger='ebooklet.utils'):
        assert eb_utils.reconcile_local_with_index(f, {}, _StubJournal(), 100) == []
    assert sorted(f.d) == ['gone']
    assert any('aborted twice' in r.message for r in caplog.records)


def test_helper_mass_purge_warns(caplog):
    f = _StubFile({f'k{i}': 50 for i in range(10)})
    with caplog.at_level(logging.INFO, logger='ebooklet.utils'):
        removed = eb_utils.reconcile_local_with_index(f, {}, _StubJournal(), 100)
    assert len(removed) == 10
    warning = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning and 'no longer provides 10' in warning[0].message
