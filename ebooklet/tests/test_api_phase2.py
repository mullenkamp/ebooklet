"""
Hermetic tests for the Phase-2 API round (0.10): the typed exception taxonomy,
the MutableMapping conformance pass (__delitem__ KeyError, dict-style update,
true clear()), PushResult, and offline read mode. All S3 interaction goes
through tests/fake_s3.py (plus one connection-refused case against
127.0.0.1:1 - purely local, no credentials, no network).

The behaviors marked PRE-FIX differ on 99ad450 (pre-Phase-2), proven by the
round's pre-fix proof scripts:
- del of a missing key was a silent no-op (now KeyError),
- update() accepted a single mapping only (now full dict.update semantics),
- clear() was local cache eviction (now a true journaled clear),
- push() returned True/False/dict and a non-empty partial dict was truthy
  (now PushResult, falsy on any failure),
- an unreachable remote hard-failed open_ebooklet (now offline=True/'auto').
"""
import warnings

import pytest
import urllib3

from ebooklet import (
    open_ebooklet,
    utils,
    Error,
    ReadOnlyError,
    UUIDMismatchError,
    RemoteMissingError,
    UnsupportedFormatError,
    GroupTooLargeError,
    RemoteIntegrityError,
    LockLostError,
    OfflineError,
    PushResult,
)
from ebooklet.remote import S3Connection
from ebooklet.tests import fake_s3


def _seed(store, db_key, tmp_path, name='seed.blt', items=None, num_groups=5):
    """Create + push a small db; returns the connection."""
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', num_groups=num_groups) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        assert eb.changes().push()
    return conn


def _dead_conn(db_key='deaddb'):
    """A real S3Connection to a guaranteed-refused local port (no network)."""
    return S3Connection(access_key_id='x', access_key='y', db_key=db_key,
                        bucket='nobucket', endpoint_url='http://127.0.0.1:1', retries=0)


#################################################
### Exception taxonomy


def test_taxonomy_parentage():
    """Every typed error derives from Error; legacy parentage is preserved via
    dual inheritance; LockLostError is deliberately NOT an HTTPError."""
    for cls in (ReadOnlyError, UUIDMismatchError, RemoteMissingError,
                UnsupportedFormatError, GroupTooLargeError):
        assert issubclass(cls, Error) and issubclass(cls, ValueError)
    assert issubclass(RemoteIntegrityError, Error)
    assert issubclass(RemoteIntegrityError, urllib3.exceptions.HTTPError)
    assert issubclass(LockLostError, Error)
    assert not issubclass(LockLostError, urllib3.exceptions.HTTPError)
    assert issubclass(OfflineError, Error)
    ## Old attribute paths keep working (imports moved to errors.py).
    assert utils.UnsupportedFormatError is UnsupportedFormatError
    assert utils.GroupTooLargeError is GroupTooLargeError
    from ebooklet import main as _main
    assert _main.RemoteIntegrityError is RemoteIntegrityError


def test_read_only_error_on_reader(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path)
    with open_ebooklet(conn, tmp_path / 'r.blt', flag='r') as eb:
        with pytest.raises(ReadOnlyError):
            eb['x'] = b'v'
        with pytest.raises(ReadOnlyError):
            eb.clear()
        with pytest.raises(ReadOnlyError):
            eb.changes().push()
        ## The dual parentage keeps pre-taxonomy handlers working.
        with pytest.raises(ValueError):
            eb['x'] = b'v'


def test_remote_missing_error_on_bare_read_open(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'nodb')
    with pytest.raises(RemoteMissingError):
        open_ebooklet(conn, tmp_path / 'none.blt', flag='r')


def test_uuid_mismatch_error_typed(tmp_path):
    """A local file whose remote was torn down and re-created raises the
    typed UUIDMismatchError (a ValueError, so old handlers still catch)."""
    store = {}
    _seed(store, 'testdb', tmp_path, 'local.blt')
    ## Re-create the remote under the same db_key -> new uuid.
    store.clear()
    _seed(store, 'testdb', tmp_path, 'other.blt', items={'z': b'z'})
    with pytest.raises(UUIDMismatchError):
        open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'local.blt', flag='r')


#################################################
### MutableMapping pass


def test_delitem_missing_raises_keyerror(tmp_path):
    """PRE-FIX: del of a missing key was a silent no-op."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        with pytest.raises(KeyError):
            del eb['never_existed']
        ## Deleting an existing key still works, and a SECOND del raises.
        del eb['k1']
        with pytest.raises(KeyError):
            del eb['k1']
        ## The guarded-consumer idiom (cfdb) stays correct.
        try:
            del eb['also_never_existed']
        except KeyError:
            pass
        ## pop with a default must swallow the KeyError (MutableMapping mixin).
        assert eb.pop('never_existed', b'dflt') == b'dflt'


def test_update_dict_semantics(tmp_path):
    """PRE-FIX: update() accepted a single positional mapping only."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb.update({'m1': b'1'})
        eb.update([('p1', b'2'), ('p2', b'3')])
        eb.update(kw1=b'4', kw2=b'5')
        eb.update({'m2': b'6'}, kw3=b'7')
        for k, v in (('m1', b'1'), ('p1', b'2'), ('p2', b'3'),
                     ('kw1', b'4'), ('kw2', b'5'), ('m2', b'6'), ('kw3', b'7')):
            assert eb[k] == v
        ## Reserved internal keys are rejected through every form.
        with pytest.raises(ValueError):
            eb.update({utils.metadata_key_str: b'x'})

    with open_ebooklet(conn, tmp_path / 'r2.blt', flag='r') as eb:
        with pytest.raises(ReadOnlyError):
            eb.update({'x': b'v'})


#################################################
### clear() - true clear


def test_clear_is_true_clear_e2e(tmp_path):
    """PRE-FIX: clear() was cache eviction (keys survived and re-pulled).
    Now: every key is journaled as a deletion and the next push commits an
    atomically-empty database (all groups emptied -> manifest {})."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items={'k1': b'v1', 'k2': b'v2', 'k3': b'v3'})
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb.clear()
        assert len(eb) == 0
        assert list(eb.keys()) == []
        assert 'k1' not in eb
        assert sorted(eb.changes().pending_deletes) == ['k1', 'k2', 'k3']
        assert eb.changes().push()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert len(eb) == 0
        assert list(eb.keys()) == []


def test_clear_survives_close_without_push(tmp_path):
    """F3 regression: the journaled deletes must be re-persisted AFTER the
    local-file truncate (which destroys the journal slot). A clear followed by
    an unpushed close must NOT resurrect the keys in the next session."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb.clear()
        ## no push

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        assert len(eb) == 0, 'cleared keys resurrected after an unpushed close'
        assert sorted(eb.changes().pending_deletes) == ['k1', 'k2']
        assert eb.changes().push()

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'fresh.blt', flag='r') as eb:
        assert len(eb) == 0


def test_clear_cancellable_via_discard(tmp_path):
    """Until pushed, a clear is cancellable: discard() restores the entries
    (forced index re-pull) and values transparently re-pull."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb.clear()
        assert len(eb) == 0
        eb.changes().discard()
        assert sorted(eb.keys()) == ['k1', 'k2']
        assert eb['k1'] == b'v1'
        assert not eb.changes().pending_deletes
        ## Nothing pending: the push is a no-op.
        result = eb.changes().push()
        assert result.updated is False

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['k1'] == b'v1' and eb['k2'] == b'v2'


def test_clear_drops_unpushed_writes_silently(tmp_path):
    """Ruling: clear() means clear - unpushed pending writes are dropped
    without a warning, and the pushed result is an empty database."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb['pending'] = b'unpushed'
        with warnings.catch_warnings():
            warnings.simplefilter('error')  # any warning -> test failure
            eb.clear()
        assert 'pending' not in eb
        assert len(eb) == 0
        assert eb.changes().push()

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'fresh.blt', flag='r') as eb:
        assert len(eb) == 0


#################################################
### PushResult


def test_pushresult_mapping(tmp_path):
    """PRE-FIX: push() returned True/False and a truthy dict on partial
    failure. Now: PushResult with explicit fields and safe truthiness."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k1'] = b'v1'
        result = eb.changes().push()
        assert isinstance(result, PushResult)
        assert result.updated is True and result.failures == {}
        assert bool(result) is True
        ## NOT the legacy bool: an accidental `push() == True` must not pass.
        assert (result == True) is False  # noqa: E712

        noop = eb.changes().push()
        assert noop.updated is False and noop.failures == {}
        assert bool(noop) is False

        ## Falsy-on-failure is proven in test_journal (non-replacement
        ## partial: updated True + failures) and test_generational
        ## (replacement partial: updated False); here just the invariant:
        assert bool(PushResult(updated=True, failures={'k': 'HTTPError: x'})) is False


#################################################
### Offline read mode


def _seed_with_partial_reader(store, tmp_path):
    """Seed k1..k3, materialize ONLY k1 into reader.blt, return its path."""
    conn = _seed(store, 'offdb', tmp_path, 'writer.blt',
                 items={'k1': b'v1', 'k2': b'v2', 'k3': b'v3'})
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'offdb'), tmp_path / 'reader.blt', flag='r') as eb:
        assert eb['k1'] == b'v1'
    return conn, tmp_path / 'reader.blt'


def test_offline_true_reads_and_typed_errors(tmp_path):
    store = {}
    conn, reader_path = _seed_with_partial_reader(store, tmp_path)

    with open_ebooklet(conn, reader_path, flag='r', offline=True) as eb:
        assert eb.offline is True
        assert eb['k1'] == b'v1'
        assert sorted(eb.keys()) == ['k1', 'k2', 'k3']
        ## Unmaterialized point read: OfflineError, NOT KeyError (the key exists).
        with pytest.raises(OfflineError):
            eb['k2']
        ## Bulk read: ONE OfflineError naming the keys, before any worker runs.
        with pytest.raises(OfflineError, match=r"k2.*k3"):
            list(eb.items())
        ## Remote-requiring ops raise typed errors.
        with pytest.raises(OfflineError):
            eb.changes().pull()
        with pytest.raises(ReadOnlyError):
            eb['new'] = b'x'


def test_offline_arg_validation(tmp_path):
    store = {}
    conn, reader_path = _seed_with_partial_reader(store, tmp_path)
    with pytest.raises(ValueError, match='read-only'):
        open_ebooklet(conn, reader_path, flag='w', offline=True)
    with pytest.raises(ValueError, match='offline must be'):
        open_ebooklet(conn, reader_path, flag='r', offline='yes')
    with pytest.raises(OfflineError):
        open_ebooklet(conn, tmp_path / 'absent.blt', flag='r', offline=True)


def test_offline_auto_online_when_reachable(tmp_path):
    store = {}
    conn, reader_path = _seed_with_partial_reader(store, tmp_path)
    with open_ebooklet(conn, reader_path, flag='r', offline='auto') as eb:
        assert eb.offline is False
        assert dict(eb.items()) == {'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}


def test_offline_auto_falls_back_on_transport_failure(tmp_path):
    """'auto' + unreachable remote -> UserWarning + offline session serving
    the local cache; without a local cache the OfflineError propagates."""
    store = {}
    _conn, reader_path = _seed_with_partial_reader(store, tmp_path)

    with pytest.warns(UserWarning, match='unreachable'):
        eb = open_ebooklet(_dead_conn('offdb'), reader_path, flag='r', offline='auto')
    try:
        assert eb.offline is True
        assert eb['k1'] == b'v1'
    finally:
        eb.close()

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with pytest.raises(OfflineError):
            open_ebooklet(_dead_conn('offdb'), tmp_path / 'absent2.blt', flag='r', offline='auto')


def test_offline_auto_never_swallows_typed_errors(tmp_path):
    """The 'auto' classifier is transport-only: a REACHABLE remote whose open
    fails with a typed ebooklet error (here: uuid mismatch) must raise, never
    silently serve stale local data."""
    store = {}
    _seed(store, 'testdb', tmp_path, 'local.blt')
    store.clear()
    _seed(store, 'testdb', tmp_path, 'other.blt', items={'z': b'z'})
    with pytest.raises(UUIDMismatchError):
        open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'local.blt',
                      flag='r', offline='auto')
