"""
Hermetic tests for 0.11.1: the commit prunes the remote index it uploads, and
an open refused by the uuid check closes the local file it opened.

The index is log-structured (booklet), so every overwrite of a key appends a new
entry and orphans the old one. Before 0.11.1 each commit uploaded the sidecar as
is, superseded entries included (90-94 % of the live ECan indexes). The tests
measure committed index BYTES: an entry count cannot catch the regression,
because the mapping API shows one entry per live key either way.
"""
import gc
import warnings
from unittest import mock

import booklet
import pytest

from ebooklet import open_ebooklet, utils, UUIDMismatchError
from ebooklet.tests import fake_s3

KEYS = [f'k{i}' for i in range(40)]


def _conn(store, db_key='d'):
    return fake_s3.FakeS3Connection(store, db_key)


def _committed_index_len(store, db_key='d'):
    _manifest, _meta, index_bytes = utils.parse_db_payload(store[db_key][0])
    return len(index_bytes)


def _overwrite_pushes(store, path, group_bytes, n_pushes):
    """Push all KEYS, then overwrite them n_pushes - 1 more times. Returns the committed index length after each push."""
    lens = []
    with open_ebooklet(_conn(store), path, flag='n', value_serializer='bytes', group_bytes=group_bytes) as eb:
        for p in range(n_pushes):
            for k in KEYS:
                eb[k] = b'%s-%d' % (k.encode(), p)
            assert eb.changes().push()
            lens.append(_committed_index_len(store))
    return lens


@pytest.mark.parametrize('group_bytes', [None, 4096], ids=['per_key', 'grouped'])
def test_committed_index_stays_flat_over_overwrites(tmp_path, group_bytes):
    store = {}
    lens = _overwrite_pushes(store, tmp_path / 'w.blt', group_bytes, n_pushes=20)
    assert lens[-1] == lens[0], f'the committed index grew with superseded entries: {lens}'

    ## and it still resolves every key to its latest value
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as eb:
        assert not eb.load_items()
        assert sorted(eb.keys()) == sorted(KEYS)
        assert all(eb[k] == b'%s-19' % k.encode() for k in KEYS)


@pytest.mark.parametrize('group_bytes', [None, 4096], ids=['per_key', 'grouped'])
def test_prune_with_deletes(tmp_path, group_bytes):
    store = {}
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='n', value_serializer='bytes', group_bytes=group_bytes) as eb:
        for k in KEYS:
            eb[k] = b'v1'
        assert eb.changes().push()
        del eb['k0']
        eb['k1'] = b'v2'
        assert eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as eb:
        assert sorted(eb.keys()) == sorted(KEYS[1:])
        assert eb['k1'] == b'v2' and eb['k2'] == b'v1'


def _half_done_prune(self, *args, **kwargs):
    """A prune killed part-way: the file it was compacting is left truncated."""
    with self._thread_lock:
        self._file.truncate(300)
        self._file.flush()
    raise RuntimeError('killed mid-prune')


@pytest.mark.parametrize('group_bytes', [None, 4096], ids=['per_key', 'grouped'])
def test_a_failed_prune_leaves_the_live_sidecar_and_remote_intact(tmp_path, group_bytes):
    """The prune runs on a throwaway copy: a crash there must not touch the local state or commit."""
    store = {}
    path = tmp_path / 'w.blt'
    _overwrite_pushes(store, path, group_bytes, n_pushes=2)
    db_before = store['d']

    with open_ebooklet(_conn(store), path, flag='w') as eb:
        for k in KEYS:
            eb[k] = b'new'
        with mock.patch.object(booklet.FixedLengthValue, 'prune', _half_done_prune):
            with pytest.raises(RuntimeError, match='killed mid-prune'):
                eb.changes().push()
    assert store['d'] == db_before, 'nothing may be committed when the prune fails'

    sidecar = utils.remote_index_sidecar_path(path)
    with booklet.FixedLengthValue(sidecar, 'r') as idx:
        assert sorted(idx.keys()) == sorted(KEYS), 'the live sidecar must stay readable and complete'

    ## the retry commits, and readers see the new values
    with open_ebooklet(_conn(store), path, flag='w') as eb:
        assert eb.changes().push()
    with open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r') as eb:
        assert all(eb[k] == b'new' for k in KEYS)


@pytest.mark.parametrize('flag', ['r', 'w', 'c'])
def test_uuid_refusal_closes_the_local_file(tmp_path, flag):
    """A refused open must close the local booklet it opened, so the file stays cleanly openable."""
    store = {}
    with open_ebooklet(_conn(store), tmp_path / 'a.blt', flag='n', value_serializer='bytes') as eb:
        eb['k'] = b'remote'
        assert eb.changes().push()

    other = tmp_path / 'other.blt'
    with booklet.open(other, 'n', key_serializer='str', value_serializer='bytes') as b:
        b['mine'] = b'local'

    with pytest.raises(UUIDMismatchError):
        open_ebooklet(_conn(store), other, flag=flag)
    gc.collect()

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with booklet.open(other, 'r', timeout=5) as b:
            assert b['mine'] == b'local'
