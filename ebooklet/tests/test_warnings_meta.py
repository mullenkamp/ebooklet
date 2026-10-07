"""
Hermetic tests for the warning + format-version behaviors: closing with
unpushed deletions (journaled since 0.10 - no more loss warning), the
db-object format_version stamp and too-new refusal, and the non-HTTPS db_url
warning.
"""
import warnings

import pytest

import ebooklet
from ebooklet import open_ebooklet, S3Connection, UnsupportedFormatError
import io

import booklet

from ebooklet import utils
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import TEST_GB


def _no_matching_warning(records, needle):
    return not [w for w in records if needle in str(w.message)]


def test_close_with_unpushed_deletes_is_quiet_and_journaled(tmp_path):
    """0.10: pending deletions survive the close in the journal (the 0.9.5
    loss warning is gone because there is no longer a loss to warn about)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k'] = b'v'
        assert eb.changes().push()

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    del eb['k']
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')

    ## The deletion survived the close and the next session's push applies it.
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        assert 'k' not in eb
        assert eb.changes().push()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'k' not in eb


def test_close_quiet_when_deletes_pushed(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k'] = b'v'
        eb['k2'] = b'v2'
        assert eb.changes().push()

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    del eb['k']
    assert eb.changes().push()
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')


def test_close_quiet_for_readers(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k'] = b'v'
        assert eb.changes().push()

    eb = open_ebooklet(conn, tmp_path / 'r.blt', flag='r')
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')


@pytest.mark.parametrize('group_bytes, stamp', [(TEST_GB, '3'), (None, '2')])
def test_db_object_metadata_carries_format_version(tmp_path, group_bytes, stamp):
    """The stamp follows the storage mode, never the reader cap: grouped
    remotes are format 3, per-key remotes stay format 2 (readable by 0.10
    clients). Neither carries the pre-0.11 num_groups field, whose presence
    marks a remote as legacy."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=group_bytes) as eb:
        eb['k'] = b'v'
        assert eb.changes().push()

    data, meta = store['testdb']
    assert meta['format_version'] == stamp
    assert 'num_groups' not in meta
    ## The index layout matches the mode.
    index_bytes = utils.parse_db_payload(data)[2]
    f = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
    try:
        assert f._value_len == (utils.INDEX_LEN_GROUPED if group_bytes else utils.INDEX_LEN_PER_KEY)
    finally:
        f.close()
    ## And the remote classifies as the mode it was pushed in, with the target
    ## a grouped remote records.
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'r.blt', flag='r') as r:
        assert r.group_bytes == group_bytes
        assert r['k'] == b'v'


def test_too_new_format_version_is_refused(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k'] = b'v'
        assert eb.changes().push()

    data, meta = store['testdb']
    meta = dict(meta)
    meta['format_version'] = '4'
    store['testdb'] = (data, meta)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with pytest.raises(UnsupportedFormatError, match='Upgrade ebooklet'):
        open_ebooklet(conn2, tmp_path / 'r.blt', flag='r')

    ## Compatibility fault, not an integrity fault - and catchable as ValueError.
    assert issubclass(UnsupportedFormatError, ValueError)
    assert not issubclass(UnsupportedFormatError, ebooklet.RemoteIntegrityError)


def _restamp_legacy(store, db_key, hash_grouped):
    """Turn a pushed remote's db-object metadata into a legacy stamp: format 2
    plus num_groups (hash-grouped, pre-0.11), or no stamp at all (format 1)."""
    data, meta = store[db_key]
    meta = dict(meta)
    if hash_grouped:
        meta['format_version'] = '2'
        meta['num_groups'] = '5'
    else:
        del meta['format_version']      # absent stamp = format 1
    store[db_key] = (data, meta)


@pytest.mark.parametrize('hash_grouped', [True, False], ids=['hash-grouped-format-2', 'format-1'])
def test_legacy_is_refused_except_replacement(tmp_path, hash_grouped):
    """0.11 no-compat contract: a legacy remote - hash-grouped format 2
    (num_groups in its metadata) or format 1 (no stamp) - refuses r/w/c
    loudly, naming the move to the current format; flag='n' replacement
    proceeds (index-fetch-suppressed) and its commit makes the remote
    format 3."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k'] = b'v'
        assert eb.changes().push()
    _restamp_legacy(store, 'testdb', hash_grouped)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    match = 'hash-grouped remote' if hash_grouped else 'format_version 1 remote'
    with pytest.raises(UnsupportedFormatError, match=match):
        open_ebooklet(conn2, tmp_path / 'r.blt', flag='r')
    with pytest.raises(UnsupportedFormatError, match='load_items'):
        open_ebooklet(conn2, tmp_path / 'w2.blt', flag='w')
    with pytest.raises(UnsupportedFormatError):
        open_ebooklet(conn2, tmp_path / 'c2.blt', flag='c')

    ## The replacement path works and makes the remote format 3.
    conn3 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn3, tmp_path / 'n.blt', flag='n') as eb:
        eb['fresh'] = b'f1'
        assert 'k' not in eb        # legacy content is never read (suppressed)
        assert eb.changes().push()

    _data2, meta2 = store['testdb']
    assert meta2['format_version'] == '3'
    assert 'num_groups' not in meta2
    conn4 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn4, tmp_path / 'r2.blt', flag='r') as eb:
        assert eb['fresh'] == b'f1'
        assert 'k' not in eb


def test_per_key_format_2_is_not_legacy(tmp_path):
    """A format-2 remote WITHOUT num_groups is per-key and stays fully
    readable and writable - the format number alone must not classify it as
    legacy (a naive reader-cap bump would refuse every per-key remote)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'pk')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=None) as eb:
        eb['a'] = b'1'
        assert eb.changes().push()
    assert store['pk'][1]['format_version'] == '2'

    with open_ebooklet(fake_s3.FakeS3Connection(store, 'pk'), tmp_path / 'w2.blt', flag='w') as eb:
        assert eb.group_bytes is None
        assert eb['a'] == b'1'
        eb['b'] = b'2'
        assert eb.changes().push()
    assert store['pk'][1]['format_version'] == '2'
    assert 'pk/a' in store and 'pk/b' in store


def test_http_db_url_warns():
    with pytest.warns(UserWarning, match='plain http'):
        S3Connection(db_url='http://example.com/bucket/db')


def test_https_db_url_is_quiet():
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        S3Connection(db_url='https://example.com/bucket/db')
    assert _no_matching_warning(records, 'plain http')
