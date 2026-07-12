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
from ebooklet.tests import fake_s3


def _no_matching_warning(records, needle):
    return not [w for w in records if needle in str(w.message)]


def test_close_with_unpushed_deletes_is_quiet_and_journaled(tmp_path):
    """0.10: pending deletions survive the close in the journal (the 0.9.5
    loss warning is gone because there is no longer a loss to warn about)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        assert eb.changes().push() is True

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    del eb['k']
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')

    ## The deletion survived the close and the next session's push applies it.
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        assert 'k' not in eb
        assert eb.changes().push() is True

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'k' not in eb


def test_close_quiet_when_deletes_pushed(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        eb['k2'] = b'v2'
        assert eb.changes().push() is True

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    del eb['k']
    assert eb.changes().push() is True
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')


def test_close_quiet_for_readers(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        assert eb.changes().push() is True

    eb = open_ebooklet(conn, tmp_path / 'r.blt', flag='r')
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb.close()
    assert _no_matching_warning(records, 'pending deletion')


def test_db_object_metadata_carries_format_version(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        assert eb.changes().push() is True

    _data, meta = store['testdb']
    assert meta['format_version'] == '1'


def test_too_new_format_version_is_refused(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        assert eb.changes().push() is True

    data, meta = store['testdb']
    meta = dict(meta)
    meta['format_version'] = '2'
    store['testdb'] = (data, meta)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with pytest.raises(UnsupportedFormatError, match='Upgrade ebooklet'):
        open_ebooklet(conn2, tmp_path / 'r.blt', flag='r')

    ## Compatibility fault, not an integrity fault - and catchable as ValueError.
    assert issubclass(UnsupportedFormatError, ValueError)
    assert not issubclass(UnsupportedFormatError, ebooklet.RemoteIntegrityError)


def test_absent_format_version_means_v1(tmp_path):
    """Remotes pushed by <=0.9.4 carry no stamp and must keep opening."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb['k'] = b'v'
        assert eb.changes().push() is True

    data, meta = store['testdb']
    meta = dict(meta)
    del meta['format_version']
    store['testdb'] = (data, meta)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn2, tmp_path / 'r.blt', flag='r') as eb:
        assert eb['k'] == b'v'


def test_http_db_url_warns():
    with pytest.warns(UserWarning, match='plain http'):
        S3Connection(db_url='http://example.com/bucket/db')


def test_https_db_url_is_quiet():
    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        S3Connection(db_url='https://example.com/bucket/db')
    assert _no_matching_warning(records, 'plain http')
