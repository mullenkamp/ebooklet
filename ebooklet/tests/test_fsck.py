"""
Hermetic tests for ebooklet.fsck (format 2): orphan detection and the
age-gated, lock-guarded sweep; claimed-but-missing and torn-teardown reports.
"""
import datetime

import pytest

from ebooklet import open_ebooklet, fsck, utils
from ebooklet.tests import fake_s3


def _seed(store, db_key, tmp_path, items=None, num_groups=5):
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / 'seed.blt', flag='n', num_groups=num_groups) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        assert eb.changes().push() is True
    return conn


def _backdate_all(conn, days=2):
    session = conn.open('w')
    try:
        old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        for k in list(session._write_session.upload_times):
            session._write_session.upload_times[k] = old
    finally:
        session.close()


def test_clean_remote_reports_clean(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path)
    report = fsck(conn)
    assert report.db_object_exists is True
    assert report.format_version == utils.SUPPORTED_FORMAT_VERSION
    assert report.orphans == []
    assert report.claimed_but_missing == []
    assert report.unmanifested_group_ids == []
    assert report.expected_objects >= 1


def test_orphans_reported_and_age_gated_sweep(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path)

    ## Plant an OLD orphan (abandoned generation) and a YOUNG one.
    store['testdb/7.deadbeefdead0'] = (b'old-orphan', {})
    store['testdb/8.deadbeefdead1'] = (b'young-orphan', {})
    session = conn.open('w')
    session._write_session.upload_times['testdb/7.deadbeefdead0'] = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2))
    session._write_session.upload_times['testdb/8.deadbeefdead1'] = (
        datetime.datetime.now(datetime.timezone.utc))
    session.close()

    ## Report mode: both reported, nothing deleted.
    report = fsck(conn)
    assert set(report.orphans) == {'7.deadbeefdead0', '8.deadbeefdead1'}
    assert 'testdb/7.deadbeefdead0' in store

    ## Sweep: only the aged orphan goes; the young one is skipped.
    report = fsck(conn, delete_orphans=True)
    assert report.swept == ['7.deadbeefdead0']
    assert report.skipped_young == ['8.deadbeefdead1']
    assert 'testdb/7.deadbeefdead0' not in store
    assert 'testdb/8.deadbeefdead1' in store

    ## The database itself is untouched.
    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'r.blt', flag='r') as r:
        assert r['k1'] == b'v1'
        assert r['k2'] == b'v2'


def test_claimed_but_missing_detected(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path)

    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    gid, gen = next(iter(manifest.items()))
    del store[f'testdb/{utils.group_obj_key(gid, gen)}']

    report = fsck(conn)
    assert report.claimed_but_missing == [utils.group_obj_key(gid, gen)]


def test_torn_teardown_detected_and_sweepable(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path)
    _backdate_all(conn)
    del store['testdb']   # the db object vanishes; children remain

    report = fsck(conn)
    assert report.db_object_exists is False
    assert report.torn_teardown is True
    assert report.orphans

    report = fsck(conn, delete_orphans=True)
    assert report.swept
    assert not any(k.startswith('testdb/') for k in store)


def test_fsck_refuses_format_1(tmp_path):
    store = {}
    _seed(store, 'testdb', tmp_path)
    data, meta = store['testdb']
    meta = dict(meta)
    del meta['format_version']
    store['testdb'] = (data, meta)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with pytest.raises(utils.UnsupportedFormatError):
        fsck(conn2)


def test_per_key_mode_expected_set(tmp_path):
    import warnings as _w
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with _w.catch_warnings():
        _w.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n') as eb:
            eb['alpha'] = b'a'
            assert eb.changes().push() is True

    store['testdb/orphan-child'] = (b'x', {})
    report = fsck(conn, check_objects=True)
    assert report.orphans == ['orphan-child']
    assert report.claimed_but_missing == []
