"""
Hermetic tests for ebooklet.fsck (format 2): orphan detection and the
age-gated, lock-guarded sweep; claimed-but-missing and torn-teardown reports.
"""
import datetime

import pytest

from ebooklet import open_ebooklet, fsck, utils
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import TEST_GB, remote_index_gids, remote_manifest


def _seed(store, db_key, tmp_path, items=None, group_bytes=TEST_GB):
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / 'seed.blt', flag='n', group_bytes=group_bytes) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        assert eb.changes().push()
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
    assert report.format_version == utils.FORMAT_GROUPED
    assert report.orphans == []
    assert report.claimed_but_missing == []
    assert report.unmanifested_group_ids == []
    assert report.empty_groups == []
    assert report.dead_fraction == {}
    assert report.bad_entry_layout is False
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


@pytest.mark.parametrize('hash_grouped', [True, False], ids=['hash-grouped-format-2', 'format-1'])
def test_fsck_refuses_legacy(tmp_path, hash_grouped):
    store = {}
    _seed(store, 'testdb', tmp_path)
    data, meta = store['testdb']
    meta = dict(meta)
    if hash_grouped:
        meta['format_version'] = '2'
        meta['num_groups'] = '5'
    else:
        del meta['format_version']
    store['testdb'] = (data, meta)

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with pytest.raises(utils.UnsupportedFormatError, match='legacy'):
        fsck(conn2)


def test_per_key_mode_expected_set(tmp_path):
    import warnings as _w
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with _w.catch_warnings():
        _w.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=None) as eb:
            eb['alpha'] = b'a'
            assert eb.changes().push()

    store['testdb/orphan-child'] = (b'x', {})
    report = fsck(conn, check_objects=True)
    assert report.format_version == utils.FORMAT_PER_KEY
    assert report.orphans == ['orphan-child']
    assert report.claimed_but_missing == []
    assert report.bad_entry_layout is False


def _rewrite_payload(store, db_key, manifest=None, meta=None):
    """Re-PUT a fake db object with an edited manifest and/or S3 metadata."""
    data, old_meta = store[db_key]
    old_manifest, meta_section, index_bytes = utils.parse_db_payload(data)
    new_manifest = old_manifest if manifest is None else manifest
    store[db_key] = (utils.build_db_payload(new_manifest, meta_section, bytes(index_bytes)),
                     dict(old_meta) if meta is None else meta)
    return old_manifest


def test_unmanifested_gid_from_index_entries(tmp_path):
    """The index's entries name their group; a gid the manifest lacks is an
    integrity fault fsck must report (read from the entries, not a hash)."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path,
                 items={f'k{i}': bytes([65 + i]) * 20 for i in range(6)})
    gids = sorted(set(remote_index_gids(store, 'testdb').values()))
    assert len(gids) >= 2, 'precondition: the seed spans several groups'
    manifest = remote_manifest(store, 'testdb')
    dropped = gids[0]
    _rewrite_payload(store, 'testdb', manifest={g: v for g, v in manifest.items() if g != dropped})
    report = fsck(conn)
    assert report.unmanifested_group_ids == [dropped]


def test_empty_group_reported(tmp_path):
    """A manifest gid no index entry points at should never survive a push
    (lazy deletes drop it); fsck reports one as waste."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path)
    manifest = remote_manifest(store, 'testdb')
    phantom = max(manifest) + 5
    _rewrite_payload(store, 'testdb', manifest={**manifest, phantom: 'feedfacecafe0'})
    store[f'testdb/{phantom}.feedfacecafe0'] = (b'\x00\x00\x00\x00', {})
    report = fsck(conn)
    assert report.empty_groups == [phantom]
    assert report.orphans == []


def test_dead_fraction_after_lazy_delete(tmp_path):
    """A lazy delete leaves the deleted value's bytes in its group object;
    fsck reports the group's dead fraction from the index alone."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, items={'aa': b'x' * 10, 'bb': b'y' * 10})
    gids = remote_index_gids(store, 'testdb')
    assert gids['aa'] == gids['bb'], 'precondition: both values share a group'
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'testdb'), tmp_path / 'seed.blt', flag='w') as eb:
        del eb['aa']
        assert eb.changes().push()
    report = fsck(conn)
    ## Group = header(4) + 2 entries of 2+2+7+4+10 = 25 -> 54 bytes; one is dead.
    assert report.dead_fraction == {gids['bb']: round(25 / 54, 4)}


def test_bad_entry_layout_reported(tmp_path):
    """A per-key (15-byte) index under a format-3 stamp cannot be read."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, group_bytes=None)
    _data, meta = store['testdb']
    meta = dict(meta)
    meta['format_version'] = '3'
    _rewrite_payload(store, 'testdb', meta=meta)
    report = fsck(conn)
    assert report.bad_entry_layout is True
