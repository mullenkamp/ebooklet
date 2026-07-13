"""
Hermetic tests for the format-2 generational storage (0.10): immutable
generation-named group objects, the manifest/metadata/index db-object payload,
the staged-commit push protocol (upload -> commit -> GC), the replacement
upload-then-commit-then-sweep, the reader heal-via-retry after generational
GC, and the metadata embed/carry-forward rules.
"""
import re
import warnings

import pytest

import urllib3

from ebooklet import open_ebooklet, utils
from ebooklet.tests import fake_s3


def _seed(store, db_key, tmp_path, name='seed.blt', items=None, num_groups=5, metadata=None):
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', num_groups=num_groups) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        if metadata is not None:
            eb.set_metadata(metadata)
        assert eb.changes().push()
    return conn


def _group_objects(store, db_key='testdb'):
    return sorted(k for k in store if re.match(rf'{db_key}/\d+\.[0-9a-f]{{13}}$', k))


def test_generations_are_never_overwritten(tmp_path):
    """The core invariant: a group object key, once PUT, is never PUT again -
    every repack goes to a fresh generation and the old one is GC'd."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb['k1'] = b'v1b'
        assert eb.changes().push()
        eb['k2'] = b'v2b'
        assert eb.changes().push()
        put_log = eb._remote_session._write_session.put_log

    group_puts = [k for k in put_log if re.match(r'testdb/\d+\.[0-9a-f]{13}$', k)]
    assert group_puts, 'no generation-named group objects were PUT'
    assert len(group_puts) == len(set(group_puts)), \
        f'a live generation object was overwritten: {group_puts}'

    ## And replaced generations were GC'd: only the live ones remain.
    live = _group_objects(store)
    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    assert sorted(f'testdb/{utils.group_obj_key(g, gen)}' for g, gen in manifest.items()) == live


def test_failed_commit_leaves_readers_and_sidecar_untouched(tmp_path):
    """A failed db-object PUT (the commit) must leave (a) fresh readers on the
    old, fully consistent state, and (b) the local sidecar free of any
    reference to the uncommitted generations (staged, not written through)."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        entry_before = eb._remote_index.get('k1')
        manifest_before = dict(eb._remote_state.manifest)

        eb['k1'] = b'CHANGED'
        session = eb._remote_session._write_session
        orig_put = session.put_object
        def failing_db_put(key, data, metadata=None):
            if key == 'testdb':
                return fake_s3.FakeResp(status=500, error={'message': 'induced commit failure'})
            return orig_put(key, data, metadata)
        session.put_object = failing_db_put

        with pytest.raises(urllib3.exceptions.HTTPError, match='db object failed'):
            eb.changes().push()

        ## Sidecar and in-memory manifest reference only COMMITTED state.
        assert eb._remote_index.get('k1') == entry_before
        assert eb._remote_state.manifest == manifest_before
        assert 'k1' in eb._journal.written, 'failed commit cleared the journal'

        ## A fresh reader sees the old state, fully readable.
        fresh = fake_s3.FakeS3Connection(store, 'testdb')
        with open_ebooklet(fresh, tmp_path / 'fresh1.blt', flag='r') as r:
            assert r['k1'] == b'v1'
            assert r['k2'] == b'v2'

        ## The retry converges.
        session.put_object = orig_put
        assert eb.changes().push(force_push=True)
    finally:
        eb.close()

    fresh2 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh2, tmp_path / 'fresh2.blt', flag='r') as r:
        assert r['k1'] == b'CHANGED'


def test_gc_failure_is_invisible_orphan(tmp_path):
    """A failed phase-D delete must not fail the push - the leftover is an
    orphan nothing references."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    before = set(_group_objects(store))

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k1'] = b'v1b'
        session = eb._remote_session._write_session
        orig_del = session.delete_object
        session.delete_object = lambda key: {'message': 'induced GC failure'}
        assert eb.changes().push(), 'a GC failure must not fail the push'
        session.delete_object = orig_del
    finally:
        eb.close()

    ## Old generation(s) remain as orphans; readers resolve only the manifest.
    assert before & set(_group_objects(store)), 'expected the old generation to linger as an orphan'
    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        assert r['k1'] == b'v1b'
        assert r['k2'] == b'v2'


def test_reader_heals_after_generational_gc(tmp_path):
    """The 0.9.3 re-check protocol now HEALS the routine reader race: value
    fetch hits a GC'd generation -> index+manifest re-pull -> one retried
    fetch against the new generation."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    reader_conn = fake_s3.FakeS3Connection(store, 'testdb')
    reader = open_ebooklet(reader_conn, tmp_path / 'r.blt', flag='r')
    try:
        ## Writer repacks k1's group and GCs the generation the reader's
        ## manifest still points at.
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
            eb['k1'] = b'v1-NEW'
            assert eb.changes().push()

        ## The reader's stale-manifest fetch 404s, re-pulls, retries, heals.
        assert reader['k1'] == b'v1-NEW'
        assert reader['k2'] == b'v2'
    finally:
        reader.close()


def test_emptied_group_lifecycle(tmp_path):
    """Deleting a group's last member: no PUT for the group, it leaves the
    manifest at commit, and its old generation is GC'd afterwards."""
    store = {}
    num_groups = 13
    def key_for_group(gid):
        i = 0
        while True:
            k = f'k{i}'
            if utils.key_to_group_id(k, num_groups) == gid:
                return k
            i += 1
    k_a = key_for_group(1)
    k_b = key_for_group(2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=num_groups) as eb:
        eb[k_a] = b'a'
        eb[k_b] = b'b'
        assert eb.changes().push()

    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    assert 1 in manifest and 2 in manifest

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        del eb[k_a]
        assert eb.changes().push()

    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    assert 1 not in manifest, 'emptied group still in the manifest'
    assert 2 in manifest
    assert not any(k.startswith('testdb/1.') for k in store), 'emptied group generation not GC\'d'

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        assert k_a not in r
        assert r[k_b] == b'b'


def test_replacement_partial_failure_commits_nothing(tmp_path):
    """A replacement push with ANY upload failure commits NOTHING: the old
    remote stays fully intact (the pre-0.10 wipe-first protocol left a wiped,
    half-uploaded remote)."""
    store = {}
    num_groups = 13
    def key_for_group(gid):
        i = 0
        while True:
            k = f'x{i}'
            if utils.key_to_group_id(k, num_groups) == gid:
                return k
            i += 1
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt', items={'old1': b'o1', 'old2': b'o2'},
                 num_groups=num_groups)

    k_a = key_for_group(1)
    k_b = key_for_group(2)
    gid_a = 1

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        eb = open_ebooklet(conn, tmp_path / 'n.blt', flag='n', num_groups=num_groups)
    try:
        eb[k_a] = b'na'
        eb[k_b] = b'nb'

        session = eb._remote_session._write_session
        orig_put = session.put_object
        def failing_put(key, data, metadata=None):
            if key.startswith(f'testdb/{gid_a}.'):
                return fake_s3.FakeResp(status=500, error={'message': 'induced failure'})
            return orig_put(key, data, metadata)
        session.put_object = failing_put

        result = eb.changes().push()
        assert result.failures, 'induced failure did not surface'
        assert result.updated is False   # a partial REPLACEMENT commits nothing

        ## The OLD remote is untouched and fully readable.
        fresh = fake_s3.FakeS3Connection(store, 'testdb')
        with open_ebooklet(fresh, tmp_path / 'freshold.blt', flag='r') as r:
            assert r['old1'] == b'o1'
            assert r['old2'] == b'o2'

        ## Retry converges to the replacement.
        session.put_object = orig_put
        assert eb.changes().push()
    finally:
        eb.close()

    fresh2 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh2, tmp_path / 'freshnew.blt', flag='r') as r:
        assert r[k_a] == b'na'
        assert r[k_b] == b'nb'
        assert 'old1' not in r and 'old2' not in r
    ## The sweep removed every old object not in the new manifest.
    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    expected = {f'testdb/{utils.group_obj_key(g, gen)}' for g, gen in manifest.items()}
    children = {k for k in store if k.startswith('testdb/')}
    assert children == expected, f'replacement sweep left unexpected objects: {children - expected}'


def test_replacement_sweep_aborts_on_lost_lock(tmp_path):
    """If the lock is lost between the replacement commit and the sweep, the
    sweep is skipped (log-only): the commit stands, leftovers are orphans."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt', items={'old1': b'o1'})
    old_children = {k for k in store if k.startswith('testdb/')}

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        eb = open_ebooklet(conn, tmp_path / 'n.blt', flag='n', num_groups=5)
    try:
        eb['new1'] = b'n1'
        ## verify() is called at push start, pre-commit, pre-sweep: fail the third.
        calls = {'n': 0}
        def verify():
            calls['n'] += 1
            return calls['n'] < 3
        eb.lock.verify = verify

        assert eb.changes().push()    # commit landed; sweep skipped
    finally:
        eb.close()

    ## Old objects linger as orphans, but the new state is what readers see.
    assert old_children & {k for k in store if k.startswith('testdb/')}, \
        'sweep ran despite the lost lock'
    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        assert r['new1'] == b'n1'
        assert 'old1' not in r


def test_metadata_carry_forward_preserves_remote(tmp_path):
    """A push from a session that never edited metadata carries the remote's
    metadata section forward - even when the local slot is empty."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt', metadata={'m': 1})

    ## Fresh local file; blank its local metadata slot BEHIND the journal's
    ## back (so meta_pending stays False - the carry-forward must not depend
    ## on the local slot).
    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb._local_file.set_metadata(None)
        eb['k3'] = b'v3'
        assert eb.changes().push()
    finally:
        eb.close()

    with fake_s3.FakeS3Connection(store, 'testdb').open('r') as rs:
        assert rs.get_user_metadata() == {'m': 1}, 'push wiped the remote metadata'


def test_metadata_replacement_never_carries_forward(tmp_path):
    """A replacement push embeds the local slot or NOTHING - the old remote's
    metadata must not resurrect into the replacement."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt', metadata={'m': 1})

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'n.blt', flag='n', num_groups=5) as eb:
            eb['fresh'] = b'f'
            assert eb.changes().push()

    with fake_s3.FakeS3Connection(store, 'testdb').open('r') as rs:
        assert rs.get_user_metadata() is None, "replacement resurrected the old remote's metadata"


def test_metadata_skewed_local_edit_wins(tmp_path):
    """meta_pending overrides the timestamp comparison: a skew-stamped local
    set_metadata still pushes (with its timestamp advanced + a warning)."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt', metadata={'m': 1})

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb.set_metadata({'m': 2}, timestamp=1_000_000_000_000_000)   # older than remote's
        with pytest.warns(UserWarning, match='metadata edit'):
            assert eb.changes().push()
    finally:
        eb.close()

    with fake_s3.FakeS3Connection(store, 'testdb').open('r') as rs:
        assert rs.get_user_metadata() == {'m': 2}, 'skew-stamped metadata edit was lost'


def test_group_too_large_guard(tmp_path):
    """F7: an over-limit group surfaces as a per-group failure (never a raise
    through the worker), with the journal retained; other groups commit."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k1'] = b'X' * 200
        orig_max = utils._MAX_GROUP_BYTES
        utils._MAX_GROUP_BYTES = 100
        try:
            result = eb.changes().push()
        finally:
            utils._MAX_GROUP_BYTES = orig_max
        assert result.failures
        assert any('GroupTooLargeError' in v for v in result.failures.values())
        assert 'k1' in eb._journal.written

        assert eb.changes().push()   # with the real limit, converges
    finally:
        eb.close()


def test_per_key_mode_on_v2_payload(tmp_path):
    """Per-key mode rides the v2 payload (empty manifest, embedded metadata,
    in-place per-key objects)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n') as eb:
            eb['alpha'] = b'a'
            eb['beta'] = b'b'
            eb.set_metadata({'mode': 'per-key'})
            assert eb.changes().push()

    manifest, meta_section, _idx = utils.parse_db_payload(store['testdb'][0])
    assert manifest == {}
    assert utils.parse_meta_section(meta_section)[1] == {'mode': 'per-key'}
    assert 'testdb/alpha' in store and 'testdb/beta' in store

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'r.blt', flag='r') as r:
        assert r['alpha'] == b'a'
        assert r.get_metadata() == {'mode': 'per-key'}


def test_copy_remote_skips_orphans(tmp_path):
    """copy_remote is manifest-driven: orphaned generations (abandoned by a
    crashed push) are NOT replicated to the target."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt')

    ## Plant an orphan (an abandoned generation nothing references).
    store['testdb/3.deadbeefdead0'] = (b'orphan-bytes', {})

    conn_target = fake_s3.FakeS3Connection(store, 'targetdb')
    with conn.open('w') as src:
        src.copy_remote(conn_target)

    assert not any(k == 'targetdb/3.deadbeefdead0' for k in store), \
        'copy_remote replicated an orphan generation'
    fresh = fake_s3.FakeS3Connection(store, 'targetdb')
    with open_ebooklet(fresh, tmp_path / 'tr.blt', flag='r') as r:
        assert r['k1'] == b'v1'
        assert r['k2'] == b'v2'
