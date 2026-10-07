"""
Hermetic tests for the persistent pending-change journal (Seam 2) and the
Seam-1 session-state decomposition (0.10). All S3 interaction goes through
tests/fake_s3.py; the code under test is the real open/read/push machinery.

The four behavioral regressions marked PRE-FIX fail on 0.9.6:
- a delete that never pushed was silently lost when the session closed,
- a clock-skewed local edit (set with an older timestamp) silently never pushed,
- a read pulled the newer remote value OVER an unpushed local edit,
- an unpushed flag='n' replacement was silently forgotten by a 'w' reopen
  (the next push half-merged new keys into the old remote).
"""
import warnings

import pytest

import msgspec

from ebooklet import open_ebooklet, open_rcg, utils, DEFAULT_GROUP_BYTES
from ebooklet.journal import JournalRecord, JOURNAL_SLOT
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import TEST_GB, gid_of


def _seed(store, db_key, tmp_path, name='seed.blt', items=None, group_bytes=TEST_GB):
    """Create + push a small db; returns the connection."""
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', group_bytes=group_bytes) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        assert eb.changes().push()
    return conn


#################################################
### PRE-FIX behavioral regressions (fail on 0.9.6)


def test_cross_session_delete_survives_close(tmp_path):
    """PRE-FIX: an unpushed delete used to die with the session; it is now
    journaled, honored by reads in the next session, and pushed."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        del eb['k1']
        # no push

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        assert 'k1' not in eb, 'journaled delete not honored after reopen'
        assert eb.changes().push()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'k1' not in eb, 'cross-session delete never reached the remote'
        assert eb['k2'] == b'v2'


def test_skewed_local_edit_pushes(tmp_path):
    """PRE-FIX: a local edit with a timestamp at/before the remote's never
    entered the timestamp-diff changelog - silently never pushed. The journal
    unions it in and normalizes its timestamp so readers pick it up."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        ## A skew-stamped edit: explicitly older than the pushed value.
        eb.set('k1', b'SKEWED', timestamp=1_000_000_000_000_000)
        with pytest.warns(UserWarning, match='timestamp was advanced'):
            result = eb.changes().push()
        assert result

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['k1'] == b'SKEWED', 'skewed local edit was silently dropped from the push'


def test_read_your_writes_gate(tmp_path):
    """PRE-FIX: a read used to pull the newer remote value OVER an unpushed
    local edit whose timestamp was older (clobbering it). The journal gate
    serves the pending local value regardless of timestamps."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb.set('k1', b'MINE', timestamp=1_000_000_000_000_000)   # older than remote
        assert eb['k1'] == b'MINE', 'read clobbered an unpushed local edit with the remote value'
        ## And it survives load_items too.
        vals = dict(eb.items())
        assert vals['k1'] == b'MINE'


def test_unpushed_replacement_survives_reopen(tmp_path):
    """PRE-FIX: an unpushed flag='n' replacement was silently forgotten by a
    'w' reopen - the next push half-merged into the old remote. The intent is
    now journaled: the reopened session warns and the push replaces."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items={'old1': b'o1', 'old2': b'o2'})

    ## Replacement session: writes one key, closes WITHOUT pushing.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
            eb['new1'] = b'n1'

    ## 'w' reopen must carry the replacement intent...
    with pytest.warns(UserWarning, match='REPLACEMENT'):
        eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        assert eb.changes().push()
    finally:
        eb.close()

    ## ...and the pushed remote is the REPLACEMENT, not a half-merge.
    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['new1'] == b'n1'
        assert 'old1' not in eb, "flag='n' replacement half-merged: old keys survived"
        assert 'old2' not in eb


#################################################
### Journal mechanics


def test_delete_journaled_immediately(tmp_path):
    """A delete persists to the journal slot at __delitem__ time (not at the
    next sync boundary) - a hard crash must not silently lose it."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        del eb['k1']
        raw = eb._local_file.get_reserved(JOURNAL_SLOT)
        assert raw is not None
        record = msgspec.json.decode(raw, type=JournalRecord)
        assert 'k1' in record.deletes


def test_union_changelog_covers_stale_journal(tmp_path):
    """The timestamp diff catches writes whose journal entry was lost (the
    crash window): blanking the in-memory journal must not stop the push."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb['k3'] = b'v3'
        ## Simulate a lost journal entry (crash window): the data block is
        ## durable, the journal never heard of it.
        eb._journal.written.clear()
        assert eb.changes().push()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['k3'] == b'v3', 'timestamp-diff leg of the union failed to push the write'


def test_journal_cleared_only_after_commit(tmp_path):
    """Journal entries clear per committed state; a broken lock aborts BEFORE
    the commit and retains everything."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k3'] = b'v3'
        del eb['k1']
        eb.lock.broken = True
        with pytest.raises(Exception, match='no longer held'):
            eb.changes().push()
        assert 'k3' in eb._journal.written, 'aborted push cleared the journal'
        assert 'k1' in eb._journal.deletes

        eb.lock.broken = False
        assert eb.changes().push()
        assert not eb._journal.written
        assert not eb._journal.deletes
    finally:
        eb.close()


def test_partial_failure_retains_failed_groups_only(tmp_path):
    """A failed group PUT retains that group's journal entries; committed
    groups clear. The retry converges."""
    store = {}
    k_a, k_b = 'ka', 'kb'

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    ## group_bytes=1: one key per group.
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=1) as eb:
        eb[k_a] = b'a1'
        eb[k_b] = b'b1'
        assert eb.changes().push()

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w', group_bytes=1)
    try:
        gid_a = gid_of(eb, k_a)
        assert gid_a is not None and gid_a != gid_of(eb, k_b), 'precondition: separate groups'
        eb[k_a] = b'a2'
        eb[k_b] = b'b2'

        ## Fail the PUT of group A's object exactly once.
        session = eb._remote_session._write_session
        orig_put = session.put_object
        def failing_put(key, data, metadata=None):
            if key.startswith(f'testdb/{gid_a}.'):   # any generation of group A
                return fake_s3.FakeResp(status=500, error={'message': 'induced failure'})
            return orig_put(key, data, metadata)
        session.put_object = failing_put

        result = eb.changes().push()
        assert result.failures, 'induced group failure did not surface'
        assert result.updated is True   # non-replacement partial: successful groups committed
        assert k_a in eb._journal.written, "failed group's journal entry was cleared"
        assert k_b not in eb._journal.written, "committed group's journal entry was retained"

        session.put_object = orig_put
        assert eb.changes().push()
        assert not eb._journal.written
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb[k_a] == b'a2'
        assert eb[k_b] == b'b2'


def test_replay_on_swap_prevents_resurrection(tmp_path):
    """A journaled delete survives an index re-pull (another writer's push
    bumps the remote timestamp): the fresh index copy is replayed against the
    journal, so the key cannot resurrect."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'a.blt')

    ## Session A: journals a delete of k1 (no push).
    eb_a = open_ebooklet(conn, tmp_path / 'a.blt', flag='w')
    try:
        del eb_a['k1']
        assert 'k1' not in eb_a

        ## Writer B pushes an unrelated change (bumps the remote timestamp).
        conn_b = fake_s3.FakeS3Connection(store, 'testdb')
        with open_ebooklet(conn_b, tmp_path / 'b.blt', flag='w') as eb_b:
            eb_b['k9'] = b'v9'
            assert eb_b.changes().push()

        ## A pulls the fresh index - k1's entry is in it, replay removes it.
        eb_a.changes().pull()
        assert 'k1' not in eb_a, 'index re-pull resurrected a journaled delete'
        assert eb_a['k9'] == b'v9'
    finally:
        eb_a.close()


def test_discard_cancels_pending_deletes(tmp_path):
    """discard() cancels journaled deletions and restores their index entries
    (forced re-pull), so the key is readable again."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        del eb['k1']
        assert 'k1' not in eb
        eb.changes().discard()
        assert not eb._journal.deletes
        assert 'k1' in eb, 'discarded delete did not restore the index entry'
        assert eb['k1'] == b'v1'
    finally:
        eb.close()


def test_discard_removes_written_from_journal(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k1'] = b'EDIT'
        eb.changes().discard()
        assert 'k1' not in eb._journal.written
        assert eb['k1'] == b'v1', 'discard did not restore the remote value'
    finally:
        eb.close()


def test_reserved_keys_rejected(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        for bad in sorted(utils.reserved_key_strs):
            with pytest.raises(ValueError, match='reserved'):
                eb[bad] = b'x'
            with pytest.raises(ValueError, match='reserved'):
                del eb[bad]
            with pytest.raises(ValueError, match='reserved'):
                eb.set_timestamp(bad, 1_600_000_000_000_000)
    finally:
        eb.close()


def test_prune_protects_journaled_writes(tmp_path):
    """A timestamp prune must not evict a journaled pending write."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb.set('k1', b'OLD-BUT-MINE', timestamp=1_000_000_000_000_000)
        eb.prune(timestamp=2_000_000_000_000_000)
        assert eb['k1'] == b'OLD-BUT-MINE', 'prune evicted a journaled pending write'
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter('always')
            assert eb.changes().push()
        assert not [w for w in records if 'DROPPED' in str(w.message)]
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['k1'] == b'OLD-BUT-MINE'


def test_dropped_journal_entry_warns(tmp_path):
    """A journaled write whose local value vanished (external eviction) is
    dropped from the push with a loud warning, once."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k3'] = b'v3'
        ## Externally remove the local value behind the journal's back.
        del eb._local_file['k3']
        with pytest.warns(UserWarning, match='DROPPED'):
            eb.changes().push()
        assert 'k3' not in eb._journal.written, 'dropped entry must leave the journal'
    finally:
        eb.close()


def test_storage_mode_recorded_for_an_unpushed_db(tmp_path):
    """The journal records the storage mode: a created-but-unpushed PER-KEY
    db reopens WITHOUT the kwarg and its first push is still per-key (the
    recorded choice beats the 0.11 grouped default)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=None) as eb:
            eb['k1'] = b'v1'
            # close without pushing

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')   # the pending-REPLACEMENT reopen warning
        eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        assert eb.group_bytes is None
        assert eb.changes().push()
    finally:
        eb.close()
    assert 'testdb/k1' in store, 'first push was not per-key'
    assert store['testdb'][1]['format_version'] == '2'


def test_new_db_defaults_to_grouped(tmp_path):
    """0.11: omitting group_bytes for a NEW database means grouped storage at
    DEFAULT_GROUP_BYTES (per-key needs an explicit None)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n') as eb:
        assert eb.group_bytes == DEFAULT_GROUP_BYTES
        eb['k1'] = b'v1'
        assert eb.changes().push()
    assert store['testdb'][1]['format_version'] == '3'
    assert 'testdb/k1' not in store


def test_explicit_mode_wins_for_an_absent_remote(tmp_path):
    """With no remote to decide, an explicit group_bytes overrides the
    journal's recorded mode (and is recorded in its place)."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
            eb['k1'] = b'v1'

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='w', group_bytes=None) as eb:
            assert eb.group_bytes is None
            assert eb._journal.storage == 'per_key'
            assert eb.changes().push()
    assert 'testdb/k1' in store


def test_remote_mode_wins_over_an_explicit_kwarg(tmp_path):
    """An existing remote decides the mode; a conflicting explicit kwarg is
    ignored with a warning (flag 'w'), or refused (flag 'n')."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=TEST_GB) as eb:
        eb['k1'] = b'v1'
        assert eb.changes().push()

    with pytest.warns(UserWarning, match='the remote wins'):
        eb = open_ebooklet(conn, tmp_path / 'w2.blt', flag='w', group_bytes=None)
    try:
        assert eb.group_bytes == TEST_GB, 'the remote wins with the value it records'
    finally:
        eb.close()
    ## A different byte target in the same (grouped) mode is not a conflict.
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        with open_ebooklet(conn, tmp_path / 'w3.blt', flag='w', group_bytes=1000) as eb:
            assert eb.group_bytes == 1000
    with pytest.raises(ValueError, match='storage mode'):
        open_ebooklet(conn, tmp_path / 'n.blt', flag='n', group_bytes=None)


def test_num_groups_shim(tmp_path):
    """An explicit num_groups=None keeps its pre-0.11 meaning - per-key - so
    callers that pass it (cfdb-ingest's and ifs-download's archive writers)
    keep their layout instead of silently becoming grouped; an int is refused,
    naming group_bytes; passing both is refused."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=None) as eb:
        assert eb.group_bytes is None
        eb['k'] = b'v'
        assert eb.changes().push()
    assert store['testdb'][1]['format_version'] == '2' and 'testdb/k' in store
    with pytest.raises(ValueError, match='group_bytes'):
        open_ebooklet(conn, tmp_path / 'x.blt', flag='n', num_groups=5)
    with pytest.raises(ValueError, match='group_bytes'):
        open_rcg(conn, tmp_path / 'y.blt', flag='n', num_groups=5)
    with pytest.raises(ValueError, match='group_bytes only'):
        open_ebooklet(conn, tmp_path / 'z.blt', flag='n', num_groups=None, group_bytes=TEST_GB)


def test_group_bytes_is_keyword_only(tmp_path):
    """group_bytes sits where 0.10's num_groups was: a positional 0.10 call
    (a group COUNT) must fail loudly, not become a byte target."""
    conn = fake_s3.FakeS3Connection({}, 'testdb')
    with pytest.raises(TypeError):
        open_ebooklet(conn, tmp_path / 'p.blt', 'n', None, 12007, 2**22, 101)
    with pytest.raises(TypeError):
        open_rcg(conn, tmp_path / 'q.blt', 'n', 12007, 2**22, 101)
    assert not (tmp_path / 'p.blt').exists() and not (tmp_path / 'q.blt').exists()


@pytest.mark.parametrize('bad', [0, -1, utils._MAX_GROUP_BYTES + 1])
def test_group_bytes_validation(tmp_path, bad):
    with pytest.raises(ValueError, match='group_bytes'):
        open_ebooklet(fake_s3.FakeS3Connection({}, 'testdb'), tmp_path / 'w.blt', flag='n', group_bytes=bad)


def test_group_bytes_type_validation(tmp_path):
    for bad in (True, 1.5, '32'):
        with pytest.raises(TypeError, match='group_bytes'):
            open_ebooklet(fake_s3.FakeS3Connection({}, 'testdb'), tmp_path / 'w.blt', flag='n', group_bytes=bad)


class _V1Record(msgspec.Struct):
    """The journal record as ebooklet 0.10.x decodes it (non-strict)."""
    v: int = 1
    written: list = []
    deletes: list = []
    num_groups: int | None = None
    num_groups_set: bool = False
    replace_pending: bool = False
    meta_pending: bool = False


def _raw_journal(eb):
    return msgspec.json.decode(eb._local_file.get_reserved(JOURNAL_SLOT))


@pytest.mark.parametrize('group_bytes, version', [(None, 1), (TEST_GB, 2)])
def test_journal_version_follows_the_mode(tmp_path, group_bytes, version):
    """Version 2 is written only for grouped files: a per-key file's journal
    stays version 1 and decodes with 0.10's struct (shared cache dirs across
    venvs), recording per-key the way 0.10 understands it."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=group_bytes) as eb:
        eb['k1'] = b'v1'
        eb.sync()
        raw = _raw_journal(eb)
        rec = msgspec.json.decode(eb._local_file.get_reserved(JOURNAL_SLOT), type=_V1Record)
    assert raw['v'] == version
    assert rec.v == version
    if version == 1:
        assert rec.num_groups is None and rec.num_groups_set is True


def test_legacy_journal_maps_to_grouped(tmp_path):
    """A pre-0.11 journal that recorded a hash num_groups resolves to grouped
    against an absent remote - the republish path for a hydrated local file."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', group_bytes=None) as eb:
        eb['k1'] = b'v1'
        ## Overwrite the slot with a 0.10-style record: hash-grouped, 13 groups.
        legacy = _V1Record(v=1, written=['k1'], num_groups=13, num_groups_set=True, replace_pending=True)
        eb._local_file.set_reserved(JOURNAL_SLOT, msgspec.json.encode(legacy))
        eb._journal._dirty = False
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
            assert eb.group_bytes == DEFAULT_GROUP_BYTES
            assert eb.changes().push()
    assert store['testdb'][1]['format_version'] == '3'


def test_iterate_while_sync_clean_journal(tmp_path):
    """sync() with a clean journal must not invalidate live iterators
    (persist-if-dirty): cfdb calls sync() before every changes()."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt',
                 items={f'k{i}': b'v%d' % i for i in range(10)})

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb.sync()   # journal persisted (clean afterwards)
        it = eb._local_file.keys()
        next(it)
        eb.sync()   # clean journal -> no reserved write -> iterator survives
        next(it)
    finally:
        eb.close()


def test_meta_pending_lifecycle(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb.set_metadata({'a': 1})
        assert eb._journal.meta_pending is True
        assert eb.changes().push()
        assert eb._journal.meta_pending is False
    finally:
        eb.close()


def test_journal_version_gate(tmp_path):
    """A journal blob from a NEWER ebooklet refuses loudly instead of
    default-parsing state it cannot understand."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb._local_file.set_reserved(JOURNAL_SLOT, msgspec.json.encode({'v': 99, 'written': []}))
    finally:
        eb.close()

    with pytest.raises(ValueError, match='Upgrade ebooklet'):
        open_ebooklet(conn, tmp_path / 'w.blt', flag='w')


def test_pending_deletes_surfaced(tmp_path):
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')
    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        del eb['k1']
        assert eb.changes().pending_deletes == frozenset({'k1'})
    finally:
        eb.close()
