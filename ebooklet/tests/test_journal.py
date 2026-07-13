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

from ebooklet import open_ebooklet, utils
from ebooklet.journal import JournalRecord, JOURNAL_SLOT
from ebooklet.tests import fake_s3


def _seed(store, db_key, tmp_path, name='seed.blt', items=None, num_groups=5):
    """Create + push a small db; returns the connection."""
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', num_groups=num_groups) as eb:
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
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
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
    num_groups = 13
    ## Find keys in two different groups.
    def key_for_group(gid, taken=()):
        i = 0
        while True:
            k = f'k{i}'
            if utils.key_to_group_id(k, num_groups) == gid and k not in taken:
                return k
            i += 1
    k_a = key_for_group(1)
    k_b = key_for_group(2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=num_groups) as eb:
        eb[k_a] = b'a1'
        eb[k_b] = b'b1'
        assert eb.changes().push()

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb[k_a] = b'a2'
        eb[k_b] = b'b2'

        ## Fail the PUT of group 1's object exactly once.
        session = eb._remote_session._write_session
        orig_put = session.put_object
        gid_a = utils.key_to_group_id(k_a, num_groups)
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


def test_num_groups_tristate_reopen(tmp_path):
    """The journal records the num_groups choice: a created-but-unpushed db
    reopens WITHOUT the kwarg, warning-free, and the first push is grouped."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
            eb['k1'] = b'v1'
            # close without pushing

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    assert not [w for w in records if 'per-key storage' in str(w.message)], \
        'journaled num_groups did not silence the reopen warning'
    try:
        assert eb.changes().push()
    finally:
        eb.close()

    assert any(k.startswith('testdb/') and k.split('/')[1].split('.')[0].isdigit() for k in store), \
        'first push was not grouped'


def test_num_groups_kwarg_conflict_raises(tmp_path):
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
            eb['k1'] = b'v1'

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')   # the pending-REPLACEMENT reopen warning
        with pytest.raises(ValueError, match='conflicts with this local file'):
            open_ebooklet(conn, tmp_path / 'w.blt', flag='w', num_groups=11)


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
