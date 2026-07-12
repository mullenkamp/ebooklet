"""
Hermetic (credential-free) regression tests for the delete-safety fixes.
All S3 interaction goes through tests/fake_s3.py; the code under test
(remote.py sessions + the full open_ebooklet/push paths) is the real thing.

The three F1 regressions FAIL on 0.9.4:
- emptying group '1' also destroyed groups '10' and '12' (prefix delete),
- delete_remote on 'mydb' also destroyed sibling 'mydb2' and the lock tickets,
- the writability probe wrote outside the db_key + '/' namespace.

The delete-then-set regressions (0.9.6) FAIL on 0.9.5: deleting a key and
re-setting it in the same session left it in the pending-deletes set, so the
push's delete pass removed the index entry the upload pass had just written.
"""
import base64
import uuid as _uuid

from ebooklet import open_ebooklet, utils
from ebooklet.tests import fake_s3


def _key_for_group(gid, num_groups, taken=()):
    """Find a short key that hashes into the wanted group id."""
    i = 0
    while True:
        k = f'k{i}'
        if utils.key_to_group_id(k, num_groups) == gid and k not in taken:
            return k
        i += 1


def _seed_db_object(store, db_key):
    """Seed a minimal parseable db object so a session sees an existing remote."""
    meta = {
        'init_bytes': base64.urlsafe_b64encode(b'\x00' * 200).decode(),
        'timestamp': '123',
        'uuid': _uuid.uuid4().hex,
        'type': 'EVariableLengthValue',
        }
    store[db_key] = (b'fake-index', meta)


def test_emptied_group_delete_spares_sibling_groups(tmp_path):
    """F1 group variant: with num_groups=13, emptying group 1 must not touch 10/12."""
    store = {}
    num_groups = 13
    k1 = _key_for_group(1, num_groups)
    k10 = _key_for_group(10, num_groups)
    k12 = _key_for_group(12, num_groups)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'local.blt', flag='n', num_groups=num_groups) as eb:
        eb[k1] = b'one'
        eb[k10] = b'ten'
        eb[k12] = b'twelve'
        assert eb.changes().push() is True

    def _has_group(gid):
        return any(k.startswith(f'testdb/{gid}.') for k in store)

    assert _has_group(1) and _has_group(10) and _has_group(12)

    with open_ebooklet(conn, tmp_path / 'local.blt', flag='w') as eb:
        del eb[k1]
        assert eb.changes().push() is True

    assert not _has_group(1)
    assert _has_group(10), 'delete of group 1 destroyed group 10'
    assert _has_group(12), 'delete of group 1 destroyed group 12'

    ## The surviving members must still be readable by a fresh reader.
    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn2, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb[k10] == b'ten'
        assert eb[k12] == b'twelve'
        assert k1 not in eb


def test_delete_remote_is_bounded(tmp_path):
    """F1 sibling variant, session level: delete_remote('mydb') must remove
    exactly the db object + its '/' children, sparing sibling databases, the
    lock namespace, and (accepted residual) legacy probe orphans."""
    store = {}
    _seed_db_object(store, 'mydb')
    store['mydb/1'] = (b'g1', {})
    store['mydb/_metadata'] = (b'meta', {})
    store['mydb.lock.abc123-0'] = (b'', {})     # a live lock ticket
    store['mydbdeadbeefdead'] = (b'0', {})      # legacy probe orphan (unsweepable)
    _seed_db_object(store, 'mydb2')
    store['mydb2/1'] = (b'g1', {})

    writer = fake_s3.make_writer(store, 'mydb')
    writer.delete_remote()

    assert 'mydb' not in store
    assert 'mydb/1' not in store
    assert 'mydb/_metadata' not in store
    assert 'mydb2' in store, "delete_remote('mydb') destroyed sibling db 'mydb2'"
    assert 'mydb2/1' in store, "delete_remote('mydb') destroyed sibling db 'mydb2' children"
    assert 'mydb.lock.abc123-0' in store, 'delete_remote deleted lock tickets'
    assert 'mydbdeadbeefdead' in store
    assert writer.uuid is None


def test_flag_n_push_spares_siblings_and_locks(tmp_path):
    """F1 end-to-end: the automatic delete_remote inside a flag='n' push must
    not touch a sibling database or the lock namespace."""
    store = {}

    conn2 = fake_s3.FakeS3Connection(store, 'mydb2')
    with open_ebooklet(conn2, tmp_path / 'db2.blt', flag='n', num_groups=5) as eb:
        eb['other'] = b'precious'
        assert eb.changes().push() is True

    ## A (foreign) lock ticket for mydb, as if another contender crashed.
    store['mydb.lock.zzz999-0'] = (b'', {})

    conn = fake_s3.FakeS3Connection(store, 'mydb')
    with open_ebooklet(conn, tmp_path / 'db1.blt', flag='n', num_groups=5) as eb:
        eb['mine'] = b'value'
        assert eb.changes().push() is True

    assert 'mydb2' in store, "flag='n' push on 'mydb' wiped sibling 'mydb2'"
    assert 'mydb.lock.zzz999-0' in store, "flag='n' push deleted lock-namespace objects"

    fresh2 = fake_s3.FakeS3Connection(store, 'mydb2')
    with open_ebooklet(fresh2, tmp_path / 'db2-fresh.blt', flag='r') as eb:
        assert eb['other'] == b'precious'


def test_writable_probe_stays_inside_namespace():
    """F1 probe fold-in: the writability probe must only ever PUT inside the
    db_key + '/' namespace (a crashed probe at the old sibling-level key would
    be indistinguishable from a sibling database)."""
    store = {}
    writer = fake_s3.make_writer(store, 'mydb')
    assert writer.writable is True

    put_keys = writer._write_session.put_log
    assert put_keys, 'the probe should have PUT a test object'
    outside = [k for k in put_keys if not k.startswith('mydb/')]
    assert not outside, f'probe wrote outside the namespace: {outside}'
    ## And it cleaned up after itself.
    assert not [k for k in store if k.startswith('mydb/_writable_probe_')]

def test_delete_then_set_survives_push(tmp_path):
    """0.9.6: del then re-set of the same key in one session must push the new
    value, not silently lose the key (set() now discards it from the pending
    deletes; previously the delete pass removed the fresh index entry)."""
    store = {}
    num_groups = 5
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'local.blt', flag='n', num_groups=num_groups) as eb:
        for i in range(6):
            eb[f'key{i}'] = b'v%d' % i
        assert eb.changes().push() is True

    with open_ebooklet(conn, tmp_path / 'local.blt', flag='w') as eb:
        del eb['key2']
        eb['key2'] = b'NEWVAL'
        assert eb.changes().push() is True

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'key2' in eb, 'delete-then-set lost the key on push'
        assert eb['key2'] == b'NEWVAL'
        ## Same-group siblings must be unaffected by the repack.
        gid = utils.key_to_group_id('key2', num_groups)
        for i in (0, 1, 3, 4, 5):
            k = f'key{i}'
            if utils.key_to_group_id(k, num_groups) == gid:
                assert eb[k] == b'v%d' % i


def test_delete_then_set_timestamp_survives_push(tmp_path):
    """0.9.6, set_timestamp variant: del then re-set then a set_timestamp on the
    same key must also leave it out of the pending deletes."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'local.blt', flag='n', num_groups=5) as eb:
        eb['key1'] = b'v1'
        assert eb.changes().push() is True

    with open_ebooklet(conn, tmp_path / 'local.blt', flag='w') as eb:
        del eb['key1']
        eb.set('key1', b'NEW', timestamp=1_500_000_000_000_000)
        eb.sync()   # set_timestamp needs the new block flushed out of the write buffer
        eb.set_timestamp('key1', 1_600_000_000_000_000)
        assert eb.changes().push() is True

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'key1' in eb
        assert eb['key1'] == b'NEW'


def test_set_then_delete_still_deletes(tmp_path):
    """0.9.6 guard for the other direction: a set followed by a delete in the
    same session must still delete the key remotely."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'local.blt', flag='n', num_groups=5) as eb:
        eb['keep'] = b'keep'
        eb['gone'] = b'v1'
        assert eb.changes().push() is True

    with open_ebooklet(conn, tmp_path / 'local.blt', flag='w') as eb:
        eb['gone'] = b'v2'
        del eb['gone']
        assert eb.changes().push() is True

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert 'gone' not in eb
        assert eb['keep'] == b'keep'


def test_delete_then_set_of_never_pushed_key(tmp_path):
    """0.9.6 edge: del of a key that was never pushed (no remote index entry)
    followed by a re-set behaves as a plain set."""
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'local.blt', flag='n', num_groups=5) as eb:
        eb['newkey'] = b'v1'
        del eb['newkey']
        eb['newkey'] = b'v2'
        assert eb.changes().push() is True

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as eb:
        assert eb['newkey'] == b'v2'
