"""
Hermetic (credential-free) regression tests for the 0.9.5 exact-key delete
fixes. All S3 interaction goes through tests/fake_s3.py; the code under test
(remote.py sessions + the full open_ebooklet/push paths) is the real thing.

The three F1 regressions here FAIL on 0.9.4:
- emptying group '1' also destroyed groups '10' and '12' (prefix delete),
- delete_remote on 'mydb' also destroyed sibling 'mydb2' and the lock tickets,
- the writability probe wrote outside the db_key + '/' namespace.
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

    assert 'testdb/1' in store and 'testdb/10' in store and 'testdb/12' in store

    with open_ebooklet(conn, tmp_path / 'local.blt', flag='w') as eb:
        del eb[k1]
        assert eb.changes().push() is True

    assert 'testdb/1' not in store
    assert 'testdb/10' in store, 'prefix delete of group 1 destroyed group 10'
    assert 'testdb/12' in store, 'prefix delete of group 1 destroyed group 12'

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
