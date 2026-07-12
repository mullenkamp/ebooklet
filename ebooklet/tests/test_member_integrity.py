"""
Hermetic tests for the 0.9.5 deleted-member recovery change: a member ABSENT
from a group object that returned HTTP 200 (while the index claims it) now
routes through the 0.9.3 re-check protocol on read paths, instead of the old
silent inconsistency (`key in db` True, `db[key]` KeyError). The push path
keeps the deliberate lost-keys self-heal.

State manufacturing: the "index claims B but the group object lacks B" state
(pre-0.8.4 partial-push damage) is created by repacking the group object in
the fake store without B, leaving the db object (index) untouched.
"""
import pytest

from ebooklet import open_ebooklet, utils, RemoteIntegrityError
from ebooklet.tests import fake_s3


def _keys_same_group(num_groups, n):
    """Find n short keys that all hash into one group id."""
    seen = {}
    i = 0
    while True:
        k = f'm{i}'
        gid = utils.key_to_group_id(k, num_groups)
        seen.setdefault(gid, []).append(k)
        if len(seen[gid]) == n:
            return seen[gid], gid
        i += 1


def _repack_group_without(store, db_key, gid, drop_keys):
    """Rewrite the group object excluding drop_keys; the index still claims them."""
    gkey = f'{db_key}/{gid}'
    data, meta = store[gkey]
    entries = [e for e in utils.unpack_group(data) if e[0] not in drop_keys]
    packed, _offsets = utils.pack_group(entries)
    store[gkey] = (packed, meta)


def _reorder_group(store, db_key, gid):
    """Rewrite the group object with entries reversed (offsets shift, all members present)."""
    gkey = f'{db_key}/{gid}'
    data, meta = store[gkey]
    entries = list(reversed(utils.unpack_group(data)))
    packed, _offsets = utils.pack_group(entries)
    store[gkey] = (packed, meta)


def test_missing_member_is_loud_on_read(tmp_path):
    """Index claims B, 200 group object lacks B, fresh index still claims it
    -> RemoteIntegrityError (0.9.4: `in` said True while `get` silently
    returned nothing)."""
    store = {}
    (ka, kb), gid = _keys_same_group(5, 2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ka] = b'aaa'
        eb[kb] = b'bbb'
        assert eb.changes().push() is True

    _repack_group_without(store, 'testdb', gid, {kb})

    conn2 = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn2, tmp_path / 'r.blt', flag='r') as eb:
        assert kb in eb                       # the index claims it
        with pytest.raises(RemoteIntegrityError):
            eb[kb]
        assert eb[ka] == b'aaa'               # the present member is unaffected


def test_legit_member_deletion_resolves_to_clean_absence(tmp_path):
    """A stale reader whose index claims B reads it AFTER another writer
    legitimately deleted B and pushed: one index re-pull -> clean absence
    (0.9.4: silent absence with the stale index still claiming B)."""
    store = {}
    (ka, kb), _gid = _keys_same_group(5, 2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ka] = b'aaa'
        eb[kb] = b'bbb'
        assert eb.changes().push() is True

    ## Reader opens now - its index snapshot claims kb.
    conn_r = fake_s3.FakeS3Connection(store, 'testdb')
    reader = open_ebooklet(conn_r, tmp_path / 'r.blt', flag='r')
    try:
        assert kb in reader

        ## Another writer deletes kb and pushes (fresh local file).
        conn_w = fake_s3.FakeS3Connection(store, 'testdb')
        with open_ebooklet(conn_w, tmp_path / 'w2.blt', flag='w') as eb2:
            del eb2[kb]
            assert eb2.changes().push() is True

        assert reader.get(kb) is None         # re-check resolves to clean absence
        assert kb not in reader               # ...and the index was refreshed
        assert reader[ka] == b'aaa'
    finally:
        reader.close()


def test_push_self_heal_preserved(tmp_path):
    """Guard (same behavior pre/post 0.9.5): a push over the damaged state
    self-heals - the dangling member is dropped from the index and the group
    repacked - instead of failing loudly. This is the recovery mechanism for
    test_missing_member_is_loud_on_read's error."""
    store = {}
    (ka, kb, kc), gid = _keys_same_group(5, 3)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ka] = b'aaa'
        eb[kb] = b'bbb'
        assert eb.changes().push() is True

    _repack_group_without(store, 'testdb', gid, {kb})

    ## Fresh-local writer (kb has no local value) touches the damaged group.
    conn_w = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_w, tmp_path / 'w2.blt', flag='w') as eb2:
        eb2[kc] = b'ccc'
        result = eb2.changes().push()
        assert result is True, f'push self-heal failed loudly: {result}'

    conn_r = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_r, tmp_path / 'r.blt', flag='r') as eb:
        assert kb not in eb                   # dangling index entry dropped
        assert eb[ka] == b'aaa'
        assert eb[kc] == b'ccc'


def test_empty_grouped_value_survives_repack(tmp_path):
    """Regression (pre-existing bug found by the 0.9.5 design review): an
    empty-valued grouped member (index length 0) that is not locally
    materialized used to fall into the lost-keys drop when its group was
    repacked - silently deleting the key."""
    store = {}
    (ke, ka), _gid = _keys_same_group(5, 2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ke] = b''
        eb[ka] = b'aaa'
        assert eb.changes().push() is True

    ## Fresh-local writer (ke not materialized) repacks the group.
    conn_w = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_w, tmp_path / 'w2.blt', flag='w') as eb2:
        eb2[ka] = b'aaa2'
        assert eb2.changes().push() is True

    conn_r = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_r, tmp_path / 'r.blt', flag='r') as eb:
        assert ke in eb, 'empty-valued member was dropped by the repack'
        assert eb[ke] == b''
        assert eb[ka] == b'aaa2'


def test_empty_grouped_value_reads_back(tmp_path):
    """The read path serves empty grouped values correctly (fresh reader)."""
    store = {}
    (ke, ka), _gid = _keys_same_group(5, 2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ke] = b''
        eb[ka] = b'aaa'
        assert eb.changes().push() is True

    conn_r = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_r, tmp_path / 'r.blt', flag='r') as eb:
        assert eb[ke] == b''
        assert eb[ka] == b'aaa'


def test_full_recovery_returns_no_marker(tmp_path):
    """Offset-mismatch recovery that recovers EVERY requested member must not
    produce a marker (an empty truthy marker would trigger a needless index
    re-pull per read)."""
    store = {}
    (ka, kb), gid = _keys_same_group(5, 2)

    conn = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='n', num_groups=5) as eb:
        eb[ka] = b'aaa'
        eb[kb] = b'bbb'
        assert eb.changes().push() is True

    _reorder_group(store, 'testdb', gid)      # offsets shift; both members present

    conn_r = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_r, tmp_path / 'r.blt', flag='r') as eb:
        repulls = []
        orig = eb._pull_remote_index
        eb._pull_remote_index = lambda *a, **k: (repulls.append(1), orig(*a, **k))[1]

        assert eb[ka] == b'aaa'
        assert eb[kb] == b'bbb'
        assert not repulls, 'full recovery triggered a needless index re-pull'
