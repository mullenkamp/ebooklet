"""
Writer-order invariants that readers outside ebooklet's own session depend on.

ebooklet.mongo's store classifies a chunk 404 as an integrity fault only when the remote has NOT
committed since its index copy. That is exact only if writers commit BEFORE they delete anything
the previous commit referenced: grouped GC, per-key deletes, and delete_remote's teardown (which
must delete the db object first). Plan review `edr-mongo-plan-1` found the per-key and
delete_remote orders unpinned by the existing suite (mutants of both passed all of it), so these
tests pin them.

Each test probes at the commit point of a REAL push: just before the db object is PUT, every
object the previous commit referenced must still exist.
"""
import io

import booklet
import pytest

from ebooklet import open_ebooklet, utils
from ebooklet.tests.groups import TEST_GB
from ebooklet.tests.mongo_support import RecordingConnection

DB = 'db'


def _referenced_objects(store):
    """Object keys the COMMITTED db object references (group generations, or per-key objects)."""
    manifest, _meta, index_bytes = utils.parse_db_payload(store[DB][0])
    f = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
    try:
        out = set()
        for key, entry in f.items():
            if key in utils.reserved_key_strs or key == utils.metadata_key_str:
                continue
            gid = utils.decode_index_entry(entry)[1]
            out.add(f'{DB}/{key}' if gid is None else f'{DB}/{utils.group_obj_key(gid, manifest[gid])}')
        return out
    finally:
        f.close()


def _seed(conn, tmp_path, group_bytes):
    with open_ebooklet(conn, tmp_path / 'seed.blt', flag='n', group_bytes=group_bytes) as eb:
        for i in range(8):
            eb[f'k{i}'] = f'value-{i}'.encode() * 4
        assert eb.changes().push()


@pytest.mark.parametrize('group_bytes', [TEST_GB, None], ids=['grouped', 'per_key'])
def test_nothing_the_last_commit_references_is_deleted_before_the_next_commit(tmp_path, group_bytes):
    store = {}
    conn = RecordingConnection(store, DB)
    _seed(conn, tmp_path, group_bytes)
    referenced = _referenced_objects(store)
    assert referenced

    missing_at_commit = []

    def probe(key):
        if key == DB:
            missing_at_commit.extend(sorted(k for k in referenced if k not in store))

    conn.ops.clear()
    conn.before_put = probe
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb['k1'] = b'CHANGED' * 4
        del eb['k5']
        assert eb.changes().push()

    assert ('PUT', DB) in conn.ops, 'the push did not commit'
    assert missing_at_commit == [], f'deleted before the commit PUT: {missing_at_commit}'
    commit_at = conn.ops.index(('PUT', DB))
    # Only objects the previous commit referenced matter (the writability probe's own scratch
    # object is PUT and deleted before the commit by design).
    early = [op for op in conn.ops[:commit_at] if op[0] == 'DELETE' and op[1] in referenced]
    assert early == [], f'referenced objects deleted before the commit PUT: {early}'
    # the push did delete something it referenced afterwards (GC of the replaced generation, or
    # the per-key delete), so the test exercises the order rather than an empty case
    assert any(op[0] == 'DELETE' and op[1] in referenced for op in conn.ops[commit_at:])


@pytest.mark.parametrize('group_bytes', [TEST_GB, None], ids=['grouped', 'per_key'])
def test_delete_remote_deletes_the_db_object_first(tmp_path, group_bytes):
    store = {}
    conn = RecordingConnection(store, DB)
    _seed(conn, tmp_path, group_bytes)
    session = conn.open('w')
    before = set(store)
    conn.ops.clear()
    session.delete_remote()
    # objects that existed before the call (the lazy writability probe's scratch object does not)
    deletes = [key for op, key in conn.ops if op == 'DELETE' and key in before]
    assert deletes, 'delete_remote deleted nothing'
    assert deletes[0] == DB, f'the first object deleted was {deletes[0]!r}, not the db object'
    assert not any(k == DB or k.startswith(DB + '/') for k in store)
