"""
ebooklet.mongo.refresh against REAL ebooklet pushes (commit + GC) into the fake S3, and a REAL
MongoDB (EBOOKLET_TEST_MONGO_URI; see mongo_support.py).

The refresh's contract: MongoDB's entries equal the remote's committed index after a refresh;
writes happen in the order claim (rev+1, applying) -> entries -> dataset document -> deletions ->
applied; a crash at any step converges on the next poll; a commit is "new" when its (uuid, ts)
DIFFERS from the applied one (stamps are wall-clock and may go backwards).
"""
import importlib
import io

import booklet
import pytest
import urllib3
from pymongo.errors import WriteError

from ebooklet import open_ebooklet, utils
from ebooklet.errors import RemoteMissingError, StaleIndexError, UnsupportedFormatError
from ebooklet.mongo import DATASETS, ENTRIES, MongoStore, ensure_schema, object_reader, refresh
from ebooklet.mongo import schema as schema_mod
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import TEST_GB
from ebooklet.tests.mongo_support import (  # noqa: F401  (mongo_client, mongo_db are fixtures)
    CountingSession,
    Crash,
    CrashAfter,
    HookConnection,
    mongo_client,
    mongo_db,
    recording_db,
)

refresh_mod = importlib.import_module('ebooklet.mongo.refresh')   # the module; `refresh` is the function
DB = 'db'
DS = 'ds1'
MODES = pytest.mark.parametrize('group_bytes', [TEST_GB, None], ids=['grouped', 'per_key'])


@pytest.fixture
def db(mongo_db):
    ensure_schema(mongo_db, 3600)
    return mongo_db


def _push(store, path, group_bytes=None, flag='w', set_items=None, delete=(), metadata=None):
    conn = fake_s3.FakeS3Connection(store, DB)
    kw = {'group_bytes': group_bytes} if flag == 'n' else {}
    with open_ebooklet(conn, path, flag=flag, **kw) as eb:
        for k, v in (set_items or {}).items():
            eb[k] = v
        for k in delete:
            del eb[k]
        if metadata is not None:
            eb.set_metadata(metadata)
        assert eb.changes().push()


def _seed(store, tmp_path, group_bytes, n=8):
    _push(store, tmp_path / 'seed.blt', group_bytes, flag='n',
          set_items={f'k{i}': f'value-{i}'.encode() * 4 for i in range(n)}, metadata={'dataset_type': 'grid', 'x': 1})


def _remote_entries(store):
    """{key: entry} parsed from the COMMITTED db object, in the refresh's entry shape."""
    data, meta = store[DB]
    manifest, _meta, index_bytes = utils.parse_db_payload(data)
    f = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
    try:
        out = {}
        for k, v in f.items():
            if k == utils.metadata_key_str or k in utils.reserved_key_strs:
                continue
            ts, gid, o, ln = utils.decode_index_entry(v)
            out[k] = {'ts': ts, 'g': gid, 'n': None if gid is None else manifest.get(gid), 'o': o, 'l': ln,
                      'u': meta['uuid']}
        return out
    finally:
        f.close()


def _mongo_entries(db):
    return {d.pop('k'): d for d in db[ENTRIES].find({'d': DS}, {'_id': 0, 'd': 0})}


def _conn(store, **hooks):
    return HookConnection(store, DB, **hooks)


def _doc(db):
    return db[DATASETS].find_one({'_id': DS})


@MODES
def test_initial_load_matches_the_remote_index_and_metadata(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied' and res['upserted'] == 8 and res['removed'] == 0
    assert _mongo_entries(db) == _remote_entries(store)
    doc = _doc(db)
    assert doc['applied'] == {'uuid': store[DB][1]['uuid'], 'ts': int(store[DB][1]['timestamp'])}
    assert doc['missing'] is False and doc['rev'] == 1 and doc['checked_at'] is not None
    with open_ebooklet(fake_s3.FakeS3Connection(store, DB), tmp_path / 'r.blt', flag='r') as reader:
        expected_meta = reader.get_metadata()
    assert MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store))).get_metadata() == expected_meta
    assert refresh(db, DS, _conn(store))['status'] == 'nochange'


@pytest.mark.parametrize('group_bytes', [4096, None], ids=['grouped', 'per_key'])   # 4096: all keys share a group
def test_push_then_refresh_applies_exactly_the_diff(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    refresh(db, DS, _conn(store))
    before = _mongo_entries(db)
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4, 'k9': b'new' * 4}, delete=('k5',))
    res = refresh(db, DS, _conn(store))
    after = _mongo_entries(db)
    assert after == _remote_entries(store)
    assert 'k5' not in after and 'k9' in after
    assert res['removed'] == 1
    changed = {k for k in after if before.get(k) != after[k]}
    assert res['upserted'] == len(changed)
    if group_bytes is not None:
        # a repack moves unchanged members to a new generation: same ts, new gen - and the
        # whole-entry diff rewrites them
        moved = [k for k in after if k in before and after[k]['ts'] == before[k]['ts'] and after[k] != before[k]]
        assert moved, 'expected a repack to move an unchanged member'
        assert all(after[k]['n'] != before[k]['n'] for k in moved)


def test_a_member_moved_by_generation_alone_is_rewritten(db, tmp_path):
    """
    A repack can change a member's generation and nothing else (same ts, offset, length): seen in
    real pushes, but only when the member happens to keep its position, which varies run to run. So
    the state is staged directly: one entry differs from the remote in its generation alone, and the
    applied pair is cleared so the next refresh diffs.
    """
    store = {}
    _seed(store, tmp_path, 4096)
    refresh(db, DS, _conn(store))
    db[ENTRIES].update_one({'_id': f'{DS}|k2'}, {'$set': {'n': '0000000000000'}})
    db[DATASETS].update_one({'_id': DS}, {'$set': {'applied': None}})
    stale = _mongo_entries(db)['k2']
    remote_k2 = _remote_entries(store)['k2']
    assert {f for f in stale if stale[f] != remote_k2[f]} == {'n'}
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied' and res['upserted'] == 1
    assert _mongo_entries(db) == _remote_entries(store)


def test_writes_happen_in_the_contract_order(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4}, delete=('k5',))
    rdb, rec, client = recording_db(db)
    try:
        assert refresh(rdb, DS, _conn(store))['status'] == 'applied'
    finally:
        client.close()
    writes = [c for c in rec.commands if c[0] in ('insert', 'update', 'delete', 'findAndModify')]
    kinds = [f'{name}:{coll}' for name, coll in writes]
    # 0 claim, 1 entries, 2 dataset document, 3 deletions, 4 applied
    assert kinds[0] == 'findAndModify:datasets', kinds
    meta = kinds.index('update:datasets')
    assert meta > 1 and set(kinds[1:meta]) == {'update:entries'}, kinds
    assert kinds[meta + 1:-1] == ['delete:entries'], kinds
    assert kinds[-1] == 'update:datasets' and len(kinds) - 1 > meta, kinds


@MODES
@pytest.mark.parametrize('crash_at', [('datasets', 'find_one_and_update'), ('entries', 'bulk_write'),
                                      ('datasets', 'update_one'), ('entries', 'delete_many')],
                         ids=['after-0-claim', 'after-1', 'after-2', 'after-3'])
def test_a_crash_after_any_step_converges_on_the_next_poll(db, tmp_path, group_bytes, crash_at):
    store = {}
    _seed(store, tmp_path, group_bytes)
    refresh(db, DS, _conn(store))
    applied_before = _doc(db)['applied']
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4}, delete=('k5',))
    with pytest.raises(Crash):
        refresh(CrashAfter(db, *crash_at), DS, _conn(store))
    assert _doc(db)['applied'] == applied_before, 'applied must only move in the final step'
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied'
    assert _mongo_entries(db) == _remote_entries(store)
    assert refresh(db, DS, _conn(store))['status'] == 'nochange'


def test_a_commit_between_the_head_and_the_get_is_applied_with_the_gets_stamp(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w1.blt', set_items={'k1': b'ONE' * 4})
    fired = []

    def commit_before_get(key):
        if key == DB and not fired:
            fired.append(1)
            _push(store, tmp_path / 'w2.blt', set_items={'k2': b'TWO' * 4})

    res = refresh(db, DS, _conn(store, on_get=commit_before_get))
    assert fired and res['status'] == 'applied'
    assert res['applied'] == (store[DB][1]['uuid'], int(store[DB][1]['timestamp']))
    assert _mongo_entries(db) == _remote_entries(store)
    assert refresh(db, DS, _conn(store))['status'] == 'nochange'


def test_a_backwards_stamped_commit_is_applied(db, tmp_path, monkeypatch):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    first = _doc(db)['applied']['ts']
    real = booklet.utils.make_timestamp_int
    monkeypatch.setattr(booklet.utils, 'make_timestamp_int', lambda *a, **k: real(*a, **k) - 3_600_000_000)
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'SKEWED' * 4}, delete=('k5',))
    monkeypatch.setattr(booklet.utils, 'make_timestamp_int', real)
    assert int(store[DB][1]['timestamp']) < first, 'the test needs a commit stamped before the applied one'
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied'
    assert _mongo_entries(db) == _remote_entries(store)


def test_no_change_poll_touches_only_checked_at(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    doc0, entries0 = _doc(db), _mongo_entries(db)
    assert refresh(db, DS, _conn(store))['status'] == 'nochange'
    doc1 = _doc(db)
    assert doc1['checked_at'] >= doc0['checked_at']
    assert {k: v for k, v in doc1.items() if k != 'checked_at'} == {k: v for k, v in doc0.items() if k != 'checked_at'}
    assert _mongo_entries(db) == entries0


def test_a_transient_head_404_sets_missing_and_the_next_poll_clears_it(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    once = []

    def head_404_once(key):
        if not once:
            once.append(1)
            return fake_s3.FakeResp(404)

    assert refresh(db, DS, _conn(store, on_head=head_404_once))['status'] == 'missing'
    with pytest.raises(RemoteMissingError):
        MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))
    assert refresh(db, DS, _conn(store))['status'] == 'nochange'
    assert _doc(db)['missing'] is False
    MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))


def test_a_deleted_remote_is_marked_missing(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    fake_s3.FakeS3Connection(store, DB).open('w').delete_remote()
    assert refresh(db, DS, _conn(store))['status'] == 'missing'
    assert _doc(db)['missing'] is True


def test_a_recreated_remote_replaces_every_entry_with_the_new_uuid(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    old_uuid = _doc(db)['applied']['uuid']
    fake_s3.FakeS3Connection(store, DB).open('w').delete_remote()
    _push(store, tmp_path / 'new.blt', TEST_GB, flag='n', set_items={'k0': b'fresh' * 4, 'k1': b'fresh1' * 4})
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied' and res['applied'][0] != old_uuid
    entries = _mongo_entries(db)
    assert entries == _remote_entries(store)
    assert {e['u'] for e in entries.values()} == {res['applied'][0]}


@pytest.mark.parametrize('meta_change', [{'format_version': '4'}, {'format_version': '2', 'num_groups': '16'}],
                         ids=['too-new', 'legacy'])
def test_an_unreadable_format_raises_and_does_not_advance_checked_at(db, tmp_path, meta_change):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    checked = _doc(db)['checked_at']
    data, meta = store[DB]
    store[DB] = (data, {**meta, **meta_change, 'timestamp': str(int(meta['timestamp']) + 1)})
    with pytest.raises(UnsupportedFormatError):
        refresh(db, DS, _conn(store))
    assert _doc(db)['checked_at'] == checked


def test_a_failed_get_raises_and_does_not_advance_checked_at(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    checked = _doc(db)['checked_at']
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    with pytest.raises(urllib3.exceptions.HTTPError):
        refresh(db, DS, _conn(store, on_get=lambda key: fake_s3.FakeResp(500, error={'Code': 'InternalError'})))
    assert _doc(db)['checked_at'] == checked


def test_an_interleaved_refresh_trips_the_rev_check_and_the_next_poll_redoes_it(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w1.blt', set_items={'k1': b'ONE' * 4})

    class Interleave:
        """Runs a second, complete refresh (of a newer commit) right after this one's step 2."""

        def __init__(self, inner):
            self.inner = inner
            self.done = False

        def __getitem__(self, name):
            coll = self.inner[name]
            if name != DATASETS:
                return coll
            outer = self

            class Coll:
                def __getattr__(self, attr):
                    fn = getattr(coll, attr)
                    if attr != 'find_one_and_update' or outer.done:
                        return fn

                    def wrapped(*a, **k):
                        out = fn(*a, **k)
                        outer.done = True
                        _push(store, tmp_path / 'w2.blt', set_items={'k2': b'TWO' * 4}, delete=('k6',))
                        assert refresh(outer.inner, DS, _conn(store))['status'] == 'applied'
                        return out

                    return wrapped

            return Coll()

    res = refresh(Interleave(db), DS, _conn(store))
    assert res['status'] == 'interleaved'
    assert _doc(db)['applied'] is None
    assert refresh(db, DS, _conn(store))['status'] == 'applied'
    assert _mongo_entries(db) == _remote_entries(store)


def test_an_unmanifested_group_is_stored_without_a_generation(db, tmp_path, caplog):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    data, meta = store[DB]
    manifest, meta_section, index_bytes = utils.parse_db_payload(data)
    dropped = sorted(manifest)[0]
    bad = {g: gen for g, gen in manifest.items() if g != dropped}
    store[DB] = (utils.build_db_payload(bad, meta_section, bytes(index_bytes)), meta)
    res = refresh(db, DS, _conn(store))
    assert res['status'] == 'applied' and res['unmanifested'] > 0
    assert any(e['n'] is None and e['g'] == dropped for e in _mongo_entries(db).values())
    assert 'manifest does not list' in caplog.text


def test_dataset_ids_with_a_separator_are_refused(db, tmp_path):
    with pytest.raises(ValueError, match='without'):
        refresh(db, 'a|b', _conn({}))


def test_ensure_schema_is_idempotent_and_changes_the_ttl(db):
    ensure_schema(db, 3600)
    assert db['chunks'].index_information()['t_ttl']['expireAfterSeconds'] == 3600
    ensure_schema(db, 60)
    assert db['chunks'].index_information()['t_ttl']['expireAfterSeconds'] == 60


# ── code review edr-mongo-code-1: the claim, and paths no test reached ─────────────────────────────

def test_the_claim_sets_applying_and_a_crash_leaves_it_for_the_next_poll_to_clear(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    rev0 = _doc(db)['rev']
    with pytest.raises(Crash):
        refresh(CrashAfter(db, DATASETS, 'find_one_and_update'), DS, _conn(store))   # crash right after the claim
    doc = _doc(db)
    assert doc['applying'] is True and doc['rev'] == rev0 + 1
    with pytest.raises(StaleIndexError):
        MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))
    assert refresh(db, DS, _conn(store))['status'] == 'applied'
    assert _doc(db)['applying'] is False and _mongo_entries(db) == _remote_entries(store)


def test_a_left_over_applying_forces_a_redo_even_when_the_pair_is_unchanged(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    db[DATASETS].update_one({'_id': DS}, {'$set': {'applying': True}})
    assert refresh(db, DS, _conn(store))['status'] == 'applied'
    assert _doc(db)['applying'] is False


def test_a_stale_get_applies_nothing_and_does_not_advance_checked_at(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    doc0 = _doc(db)
    applied_db_object = store[DB]
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    stale = lambda key: fake_s3.FakeResp(200, applied_db_object[0], dict(applied_db_object[1])) if key == DB else None  # noqa: E731
    res = refresh(db, DS, _conn(store, on_get=stale))
    assert res['status'] == 'stale_get'
    doc1 = _doc(db)
    assert doc1['checked_at'] == doc0['checked_at'] and doc1['applied'] == doc0['applied'] and doc1['rev'] == doc0['rev']


def test_a_get_404_after_a_head_200_is_missing(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    res = refresh(db, DS, _conn(store, on_get=lambda key: fake_s3.FakeResp(404) if key == DB else None))
    assert res['status'] == 'missing' and _doc(db)['missing'] is True


def test_a_legacy_get_is_refused_even_when_the_head_looks_current(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    data, meta = store[DB]
    legacy = lambda key: fake_s3.FakeResp(200, data, {**meta, 'format_version': '2', 'num_groups': '8'}) if key == DB else None  # noqa: E731
    with pytest.raises(UnsupportedFormatError):
        refresh(db, DS, _conn(store, on_get=legacy))


def test_missing_then_recreated_clears_missing(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    fake_s3.FakeS3Connection(store, DB).open('w').delete_remote()
    assert refresh(db, DS, _conn(store))['status'] == 'missing'
    _push(store, tmp_path / 'again.blt', TEST_GB, flag='n', set_items={'k0': b'back' * 4})
    assert refresh(db, DS, _conn(store))['status'] == 'applied'
    assert _doc(db)['missing'] is False and _mongo_entries(db) == _remote_entries(store)


def test_a_first_ever_refresh_of_a_missing_remote_writes_a_valid_document(db):
    res = refresh(db, DS, _conn({}))
    assert res['status'] == 'missing'
    doc = _doc(db)
    assert doc['missing'] is True and doc['rev'] == 0 and doc['applied'] is None


def test_ensure_schema_re_applies_validators_to_an_existing_collection_and_indexes_d(mongo_db):
    mongo_db.create_collection('chunks')
    mongo_db['chunks'].insert_one({'_id': 'before', 'v': 1})          # no validator yet: accepted
    ensure_schema(mongo_db, 3600)
    with pytest.raises(WriteError):
        mongo_db['chunks'].insert_one({'_id': 'after', 'v': 1})
    assert 'd_1' in mongo_db[ENTRIES].index_information()
    with pytest.raises(ValueError):
        ensure_schema(mongo_db, True)


def test_the_ebooklet_type_is_recorded_and_exposed(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    refresh(db, DS, _conn(store))
    assert _doc(db)['ebooklet_type'] == 'EVariableLengthValue'
    data, meta = store[DB]
    store[DB] = (data, {**meta, 'type': 'RemoteConnGroup', 'timestamp': str(int(meta['timestamp']) + 1)})
    assert refresh(db, DS, _conn(store))['status'] == 'applied'
    assert _doc(db)['ebooklet_type'] == 'RemoteConnGroup'
    st = MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))
    assert st.ebooklet_type == 'RemoteConnGroup'


def test_ensure_schema_sets_the_block_compressors(db):
    assert {name: schema_mod.block_compressor(db, name) for name in schema_mod.COMPRESSORS} == \
        {'datasets': 'zstd', 'entries': 'zstd', 'chunks': 'none'}


def test_ensure_schema_warns_about_an_existing_collection_with_another_compressor(mongo_db, caplog):
    mongo_db.create_collection('entries', storageEngine={'wiredTiger': {'configString': 'block_compressor=snappy'}})
    ensure_schema(mongo_db, 3600)
    assert schema_mod.block_compressor(mongo_db, 'entries') == 'snappy'      # unchanged: fixed at creation
    assert "entries uses block compressor 'snappy', not 'zstd'" in caplog.text
    assert 'datasets uses block compressor' not in caplog.text


def test_writes_and_lookups_cross_batch_boundaries(db, tmp_path, monkeypatch):
    monkeypatch.setattr(refresh_mod, 'WRITE_BATCH', 3)
    monkeypatch.setattr(schema_mod, 'IN_BATCH', 2)
    store = {}
    items = {f'k{i:02d}': f'value-{i}'.encode() * 4 for i in range(23)}
    _push(store, tmp_path / 'seed.blt', TEST_GB, flag='n', set_items=items)
    assert refresh(db, DS, _conn(store))['upserted'] == 23
    _push(store, tmp_path / 'w.blt', set_items={'k00': b'CHANGED' * 4}, delete=('k01', 'k02', 'k03', 'k04', 'k05'))
    res = refresh(db, DS, _conn(store))
    assert res['removed'] == 5 and _mongo_entries(db) == _remote_entries(store)
    st = MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))
    keys = sorted(_remote_entries(store))
    st.load_items(keys)
    with open_ebooklet(fake_s3.FakeS3Connection(store, DB), tmp_path / 'r.blt', flag='r') as reader:
        assert {k: st.get(k) for k in keys} == {k: reader[k] for k in keys}
