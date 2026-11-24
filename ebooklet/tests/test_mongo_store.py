"""
ebooklet.mongo.MongoStore against REAL ebooklet pushes (commit + GC) into the fake S3, ebooklet's
real fetchers, and a REAL MongoDB (EBOOKLET_TEST_MONGO_URI; see mongo_support.py).

The store's contract: it returns exactly the bytes an ebooklet reader returns; it never returns
None for a failed fetch; a key with no entry is None only if no refresh changed the dataset during
the request (else StaleIndexError); a chunk 404 is classified against the applied pair read at
construction - remote unchanged -> RemoteIntegrityError, remote committed since -> StaleIndexError.
"""
import datetime
import importlib
import subprocess
import sys
import threading
import time

import pytest
import s3func
import urllib3
from bson.binary import Binary
from pymongo.errors import PyMongoError, WriteError

from ebooklet import open_ebooklet, utils
from ebooklet.errors import ReadOnlyError, RemoteIntegrityError, RemoteMissingError, StaleIndexError
from ebooklet.mongo import CHUNKS, DATASETS, ENTRIES, MongoStore, ensure_schema, object_reader, refresh
from ebooklet.mongo import schema as schema_mod
from ebooklet.mongo.schema import chunk_id, entry_id
from ebooklet.tests import fake_s3
from ebooklet.tests.groups import TEST_GB
from ebooklet.tests.mongo_support import (  # noqa: F401  (mongo_client, mongo_db are fixtures)
    CountingSession,
    Crash,
    CrashAfter,
    HookConnection,
    RecordingConnection,
    mongo_client,
    mongo_db,
    recording_db,
)

refresh_mod = importlib.import_module('ebooklet.mongo.refresh')   # the module; `refresh` is the function
DB = 'db'
DS = 'ds1'
MODES = pytest.mark.parametrize('group_bytes', [TEST_GB, None], ids=['grouped', 'per_key'])
VALUES = {f'k{i}': f'value-{i}'.encode() * 4 for i in range(8)}


@pytest.fixture
def db(mongo_db):
    ensure_schema(mongo_db, 3600)
    return mongo_db


def _push(store, path, group_bytes=None, flag='w', set_items=None, delete=()):
    conn = fake_s3.FakeS3Connection(store, DB)
    kw = {'group_bytes': group_bytes} if flag == 'n' else {}
    with open_ebooklet(conn, path, flag=flag, **kw) as eb:
        for k, v in (set_items or {}).items():
            eb[k] = v
        for k in delete:
            del eb[k]
        assert eb.changes().push()


def _seed(store, tmp_path, group_bytes, items=VALUES):
    _push(store, tmp_path / 'seed.blt', group_bytes, flag='n', set_items=items)


def _refresh(db, store):
    res = refresh(db, DS, HookConnection(store, DB))
    assert res['status'] in ('applied', 'nochange'), res
    return res


def _mstore(db, store, **hooks):
    session = CountingSession(store, **hooks)
    return MongoStore(db, DS, object_reader(DB, http_session=session)), session


def _reader_values(store, tmp_path, name='r.blt'):
    with open_ebooklet(fake_s3.FakeS3Connection(store, DB), tmp_path / name, flag='r') as eb:
        return {k: eb[k] for k in eb.keys()}


@MODES
def test_bytes_equal_an_ebooklet_reader_cold_and_warm_with_no_remote_calls_when_warm(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    expected = _reader_values(store, tmp_path)
    cold, s_cold = _mstore(db, store)
    assert s_cold.calls == {}, 'construction must make no remote call'
    cold.load_items(expected)
    assert {k: cold.get(k) for k in expected} == expected
    assert s_cold.calls['GET'] > 0 and s_cold.calls['HEAD'] == 0
    warm, s_warm = _mstore(db, store)
    warm.load_items(iter(list(expected)))            # cfdb passes a generator
    assert {k: warm.get(k) for k in expected} == expected
    assert s_warm.calls == {}, f'warm reads must make no remote call, made {dict(s_warm.calls)}'


def test_get_serves_from_the_memo_after_load_items(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    rdb, rec, client = recording_db(db)
    try:
        st = MongoStore(rdb, DS, object_reader(DB, http_session=CountingSession(store)))
        st.load_items(VALUES)
        n = len(rec.commands)
        for k in VALUES:
            st.get(k)
        assert len(rec.commands) == n, 'get() after load_items() must not query MongoDB'
    finally:
        client.close()


@MODES
def test_a_genuinely_absent_key_is_none(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    assert st.get('nope') is None
    assert st.get('nope', b'default') == b'default'


@MODES
def test_a_deleting_refresh_during_a_request_raises_instead_of_returning_fill(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    st, _ = _mstore(db, store)                       # built on the old dataset document
    _push(store, tmp_path / 'w.blt', delete=('k3', 'k4'))
    _refresh(db, store)                              # deletes k3/k4 entries, bumps rev
    with pytest.raises(StaleIndexError):
        st.load_items(['k3', 'k4'])


@MODES
def test_an_appending_refresh_during_a_request_reads_correctly(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    _push(store, tmp_path / 'w.blt', set_items={'k20': b'appended' * 4})
    _refresh(db, store)
    st.load_items(['k0', 'k1'])
    assert st.get('k0') == VALUES['k0'] and st.get('k1') == VALUES['k1']


def test_a_real_commit_with_gc_then_an_uncached_read_is_stale_and_heals_after_a_refresh(db, tmp_path):
    store = {}
    _seed(store, tmp_path, 4096)                     # one group holds every key
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})   # repack + GC of the old generation
    st, _ = _mstore(db, store)
    with pytest.raises(StaleIndexError):
        st.get('k0')
    _refresh(db, store)
    st2, _ = _mstore(db, store)
    assert st2.get('k0') == VALUES['k0'] and st2.get('k1') == b'CHANGED' * 4


def test_a_per_key_delete_then_an_uncached_read_is_stale(db, tmp_path):
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', delete=('k3',))
    st, _ = _mstore(db, store)
    with pytest.raises(StaleIndexError):
        st.get('k3')


def test_a_refresh_landing_during_the_classification_still_classifies_as_stale(db, tmp_path):
    """The pair compared is the one read at construction, not a fresh read (false-500 trap)."""
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', delete=('k3',))
    ran = []

    def refresh_before_head(key):
        if not ran:
            ran.append(1)
            refresh(db, DS, HookConnection(store, DB))

    st, _ = _mstore(db, store, on_head=refresh_before_head)
    with pytest.raises(StaleIndexError):
        st.get('k3')
    assert ran, 'the classification HEAD did not run'


@MODES
def test_one_injected_404_heals_by_refetching(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    once = []

    def first_get_404(key):
        if not once:
            once.append(key)
            return fake_s3.FakeResp(404)

    st, session = _mstore(db, store, on_get=first_get_404)
    assert st.get('k2') == VALUES['k2']
    assert once and session.calls['HEAD'] == 0


@MODES
def test_a_missing_object_with_a_current_mirror_is_an_integrity_fault(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    entry = db[ENTRIES].find_one({'_id': entry_id(DS, 'k2')})
    obj = f'{DB}/k2' if entry['g'] is None else f'{DB}/{utils.group_obj_key(entry["g"], entry["n"])}'
    del store[obj]                                   # out-of-band loss, no commit
    st, _ = _mstore(db, store)
    with pytest.raises(RemoteIntegrityError):
        st.get('k2')


def test_an_unmanifested_group_is_an_integrity_fault(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    db[ENTRIES].update_one({'_id': entry_id(DS, 'k2')}, {'$set': {'n': None}})
    st, session = _mstore(db, store)
    with pytest.raises(RemoteIntegrityError):
        st.get('k2')
    assert session.calls['GET'] == 0, 'there is no object to fetch for an unmanifested group'


def test_an_entry_gone_on_re_read_after_a_404_is_stale(db, tmp_path):
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', delete=('k3',))
    once = []

    def refresh_on_first_404(key):
        if key.endswith('/k3') and not once:
            once.append(1)
            resp = fake_s3.FakeS3Session(store).get_object(key)
            refresh(db, DS, HookConnection(store, DB))     # the entry disappears before the re-read
            return resp

    st, _ = _mstore(db, store, on_get=refresh_on_first_404)
    with pytest.raises(StaleIndexError):
        st.get('k3')


def test_a_remote_deleted_after_the_refresh_is_missing(db, tmp_path):
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    fake_s3.FakeS3Connection(store, DB).open('w').delete_remote()
    st, _ = _mstore(db, store)
    with pytest.raises(RemoteMissingError):
        st.get('k1')


def test_a_per_key_overwrite_is_stale_and_cached_under_the_objects_own_timestamp(db, tmp_path):
    """An object overwritten since the index was copied does not belong to this request's commit."""
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    entry_ts = db[ENTRIES].find_one({'_id': entry_id(DS, 'k1')})['ts']
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'OVERWRITTEN' * 2})   # not refreshed
    object_ts = int(store[f'{DB}/k1'][1]['timestamp'])
    assert object_ts != entry_ts
    st, _ = _mstore(db, store)
    with pytest.raises(StaleIndexError, match='overwritten'):
        st.get('k1')
    u = db[DATASETS].find_one({'_id': DS})['applied']['uuid']
    assert db[CHUNKS].find_one({'_id': chunk_id(DS, u, 'k1', object_ts)}) is not None
    assert db[CHUNKS].find_one({'_id': chunk_id(DS, u, 'k1', entry_ts)}) is None
    _refresh(db, store)
    fresh, session = _mstore(db, store)
    assert fresh.get('k1') == b'OVERWRITTEN' * 2
    assert session.calls == {}, 'after the refresh the cached bytes are hit under their own label'


def test_a_per_key_overwrite_mid_push_is_stale_not_mixed(db, tmp_path):
    """Fable's case: during a push's uploads, before its commit, a partly warm request must not mix."""
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    warm, _ = _mstore(db, store)
    assert warm.get('k0') == VALUES['k0']            # k0 cached, k1 not
    seen = {}

    def read_at_commit(key):
        if key == DB and not seen:
            st, _ = _mstore(db, store)
            try:
                seen['k0'] = st.get('k0')
                seen['k1'] = st.get('k1')
            except StaleIndexError as err:
                seen['error'] = err

    conn = RecordingConnection(store, DB)
    conn.before_put = read_at_commit
    with open_ebooklet(conn, tmp_path / 'w.blt', flag='w') as eb:
        eb['k0'] = b'NEW-0' * 4
        eb['k1'] = b'NEW-1' * 4
        assert eb.changes().push()
    assert 'error' in seen, f'a mixed response was served: {seen}'


@MODES
def test_a_recreated_remote_never_serves_the_old_remotes_cached_bytes(db, tmp_path, group_bytes):
    """Same key and SAME timestamp, different bytes, new uuid: only the uuid in the chunk id tells them apart."""
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    first, _ = _mstore(db, store)
    assert first.get('k1') == VALUES['k1']          # now cached under the first remote's uuid
    ts = db[ENTRIES].find_one({'_id': entry_id(DS, 'k1')})['ts']
    fake_s3.FakeS3Connection(store, DB).open('w').delete_remote()
    conn = fake_s3.FakeS3Connection(store, DB)
    kw = {'group_bytes': group_bytes}
    with open_ebooklet(conn, tmp_path / 'recreated.blt', flag='n', **kw) as eb:
        eb.set('k1', b'RECREATED' * 3, timestamp=ts)
        assert eb.changes().push()
    _refresh(db, store)
    assert db[ENTRIES].find_one({'_id': entry_id(DS, 'k1')})['ts'] == ts, 'the test needs the same timestamp'
    again, _ = _mstore(db, store)
    assert again.get('k1') == b'RECREATED' * 3


def test_a_failed_fetch_raises_and_never_returns_none(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store, on_get=lambda key: fake_s3.FakeResp(500, error={'Code': 'InternalError'}))
    with pytest.raises(urllib3.exceptions.HTTPError):
        st.get('k1')


def test_a_per_key_object_without_a_timestamp_raises(db, tmp_path):
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    data, _meta = store[f'{DB}/k1']
    store[f'{DB}/k1'] = (data, {})
    st, _ = _mstore(db, store)
    with pytest.raises(KeyError):
        st.get('k1')


def test_concurrent_fills_of_one_chunk_leave_one_document(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    a, _ = _mstore(db, store)
    b, _ = _mstore(db, store)
    a._fill(['k1'])                                  # both miss, both insert (the second is a duplicate)
    b._fetch_and_heal(b._read_entries(['k1']))
    assert a.get('k1') == b.get('k1') == VALUES['k1']
    assert db[CHUNKS].count_documents({'_id': {'$regex': r'\|k1\|'}}) == 1


def test_an_oversized_chunk_is_served_uncached(db, tmp_path, monkeypatch):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    monkeypatch.setattr(schema_mod, 'MONGO_DOC_LIMIT', 4)
    st, _ = _mstore(db, store)
    assert st.get('k1') == VALUES['k1']
    assert db[CHUNKS].count_documents({}) == 0


def test_a_cache_insert_failure_still_serves_the_bytes(db, tmp_path, monkeypatch, caplog):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store)

    class Broken:
        def __init__(self, coll):
            self.coll = coll

        def __getattr__(self, name):
            if name == 'insert_many':
                def fail(*a, **k):
                    raise PyMongoError('boom')
                return fail
            return getattr(self.coll, name)

    real_db = st._db

    class DbProxy:
        def __getitem__(self, name):
            return Broken(real_db[name]) if name == CHUNKS else real_db[name]

    st._db = DbProxy()
    assert st.get('k1') == VALUES['k1']
    assert 'served anyway' in caplog.text


def test_a_gid_with_two_generations_mid_apply_fetches_each_from_its_own_object(db, tmp_path):
    store = {}
    _seed(store, tmp_path, 4096)
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    # a refresh interrupted part-way through step 1: k1's entry is new, k0's still names the GC'd generation
    new = refresh.__globals__['_parse_entries']
    manifest, _m, index_bytes = utils.parse_db_payload(store[DB][0])
    entries, _ = new(index_bytes, manifest, store[DB][1]['uuid'])
    db[ENTRIES].replace_one({'_id': entry_id(DS, 'k1')}, {'d': DS, 'k': 'k1', **entries['k1']})
    st, _ = _mstore(db, store)
    assert st.get('k1') == b'CHANGED' * 4
    with pytest.raises(StaleIndexError):
        st.get('k0')


def test_an_empty_fetch_is_a_no_op(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, session = _mstore(db, store)
    assert st._fetch({}) == ({}, {})
    assert session.calls == {}


def test_read_only_operations_give_clear_errors(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    assert st.writable is False
    with pytest.raises(NotImplementedError, match='map'):
        st.map(lambda k, v: v, keys=['k1'])
    with pytest.raises(ReadOnlyError):
        st.prune()
    with pytest.raises(NotImplementedError, match='copy'):
        st._n_buckets  # noqa: B018
    with pytest.raises(NotImplementedError):
        st.load_items()
    st.sync()
    st.close()


def test_an_unknown_dataset_is_missing(db):
    with pytest.raises(RemoteMissingError):
        MongoStore(db, 'nope', object_reader(DB, http_session=CountingSession({})))


def test_a_cache_insert_with_one_duplicate_still_caches_every_other_chunk(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    a, _ = _mstore(db, store)
    a.load_items(['k1'])                                 # k1 is cached first, by another request
    assert db[CHUNKS].count_documents({}) == 1
    b, _ = _mstore(db, store)
    b._fetch_and_heal(b._read_entries(list(VALUES)))     # b's insert batch includes the duplicate k1
    assert db[CHUNKS].count_documents({}) == len(VALUES), 'a duplicate must not stop the rest of the batch'


# ── plan review edr-worker-plan-2: a dataset document deleted and recreated (ABA on rev) ───────────

def _drop_dataset(db):
    db[DATASETS].delete_one({'_id': DS})
    db[ENTRIES].delete_many({'d': DS})


@MODES
def test_a_recreated_dataset_document_mid_re_mirror_is_stale_not_absent(db, tmp_path, group_bytes):
    store = {}
    _seed(store, tmp_path, group_bytes)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    st.load_items(['k0'])
    _drop_dataset(db)
    with pytest.raises(Crash):                           # the re-mirror stops right after its claim
        refresh(CrashAfter(db, DATASETS, 'find_one_and_update'), DS, HookConnection(store, DB))
    assert db[DATASETS].find_one({'_id': DS})['rev'] == st._rev, 'premise: rev restarted at the old value'
    with pytest.raises(StaleIndexError):
        st.get('k1')                                     # no entry yet: must not read as absent (fill)
    with pytest.raises(StaleIndexError):
        st.verify()


def test_a_recreated_dataset_document_after_a_full_re_mirror_fails_verify(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    _drop_dataset(db)
    _refresh(db, store)
    assert db[DATASETS].find_one({'_id': DS})['rev'] == st._rev, 'premise: rev restarted at the old value'
    with pytest.raises(StaleIndexError):
        st.verify()


def test_a_document_recreated_through_a_missing_remote_is_detected(db, tmp_path):
    """Both incarnations begin in _mark_missing, so both get their `inc` there (code review edr-worker-code-2)."""
    store = {}
    assert refresh(db, DS, HookConnection({}, DB))['status'] == 'missing'  # first seen while missing
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store)
    _drop_dataset(db)
    assert refresh(db, DS, HookConnection({}, DB))['status'] == 'missing'  # recreated while missing
    _refresh(db, store)
    doc = db[DATASETS].find_one({'_id': DS})
    assert doc['rev'] == st._rev, 'premise: rev restarted at the old value'
    assert doc['inc'] is not None and doc['inc'] != st._inc
    with pytest.raises(StaleIndexError):
        st.verify()


def test_a_document_written_by_0_13_0_still_serves_and_detects_a_recreation(db, tmp_path):
    """A 0.13.0 document has no `inc`: it reads as before, and a recreated one is still detected."""
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    db[DATASETS].update_one({'_id': DS}, {'$unset': {'inc': ''}})  # as 0.13.0 wrote it
    st, _ = _mstore(db, store)
    assert st.get('k1') == VALUES['k1'] and st.get('nope') is None
    st.verify()
    _drop_dataset(db)
    _refresh(db, store)  # recreated by 0.13.1, so it now has an inc
    with pytest.raises(StaleIndexError):
        st.verify()


def test_the_validators_take_large_integers_and_require_a_date(db):
    db[ENTRIES].insert_one({'_id': 'x|k', 'd': 'x', 'k': 'k', 'ts': 2**52, 'g': 4_000_000_000,
                            'n': 'abc', 'o': 3_000_000_000, 'l': 2**32 - 1, 'u': 'u'})
    with pytest.raises(WriteError):
        db[CHUNKS].insert_one({'_id': 'x|u|k|1', 'v': Binary(b'v'), 'n': 1, 't': 1700000000})
    db[CHUNKS].insert_one({'_id': 'x|u|k|2', 'v': Binary(b'v'), 'n': 1,
                           't': datetime.datetime.now(datetime.timezone.utc)})


def test_expired_chunks_are_removed_by_the_ttl_monitor(mongo_db, mongo_client):
    """
    ~1-2 s: waits for MongoDB's TTL monitor (its period set to 1 s for the test). A fresh database
    and ONE ensure_schema call, so the TTL index under test is the one created (not repaired by a
    second call's collMod - review edr-worker-plan-2).
    """
    db = mongo_db
    admin = mongo_client.admin
    old = admin.command('getParameter', 1, ttlMonitorSleepSecs=1)['ttlMonitorSleepSecs']
    admin.command('setParameter', 1, ttlMonitorSleepSecs=1)
    try:
        ensure_schema(db, 60)
        assert db[CHUNKS].index_information()[schema_mod.TTL_INDEX]['expireAfterSeconds'] == 60
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        db[CHUNKS].insert_one({'_id': 'x|u|k|1', 'v': Binary(b'v'), 'n': 1, 't': past})
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=5)
        while db[CHUNKS].count_documents({}) and datetime.datetime.now() < deadline:
            mongo_client.admin.command('ping')
        assert db[CHUNKS].count_documents({}) == 0, 'the TTL monitor did not expire the chunk'
    finally:
        admin.command('setParameter', 1, ttlMonitorSleepSecs=old)


def test_ebooklet_imports_without_pymongo_and_ebooklet_mongo_names_the_extra():
    code = (
        "import sys; sys.modules['pymongo'] = None\n"
        "import ebooklet\n"
        "try:\n    import ebooklet.mongo\nexcept ImportError as e:\n    print('IMPORT-ERROR', e)\n"
    )
    out = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert "IMPORT-ERROR" in out.stdout and "ebooklet[mongo]" in out.stdout


# ── code review edr-mongo-code-1: the consistency rule, and paths no test reached ──────────────────

def test_verify_passes_without_a_refresh_and_raises_after_one_during_the_request(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    quiet, _ = _mstore(db, store)
    quiet.load_items(['k0', 'k1'])
    quiet.verify()
    st, _ = _mstore(db, store)
    st.load_items(['k0'])
    _push(store, tmp_path / 'w.blt', set_items={'k20': b'appended' * 4})   # an append only
    _refresh(db, store)
    assert st.get('k0') == VALUES['k0']
    with pytest.raises(StaleIndexError, match='during this request'):
        st.verify()


def test_a_store_refuses_to_build_while_an_apply_is_in_progress(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    db[DATASETS].update_one({'_id': DS}, {'$set': {'applying': True}})
    with pytest.raises(StaleIndexError, match='applying'):
        _mstore(db, store)


def test_a_store_on_a_never_applied_dataset_is_stale(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    db[DATASETS].update_one({'_id': DS}, {'$set': {'applied': None}})
    with pytest.raises(StaleIndexError):
        _mstore(db, store)


def test_a_heal_fetches_from_the_entrys_new_location(db, tmp_path):
    """404 at the old generation, a refresh lands, the re-read entry names the new one: fetched from there."""
    store = {}
    _seed(store, tmp_path, 4096)
    _refresh(db, store)
    old = db[ENTRIES].find_one({'_id': entry_id(DS, 'k0')})
    old_obj = f'{DB}/{utils.group_obj_key(old["g"], old["n"])}'
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})  # repack + GC of the old generation
    ran = []

    def refresh_on_old_generation(key):
        if key == old_obj and not ran:
            ran.append(1)
            refresh(db, DS, HookConnection(store, DB))

    st, session = _mstore(db, store, on_get=refresh_on_old_generation)
    assert st.get('k0') == VALUES['k0']
    assert ran and session.calls['HEAD'] == 0, 'healed by the re-read entry, not by the HEAD classification'
    with pytest.raises(StaleIndexError):
        st.verify()                                  # the request overlapped an apply


def test_one_load_groups_by_group_and_generation(db, tmp_path):
    store = {}
    _seed(store, tmp_path, 4096)
    _refresh(db, store)
    _push(store, tmp_path / 'w.blt', set_items={'k1': b'CHANGED' * 4})
    manifest, _m, index_bytes = utils.parse_db_payload(store[DB][0])
    entries, _ = refresh_mod._parse_entries(index_bytes, manifest, store[DB][1]['uuid'])
    db[ENTRIES].replace_one({'_id': entry_id(DS, 'k1')}, {'d': DS, 'k': 'k1', **entries['k1']})  # mid-apply
    st, _ = _mstore(db, store)
    with pytest.raises(StaleIndexError):
        st.load_items(['k0', 'k1'])                  # one batch: k1 at the new generation, k0 at the GC'd one
    assert st._values.get('k1') == b'CHANGED' * 4


def test_a_5xx_is_an_http_error_not_an_integrity_fault(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)
    st, _ = _mstore(db, store, on_get=lambda key: fake_s3.FakeResp(503, error={'Code': 'SlowDown'}))
    with pytest.raises(urllib3.exceptions.HTTPError) as info:
        st.get('k1')
    assert not isinstance(info.value, RemoteIntegrityError)
    assert 'remote fetch failed' in str(info.value)


def test_a_failed_classification_head_is_an_http_error(db, tmp_path):
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    del store[f'{DB}/k2']
    st, _ = _mstore(db, store, on_head=lambda key: fake_s3.FakeResp(500, error={'Code': 'InternalError'}))
    with pytest.raises(urllib3.exceptions.HTTPError) as info:
        st.get('k2')
    assert not isinstance(info.value, RemoteIntegrityError) and 'HEAD failed' in str(info.value)


def test_fetches_within_one_request_run_concurrently(db, tmp_path):
    """~0.1 s: eight per-key fetches each held for 30 ms; at least two must overlap."""
    store = {}
    _seed(store, tmp_path, None)
    _refresh(db, store)
    lock = threading.Lock()
    state = {'now': 0, 'max': 0}

    def slow(key):
        with lock:
            state['now'] += 1
            state['max'] = max(state['max'], state['now'])
        time.sleep(0.03)
        with lock:
            state['now'] -= 1

    st, _ = _mstore(db, store, on_get=slow)
    st.load_items(VALUES)
    assert {k: st.get(k) for k in VALUES} == VALUES
    assert state['max'] > 1, 'fetches ran one at a time'


class _RepointAfterFirstRead:
    """A database whose first datasets.find_one is followed at once by a re-point of that document."""

    def __init__(self, db):
        self._db = db

    def __getitem__(self, name):
        coll = self._db[name]
        if name != DATASETS:
            return coll
        outer = self

        class _Coll:
            def find_one(self, *args, **kwargs):
                doc = coll.find_one(*args, **kwargs)
                if not getattr(outer, 'fired', False):
                    outer.fired = True
                    coll.update_one(
                        {'_id': DS},
                        {'$set': {'db_url': 'elsewhere', 'applied': {'uuid': 'f' * 32, 'ts': 1}}, '$inc': {'rev': 1}},
                    )
                return doc

            def __getattr__(self, attr):
                return getattr(coll, attr)

        return _Coll()


def _refresh_with_url(db, store):
    res = refresh(db, DS, HookConnection(store, DB), db_url=DB)
    assert res['status'] in ('applied', 'nochange'), res


def test_a_store_built_from_a_session_reads_through_the_db_url_of_its_own_document(db, tmp_path):
    store = {}
    other = 'another-db'   # not DB, so a reader built from any other name reads nothing
    conn = fake_s3.FakeS3Connection(store, other)
    with open_ebooklet(conn, tmp_path / 'other.blt', flag='n', group_bytes=TEST_GB) as eb:
        for k, v in VALUES.items():
            eb[k] = v
        assert eb.changes().push()
    assert refresh(db, DS, conn, db_url=other)['status'] == 'applied'
    doc = db[DATASETS].find_one({'_id': DS})
    st = MongoStore(db, DS, http_session=CountingSession(store))
    assert st.db_url == other and st._reader.read_db_key == other
    assert st.applied == (doc['applied']['uuid'], doc['applied']['ts'])
    st.load_items(VALUES)
    assert {k: st.get(k) for k in VALUES} == VALUES
    st.verify()


def test_a_repoint_after_construction_changes_neither_the_db_url_nor_the_reader(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh_with_url(db, store)
    before = db[DATASETS].find_one({'_id': DS})['applied']
    st = MongoStore(_RepointAfterFirstRead(db), DS, http_session=CountingSession(store))
    assert db[DATASETS].find_one({'_id': DS})['db_url'] == 'elsewhere', 'the re-point did not land'
    assert st.applied == (before['uuid'], before['ts'])
    ## The store keeps the document it read: the re-point happened after it, so the reader, db_url
    ## and the request's verdict all belong to that document.
    assert st.db_url == DB and st._reader.read_db_key == DB
    st.load_items(VALUES)
    assert {k: st.get(k) for k in VALUES} == VALUES
    with pytest.raises(StaleIndexError):
        st.verify()


def test_a_store_takes_exactly_one_of_a_reader_or_a_session(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh_with_url(db, store)
    session = CountingSession(store)
    with pytest.raises(TypeError, match='exactly one'):
        MongoStore(db, DS)
    with pytest.raises(TypeError, match='exactly one'):
        MongoStore(db, DS, object_reader(DB, http_session=session), http_session=session)


def test_a_session_store_refuses_a_document_without_a_db_url(db, tmp_path):
    store = {}
    _seed(store, tmp_path, TEST_GB)
    _refresh(db, store)   # a FakeS3Connection has no db_url, so none is recorded
    assert db[DATASETS].find_one({'_id': DS}).get('db_url') is None
    with pytest.raises(ValueError, match='no db_url'):
        MongoStore(db, DS, http_session=CountingSession(store))
    assert MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store))).db_url is None


def test_object_reader_builds_a_non_streaming_session_and_refuses_a_streaming_one():
    reader = object_reader('https://example.invalid/db')   # no request is made
    assert reader._read_session._stream is False and reader.read_db_key == 'https://example.invalid/db'
    with pytest.raises(ValueError, match='stream=False'):
        object_reader('https://example.invalid/db', http_session=s3func.HttpSession())
    object_reader('https://example.invalid/db', http_session=s3func.HttpSession(stream=False))


def test_metadata_with_a_timestamp_is_none_when_there_is_no_metadata(db, tmp_path):
    store = {}
    _push(store, tmp_path / 'seed.blt', TEST_GB, flag='n', set_items={'k0': b'v'})
    _refresh(db, store)
    st, _ = _mstore(db, store)
    assert st.get_metadata() is None and st.get_metadata(include_timestamp=True) is None
