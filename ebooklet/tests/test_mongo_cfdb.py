"""
cfdb read through ebooklet.mongo.MongoStore equals cfdb read through open_edataset, for all four
dataset types, hermetically (cfdb EDatasets pushed to the fake S3, a REAL MongoDB). Datasets are
opened on the store with cfdb's public `open_store_dataset` (cfdb >= 0.12.0).
"""
import cfdb
import numpy as np
import pytest
import shapely
from cfdb import dtypes

from ebooklet.errors import StaleIndexError
from ebooklet.mongo import MongoStore, ensure_schema, object_reader, refresh
from ebooklet.tests import fake_s3
from ebooklet.tests.mongo_support import (  # noqa: F401  (mongo_client, mongo_db are fixtures)
    CountingSession,
    HookConnection,
    mongo_client,
    mongo_db,
)

DB = 'cf'
DS = 'cf1'
TIME = np.arange('2024-01-01T00', '2024-01-03T00', dtype='datetime64[h]')
FRT = np.array(['2024-01-01T00', '2024-01-01T06', '2024-01-01T12'], dtype='datetime64[m]')
LEAD = np.arange(1, 7, dtype='int32')
F32 = dtypes.dtype('float32', precision=2, min_value=-100, max_value=1000)
RNG = np.random.default_rng(7)


def _data(shape):
    arr = np.round(RNG.uniform(0, 50, shape), 2).astype('float32')
    arr[(RNG.uniform(size=shape) < 0.1)] = np.nan
    return arr


def _grid(ds):
    ds.create.coord.time(data=TIME, step=1)
    ds.create.coord.y(data=np.linspace(-44, -43, 5, dtype='float32'), step=0.25)
    ds.create.coord.x(data=np.array([170.0, 170.5, 171.0, 171.5], dtype='float32'), step=0.5)
    v = ds.create.data_var.generic('t', ('time', 'y', 'x'), dtype=F32, chunk_shape=(12, 3, 2))
    v[:] = _data(v.shape)
    v.attrs['units'] = 'degC'


def _ts_ortho(ds):
    ds.create.coord.point(data=np.array([shapely.Point(172.0, -43.5), shapely.Point(171.5, -43.0),
                                         shapely.Point(171.0, -42.5)], dtype='O'))
    ds.create.coord.time(data=TIME, step=1)
    v = ds.create.data_var.generic('flow', ('point', 'time'), dtype=F32, chunk_shape=(1, 10))
    v[:] = _data(v.shape)


def _ts_forecast(ds):
    ds.create.coord.point(data=np.array([shapely.Point(172.0, -43.5), shapely.Point(171.5, -43.0)], dtype='O'))
    ds.create.coord.forecast_reference_time(data=FRT, step=360)
    ds.create.coord.forecast_period(data=LEAD, step=1)
    v = ds.create.data_var.generic('q', ('point', 'forecast_reference_time', 'forecast_period'),
                                   dtype=F32, chunk_shape=(2, 1, 6))
    v[:] = _data(v.shape)


def _grid_forecast(ds):
    ds.create.coord.x(data=np.array([170.0, 170.5, 171.0], dtype='float32'), step=0.5)
    ds.create.coord.y(data=np.array([-44.0, -43.5], dtype='float32'), step=0.5)
    ds.create.coord.forecast_reference_time(data=FRT, step=360)
    ds.create.coord.forecast_period(data=LEAD, step=1)
    v = ds.create.data_var.generic('p', ('x', 'y', 'forecast_reference_time', 'forecast_period'),
                                   dtype=F32, chunk_shape=(2, 2, 1, 3))
    v[:] = _data(v.shape)


BUILDERS = {'grid': _grid, 'ts_ortho': _ts_ortho, 'ts_forecast': _ts_forecast, 'grid_forecast': _grid_forecast}


def _same(a, b):
    a, b = np.asarray(a), np.asarray(b)
    if a.dtype == object:
        return a.shape == b.shape and all(x == y for x, y in zip(a.ravel(), b.ravel(), strict=True))
    return a.shape == b.shape and a.dtype == b.dtype and np.array_equal(a, b, equal_nan=a.dtype.kind in 'fc')


@pytest.fixture
def db(mongo_db):
    ensure_schema(mongo_db, 3600)
    return mongo_db


@pytest.mark.parametrize('dataset_type', list(BUILDERS))
def test_cfdb_through_the_store_equals_open_edataset(db, tmp_path, dataset_type):
    store = {}
    conn = fake_s3.FakeS3Connection(store, DB)
    with cfdb.open_edataset(conn, tmp_path / 'w.cfdb', flag='n', dataset_type=dataset_type) as ds:
        BUILDERS[dataset_type](ds)
        ds.attrs['title'] = f'a {dataset_type}'
        ds.push()
    assert refresh(db, DS, HookConnection(store, DB))['status'] == 'applied'

    session = CountingSession(store)
    with cfdb.open_edataset(fake_s3.FakeS3Connection(store, DB), tmp_path / 'r.cfdb', flag='r') as ref, \
            cfdb.open_store_dataset(MongoStore(db, DS, object_reader(DB, http_session=session))) as got:
        assert got.dataset_type == ref.dataset_type == dataset_type
        assert dict(got.attrs) == dict(ref.attrs)
        assert sorted(got.coord_names) == sorted(ref.coord_names)
        assert sorted(got.data_var_names) == sorted(ref.data_var_names)
        for name in list(ref.coord_names) + list(ref.data_var_names):
            assert _same(got[name].data, ref[name].data), name
            assert dict(got[name].attrs) == dict(ref[name].attrs), name
        var = ref.data_var_names[0]
        sel = tuple(slice(0, max(1, n // 2)) for n in ref[var].shape)
        assert _same(got[var][sel].data, ref[var][sel].data)
        dims = ref[var].coord_names
        if 'time' in dims:                          # selection by label reads the time coordinate
            key = tuple(slice(TIME[5], TIME[20]) if c == 'time' else slice(None) for c in dims)
            assert _same(got[var].loc[key].data, ref[var].loc[key].data)
    assert session.calls['HEAD'] == 0


@pytest.mark.parametrize('group_bytes', [4096, None], ids=['grouped', 'per_key'])
def test_a_request_spanning_a_re_encoding_refresh_is_refused_by_verify(db, tmp_path, group_bytes):
    """
    Code review edr-mongo-code-1 (both arms): the same keys re-encoded under a replaced remote decode,
    with the request's old metadata, to values from neither commit. Whatever the reads return, the
    request must not be answered: a read raises StaleIndexError, or verify() does.
    """
    store = {}

    def build(path, lo, hi, values):
        with cfdb.open_edataset(fake_s3.FakeS3Connection(store, DB), path, flag='n', dataset_type='grid',
                                group_bytes=group_bytes) as ds:
            ds.create.coord.time(data=TIME[:12], step=1)
            ds.create.coord.y(data=np.array([-44.0, -43.5], dtype='float32'), step=0.5)
            v = ds.create.data_var.generic('t', ('time', 'y'), chunk_shape=(4, 2),
                                           dtype=dtypes.dtype('float32', precision=1, min_value=lo, max_value=hi))
            v[:] = values
            for name in ('time', 'y', 't'):
                ds[name].attrs['units'] = 'x'
            ds.push()

    build(tmp_path / 'a.cfdb', -100, 100, np.full((12, 2), 5.0, dtype='float32'))
    refresh(db, DS, HookConnection(store, DB))
    st = MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))
    with cfdb.open_store_dataset(st) as old:
        build(tmp_path / 'b.cfdb', 0, 5000, np.full((12, 2), 4000.0, dtype='float32'))   # same keys, new packing
        refresh(db, DS, HookConnection(store, DB))
        try:
            old['t'].data
        except StaleIndexError:
            return
        with pytest.raises(StaleIndexError):
            st.verify()


def test_dataset_operations_the_store_does_not_support_fail_clearly(db, tmp_path):
    store = {}
    with cfdb.open_edataset(fake_s3.FakeS3Connection(store, DB), tmp_path / 'w.cfdb', flag='n', dataset_type='grid') as ds:
        _grid(ds)
        ds.push()
    refresh(db, DS, HookConnection(store, DB))
    with cfdb.open_store_dataset(MongoStore(db, DS, object_reader(DB, http_session=CountingSession(store)))) as got:
        with pytest.raises(NotImplementedError, match='copy'):
            got.copy(tmp_path / 'copy.cfdb')
