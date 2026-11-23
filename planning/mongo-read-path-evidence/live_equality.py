"""
Live close-check for the MongoDB read path (plan `mongo-read-path-plan.md`, Step 8).

Refreshes live public remotes into a throwaway MongoDB, then reads each selection twice: through
cfdb.open_store_dataset(MongoStore(...)) and through cfdb.open_edataset on a fresh cache. They must
be exactly equal (NaN positions included). The store is read cold (empty chunk cache) and warm (a
new store on the filled cache).

usage: python live_equality.py MONGO_URI
  (an environment with editable ebooklet[mongo] and cfdb; e.g. a throwaway
   docker run -d --rm -p 127.0.0.1:27019:27017 --tmpfs /data/db mongo:8.0)
"""
import json
import pathlib
import sys
import tempfile
import time
import warnings

import cfdb
import numpy as np
import pymongo

import ebooklet
from ebooklet.mongo import MongoStore, ensure_schema, object_reader, refresh

warnings.filterwarnings('ignore')

DATASETS = {
    'ecan_raw_streamflow': 'https://b2.tethys-ts.xyz/file/ecan-env-monitoring/envlib/ecan-env/ecan-streamflow',
    'wrf_precip': 'https://b2.envlib.xyz/file/envlib/envlib/wrf-3km-nz-precipitation',
    'flow_fc': 'https://b2.tethys-ts.xyz/file/point-forecasts/envlib/flow-forecasts/streamflow',
}

# (name, dataset, variable, selection from the variable's shape and the dataset)
CASES = [
    ('ecan_1stn_2wk', 'ecan_raw_streamflow', 'streamflow', lambda s, ds: (slice(10, 11), slice(s[1] - 336, s[1]))),
    ('ecan_all_48h', 'ecan_raw_streamflow', 'streamflow', lambda s, ds: (slice(0, s[0]), slice(s[1] - 48, s[1]))),
    ('ecan_loc_week', 'ecan_raw_streamflow', 'streamflow', 'loc'),
    ('wrf_cell_1y', 'wrf_precip', 'precipitation', lambda s, ds: (slice(s[0] - 8760, s[0]), slice(267, 268), slice(157, 158))),
    ('wrf_map_1h', 'wrf_precip', 'precipitation', lambda s, ds: (slice(300000, 300001), slice(0, s[1]), slice(0, s[2]))),
    ('fc_latest_run', 'flow_fc', 'streamflow', lambda s, ds: (slice(0, s[0]), slice(s[1] - 1, s[1]), slice(0, s[2]))),
    ('fc_last_120_runs', 'flow_fc', 'streamflow', lambda s, ds: (slice(0, s[0]), slice(s[1] - 120, s[1]), slice(0, s[2]))),
]


def read(ds, var, sel):
    if sel == 'loc':
        t = np.asarray(ds['time'][-200:].data)
        return np.asarray(ds[var].loc[:, t[10]:t[178]].data)
    return np.asarray(ds[var][sel(ds[var].shape, ds)].data)


def same(a, b):
    return a.shape == b.shape and a.dtype == b.dtype and np.array_equal(a, b, equal_nan=a.dtype.kind in 'fc')


def main(uri):
    client = pymongo.MongoClient(uri, tz_aware=True)
    db = client[f'live_equality_{int(time.time())}']
    ensure_schema(db, 3600)
    tmp = pathlib.Path(tempfile.mkdtemp(prefix='live-eq-'))
    report = {'versions': {'ebooklet': ebooklet.__version__, 'cfdb': cfdb.__version__}, 'refresh': {}, 'cases': {}}
    try:
        for name, url in DATASETS.items():
            t0 = time.perf_counter()
            res = refresh(db, name, ebooklet.S3Connection(db_url=url))
            report['refresh'][name] = {**res, 'seconds': round(time.perf_counter() - t0, 2)}
            assert res['status'] == 'applied', res
            again = refresh(db, name, ebooklet.S3Connection(db_url=url))
            report['refresh'][name]['second_poll'] = again['status']
        ref = {name: cfdb.open_edataset(ebooklet.S3Connection(db_url=url), tmp / f'{name}.cfdb', flag='r')
               for name, url in DATASETS.items()}
        bad = 0
        for case, dsn, var, sel in CASES:
            expected = read(ref[dsn], var, sel)
            out = {'shape': list(expected.shape), 'finite': int(np.isfinite(expected).sum())}
            for label in ('cold', 'warm'):
                t0 = time.perf_counter()
                store = MongoStore(db, dsn, object_reader(DATASETS[dsn]))
                with cfdb.open_store_dataset(store) as ds:
                    got = read(ds, var, sel)
                store.verify()                    # the API's contract: no commit applied during the request
                out[f'{label}_s'] = round(time.perf_counter() - t0, 3)
                out[f'{label}_equal'] = same(got, expected)
                bad += not out[f'{label}_equal']
            report['cases'][case] = out
        for ds in ref.values():
            ds.close()
        report['chunks_cached'] = db['chunks'].count_documents({})
        report['all_equal'] = bad == 0
        print(json.dumps(report, indent=1, default=str))
        return 0 if bad == 0 else 1
    finally:
        client.drop_database(db.name)
        client.close()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
