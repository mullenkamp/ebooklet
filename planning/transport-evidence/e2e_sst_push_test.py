#!/usr/bin/env python3
"""Pre-release E2E test of the pipelined push on REAL cfdb data.

Builds a ~2GB chunk-aligned time-slice copy of the SST publish artifact,
pushes it through cfdb.open_edataset -> ebooklet 0.10.1 (local) -> the
achelous TEST bucket, verifies by fresh-reader pulls (byte equality vs the
local subset), runs an incremental second push, and deletes the remote.

Never touches the commons or the 20GB artifact (source is opened read-only).
"""

import io
import logging
import os
import resource
import sys
import time
from pathlib import Path

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

import numpy as np
import booklet
import ebooklet
import cfdb
from cfdb import open_dataset, open_edataset, dtypes

## --- overlay guards (the uv stale-cache rule) ---
assert booklet.__version__ == '0.12.8', booklet.__version__
assert ebooklet.__version__ == '0.10.1', ebooklet.__version__
assert cfdb.__version__ == '0.9.3', cfdb.__version__
assert hasattr(booklet.VariableLengthValue, 'locations')

SRC_PATH = Path('~/data/esa/sst/envlib_sst_cci_v3_nz.cfdb').expanduser()
TEST_DIR = Path('~/data/tmp_pipeline_test').expanduser()
DST_PATH = TEST_DIR / 'sst_subset.cfdb'
FRESH_PATH = TEST_DIR / 'fresh_reader.cfdb'
N_TIME = 1680                     # 14 time-chunks of 120 days ~= 2.0GB
NUM_GROUPS = 23                   # prime; ~90MB/group
DB_KEY = 'pipeline-test-sst-2gb'
DATA_VARS = ('temperature', 'sea_ice_fraction')

logging.basicConfig(format='%(asctime)s %(name)s %(message)s', datefmt='%H:%M:%S')
logging.getLogger('ebooklet.push').setLevel(logging.INFO)


def load_test_conn_config():
    cfg_path = Path('~/git/ebooklet/ebooklet/tests/s3_config.toml').expanduser()
    with io.open(cfg_path, 'rb') as f:
        cc = toml.load(f)['connection_config']
    return dict(endpoint_url=cc['endpoint_url'], access_key_id=cc['access_key_id'],
                access_key=cc['access_key'], bucket='achelous')


def build_subset():
    ## Reuse only a COMPLETE subset (a crashed build leaves a husk - rebuild).
    if DST_PATH.exists():
        if DST_PATH.stat().st_size > 1.5e9:
            print(f'[build] reusing {DST_PATH} ({DST_PATH.stat().st_size / 1e9:.2f} GB)')
            return
        print(f'[build] removing incomplete {DST_PATH} ({DST_PATH.stat().st_size / 1e9:.2f} GB)')
        DST_PATH.unlink()
    TEST_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    with open_dataset(str(SRC_PATH), allow_partial=True) as src, \
            open_dataset(str(DST_PATH), flag='n') as dst:
        lat = src['latitude'].data
        lon = src['longitude'].data
        tdata = np.asarray(src['time'].data, 'datetime64[D]')[:N_TIME]
        dst.create.coord.lat(data=lat)
        dst.create.coord.lon(data=lon)
        dst.create.coord.time(data=tdata, step=1)

        for name in DATA_VARS:
            sv = src[name]
            dv = dst.create.data_var.generic(
                name, sv.coord_names, dtype=dtypes.dtype(sv.dtype), chunk_shape=sv.chunk_shape
            )
            dv.attrs.update(sv.attrs.data)
            n = 0
            for chunk_slices in sv.iter_chunks(include_data=False):
                if chunk_slices[0].start >= N_TIME:
                    continue
                dv.set(chunk_slices, sv[chunk_slices].data)
                n += 1
            print(f'[build] {name}: {n} chunks copied ({time.monotonic() - t0:.0f}s elapsed)')
    print(f'[build] done: {DST_PATH} ({DST_PATH.stat().st_size / 1e9:.2f} GB in {time.monotonic() - t0:.0f}s)')


def drop_cache(path):
    os.sync()
    fd = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def push_full(conn):
    drop_cache(DST_PATH)
    t0 = time.monotonic()
    with open_edataset(conn, str(DST_PATH), flag='w', num_groups=NUM_GROUPS) as eds:
        result = eds.push()
    dt = time.monotonic() - t0
    assert result, f'full push failed: {result.failures}'
    print(f'[push] FULL push OK in {dt / 60:.1f} min '
          f'(peak RSS {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6:.2f} GB)')


def verify(conn):
    """Fresh reader: sample days across the record, byte-equal vs the subset."""
    ## A leftover reader file from a PREVIOUS test run carries the uuid of a
    ## deleted-and-recreated remote - ebooklet's stale-cache guard rightly
    ## refuses it (UUIDMismatchError). Start truly fresh.
    for suffix in ('', '.remote_index', '.changelog'):
        Path(str(FRESH_PATH) + suffix).unlink(missing_ok=True)
    sample_idx = [0, 300, 800, 1200, 1679]
    with open_edataset(conn, str(FRESH_PATH), flag='r') as remote_ds, \
            open_dataset(str(DST_PATH), allow_partial=True) as local_ds:
        for name in DATA_VARS:
            rv, lv = remote_ds[name], local_ds[name]
            for i in sample_idx:
                got = rv[slice(i, i + 1), :, :].data
                want = lv[slice(i, i + 1), :, :].data
                ## Guard against vacuous equality (an empty variable is all NaN).
                assert np.isfinite(want).any(), f'{name} t={i}: local sample is all-NaN - bad subset'
                assert np.array_equal(got, want, equal_nan=True), f'{name} t={i} differs via remote'
            print(f'[verify] {name}: {len(sample_idx)} sampled days byte-equal via the test remote')


def push_incremental(conn):
    """Rewrite one day's chunk-row and push again - the small-push path."""
    t0 = time.monotonic()
    with open_edataset(conn, str(DST_PATH), flag='w') as eds:
        v = eds['temperature']
        day = v[slice(600, 601), :, :].data
        v[slice(600, 601), :, :] = day   # same values, new timestamps -> real push
        result = eds.push()
    dt = time.monotonic() - t0
    assert result, f'incremental push failed: {result.failures}'
    print(f'[push] INCREMENTAL push OK in {dt:.0f}s')


def cleanup_remote(conn):
    session = conn.open('w')
    session.delete_remote()
    print('[cleanup] test remote deleted from the bucket')


def main():
    build_subset()
    cc = load_test_conn_config()
    conn = ebooklet.S3Connection(db_key=DB_KEY, **cc)
    try:
        push_full(conn)
        verify(conn)
        push_incremental(conn)
    finally:
        try:
            cleanup_remote(conn)
        except Exception as err:
            print(f'[cleanup] WARNING: remote cleanup failed ({err}) - '
                  f'delete db_key={DB_KEY} from achelous manually.')
    if FRESH_PATH.exists():
        FRESH_PATH.unlink()
    print('\nE2E PASSED - the pipelined push is verified on real cfdb data.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
