"""group_bytes benchmark (ebooklet 0.11, write-order groups) on Mike's member bucket, scratch keys only.

  push --size MiB --phase initial   copy initial.cfdb (4 bands + 217 rows) and push it, grouped at size MiB
  push --size MiB --phase append    append one band of rows (finishing band 5, 217 rows into band 6) through the
                                    remote handle, then push
  read --size MiB --rep N           credential-free reads through the public db_url, a fresh cache per read
  fsck --size MiB | delete --size MiB

Each command prints one JSON line. Run with the scratch 0.11 venv from this directory.
"""
import argparse
import json
import pathlib
import resource
import shutil
import sys
import tempfile
import threading
import time
import warnings

import booklet
import cfdb
import ebooklet
import numpy as np
from ebooklet import remote, utils

HERE = pathlib.Path(__file__).resolve().parent
WRF3K = pathlib.Path.home() / 'git/envlib-repos/ingest/envlib-ingest-wrf-3k'
sys.path.insert(0, str(WRF3K))
import wrf3k  # noqa: E402

warnings.simplefilter('ignore')
MIB = 2**20
R0 = 840 * 359
APPEND = (3577, 3577 + 840)          # finish band 5 (rows 3577..4200), then 217 rows of band 6
YCHUNK = 24

## Instrumentation: every object PUT and GET, counted at the session class.
_lock = threading.Lock()
PUTS, GETS = [], []
_put, _get = remote.S3SessionWriter.put_object, remote.S3SessionReader.get_object
_put_db = remote.S3SessionWriter.put_db_object


def put_object(self, key, data, metadata=None):
    with _lock:
        PUTS.append((key, len(data)))
    return _put(self, key, data, metadata)


def put_db_object(self, data, metadata):
    with _lock:
        PUTS.append((None, len(data)))
    return _put_db(self, data, metadata)


def get_object(self, key=None, range_start=None, range_end=None):
    resp = _get(self, key, range_start, range_end)
    with _lock:
        GETS.append((key, len(resp.data) if resp.data is not None else 0))
    return resp


remote.S3SessionWriter.put_object = put_object
remote.S3SessionWriter.put_db_object = put_db_object
remote.S3SessionReader.get_object = get_object


def member_key(size):
    key = f'scratch-bench-gb-{size}'
    assert key.startswith('scratch-bench-gb-')
    return key


def work_path(size):
    return HERE / 'work' / str(size) / 'bench.cfdb'


def index_lengths(path):
    """{key: (ts, length)} of the local copy of the remote index (the sidecar), chunk keys only."""
    out = {}
    with booklet.FixedLengthValue(utils.remote_index_sidecar_path(path)) as idx:
        for key, val in idx.items():
            if key != utils.metadata_key_str:
                ts, gid, _off, ln = utils.decode_index_entry(val)
                out[key] = (ts, ln, gid)
    return out


def write_rows(dst, src, r0, r1):
    dst['time'].append(src['time'][R0 + r0:R0 + r1].data)
    ny = src['y'].shape[0]
    for y0 in range(0, ny, YCHUNK):
        y1 = min(y0 + YCHUNK, ny)
        dst['temperature'][r0:r1, y0:y1, :] = src['temperature'][R0 + r0:R0 + r1, y0:y1, :].data


def cmd_push(size, phase):
    path = work_path(size)
    conn = wrf3k.member_connection(member_key(size))
    if phase == 'initial':
        rep = ebooklet.fsck(wrf3k.member_connection(member_key(size), with_url=False))
        assert not rep.db_object_exists and rep.expected_objects == 0 and not rep.orphans, \
            f'{member_key(size)} is not empty: refusing'
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(HERE / 'initial.cfdb', path)
        before = {}
        t0 = time.perf_counter()
        with cfdb.open_edataset(conn, path, flag='w', group_bytes=size * MIB) as ds:
            res = ds.push()
        push_s = time.perf_counter() - t0
        write_s = 0.0
    else:
        before = index_lengths(path)
        t0 = time.perf_counter()
        ## group_bytes is writer-side and never stored on the remote: an append must pass it again, or it
        ## packs to the 32 MiB default (as wrf-3k's publish.py does, from its config).
        with cfdb.open_edataset(conn, path, flag='w', group_bytes=size * MIB) as ds, \
                cfdb.open_dataset(HERE / 'pull.cfdb', allow_partial=True) as src:
            write_rows(ds, src, *APPEND)
            write_s = time.perf_counter() - t0
            mark = len(PUTS)
            t1 = time.perf_counter()
            res = ds.push()
            push_s = time.perf_counter() - t1
            del PUTS[:mark]
    assert res.updated and not res.failures, res
    after = index_lengths(path)
    changed = {k: v for k, v in after.items() if before.get(k, (None,))[0] != v[0]}
    group_puts = [(k, n) for k, n in PUTS if k is not None]
    db_puts = [n for k, n in PUTS if k is None]
    gids_after = sorted({v[2] for v in after.values()})
    print(json.dumps(dict(
        cmd='push', size=size, phase=phase, push_s=round(push_s, 2), write_s=round(write_s, 2),
        group_puts=len(group_puts), group_put_bytes=sum(n for _, n in group_puts),
        db_object_bytes=sum(db_puts), puts_by_gid=sorted(int(k.split('.')[0]) for k, _ in group_puts),
        changed_keys=len(changed), changed_bytes=sum(v[1] for v in changed.values()),
        n_groups=len(gids_after), index_keys=len(after),
        peak_rss_mb=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
    )))


## The reads, on the final data (rows 0..4417 = 5 full bands + 217 rows).
READS = {
    'point_series': lambda v: v[:, 260, 150],                 # one cell, all time: 6 chunks
    'region_2x2': lambda v: v[:, 240:288, 144:192],           # 2x2 chunks, all time: 24 chunks
    'field_1h': lambda v: v[1260, :, :],                       # one hour: all 322 chunks of one band
}


def cmd_read(size, rep):
    url = wrf3k.member_db_url(member_key(size))
    conn = ebooklet.S3Connection(db_url=url)                  # credential-free: the production read path
    out = dict(cmd='read', size=size, rep=rep)
    with cfdb.open_dataset(HERE / 'pull.cfdb', allow_partial=True) as src:
        for name, sel in READS.items():
            with tempfile.TemporaryDirectory(dir=HERE / 'work') as tmp:
                del GETS[:]
                t0 = time.perf_counter()
                with cfdb.open_edataset(conn, pathlib.Path(tmp) / 'r.cfdb', flag='r') as ds:
                    open_s = time.perf_counter() - t0
                    n_open = len(GETS)
                    del GETS[:]
                    t1 = time.perf_counter()
                    got = sel(ds['temperature']).data
                    read_s = time.perf_counter() - t1
                gets = list(GETS)
            ## Correctness: the remote read equals the pulled source.
            if name == 'point_series':
                want = src['temperature'][R0:R0 + APPEND[1], 260, 150].data
            elif name == 'region_2x2':
                want = src['temperature'][R0:R0 + APPEND[1], 240:288, 144:192].data
            else:
                want = src['temperature'][R0 + 1260, :, :].data
            assert np.array_equal(got, want, equal_nan=True), f'{name}: remote read differs from the source'
            out[name] = dict(read_s=round(read_s, 3), open_s=round(open_s, 3), open_gets=n_open,
                             gets=len(gets), mb=round(sum(n for _, n in gets) / 1e6, 2))
    print(json.dumps(out))


def cmd_fsck(size):
    rep = ebooklet.fsck(wrf3k.member_connection(member_key(size), with_url=False))
    print(json.dumps(dict(cmd='fsck', size=size, exists=rep.db_object_exists, expected=rep.expected_objects,
                          orphans=len(rep.orphans), missing=len(rep.claimed_but_missing),
                          empty_groups=len(rep.empty_groups))))


def cmd_delete(size):
    with cfdb.open_edataset(wrf3k.member_connection(member_key(size)), work_path(size), flag='w') as ds:
        ds.delete_remote()
    cmd_fsck(size)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('cmd', choices=['push', 'read', 'fsck', 'delete'])
    p.add_argument('--size', type=int, required=True)
    p.add_argument('--phase', choices=['initial', 'append'])
    p.add_argument('--rep', type=int, default=1)
    a = p.parse_args()
    assert ebooklet.__version__.startswith('0.11'), ebooklet.__version__
    {'push': lambda: cmd_push(a.size, a.phase), 'read': lambda: cmd_read(a.size, a.rep),
     'fsck': lambda: cmd_fsck(a.size), 'delete': lambda: cmd_delete(a.size)}[a.cmd]()
