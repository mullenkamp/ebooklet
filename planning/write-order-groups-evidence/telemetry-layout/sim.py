"""ECan-like ts_ortho update pattern under per-key vs write-order groups (ebooklet 0.11, fake S3).

Keys 'v!{station}.{block}' (chunk = 1 station x 25,000 steps). 144 stations, blocks 0..7 (7 = current).
Full chunk ~33 KB, current chunk ~16 KB growing ~1 KB/hour. Each hour rewrites every station's current
chunk (what the hourly update does). Then a rollover: block 8 starts for every station in one push.
"""
import os, sys, pathlib, shutil, warnings
warnings.simplefilter('ignore')
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3

N_ST, N_BLK, FULL, CUR = 144, 8, 33_000, 16_000
MIB = 2**20

def val(n, seed):
    return os.urandom(n)

def run(label, group_bytes, order):
    d = pathlib.Path('w') / label; shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    store = {}
    eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'x.blt', flag='n', group_bytes=group_bytes)
    keys = [(s, b) for s in range(N_ST) for b in range(N_BLK)]            # station-major
    if order == 'time-major':
        keys = sorted(keys, key=lambda sb: (sb[1], sb[0]))
    elif order == 'download':                                               # a hydrated file: arbitrary order
        import random; random.seed(1); random.shuffle(keys)
    for s, b in keys:
        eb[f'v!{s}.{b}'] = val(CUR if b == N_BLK - 1 else FULL, 0)
    assert eb.changes().push()
    sess = eb._remote_session._write_session
    n_obj = len([k for k in store if k.startswith('d/')])
    def hour(cur_len, block):
        mark = len(sess.put_log)
        before = {k: len(v[0]) if isinstance(v, tuple) else 0 for k, v in store.items()}
        for s in range(N_ST):
            eb[f'v!{s}.{block}'] = val(cur_len, 0)
        r = eb.changes().push(); assert r and not r.failures
        put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
        nbytes = sum(len(store[k][0]) for k in put if k in store)
        return len(put), nbytes
    h = [hour(CUR + 1000 * i, N_BLK - 1) for i in range(1, 4)]
    roll = hour(500, N_BLK)                        # rollover: every station's NEW block in one push
    after = [hour(500 + 1000 * i, N_BLK) for i in range(1, 4)]
    n_obj_end = len([k for k in store if k.startswith('d/')])
    eb.close()
    fmt = lambda xs: ', '.join(f'{n} PUTs/{b/1e6:.1f} MB' for n, b in xs)
    print(f'{label:<34} objects {n_obj:>5} -> {n_obj_end:>5} | hourly: {fmt(h)} | rollover: {fmt([roll])} | after: {fmt(after)}')

run('per-key', None, 'station-major')
for gb in (1, 8):
    for order in ('station-major', 'download', 'time-major'):
        run(f'grouped {gb} MiB, {order}', gb * MIB, order)
