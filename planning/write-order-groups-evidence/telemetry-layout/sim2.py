"""Chunk length x layout for an hourly-updated station dataset (ECan streamflow shape), real ebooklet 0.11.

144 stations, 21 years hourly (183,587 steps), ~1.3 B/step + 60 B per chunk. Build writes station by station
(today's build). Hourly update rewrites every station's current chunk. Reads go through load_items (one
ranged GET per group, first-to-last wanted member).
"""
import math, os, pathlib, shutil, sys, threading, warnings
warnings.simplefilter('ignore')
from ebooklet import open_ebooklet, remote
from ebooklet.tests import fake_s3

N_ST, STEPS, BPS, OVH = 144, 183_587, 1.3, 60
GETS = []; _lock = threading.Lock(); _get = remote.S3SessionReader.get_object
def get_object(self, key=None, range_start=None, range_end=None):
    r = _get(self, key, range_start, range_end)
    if key is not None:
        with _lock: GETS.append(len(r.data or b''))
    return r
remote.S3SessionReader.get_object = get_object

def size(steps):
    return int(steps * BPS) + OVH

READER = [0]

def run(T, group_bytes, order='station'):
    label = f'T={T} {"per-key" if group_bytes is None else f"{group_bytes >> 10} KiB {order}-major"}'
    d = pathlib.Path('w2') / label.replace(' ', '_'); shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    n_blk = math.ceil(STEPS / T); cur_fill = STEPS - (n_blk - 1) * T
    store = {}
    eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=group_bytes)
    order_keys = [(s, b) for s in range(N_ST) for b in range(n_blk)]
    if order == 'block':                                        # the long-run layout (or a block-major build)
        order_keys.sort(key=lambda sb: (sb[1], sb[0]))
    for s, b in order_keys:
        eb[f'v!{s}.{b}'] = os.urandom(size(cur_fill if b == n_blk - 1 else T))
    assert eb.changes().push()
    sess = eb._remote_session._write_session
    objs = len([k for k in store if k.startswith('d/')])
    total = sum(size(T) for _ in range(N_ST * (n_blk - 1))) + N_ST * size(cur_fill)
    def hour(block, steps):
        mark = len(sess.put_log)
        for s in range(N_ST):
            eb[f'v!{s}.{block}'] = os.urandom(size(steps))
        assert eb.changes().push()
        put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
        return len(put), sum(len(store[k][0]) for k in put if k in store)
    h_build = hour(n_blk - 1, min(cur_fill + 1, T))
    hour(n_blk, 1)                                              # rollover: a new block for every station
    h_steady = hour(n_blk, 2)
    eb.close()
    def read(keys):
        READER[0] += 1
        with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / f'r{READER[0]}.blt', flag='r') as r:
            del GETS[:]
            fails = r.load_items(keys)
            assert not fails, fails
            return len(GETS), sum(GETS)
    st = 77
    need_hist = (n_blk - 1) * size(T) + size(cur_fill)
    hist = read([f'v!{st}.{b}' for b in range(n_blk + 1)])
    recent_blocks = [b for b in range(n_blk + 1) if (n_blk - b) * T <= 24 * 31 + T]
    recent = read([f'v!{st}.{b}' for b in recent_blocks])
    whole = read([f'v!{s}.{b}' for s in range(N_ST) for b in range(n_blk + 1)])
    print(f'{label:<20} chunk {size(T)/1e3:6.1f} KB | objects {objs:>6} | hourly after build {h_build[0]:>4} PUTs {h_build[1]/1e6:5.1f} MB'
          f' | hourly steady {h_steady[0]:>3} PUTs {h_steady[1]/1e6:5.2f} MB'
          f' | 1-station history {hist[0]:>4} GETs {hist[1]/1e6:6.2f} MB (needs {need_hist/1e6:.2f})'
          f' | last month {recent[0]:>2} GETs {recent[1]/1e3:7.1f} KB | whole {whole[0]:>5} GETs {whole[1]/1e6:.0f} MB', flush=True)

for T in (336, 2190, 8760, 25000):
    run(T, None)
    run(T, 1 << 20, 'station')
    run(T, 1 << 20, 'block')
