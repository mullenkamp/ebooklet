"""Operational telemetry: time chunk T x layout, measured with real ebooklet 0.11 on fake S3.
Streamflow shape (144 stations, ~1.3 B/step + 60 B/chunk), 3 years of history laid out block by block
(what rollovers produce). At 10/50/90% into the current block: the hourly push, and 2-week reads."""
import math, os, pathlib, shutil, statistics, threading, warnings
warnings.simplefilter('ignore')
from ebooklet import open_ebooklet, remote
from ebooklet.tests import fake_s3
GETS = []; _lock = threading.Lock(); _get = remote.S3SessionReader.get_object
def get_object(self, key=None, range_start=None, range_end=None):
    r = _get(self, key, range_start, range_end)
    if key is not None:
        with _lock: GETS.append(len(r.data or b''))
    return r
remote.S3SessionReader.get_object = get_object
N_ST, BPS, OVH, YEARS, WIN = 144, 1.3, 60, 3, 336
size = lambda steps: int(steps * BPS) + OVH
n_r = [0]

def run(T, gb):
    n_full = math.ceil(YEARS * 8760 / T)                     # full blocks of history
    cur = n_full                                             # the current block's index
    lab = f'T={T:>5} h, {"per-key" if gb is None else f"{gb >> 10} KiB"}'
    d = pathlib.Path('w8') / lab.replace(' ', '').replace(',', '_'); shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    store = {}
    eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=gb)
    for b in range(n_full):
        for s in range(N_ST):
            eb[f'v!{s}.{b}'] = os.urandom(size(T))
    assert eb.changes().push()
    sess = eb._remote_session._write_session
    rows = []
    for frac in (0.1, 0.5, 0.9):
        h = max(1, int(frac * T))
        for s in range(N_ST):                                # the current block so far (as hourly runs built it)
            eb[f'v!{s}.{cur}'] = os.urandom(size(h))
        assert eb.changes().push()
        mark = len(sess.put_log)
        for s in range(N_ST):                                # ONE hourly update
            eb[f'v!{s}.{cur}'] = os.urandom(size(h + 1))
        assert eb.changes().push()
        put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
        push = (len(put), sum(len(store[k][0]) for k in put if k in store))
        ## the 2-week window ending now covers the current block and, early in it, the previous one
        blocks = [cur] + ([cur - 1] if h + 1 < WIN else [])
        def read(keys):
            n_r[0] += 1
            with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / f'r{n_r[0]}.blt', flag='r') as r:
                del GETS[:]
                assert not r.load_items(keys)
                return len(GETS), sum(GETS)
        one = read([f'v!77.{b}' for b in blocks])
        allst = read([f'v!{s}.{b}' for s in range(N_ST) for b in blocks])
        rows.append((push, one, allst))
    objs = len([k for k in store if k.startswith('d/')])
    eb.close()
    keys = N_ST * (n_full + 1)
    mp = lambda i, j: statistics.mean(r[i][j] for r in rows)
    print(f'{lab:<22} objects {objs:>6} (keys {keys:>6}) | hourly push {mp(0,0):5.1f} PUTs {mp(0,1)/1e3:7.0f} KB'
          f' | 1 station, 2 wk: {mp(1,0):4.1f} GETs {mp(1,1)/1e3:6.1f} KB | all stations, 2 wk: {mp(2,0):5.1f} GETs {mp(2,1)/1e3:6.0f} KB', flush=True)

for T in (840, 1680, 2520):
    for gb in (None, 64 << 10, 128 << 10):
        run(T, gb)
