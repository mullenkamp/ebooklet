"""One-year chunks (8,760 h) x group size, block-major layout, for each ECan dataset's shape. Real ebooklet 0.11.
Reports: objects, hourly push mid-block (half-filled current chunks), one station's full history read, whole read."""
import math, os, pathlib, shutil, threading, warnings
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
SHAPES = {'streamflow': (144, 183_587, 1.3), 'gage_height': (168, 234_646, 0.56),
          'precipitation (at 10 yr)': (95, 87_600, 0.5)}
T, OVH = 8_760, 60
size = lambda steps, bps: int(steps * bps) + OVH
n = [0]
for name, (n_st, steps, bps) in SHAPES.items():
    n_blk = math.ceil(steps / T)
    slab = n_st * size(T, bps)
    for gb in (None, 256 << 10, 512 << 10, 1 << 20):
        d = pathlib.Path('w4') / f'{name.split()[0]}-{gb}'; shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
        store = {}
        eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=gb)
        for b in range(n_blk):
            for s in range(n_st):
                eb[f'v!{s}.{b}'] = os.urandom(size(T, bps))
        assert eb.changes().push()
        sess = eb._remote_session._write_session
        for h in (1, T // 2):                      # rollover, then half a year into the new block
            mark = len(sess.put_log)
            for s in range(n_st):
                eb[f'v!{s}.{n_blk}'] = os.urandom(size(h, bps))
            assert eb.changes().push()
        put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
        hourly = (len(put), sum(len(store[k][0]) for k in put if k in store))
        objs = len([k for k in store if k.startswith('d/')])
        eb.close()
        def read(keys):
            n[0] += 1
            with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / f'r{n[0]}.blt', flag='r') as r:
                del GETS[:]
                assert not r.load_items(keys)
                return len(GETS), sum(GETS)
        hist = read([f'v!77.{b}' for b in range(n_blk + 1)])
        need = n_blk * size(T, bps) + size(T // 2, bps)
        whole = read([f'v!{s}.{b}' for s in range(n_st) for b in range(n_blk + 1)])
        lab = 'per-key' if gb is None else f'{gb >> 10} KiB'
        print(f'{name:<25} slab {slab/1e6:4.2f} MB | {lab:>8}: objects {objs:>5} | hourly mid-block {hourly[0]:>3} PUTs {hourly[1]/1e6:4.2f} MB'
              f' | 1-station history {hist[0]:>3} GETs {hist[1]/1e3:7.0f} KB (needs {need/1e3:.0f}) | whole {whole[0]:>5} GETs', flush=True)
