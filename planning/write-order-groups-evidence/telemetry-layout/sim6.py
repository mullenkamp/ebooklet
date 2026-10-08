"""A station added mid-block (streamflow shape, 1-year chunks, 256 KiB groups, block-major history). Real ebooklet 0.11."""
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
N_ST, STEPS, BPS, OVH, T, GB = 144, 183_587, 1.3, 60, 8_760, 256 << 10
size = lambda steps: int(steps * BPS) + OVH
n_blk = math.ceil(STEPS / T)            # 21 full blocks below; block n_blk is the current one
d = pathlib.Path('w6'); shutil.rmtree(d, ignore_errors=True); d.mkdir()
store = {}
eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=GB)
for b in range(n_blk):
    for s in range(N_ST):
        eb[f'v!{s}.{b}'] = os.urandom(size(T))
assert eb.changes().push()
sess = eb._remote_session._write_session
def push(label, writes):
    mark = len(sess.put_log)
    for k, n in writes:
        eb[k] = os.urandom(size(n))
    assert eb.changes().push()
    put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
    print(f'{label:<52} {len(put):>3} PUTs {sum(len(store[k][0]) for k in put if k in store)/1e6:5.2f} MB')
cur = lambda h, extra=(): [(f'v!{s}.{n_blk}', h) for s in list(range(N_ST)) + list(extra)]
push('rollover (new block, hour 1)', cur(1))
push('hourly, half a year in', cur(T // 2))
NEW = N_ST                                # the new station, with 21 years of history
push('NEW STATION starts reporting (1 h of data)', [(f'v!{NEW}.{n_blk}', 1)] + cur(T // 2 + 1))
for h in (2, 3):
    push(f'hourly after the addition (+{h - 1} h)', cur(T // 2 + h) + [(f'v!{NEW}.{n_blk}', h)])
push('next rollover (block n+1, hour 1)', [(f'v!{s}.{n_blk + 1}', 1) for s in range(N_ST + 1)] + cur(T) + [(f'v!{NEW}.{n_blk}', T // 2)])
push('hourly after the next rollover', [(f'v!{s}.{n_blk + 1}', 2) for s in range(N_ST + 1)])
eb.close()
for st, label in ((NEW, 'new station'), (77, 'old station 77')):
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / f'r{st}.blt', flag='r') as r:
        del GETS[:]
        keys = [f'v!{st}.{b}' for b in range(n_blk + 2)]
        need = sum(len(r._remote_index[k]) and 0 for k in []) or None
        assert not r.load_items(keys)
        print(f'read {label:<14} history: {len(GETS):>2} GETs {sum(GETS)/1e3:6.0f} KB')
