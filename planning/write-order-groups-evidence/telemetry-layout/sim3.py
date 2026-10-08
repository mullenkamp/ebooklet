"""After a rollover (1 MiB groups, 25,000 h chunks): the hot group across the new block's life."""
import os, pathlib, shutil, warnings
warnings.simplefilter('ignore')
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
N_ST, T, BPS, OVH, N_BLK = 144, 25_000, 1.3, 60, 8
size = lambda steps: int(steps * BPS) + OVH
for layout in ('block-major', 'per-key'):
    gb = None if layout == 'per-key' else 1 << 20
    d = pathlib.Path('w3') / layout; shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True)
    store = {}
    eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=gb)
    for b in range(N_BLK):                                 # block-major history, all blocks full
        for s in range(N_ST):
            eb[f'v!{s}.{b}'] = os.urandom(size(T))
    assert eb.changes().push()
    sess = eb._remote_session._write_session
    for hours in (1, 2, 1000, 12_500, 24_999):             # rollover at hour 1, then the block fills
        mark = len(sess.put_log)
        for s in range(N_ST):
            eb[f'v!{s}.{N_BLK}'] = os.urandom(size(hours))
        assert eb.changes().push()
        put = [k for k in sess.put_log[mark:] if k.startswith('d/')]
        mb = sum(len(store[k][0]) for k in put if k in store) / 1e6
        print(f'{layout:<12} new block at hour {hours:>6} ({hours/24/365:4.2f} yr): {len(put):>3} PUTs, {mb:5.2f} MB'
              f'  (the {N_ST} current chunks total {N_ST * size(hours) / 1e6:.2f} MB)')
    eb.close()
