import pathlib, tempfile, warnings, io
from unittest import mock
from ebooklet import open_ebooklet, utils
from ebooklet.errors import LockLostError
from ebooklet.tests import fake_s3
import booklet
warnings.simplefilter('ignore')
tmp = pathlib.Path(tempfile.mkdtemp(dir='.')); store = {}
conn = lambda k: fake_s3.FakeS3Connection(store, k)
def remote_view(k):
    man, _m, idx = utils.parse_db_payload(store[k][0])
    f = booklet.FixedLengthValue(io.BytesIO(bytes(idx)), 'r')
    out = {key: (utils.bytes_to_int(v[7:11]), utils.bytes_to_int(v[11:15])) for key, v in f.items()}; f.close()
    return man, out
# --- E4: crash right after commit PUT
with open_ebooklet(conn('d'), tmp/'p.blt', 'n', num_groups=1) as eb:
    eb['old1'] = b'A'*40; eb['old2'] = b'B'*40
    assert eb.changes().push()
man0, idx0 = remote_view('d')
class Crash(BaseException): pass
real_info = utils.push_logger.info
def info(msg, *a, **k):
    if 'commit succeeded' in str(msg): raise Crash()
    return real_info(msg, *a, **k)
eb = open_ebooklet(conn('d'), tmp/'p.blt', 'w')
eb['old1'] = b'a'*90; eb['new1'] = b'N'*40
try:
    with mock.patch.object(utils.push_logger, 'info', info): eb.changes().push()
except Crash: print('crashed after commit PUT')
eb._finalizer.detach(); eb._local_file.close(); eb._remote_index.close()
man1, idx1 = remote_view('d')
eb = open_ebooklet(conn('d'), tmp/'p.blt', 'w')
lman = dict(eb._remote_state.manifest); lidx = {k: (utils.bytes_to_int(v[7:11]), utils.bytes_to_int(v[11:15])) for k, v in eb._remote_index.items()}
print('E4 manifest==commit2:', lman == man1, '| sidecar==commit2:', lidx == idx1, '| sidecar==commit1:', lidx == idx0)
eb.close()
# --- E4c: lock lost mid-push
NG = 7
with open_ebooklet(conn('e'), tmp/'w1.blt', 'n', num_groups=NG) as eb:
    for i in range(6): eb[f'k{i}'] = b'A'*40
    assert eb.changes().push()
with open_ebooklet(conn('e'), tmp/'w2.blt', 'w') as eb: pass
w1 = open_ebooklet(conn('e'), tmp/'w1.blt', 'w'); w1['w1_new'] = b'B'*40
real_upload = utils.upload_group; state = {'done': False}
def preempt(*a, **k):
    if not state['done']:
        state['done'] = True
        with open_ebooklet(conn('e'), tmp/'w2.blt', 'w', force_lock=True) as w2:
            for i in range(5): w2[f'w2_new{i}'] = b'C'*40
            assert w2.changes().push()
        w1.lock.broken = True
    return real_upload(*a, **k)
try:
    with mock.patch.object(utils, 'upload_group', preempt): w1.changes().push()
except LockLostError as e: print('W1 LockLostError')
w1.close()
print('after W2 remote keys:', sorted(remote_view('e')[1]))
w1 = open_ebooklet(conn('e'), tmp/'w1.blt', 'w')
print('W1 reopen sees w2_new0:', 'w2_new0' in w1)
print('W1 retry:', w1.changes().push()); w1.close()
with open_ebooklet(conn('e'), tmp/'fresh.blt', 'r') as r:
    print('final keys:', sorted(r.keys()))
