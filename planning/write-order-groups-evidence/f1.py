import pathlib, pickle, shutil, sys, warnings
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
warnings.simplefilter('ignore')
FIX = pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fixtures/legacy_0105'
mode = sys.argv[1]
work = pathlib.Path('w') / mode; shutil.copytree(FIX, work)
store = pickle.load(open(work / 'store.pkl', 'rb'))
conn = lambda: fake_s3.FakeS3Connection(store, 'legacy')
with conn().open('w') as s: s.delete_remote()
eb = open_ebooklet(conn(), work / 'writer.blt', flag='w', group_bytes=64)
sess = eb._remote_session._write_session; orig = sess.put_object
def failing(key, data, metadata=None):
    if key.startswith('legacy/1.'): return fake_s3.FakeResp(status=500, error={'message': 'induced'})
    return orig(key, data, metadata)
sess.put_object = failing
print('journal written before:', len(eb._journal.written))
print('push 1:', eb.changes().push()); sess.put_object = orig
if mode == 'reopen':
    eb.close(); eb = open_ebooklet(conn(), work / 'writer.blt', flag='w', group_bytes=64)
print('push 2:', eb.changes().push())
print('local :', sorted(eb._local_file.keys())); eb.close()
with open_ebooklet(conn(), work / 'r.blt', 'r') as r: print('REMOTE:', sorted(r.keys()))
