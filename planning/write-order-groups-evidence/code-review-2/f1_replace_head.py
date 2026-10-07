"""Fable F1 (round 2): a replacement push whose post-commit HEAD fails, then a retry."""
import pathlib, sys
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
gb = None if sys.argv[1] == 'perkey' else 64
d = pathlib.Path('f1') / sys.argv[1]; d.mkdir(parents=True)
store = {}
with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'old.blt', flag='n', group_bytes=gb) as eb:
    eb['old'] = b'x'; assert eb.changes().push()
eb = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', flag='n', group_bytes=gb)
for i in range(5):
    eb[f'k{i}'] = f'v{i}'.encode()
sess = eb._remote_session
real_head = sess.head_object
state = {'fail': True}
def head(key=None):
    if key is None and state['fail'] and 'put' in state:
        state['fail'] = False
        return fake_s3.FakeResp(status=503, error={'message': 'induced HEAD failure'})
    return real_head(key)
real_put = sess.put_db_object
def put_db(data, metadata):
    state['put'] = True
    return real_put(data, metadata)
sess.head_object = head; sess.put_db_object = put_db
try:
    eb.changes().push(); print('push 1 returned')
except Exception as e:
    print('push 1 raised:', type(e).__name__, e)
print('journal after push 1: written', sorted(eb._journal.written), 'replace_pending', eb._journal.replace_pending)
print('local keys before retry:', sorted(eb._local_file.keys()))
print('push 2:', eb.changes().push())
print('local keys after retry:', sorted(eb._local_file.keys()))
eb.close()
with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'r.blt', flag='r') as r:
    print('REMOTE keys:', sorted(r.keys()))
