"""Under ebooklet 0.11: republish the hydrated file after the 0.10.5 in-session delete."""
import pathlib, pickle
import ebooklet
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
assert ebooklet.__version__.startswith('0.11')
w = pathlib.Path('g1')
store = pickle.load(open(w / 'store_after_delete.pkl', 'rb'))
with open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), w / 'writer.blt', flag='w') as eb:
    print('0.11 open: local keys', sorted(eb._local_file.keys()), 'remote_ts', eb._remote_state.remote_ts, 'initialized', eb._remote_session.initialized)
    r = eb.changes().push()
    print('push:', r)
with open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), w / 'fresh.blt', flag='r') as r:
    print('REMOTE keys:', sorted(r.keys()), {k: r[k] for k in ['k0', 'k9']})
