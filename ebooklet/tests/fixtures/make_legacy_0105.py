"""
Generate the legacy (ebooklet 0.10.5, hash-grouped format 2) fixture used by
test_write_order_groups.py's republish-path tests. Run it under the OLD
release - the files it writes are what a 0.10.5 producer leaves on disk:

    cd ebooklet/tests/fixtures
    uv run --no-project --with 'ebooklet==0.10.5' python make_legacy_0105.py

It writes legacy_0105/:
  store.pkl               the fake S3 store {key: (bytes, metadata)} of a
                          hash-grouped remote 'legacy' (num_groups=5, 10 keys,
                          user metadata)
  writer.blt(+sidecar)    the producer's local file: every value local, its
                          journal recording num_groups=5
  reader.blt(+sidecar)    a reader cache that materialized only k0..k2 (a
                          15-byte sidecar claiming all ten keys)

0.10.5 does not ship its tests, so the repo's fake_s3.py is loaded by path; it
imports `ebooklet.remote`, which resolves to the installed 0.10.5.
"""
import importlib.util
import pathlib
import pickle
import shutil

import ebooklet
from ebooklet import open_ebooklet

assert ebooklet.__version__ == '0.10.5', f'run under ebooklet 0.10.5, not {ebooklet.__version__}'

here = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('fake_s3', here.parent / 'fake_s3.py')
fake_s3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fake_s3)

out = here / 'legacy_0105'
if out.exists():
    shutil.rmtree(out)
out.mkdir()

store = {}
conn = fake_s3.FakeS3Connection(store, 'legacy')
with open_ebooklet(conn, out / 'writer.blt', flag='n', num_groups=5, n_buckets=101) as eb:
    for i in range(10):
        eb[f'k{i}'] = f'value-{i}'.encode()
    eb.set_metadata({'schema': 'legacy', 'n': 10})
    assert eb.changes().push()

with open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), out / 'reader.blt', flag='r', n_buckets=101) as r:
    for i in range(3):
        assert r[f'k{i}'] == f'value-{i}'.encode()

assert store['legacy'][1]['num_groups'] == '5'
assert store['legacy'][1]['format_version'] == '2'
with open(out / 'store.pkl', 'wb') as f:
    pickle.dump(store, f)
print('wrote', sorted(p.name for p in out.iterdir()))
