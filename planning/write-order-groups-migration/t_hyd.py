import importlib.util, pathlib, pickle, shutil, sys
FIX = pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fixtures/legacy_0105'
spec = importlib.util.spec_from_file_location('fake_s3', pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fake_s3.py')
fake_s3 = importlib.util.module_from_spec(spec); spec.loader.exec_module(fake_s3)
spec = importlib.util.spec_from_file_location('hyd', pathlib.Path.home() / 'git/ebooklet/planning/write-order-groups-migration/hydrate_and_delete.py')
hyd = importlib.util.module_from_spec(spec); spec.loader.exec_module(hyd)
import ebooklet
print('ebooklet', ebooklet.__version__)

def run(store, db_key, local, *extra):
    hyd.connection = lambda args, db_key=None, _s=store, _k=db_key: fake_s3.FakeS3Connection(_s, db_key or _k)
    sys.argv = ['x', '--toml', 'unused', '--table', 'unused', '--local', str(local), *extra]
    try:
        rc = hyd.main()
    except SystemExit as e:
        rc = e.code
    print('->', rc if isinstance(rc, int) or rc is None else str(rc)[:110])
    return rc

def fresh(name):
    d = pathlib.Path('w') / name; shutil.copytree(FIX, d)
    return d, pickle.load(open(d / 'store.pkl', 'rb'))

print('== 1. complete writer file, check only')
d, store = fresh('a'); run(store, 'legacy', d / 'writer.blt')
print('== 2. partial reader cache hydrates, then --delete')
d, store = fresh('b'); run(store, 'legacy', d / 'reader.blt', '--delete'); print('store keys left:', [k for k in store if k.startswith('legacy')])
print('== 3. pending local change aborts')
d, store = fresh('c')
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), d / 'writer.blt', 'w') as eb:
    eb['draft'] = b'x'
run(store, 'legacy', d / 'writer.blt', '--delete'); print('remote still there:', 'legacy' in store)
print('== 4. per-key remote aborts')
store = {}
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'pk'), pathlib.Path('w') / 'pk.blt', 'n') as eb:
    eb['a'] = b'1'; eb.changes().push()
run(store, 'pk', pathlib.Path('w') / 'pk.blt', '--delete'); print('remote still there:', 'pk' in store)
print('== 5. missing values with remote objects gone -> load_items fails -> abort')
d, store = fresh('e')
for k in [k for k in store if k.startswith('legacy/')][:1]:
    del store[k]
run(store, 'legacy', d / 'reader.blt', '--delete'); print('remote still there:', 'legacy' in store)
print('== 6. backup then delete')
d, store = fresh('f'); run(store, 'legacy', d / 'writer.blt', '--delete', '--backup-key', 'legacy-backup')
print('backup present:', 'legacy-backup' in store, '| original gone:', 'legacy' not in store)
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy-backup'), pathlib.Path('w') / 'bk.blt', 'r') as r:
    print('backup readable by 0.10:', len(list(r.keys())), r['k3'])
print('== 7. stale local sidecar (another machine pushed; local stamp ahead) -> abort')
import booklet
d, store = fresh('g')
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), d / 'other.blt', 'w') as eb:
    for i in range(3):
        eb[f'other{i}'] = b'o'
    eb.changes().push()
with booklet.open(d / 'writer.blt', 'w') as b:
    b._set_file_timestamp(int(store['legacy'][1]['timestamp']) + 10**9)
run(store, 'legacy', d / 'writer.blt', '--delete'); print('remote still there:', 'legacy' in store)
print('== 8. format-3 remote -> clean abort')
d, store = fresh('h')
store['legacy'][1]['format_version'] = '3'
run(store, 'legacy', d / 'writer.blt', '--delete'); print('remote still there:', 'legacy' in store)
