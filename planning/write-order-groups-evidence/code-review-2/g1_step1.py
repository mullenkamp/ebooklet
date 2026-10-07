"""Under ebooklet 0.10.5: the migration script's delete - eb.delete_remote() inside the writer session."""
import importlib.util, pathlib, pickle, shutil, sys
import ebooklet
assert ebooklet.__version__ == '0.10.5'
FIX = pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fixtures/legacy_0105'
spec = importlib.util.spec_from_file_location('fake_s3', pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fake_s3.py')
fake_s3 = importlib.util.module_from_spec(spec); spec.loader.exec_module(fake_s3)
w = pathlib.Path('g1'); shutil.copytree(FIX, w)
store = pickle.load(open(w / 'store.pkl', 'rb'))
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'legacy'), w / 'writer.blt', 'w') as eb:
    assert not eb.load_items()
    print('0.10.5 before delete: keys', sorted(eb.keys()), 'journal written', sorted(eb._journal.written), 'remote_ts', eb._remote_state.remote_ts)
    eb.delete_remote()
    print('0.10.5 after delete: remote_ts kept =', eb._remote_state.remote_ts)
print('remote objects left:', [k for k in store if k.startswith('legacy')])
pickle.dump(store, open(w / 'store_after_delete.pkl', 'wb'))
