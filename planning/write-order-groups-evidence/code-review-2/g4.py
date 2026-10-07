import pathlib, sys, ebooklet
if ebooklet.__version__.startswith('0.10'):
    import importlib.util
    spec = importlib.util.spec_from_file_location('fake_s3', pathlib.Path.home() / 'git/ebooklet/ebooklet/tests/fake_s3.py')
    fake_s3 = importlib.util.module_from_spec(spec); spec.loader.exec_module(fake_s3)
else:
    from ebooklet.tests import fake_s3
d = pathlib.Path('g4') / ebooklet.__version__; d.mkdir(parents=True)
store = {}
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', 'n') as eb:
    eb['a'] = b'1'; print('push 1', eb.changes().push())
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'w.blt', 'w') as eb:
    for i in range(3):
        print('no-op push', i, eb.changes().push(), 'files:', sorted(p.name for p in d.iterdir()))
print(ebooklet.__version__, 'after close:', sorted(p.name for p in d.iterdir()))
