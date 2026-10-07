import pathlib, tempfile, warnings
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
warnings.simplefilter('ignore')
tmp = pathlib.Path(tempfile.mkdtemp(dir='.')); store = {}
for ng in (5, None):
    key = f'd{ng}'; c = fake_s3.FakeS3Connection(store, key)
    with open_ebooklet(c, tmp/f'{key}.blt', 'n', num_groups=ng) as w:
        w['a'] = b'a1'; w['b'] = b'b1'; assert w.changes().push()
    with open_ebooklet(c, tmp/f'{key}.blt', 'w') as w:
        del w['a']; w['a'] = b'a2'; w.changes().discard()
        w['z'] = b'unrelated'; w.changes().push()
    with open_ebooklet(c, tmp/f'r{key}.blt', 'r') as r:
        print(ng, 'remote keys after unrelated push:', sorted(r.keys()))
