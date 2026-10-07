import pathlib, warnings
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
warnings.simplefilter('ignore')
for gb in (None, 64):
    store = {}; conn = lambda: fake_s3.FakeS3Connection(store, 'd'); t = pathlib.Path('w') / f'f3-{gb}'; t.mkdir(parents=True, exist_ok=True)
    with open_ebooklet(conn(), t / 'w.blt', 'n', group_bytes=gb) as eb:
        eb['k'] = b'v1'; eb['other'] = b'o'; eb.changes().push()
    with open_ebooklet(conn(), t / 'w.blt', 'w') as eb:
        del eb['k']; eb['k'] = b'v2'; del eb['k']
        print(gb, 'journal deletes', sorted(eb._journal.deletes), 'written', sorted(eb._journal.written), "'k' in eb:", 'k' in eb)
        print('  push 1:', eb.changes().push())
    with open_ebooklet(conn(), t / 'r1.blt', 'r') as r: print('  fresh reader after push 1: k ->', r.get('k'))
    with open_ebooklet(conn(), t / 'w.blt', 'w') as eb:
        eb['z'] = b'z'; print('  push 2:', eb.changes().push())
    with open_ebooklet(conn(), t / 'r2.blt', 'r') as r: print('  fresh reader after push 2: k ->', r.get('k'))
