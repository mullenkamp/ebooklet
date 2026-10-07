import importlib.util, pathlib, sys
exec(open('t_hyd.py').read().split("print('== 1.")[0])
store = {}
w = pathlib.Path('w'); w.mkdir(exist_ok=True)
with ebooklet.open_ebooklet(fake_s3.FakeS3Connection(store, 'm1'), w / 'm1.blt', 'n', num_groups=3) as eb:
    eb['a'] = b'1'; eb.changes().push()
with ebooklet.open_rcg(fake_s3.FakeS3Connection(store, 'cat'), w / 'producer.rcg', 'n', num_groups=13) as rcg:
    rcg.add(fake_s3.FakeS3Connection(store, 'm1'), key='e1'); rcg.changes().push()
## a FRESH local file (the producer's is incomplete in real life) hydrated from the remote
run(store, 'cat', w / 'fresh-cat.rcg', '--catalogue', '--delete', '--backup-key', 'cat-backup')
print('cat gone:', 'cat' not in store, '| backup:', 'cat-backup' in store)
with ebooklet.open_rcg(fake_s3.FakeS3Connection(store, 'cat-backup'), w / 'bk.rcg', 'r') as r:
    print('backup entries:', list(r.keys()))
with ebooklet.open_rcg(fake_s3.FakeS3Connection(store, 'cat'), w / 'fresh-cat.rcg', 'r', offline=True) as r:
    print('hydrated local copy entries (offline):', list(r.keys()), r['e1']['remote_conn']['db_key'])
