import warnings
from ebooklet import open_ebooklet
from ebooklet.tests.fake_s3 import FakeS3Connection
store = {}
conn = FakeS3Connection(store, 'migrate_key')
with open_ebooklet(conn, 'src.blt', flag='n', num_groups=5) as db:
    for i in range(20):
        db[f'k{i}'] = f'v{i}'.encode()
    db.set_metadata({'meta': 'data'})
    db.changes().push()
print('before:', sorted(store))
store.clear()   # stand-in for delete_remote()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    with open_ebooklet(conn, 'src.blt', flag='w') as db:   # no num_groups passed
        print('resolved num_groups:', db._num_groups)
        r = db.changes().push()
        print('push result:', r)
    print('warnings:', [str(x.message)[:100] for x in w])
print('after:', sorted(store))
with open_ebooklet(conn, 'reader.blt', flag='r') as rd:
    print('keys:', len(list(rd.keys())), 'meta:', rd.get_metadata(), 'vals ok:', all(rd[f'k{i}'] == f'v{i}'.encode() for i in range(20)))
