"""What does the push-time adoption of a remote created by a DIFFERENT local file do today?"""
import pathlib, warnings
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3
warnings.simplefilter('ignore')
d = pathlib.Path('o1'); store = {}
late = open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'late.blt', flag='w')   # remote absent at open
with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'first.blt', flag='n') as first:
    first['a'] = b'A'; first.changes().push()
    first_uuid = first._local_file.uuid
late['b'] = b'B'
print('late push:', late.changes().push())
print('remote uuid now == late file uuid:', store['d'][1]['uuid'] == late._local_file.uuid.hex, '| == first file uuid:', store['d'][1]['uuid'] == first_uuid.hex)
late.close()
try:
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'd'), d / 'first.blt', flag='w') as first:
        print('first writer reopens OK, keys', sorted(first.keys()))
except Exception as e:
    print('first writer reopen raised:', type(e).__name__, e)
