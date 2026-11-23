"""
Targeted benchmark of the paths the `remote.py` refactor touches (classify_db_metadata /
storage_kind_of), for an A/B between two ebooklet trees.

Run it from inside a scratch copy of a tree, so that tree's `ebooklet` package is the one imported:
    cd <copy> && python <this file> --label A
Prints one JSON line of median seconds per operation. The remote is the in-memory fake S3.

  reader_open    S3Connection.open('r'): one HEAD plus the metadata parse (the refactored code)
  storage_kind   the storage_kind property (now delegating to storage_kind_of)
  ebooklet_open  open_ebooklet(conn, cache, 'r') on a warm local cache, then close
"""
import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

import ebooklet
from ebooklet import open_ebooklet
from ebooklet.tests import fake_s3


def timed(fn, n):
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    return time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--label', required=True)
    ap.add_argument('--repeats', type=int, default=5)
    args = ap.parse_args()
    tmp = Path(tempfile.mkdtemp(prefix='bench-open-'))
    store = {}
    conn = fake_s3.FakeS3Connection(store, 'db')
    with open_ebooklet(conn, tmp / 'seed.blt', flag='n', group_bytes=2**20) as eb:
        for i in range(2000):
            eb[f'k{i}'] = b'x' * 200
        assert eb.changes().push()
    with open_ebooklet(fake_s3.FakeS3Connection(store, 'db'), tmp / 'r.blt', flag='r') as eb:
        eb.load_items()
    reader = fake_s3.FakeS3Connection(store, 'db').open('r')

    ops = {
        'reader_open': (lambda: fake_s3.FakeS3Connection(store, 'db').open('r'), 5000),
        'storage_kind': (lambda: reader.storage_kind, 200_000),
        'ebooklet_open': (lambda: open_ebooklet(fake_s3.FakeS3Connection(store, 'db'), tmp / 'r.blt', flag='r').close(), 300),
    }
    out = {'label': args.label, 'ebooklet_file': ebooklet.__file__}
    for name, (fn, n) in ops.items():
        fn()
        times = [timed(fn, n) for _ in range(args.repeats)]
        out[name] = {'n': n, 'median_s': statistics.median(times), 'min_s': min(times), 'max_s': max(times)}
    print(json.dumps(out))


if __name__ == '__main__':
    main()
