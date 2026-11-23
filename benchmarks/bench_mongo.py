#!/usr/bin/env python3
"""
Benchmarks for ebooklet's MongoDB read path (ebooklet.mongo), with booklet and ebooklet baselines run
on the same keys and values in the same process, so the ratios compare like with like.

The remote is ebooklet's in-memory fake S3, so these time the code's own overhead (MongoDB round
trips, decoding, inserting), not network latency to B2. Run against a local mongod:

    docker run -d --rm --name bench-mongo -p 127.0.0.1:27019:27017 --tmpfs /data/db mongo:8.0
    uv run python benchmarks/bench_mongo.py mongodb://127.0.0.1:27019 [--sizes 1000 10000] [--out FILE]

Run it from a scratch copy, not the repo (it can write a results file). It is a release-step
benchmark, not a CI gate: timings vary by machine, so compare two runs only on one machine, at one
time, with nothing else CPU-heavy running.

Operations (keys/sec unless stated):
  booklet_random_read     booklet's own benchmark op: open a local booklet 'r', read every key once
  booklet_random_miss     the same for absent keys
  ebooklet_warm_read      an ebooklet reader whose local cache holds every value
  mongo_get_warm          a MongoStore with a warm chunk cache, get() per key (2 round trips each)
  mongo_load_warm         a MongoStore, one load_items() for all keys, then get() each (batched)
  mongo_get_miss          get() of absent keys (an entries lookup plus the rev check)
  mongo_load_cold         an empty chunk cache: one load_items() fetching everything from the remote
  mongo_get_cold          an empty chunk cache, get() per key (sampled to at most 1,000 keys)
  store_open              MongoStore construction (stores/sec)
  refresh_initial         first refresh of a dataset (entries/sec)
  refresh_diff_10pct      refresh after a push changing 10% of the values (total time; ops = changed keys)
  refresh_nochange        a poll that finds nothing to do (polls/sec)
"""
import argparse
import json
import random
import statistics
import string
import sys
import tempfile
import time
import uuid
from pathlib import Path

import booklet
import pymongo

from ebooklet import open_ebooklet
from ebooklet.mongo import CHUNKS, MongoStore, ensure_schema, object_reader, refresh
from ebooklet.tests import fake_s3

DB_KEY = 'db'
DS = 'bench'


class Timer:
    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self._start


def fmt(ops):
    if ops >= 1_000_000:
        return f'{ops / 1_000_000:.2f}M'
    if ops >= 1_000:
        return f'{ops / 1_000:.1f}K'
    return f'{ops:.0f}'


def random_value(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length)).encode()


def make_data(n, val_len):
    keys = [f'v!{i * 24}.0' for i in range(n)]          # cfdb-shaped chunk keys
    return keys, {k: random_value(val_len) for k in keys}


def push(store, path, items, group_bytes, flag='w'):
    conn = fake_s3.FakeS3Connection(store, DB_KEY)
    kw = {'group_bytes': group_bytes} if flag == 'n' else {}
    with open_ebooklet(conn, path, flag=flag, **kw) as eb:
        for k, v in items.items():
            eb[k] = v
        assert eb.changes().push()


def new_db(client):
    db = client[f'bench_{uuid.uuid4().hex[:10]}']
    ensure_schema(db, 3600)
    return db


def store_for(db, store):
    return MongoStore(db, DS, object_reader(DB_KEY, http_session=fake_s3.FakeS3Session(store)))


def run_size(client, n, val_len, group_bytes, repeats, tmp):
    keys, values = make_data(n, val_len)
    shuffled = list(keys)
    random.shuffle(shuffled)
    absent = [f'nope!{i}' for i in range(n)]
    sample = shuffled[: min(n, 1000)]
    results = {}

    def record(name, ops, times):
        med = statistics.median(times)
        results[name] = {'ops': ops, 'median_s': med, 'ops_sec': ops / med if med else float('inf'), 'times': times}

    # ── baselines: booklet and an ebooklet reader on the same keys/values ─────────────────────────
    bpath = tmp / f'base_{n}.blt'
    with booklet.open(str(bpath), 'n', key_serializer='str') as b:
        for k in keys:
            b[k] = values[k]
    times = []
    for _ in range(repeats):
        with booklet.open(str(bpath), 'r') as b, Timer() as t:
            for k in shuffled:
                b[k]
        times.append(t.elapsed)
    record('booklet_random_read', n, times)
    times = []
    for _ in range(repeats):
        with booklet.open(str(bpath), 'r') as b, Timer() as t:
            for k in absent:
                b.get(k)
        times.append(t.elapsed)
    record('booklet_random_miss', n, times)

    store = {}
    push(store, tmp / f'seed_{n}.blt', values, group_bytes, flag='n')
    epath = tmp / f'reader_{n}.blt'
    with open_ebooklet(fake_s3.FakeS3Connection(store, DB_KEY), epath, flag='r') as eb:
        eb.load_items(keys)
    times = []
    for _ in range(repeats):
        with open_ebooklet(fake_s3.FakeS3Connection(store, DB_KEY), epath, flag='r') as eb, Timer() as t:
            for k in shuffled:
                eb[k]
        times.append(t.elapsed)
    record('ebooklet_warm_read', n, times)

    # ── refresh ─────────────────────────────────────────────────────────────────────────────────────
    times = []
    for _ in range(repeats):
        db = new_db(client)
        with Timer() as t:
            res = refresh(db, DS, fake_s3.FakeS3Connection(store, DB_KEY))
        assert res['status'] == 'applied' and res['upserted'] == n, res
        times.append(t.elapsed)
        client.drop_database(db.name)
    record('refresh_initial', n, times)

    db = new_db(client)
    refresh(db, DS, fake_s3.FakeS3Connection(store, DB_KEY))
    polls = 20
    times = []
    for _ in range(repeats):
        with Timer() as t:
            for _ in range(polls):
                assert refresh(db, DS, fake_s3.FakeS3Connection(store, DB_KEY))['status'] == 'nochange'
        times.append(t.elapsed)
    record('refresh_nochange', polls, times)

    times = []
    changed = keys[:: 10]
    for r in range(repeats):
        push(store, tmp / f'w_{n}_{r}.blt', {k: random_value(val_len) for k in changed}, group_bytes)
        with Timer() as t:
            res = refresh(db, DS, fake_s3.FakeS3Connection(store, DB_KEY))
        assert res['status'] == 'applied', res
        times.append(t.elapsed)
    record('refresh_diff_10pct', len(changed), times)
    for k in changed:                                   # the values the remote now holds
        values[k] = None
    with open_ebooklet(fake_s3.FakeS3Connection(store, DB_KEY), tmp / f'check_{n}.blt', flag='r') as eb:
        current = {k: eb[k] for k in keys}

    # ── the store ───────────────────────────────────────────────────────────────────────────────────
    opens = 200
    times = []
    for _ in range(repeats):
        with Timer() as t:
            for _ in range(opens):
                store_for(db, store)
        times.append(t.elapsed)
    record('store_open', opens, times)

    times = []
    for _ in range(repeats):
        db[CHUNKS].delete_many({})
        st = store_for(db, store)
        with Timer() as t:
            st.load_items(keys)
        times.append(t.elapsed)
    record('mongo_load_cold', n, times)

    times = []
    for _ in range(repeats):
        db[CHUNKS].delete_many({})
        st = store_for(db, store)
        with Timer() as t:
            for k in sample:
                st.get(k)
        times.append(t.elapsed)
    record('mongo_get_cold', len(sample), times)

    store_for(db, store).load_items(keys)                # warm the chunk cache fully
    times = []
    for _ in range(repeats):
        st = store_for(db, store)
        with Timer() as t:
            for k in shuffled:
                v = st.get(k)
        times.append(t.elapsed)
        assert v == current[k]
    record('mongo_get_warm', n, times)

    times = []
    for _ in range(repeats):
        st = store_for(db, store)
        with Timer() as t:
            st.load_items(keys)
            for k in shuffled:
                st.get(k)
        times.append(t.elapsed)
    assert all(st.get(k) == current[k] for k in keys)
    record('mongo_load_warm', n, times)

    times = []
    for _ in range(repeats):
        st = store_for(db, store)
        with Timer() as t:
            for k in absent:
                st.get(k)
        times.append(t.elapsed)
    record('mongo_get_miss', n, times)

    client.drop_database(db.name)
    return results


ORDER = ['booklet_random_read', 'ebooklet_warm_read', 'mongo_get_warm', 'mongo_load_warm', 'mongo_load_cold',
         'mongo_get_cold', 'booklet_random_miss', 'mongo_get_miss', 'store_open', 'refresh_initial',
         'refresh_diff_10pct', 'refresh_nochange']
VERSUS = {'ebooklet_warm_read': 'booklet_random_read', 'mongo_get_warm': 'booklet_random_read',
          'mongo_load_warm': 'booklet_random_read', 'mongo_load_cold': 'booklet_random_read',
          'mongo_get_cold': 'booklet_random_read', 'mongo_get_miss': 'booklet_random_miss'}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('mongo_uri')
    ap.add_argument('--sizes', type=int, nargs='+', default=[1_000, 10_000])
    ap.add_argument('--val-len', type=int, default=200, help='value size in bytes (booklet bench default: 200)')
    ap.add_argument('--group-bytes', type=int, default=4 * 2**20, help='0 for per-key storage')
    ap.add_argument('--repeats', type=int, default=3)
    ap.add_argument('--out', help='write the results as JSON here')
    args = ap.parse_args()
    random.seed(42)
    group_bytes = args.group_bytes or None
    client = pymongo.MongoClient(args.mongo_uri, tz_aware=True)
    tmp = Path(tempfile.mkdtemp(prefix='bench-mongo-'))
    print(f'ebooklet.mongo benchmarks: sizes {args.sizes}, values {args.val_len} B, '
          f'{"grouped " + str(group_bytes) if group_bytes else "per-key"}, repeats {args.repeats} (median)')
    everything = []
    for n in args.sizes:
        res = run_size(client, n, args.val_len, group_bytes, args.repeats, tmp)
        print(f'\n  n = {n:,}')
        print(f'  {"operation":<22} {"ops/sec":>10} {"median s":>10}  {"vs":<20} {"ratio":>7}')
        for name in ORDER:
            r = res[name]
            base = VERSUS.get(name)
            ratio = f'{r["ops_sec"] / res[base]["ops_sec"]:.3f}' if base else ''
            print(f'  {name:<22} {fmt(r["ops_sec"]):>10} {r["median_s"]:>10.4f}  {base or "":<20} {ratio:>7}')
            everything.append({'size': n, 'op': name, **r})
    if args.out:
        Path(args.out).write_text(json.dumps(everything, indent=1))
        print(f'\nresults written to {args.out}')
    client.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
