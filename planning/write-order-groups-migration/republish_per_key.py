"""
The second half of moving a hash-grouped (0.10) remote: after hydrate_and_delete.py has hydrated a
local file and deleted the remote, push that SAME file back with ebooklet 0.11, PER-KEY, so the
remote keeps its uuid and stays readable by 0.10 and 0.11 clients alike.

    uv run --no-project --with 'ebooklet==0.11.1' python republish_per_key.py \\
        --toml ~/.envlib/commons.toml --table commons --catalogue \\
        --local ~/.envlib/commons/catalogue-hydrated-20261014.rcg \\
        --baseline catalogue-baseline-20261014.json            # checks only
    ... same ... --push                                          # checks, push, verify

`--baseline` is the JSON written under 0.10.5 before the delete (remote_uuid, and an sha256 per
entry). `--db-key` overrides the toml's db_key (a rehearsal on a scratch copy). Credentials are
never printed.

Refuses (exit 1, nothing pushed) when: not ebooklet 0.11; the remote still exists; the local file's
entries differ from the baseline. `group_bytes=None` is passed explicitly: a file whose journal
recorded hash groups would otherwise republish GROUPED (format 3), which 0.10 readers refuse.
After the push it re-reads the remote into a fresh file and requires: format 2 with no groups, the
baseline uuid, and every entry equal to the baseline.
"""
import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile
import tomllib

import ebooklet

KEYS = ('access_key_id', 'access_key', 'bucket', 'endpoint_url', 'db_key')


def digests(rcg) -> dict:
    return {k: hashlib.sha256(json.dumps(rcg[k], sort_keys=True, default=str).encode()).hexdigest()
            for k in sorted(rcg.keys())}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--toml', required=True)
    p.add_argument('--table', required=True)
    p.add_argument('--db-key', default=None, help='override the toml db_key (a rehearsal on a scratch copy)')
    p.add_argument('--local', required=True)
    p.add_argument('--baseline', required=True)
    p.add_argument('--catalogue', action='store_true', help='the remote is a RemoteConnGroup (open_rcg)')
    p.add_argument('--push', action='store_true')
    args = p.parse_args()
    if not args.catalogue:
        sys.exit('ABORT: only the catalogue (--catalogue) path is implemented.')
    if not ebooklet.__version__.startswith('0.11.'):
        sys.exit(f'ABORT: run under ebooklet 0.11.x (this is {ebooklet.__version__}).')
    with open(pathlib.Path(args.toml).expanduser(), 'rb') as f:
        table = tomllib.load(f)[args.table]
    kwargs = {k: table[k] for k in KEYS}
    if args.db_key:
        kwargs['db_key'] = args.db_key
    conn = ebooklet.S3Connection(**kwargs)
    local = pathlib.Path(args.local).expanduser()
    base = json.load(open(args.baseline))
    print(f'remote : {conn.bucket}/{conn.db_key}')
    print(f'local  : {local}')

    with conn.open('r') as s:
        if s.initialized:
            sys.exit('ABORT: the remote still exists; delete it first (hydrate_and_delete.py --delete). Nothing pushed.')
    print('remote : absent (as required)')

    with ebooklet.open_rcg(conn, local, flag='r', offline=True) as r:
        got = digests(r)
    if got != base['local_entries'] or got != base['remote_entries']:
        sys.exit(f'ABORT: the local file differs from the baseline ({len(got)} entries vs '
                 f'{len(base["remote_entries"])}). Nothing pushed.')
    print(f'local  : {len(got)} entries, identical to the baseline')
    if not args.push:
        print('checks only (no --push): nothing pushed.')
        return 0

    with ebooklet.open_rcg(conn, local, flag='w', group_bytes=None) as rcg:
        result = rcg.changes().push()
    print(f'push   : {result}')

    with tempfile.TemporaryDirectory() as tmp:
        with conn.open('r') as s:
            fmt, groups, uuid_ = s.format_version, getattr(s, 'num_groups', None), str(s.get_uuid())
        with ebooklet.open_rcg(conn, pathlib.Path(tmp) / 'fresh.rcg', flag='r') as r:
            back = digests(r)
    ok = fmt == 2 and groups is None and uuid_ == base['remote_uuid'] and back == base['remote_entries']
    print(f'verify : format {fmt}, num_groups {groups}, uuid {"kept" if uuid_ == base["remote_uuid"] else "CHANGED " + uuid_}, '
          f'entries {"identical" if back == base["remote_entries"] else "DIFFER"} ({len(back)})')
    print('REPUBLISHED' if ok else 'VERIFY FAILED')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
