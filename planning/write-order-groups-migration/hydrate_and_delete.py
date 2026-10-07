"""
Step 4.2 of the write-order-groups plan: make a local file hold EVERY value of
its hash-grouped (ebooklet 0.10) remote, check it, and only then delete the
remote, so the local file can be republished with ebooklet 0.11.

Runs under the OLD ebooklet, independent of any repo's environment:

    uv run --no-project --with 'ebooklet==0.10.5' python hydrate_and_delete.py \\
        --toml ~/.envlib/commons.toml --table member --member-key wrf-3km-nz-temperature \\
        --local ~/data/wrf/cfdb/wrf_3km_nz_temperature.cfdb            # check only
    ... same ... --delete                                               # check, then delete

The connection is built from one toml table holding S3Connection fields
(access_key_id, access_key, bucket, endpoint_url, db_key); --member-key appends
'/<member-key>' to its db_key (the envlib [member] convention). Credentials are
never printed.

Refuses (exit 1, nothing deleted) when: the remote is not a hash-grouped
format-2 remote (including a format-3 remote 0.10 cannot read); the local file
has pending (unpushed) writes, deletes or metadata; load_items() reports
failures; or any key of the remote's CURRENT index is missing locally or older
locally than on the remote. The index is taken from a fresh GET of the db
object, never from the local sidecar (which can be stale), and the check,
backup and delete all run inside one session holding the write lock, so no push
can land between them.

--catalogue: for the envlib commons catalogue (a RemoteConnGroup). Hydrates it
too (a complete local copy of every entry, kept as a record), then --delete
deletes it. With --backup-key, the remote is first copied server-side to that
db_key in the same bucket - a restore point that 0.10 clients can read.
"""
import argparse
import pathlib
import sys
import tempfile
import tomllib

import booklet
import ebooklet
from ebooklet import utils

KEYS = ('access_key_id', 'access_key', 'bucket', 'endpoint_url', 'db_key')


def connection(args, db_key=None):
    with open(pathlib.Path(args.toml).expanduser(), 'rb') as f:
        table = tomllib.load(f)[args.table]
    missing = [k for k in KEYS if k not in table]
    if missing:
        sys.exit(f'ABORT: [{args.table}] in {args.toml} lacks {missing}')
    kwargs = {k: table[k] for k in KEYS}
    if db_key is not None:
        kwargs['db_key'] = db_key
    elif args.member_key:
        kwargs['db_key'] = f"{table['db_key'].rstrip('/')}/{args.member_key}"
    return ebooklet.S3Connection(**kwargs)


def current_remote_index(session):
    """{key: timestamp} of the remote's CURRENT index, parsed from a fresh GET
    of the db object."""
    resp = session.get_object()
    if resp.status != 200:
        sys.exit(f'ABORT: the GET of the db object failed (status {resp.status}). Nothing deleted.')
    _manifest, _meta, index_bytes = utils.parse_db_payload(resp.data)
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / 'remote_index'
        path.write_bytes(index_bytes)
        with booklet.FixedLengthValue(path) as index:
            return {k: utils.bytes_to_int(v[:7]) for k, v in index.items() if k != utils.metadata_key_str}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--toml', required=True)
    p.add_argument('--table', required=True)
    p.add_argument('--member-key', default=None)
    p.add_argument('--local', required=True, help='the local file to hydrate (and later republish)')
    p.add_argument('--catalogue', action='store_true', help='the remote is a RemoteConnGroup (open_rcg)')
    p.add_argument('--backup-key', default=None, help='copy the remote to this db_key before deleting it')
    p.add_argument('--delete', action='store_true', help='delete the remote after every check passed')
    args = p.parse_args()

    if not ebooklet.__version__.startswith('0.10.'):
        sys.exit(f'ABORT: run under ebooklet 0.10.x (this is {ebooklet.__version__}); 0.11 cannot read hash-grouped remotes.')

    conn = connection(args)
    local = pathlib.Path(args.local).expanduser()
    print(f'remote : {conn.bucket}/{conn.db_key}')
    print(f'local  : {local}')

    try:
        with conn.open('r') as s:
            if not s.initialized:
                sys.exit('ABORT: the remote does not exist (already deleted?). Nothing to do.')
            print(f'format : {s.format_version}  num_groups={s.num_groups}  type={s.type}')
            if s.num_groups is None:
                sys.exit('ABORT: the remote is not hash-grouped (per-key remotes need no move). Nothing deleted.')
    except utils.UnsupportedFormatError as err:
        sys.exit(f'ABORT: ebooklet {ebooklet.__version__} cannot read this remote ({err}). It is not a '
                 'hash-grouped 0.10 remote (a format-3 remote has already moved). Nothing deleted.')

    opener = ebooklet.open_rcg if args.catalogue else ebooklet.open_ebooklet
    with opener(conn, local, flag='w') as eb:
        j = eb._journal
        if j.written or j.deletes or j.meta_pending or j.replace_pending:
            sys.exit(f'ABORT: the local file has pending changes (writes={len(j.written)}, '
                     f'deletes={len(j.deletes)}, metadata={j.meta_pending}, replacement={j.replace_pending}). '
                     'Push or discard them with ebooklet 0.10 first. Nothing deleted.')

        failures = eb.load_items()
        if failures:
            sys.exit(f'ABORT: load_items() failed for {len(failures)} object(s): {sorted(failures)[:10]}. Nothing deleted.')
        eb.sync()

        remote = current_remote_index(eb._remote_session)
        local_ts = {k: ts for k, ts in eb._local_file.timestamps()}
        missing = sorted(k for k in remote if k not in local_ts)
        older = sorted(k for k, ts in remote.items() if k in local_ts and local_ts[k] < ts)
        extra = sorted(k for k in local_ts if k not in remote)
        print(f'keys   : remote index {len(remote)}, local {len(local_ts)}, '
              f'missing locally {len(missing)}, older locally {len(older)}, local-only {len(extra)}')
        if missing or older:
            sys.exit(f'ABORT: the local file is not a complete copy (missing {missing[:10]}, older {older[:10]}). '
                     'Nothing deleted.')
        if extra:
            print(f'NOTE   : {len(extra)} local-only key(s) will be published with the next push: {extra[:10]}')
        meta = eb.get_metadata()
        print(f'meta   : {"present" if meta is not None else "none"}')

        print('CHECK PASSED: the local file holds every value of the remote.')
        if not args.delete:
            print('dry run (no --delete): nothing deleted.')
            return 0

        ## Still inside the checked session: its write lock is what keeps a
        ## push from landing between the check and the delete.
        session = eb._remote_session
        if args.backup_key:
            backup = connection(args, db_key=args.backup_key)
            failed = session.copy_remote(backup)
            if failed:
                sys.exit(f'ABORT: the backup copy reported failures: {failed}. Nothing deleted.')
            print(f'backup : copied to {backup.bucket}/{backup.db_key}')
        eb.delete_remote()
        left = list(session.list_objects().iter_objects())
        gone = session.get_uuid() is None and not left
    print(f'DELETED: remote gone={gone} (objects left: {len(left)})')
    return 0 if gone else 1


if __name__ == '__main__':
    sys.exit(main())
