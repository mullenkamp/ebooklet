# Plan review — ebooklet 0.10.5: forget a dead remote incarnation at open (and on `delete_remote()`)

## Framing

I am the author of the work below and I am reviewing my own plan. I want an independent,
critical design review before writing any code. Be direct; if the reasoning is wrong, say so and
show why.

ebooklet is a small Python library: a local key-value file (built on `booklet`) that syncs to an
S3-compatible remote. A local file keeps two caches of the remote it was last in sync with: a
`.remote_index` sidecar file (key → claim) and a "remote state" record in a reserved slot of the
local file (manifest of group generations, remote timestamp, metadata section). This is ordinary
data-storage tooling; nothing here targets a third party.

Two scopes are staged: `ebooklet` (the package under review — source, tests, docs, CHANGELOG) and
`cfdb` (its main downstream caller; only `cfdb/edataset.py` is relevant, as context for how
`open_edataset` calls `open_ebooklet` and reads `get_metadata()`). Please do not open any
`s3_config.toml` if one is present — git-ignored local machine configuration, out of scope.

## The defect (found live on 2026-09-07)

A production writer re-opened a local file whose sidecar and remote-state slot described a
PREVIOUS incarnation of the remote database. The remote had been deleted with `delete_remote()` and
rebuilt in the same working directory. At open, the remote was uninitialised (HEAD 404). The open
reused the stale sidecar verbatim, and the next push committed an index that still claimed a key of
the dead incarnation with no backing object. Eight days later, when the dataset grew to that key,
every writer failed with `RemoteIntegrityError` on every run.

Mechanism, verified by reading the code (paths are under `/scratch/work/ebooklet/`):

- `ebooklet/utils.py:295-306` `open_remote_conn` accepts an uninitialised remote for `w`/`c`/`n`.
- `ebooklet/utils.py:309-326` `check_local_remote_sync`: the whole body is guarded by
  `if remote_uuid and flag != 'n'`. An uninitialised remote has `uuid None`, so there is no uuid
  check and `overwrite_remote_index` stays False.
- `ebooklet/utils.py:367-386` `get_remote_index_file` re-fetches only if the sidecar is absent or
  overwrite is set; `fetch_remote_index` (`utils.py:389-409`) returns `(False, None, None)` when
  `not remote_session.initialized`, leaving an existing sidecar file untouched. The only sidecar
  unlink in the package is `ebooklet/main.py:469-470`, on the format-1 `index_fetch_suppressed`
  path, which is disjoint (`v1_remote` requires `initialized`, `main.py:415-416`).
- `ebooklet/main.py:484` loads `RemoteState`; both refresh branches (`main.py:490-501`) are skipped
  when the remote is uninitialised, so a stale manifest/remote_ts/meta_section survive into
  `pre_push_manifest` (`utils.py:1183`) and `_build_meta_section_for_push` (`utils.py:1078`).
- The committed index is built from the live sidecar bytes (`utils.py:1470-1473`, `1538-1547`).
- `delete_remote()` (`ebooklet/remote.py:407-429`) touches no local file and resets only
  `_init_bytes` and `uuid` on the session (not timestamp/type/num_groups/format_version).
- Crash-recovery path that must be preserved: `journal.replace_pending` True on a non-`'n'` open
  (`main.py:435-443`, pinned by `ebooklet/tests/test_journal.py:92`).
- `OfflineSession.initialized` is False (`remote.py:645`) and offline is `'r'`-only
  (`main.py:1535`).
- `cfdb/edataset.py:140` calls `open_ebooklet` with no cache control and decides create-vs-attach
  from `get_metadata()` (`edataset.py:143-147`).

## The proposed fix (ebooklet only)

**New helpers, `ebooklet/utils.py` (above `get_remote_index_file`):**

- `remote_index_sidecar_path(local_file_path)` → `parent / (name + '.remote_index')`; replaces the
  duplicated literal at `utils.py:375` and `main.py:468`.
- `forget_remote_incarnation(local_file, journal, remote_state, reason)`:
  `remote_state.reset(); remote_state.persist(local_file); journal.clear_deletes();`
  if `not journal.meta_pending and local_file.get_metadata() is not None: journal.set_meta_pending(True)`;
  `journal.persist(local_file)`; one `logger.info`. Idempotent. The sidecar file is the caller's job.

**`ebooklet/journal.py`:** `JournalState.clear_deletes()` (same shape as `clear_written`,
`journal.py:150-155`; not `clear_committed`, whose contract is "a commit made these durable");
`RemoteState.reset()` (manifest `{}`, meta_section `None`, remote_ts `None`, dirty).

**`ebooklet/main.py`, `_init_common`:**

1. Move `remote_state = RemoteState.load(local_file)` from `main.py:484` to directly after
   `journal = JournalState.load(local_file)` (`main.py:432`). Move, not copy: a second load after
   the reset would silently reload the stale slot.
2. After the `replace_pending` warning (`main.py:443`) and before the format gate (`main.py:445`):
   ```python
   if (flag != 'r' and not remote_session.initialized
           and (flag == 'n' or not journal.replace_pending)):
       stale = utils.remote_index_sidecar_path(local_file_path)
       if stale.exists():
           stale.unlink()
           logger.info(...)
       utils.forget_remote_incarnation(local_file, journal, remote_state, reason=f"open flag='{flag}'")
   ```
   The unlink precedes `get_remote_index_file` (`:475`) and `open_remote_index` (`:478`), so the
   sidecar is recreated empty; the reset precedes `prev_synced_ts` (`:489`); the deletes replay at
   `:506-508` becomes a no-op.

Flag decisions: `'w'`, `'c'` yes (the incident). `'n'` yes: `init_local_file` already recreated the
booklet (slots fresh, journal fresh with `replace_pending=True`), but the sidecar file survives
because `fetch_remote_index` bails, so pre-push `keys()` lists ghosts today; the replacement purge
at push (`main.py:273-284`) already protected the commit. `'w'/'c'` with `replace_pending` True:
NO change (a `'w'` recovering a crashed `'n'` keeps its sidecar; the purge drops every non-written
entry at push). `'r'`: NO change (readers heal per key through `_resolve_missing`'s `remote_gone`
path, `main.py:1060-1160`; offline sessions must keep their cache).

Journal handling: `deletes` cleared (nothing remote to delete; in grouped mode they would inflate
`affected_group_ids`, `utils.py:1204`); `written` kept (`create_changelog`'s else-branch at
`utils.py:929` pushes every local key when the remote uuid is None anyway); `num_groups` kept (the
journal wins when the remote has none, `main.py:530-537`); `meta_pending` set True when the local
metadata slot is populated, the same mechanism `set_metadata` uses (`main.py:634-639`), so the
recreated remote is not born without metadata (cfdb would otherwise treat it as "no dataset").

**Same-session case, `EVariableLengthValue.delete_remote` (`main.py:1374-1381`):** under
`self._index_lock`, call the session's `delete_remote()`, clear the live `_remote_index` in place
(the purge idiom at `main.py:281-284`), `sync()` it, then `forget_remote_incarnation(...)`. And
`remote.py:426-427` resets all six metadata fields the 404 branch resets (`remote.py:215-221`),
not two. `UUIDMismatchError` is unchanged (still the guard for "recreated from a DIFFERENT local
file": a push stamps the local file's uuid on the new remote, `utils.py:~1551`, so the same local
file re-creating the remote inherits the old uuid).

**Tests, new `ebooklet/tests/test_stale_incarnation.py` (hermetic via `ebooklet/tests/fake_s3.py`):**
(1) the incident in per-key mode, parametrised over `'w'`/`'c'`: reopen shows an empty index and
reset state; write; push; a fresh reader sees exactly `['k1','k4']` (k1 was materialised locally
and re-pushes); `fsck(conn, check_objects=True).claimed_but_missing == []`. Must FAIL on 0.10.4.
(2) the grouped-mode twin asserting the committed manifest shares no `(gid, gen)` with the old one.
(3) the incident's literal shape: local main file removed, sidecar left behind, open `'w'`.
(4) `'n'` after delete lists no ghosts before push. (5) crashed replacement still recovers when the
remote was deleted in between: `'w'` reopen warns, sidecar KEPT, push, reader sees only the new
key. (6) `'r'` and offline `'r'` against a deleted remote unchanged. (7) same-session
delete-then-push. (8) helper units. (9) one `open_rcg` flavour.

**Docs/version:** `0.10.5`; CHANGELOG entry; `docs/ops.md` section on what survives
`delete_remote()` locally.

## How I would like you to work

- **Check the claims, do not take them.** The trace above is what I read; the line numbers were
  verified today against the staged code, but the reasoning about ordering and about what each
  branch does is mine. Re-read anything a conclusion of yours depends on.
- **Please write and run experiments.** You have a writable working copy and can build an
  environment for the `ebooklet` scope with `uv sync` (small stack, tens of seconds). The
  hermetic tests need no network. Mutating the copy to check whether a guard actually fires is
  encouraged. If you prototype the fix to test it, say so and report what the tests then do.
- **Look for what I did not think to check.** The questions below are where I suspect I am weak,
  which is exactly the wrong place to stop.
- **Separate severity.** "This will corrupt data" is a different class from "I would design this
  differently". Both are useful; conflating them is not.
- Do not open `ebooklet/tests/s3_config.toml` or `cfdb/tests/s3_config.toml` if present:
  git-ignored local machine configuration, out of scope.

## Reproduction notes

Run from `/scratch/work/ebooklet` after building its environment.

The hermetic tier (the live-remote modules import a config file that is not staged, so ignore them):

```
uv run pytest --ignore=ebooklet/tests/test_404_integrity.py --ignore=ebooklet/tests/test_map.py \
  --ignore=ebooklet/tests/test_rcg_entries.py --ignore=ebooklet/tests/test_ebooklet.py \
  --ignore=ebooklet/tests/test_delete_safety_live.py --ignore=ebooklet/tests/test_push_integrity.py \
  --ignore=ebooklet/tests/test_scale_push.py -q
```

The defect, reproduced hermetically on the staged 0.10.4 (write this to `repro.py` in the scope
root and run `uv run python repro.py` and `uv run python repro.py 5`):

```python
import sys, tempfile, pathlib, warnings
from ebooklet import open_ebooklet, fsck
from ebooklet.tests import fake_s3

num_groups = int(sys.argv[1]) if len(sys.argv) > 1 else None
tmp = pathlib.Path(tempfile.mkdtemp(prefix='stale-'))
store, db = {}, 'db1'
conn = lambda: fake_s3.FakeS3Connection(store, db)

with open_ebooklet(conn(), tmp / 'a.blt', flag='n', num_groups=num_groups) as eb:
    for k, v in {'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}.items():
        eb[k] = v
    assert eb.changes().push()

with open_ebooklet(conn(), tmp / 'b.blt', flag='w') as eb:
    assert eb['k1'] == b'v1'                       # materialise ONE key only
    print('B sidecar claims before delete:', sorted(eb._remote_index.keys()))

with conn().open('w') as s:
    s.delete_remote()
print('store keys after delete_remote:', sorted(store))

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    with open_ebooklet(conn(), tmp / 'b.blt', flag='w', num_groups=num_groups) as eb:
        print('B sidecar claims at reopen:', sorted(eb._remote_index.keys()),
              '| remote_ts:', eb._remote_state.remote_ts, '| manifest:', eb._remote_state.manifest)
        eb['k4'] = b'v4'
        print('push:', eb.changes().push())

with open_ebooklet(conn(), tmp / 'fresh.blt', flag='r') as eb:
    print('fresh reader keys():', sorted(eb.keys()))
    try:
        print('fresh reader get(k2):', eb.get('k2'))
    except Exception as e:
        print('fresh reader get(k2) RAISED', type(e).__name__)
rep = fsck(conn(), check_objects=True)
print('fsck claimed_but_missing:', rep.claimed_but_missing, '| unmanifested:', rep.unmanifested_group_ids)
```

What I observed on the author's machine, 2026-09-08, per-key mode (`num_groups=None`):

```
B sidecar claims before delete: ['k1', 'k2', 'k3']
store keys after delete_remote: []
B sidecar claims at reopen: ['k1', 'k2', 'k3'] | remote_ts: 1788832756505716 | manifest: {}
push: PushResult(updated=True, failures={})
fresh reader keys(): ['k1', 'k2', 'k3', 'k4']
fresh reader get(k2) RAISED RemoteIntegrityError
fsck claimed_but_missing: ['k2', 'k3'] | unmanifested: []
```

and grouped (`num_groups=5`): the manifest at reopen still named three generations of the deleted
incarnation, and fsck reported two group objects missing. The same shape as production.

## Questions I am least confident about

These are starting points, not boundaries. Questions 1, 2 and 3 would each invalidate a stated
guarantee if answered badly.

1. **Is `journal.replace_pending` the complete crash-recovery discriminator?** Is there any
   legitimate state in which a `'w'`/`'c'` open against an uninitialised remote has
   `replace_pending` False and yet the sidecar or remote-state slot carries something that must
   survive? (For example: a first push that failed mid-way; a `'c'` session; RemoteConnGroup
   entries; a session whose previous push committed the db object but whose local slots were
   never persisted.)
2. **Can `remote_session.initialized` be False for a reason other than "the remote is gone"?** If a
   transient condition can present as a 404 at open, the fix would truncate the committed index to
   local keys instead of committing ghosts. Read `remote.py:193-223` and the s3func session
   behaviour it relies on, and say which failure shapes reach that branch. Is that trade
   acceptable, and is it the same trust boundary `_resolve_missing`'s `remote_gone` path already
   accepts?
3. **Does dropping journaled deletes lose anything?** Consider grouped mode, a delete recorded
   before the remote vanished, and a local file that still holds a materialised value for that
   key. After the reset, does the value re-push (because `create_changelog` pushes every local key
   when the uuid is None)? Is that the documented "local file is the source" semantics or a
   resurrection?
4. **Ordering.** Does moving `RemoteState.load` earlier change anything for the format-1
   (`index_fetch_suppressed`) path or for `refresh_local_metadata` / `reconcile_local_with_index`?
5. **Journaled writes that also appear in the stale sidecar.** A key written locally but not yet
   pushed when the remote was deleted: after the sidecar is unlinked, does it still push correctly
   in both storage modes, and does the read-your-writes gate still hold?
6. **The `meta_pending` rule.** Does flagging pending metadata interact badly with
   `refresh_local_metadata` (`utils.py`), `_resolve_missing`'s `set_metadata(None)` path
   (`main.py:~1127`), or a `'c'` open on a local file that never had metadata? For cfdb, does the
   recreated remote come back with a metadata section such that `open_edataset` attaches rather
   than re-creates?
7. **The same-session `delete_remote()` change.** Clearing the live `_remote_index` in place under
   `_index_lock` while a reader thread of the same object may be mid-`get`: is the existing purge
   idiom safe here, or does it need the handle-swap path `_pull_remote_index` uses?
8. **Readers.** I left `'r'` alone. After the deletion `keys()` still lists ghosts on a warm reader
   until the next fresh ingest. Is there a cheap, safe improvement, or is leaving it the right
   call? Does the offline path (`offline=True`, `'r'` only) have any way to reach the new block?
9. **Tests as the gate.** The tests were designed by the same author as the fix. Which of them
   would still pass with the fix reverted (a vacuous test), and what is missing?
10. **Anything else.** The failure mode you would look for that I have not named.

## Deliverable

A ranked list of findings, most serious first. For each: what it is, why it matters, your
confidence, and concrete evidence — the file and line you read, or the experiment you ran with its
actual output. For every finding you mark verified, state (a) what you actually executed and (b)
what the claim depends on that you did NOT check. State explicitly which claims you are accepting
on trust. Refuting one of my claims with evidence is a valuable outcome, not a failure.

Please put the findings in your reply, not in a file: the working copy is discarded when you finish.

**Please do not report a version number, a timing, or command output you did not actually
observe.** "I reasoned this from the code" is a perfectly good answer and I will weigh it properly;
invented provenance is worse than no provenance. If a tool you expected is not installed, say so.

If part of the design is sound, saying so is useful — but say what you checked to conclude it,
not that it looks reasonable.
