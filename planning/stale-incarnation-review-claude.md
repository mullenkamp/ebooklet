<!-- ai-review-harness
round:      ebooklet-stale-incarnation-1
agent:      claude
requested:  model=claude-opus-5 effort=high
resolved:   model=claude-opus-5
authored-by: claude-fable-5-1
brief:      /tmp/claude-1000/-home-mike-git-forecasts-repos-flow-forecast-app-envlib/99495178-8643-45cc-869e-ff953bea7417/scratchpad/brief-ebooklet-stale-incarnation-1.md
scopes:     ebooklet cfdb
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc
started:    2026-09-08T14:01:26+12:00
-->

<!-- staged cfdb: 13808 KiB of 505184 KiB (491376 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc) -->
<!-- staged ebooklet: 7640 KiB of 227612 KiB (219972 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache s3_config.toml .claude logs site *.nc) -->
<!-- rebuilt from 3 agent turn(s); the report is whichever is substantive -->
I'll start by orienting in the code, then build an environment and run the repro.
Now let me prototype the fix exactly as the plan describes.
## Setup (so you can weigh the evidence)

Built `/opt/envs/ebooklet` with `uv sync` — CPython 3.12.13, `booklet 0.12.10`, `s3func 0.9.6`. Baseline hermetic suite: **129 passed**. I ran your `repro.py` verbatim and reproduced your output exactly in both modes (per-key: `manifest: {}`, `claimed_but_missing: ['k2','k3']`; grouped: manifest naming three dead generations, two missing group objects). The diagnosis is right and the mechanism is exactly as you traced it.

I then **prototyped your fix as written** (new `clear_deletes`/`reset`/`remote_index_sidecar_path`/`forget_remote_incarnation`, the `_init_common` block, the moved `RemoteState.load`, the `delete_remote()` change, the six-field reset in `remote.py`) and attacked it. Suite stayed at 129 passed; the repro became `keys() == ['k1','k4']`, `get('k2') -> None`, `claimed_but_missing: []` in both modes. So the fix does what you designed it to do. The findings below are about what it *also* does.

---

# Findings, most serious first

## 1. CRITICAL — the fix converts a spurious 404 into silent, permanent removal of live remote keys

**Confidence: high (verified, both storage modes, and verified absent on pristine 0.10.4).**

The block unlinks the sidecar, but nothing invalidates `local_file._file_timestamp` — the stamp that `check_local_remote_sync` (`utils.py:318-322`) uses to decide whether to re-fetch. If the 404 was wrong and the remote's timestamp is unchanged, the *next* open sees `remote_session.timestamp > local_file._file_timestamp` → False, and `get_remote_index_file` (`utils.py:390-397`) sees the sidecar file present. **The local file is stranded forever believing the remote's index is empty**, and `create_changelog`'s `remote_session.uuid and remote_index is not None` branch (`utils.py:929`) then treats every local key as new. The commit publishes a truncated index.

Experiment (cold second local file, one spurious HEAD-404 open, nothing else wrong):

```
--- your fix, per-key ---
b sees: ['k1', 'k2', 'k3']
remote object store intact: ['db1', 'db1/k1', 'db1/k2', 'db1/k3']
b AFTER blip keys(): ['k1'] | sidecar: []
push: PushResult(updated=True, failures={})
REMOTE TRUTH after push: ['k1', 'k9']
fsck: ... orphans=['k2','k3'] ... claimed_but_missing=[]
--- your fix, grouped(5) ---
REMOTE TRUTH after push: ['k1', 'k9']
```

Same script on the pristine `/staging` 0.10.4, both modes:

```
b AFTER blip keys(): ['k1', 'k2', 'k3'] | sidecar: ['k1', 'k2', 'k3']
REMOTE TRUTH after push: ['k1', 'k2', 'k3', 'k9']
```

This is the same severity class as the bug you are fixing, with the failure inverted: 0.10.4 commits claims with no objects (loud, `RemoteIntegrityError`); the fix commits objects with no claims (silent, and `fsck` reports it as `orphans`, not as an integrity failure). Note the grouped case is quieter still — `fsck` reported `orphans=[]` there because the old generations are unreferenced but the new manifest was built from `dict(pre_push_manifest)` (empty) updated with `new_gens`, so nothing flags.

**Fix:** invalidate the stamp inside `forget_remote_incarnation` — `local_file._set_file_timestamp(0)`. One line. With it applied, the same experiment reproduces 0.10.4's behaviour exactly (`['k1','k2','k3']`, `REMOTE TRUTH ... ['k1','k2','k3','k9']`) while the real-defect repro still yields `['k1','k4']` / `claimed_but_missing: []`. It is also inert on the legitimate path: `prev_synced_ts` is already `None` after `remote_state.reset()`, so the forced re-fetch's `reconcile_local_with_index` returns `[]` immediately (`utils.py:519-520`), and the push re-stamps at commit (`utils.py:1512`).

- (a) Executed: the cold-copy poison script above against the prototype, against the prototype+stamp-reset, and against `/staging` 0.10.4, per-key and `num_groups=5`; full suite after each.
- (b) Not checked: whether `_set_file_timestamp(0)` is a supported booklet value (I used the same private API `_pull_remote_index` uses at `main.py:1055`; 0 round-tripped correctly here but I did not read booklet's header code). Also not checked: real S3 rather than `fake_s3`.

## 2. HIGH — `journal.clear_deletes()` is the one *irreversible* leg, and it buys nothing

**Confidence: high (verified both directions).**

Everything else `forget_remote_incarnation` drops is re-derivable from the remote (given finding 1's stamp reset). Journaled deletes are not: they exist only in slot 1. On a misfire they are silently cancelled, and the deleted keys come back:

```
pending_deletes at close: ['k2']
during blip: pending_deletes = []
store still has k2 object: True ['db1','db1/k1','db1/k2','db1/k3']
AFTER blip: pending_deletes = []
AFTER blip: keys() = ['k1','k2','k3']   'k2' in eb = True | value: b'k2'
```

(That output is *with* the stamp reset from finding 1 — the healed re-fetch is what makes the resurrection visible. Without the stamp reset the deletion looks honoured but only because the file is stranded, which is finding 1.)

I then simply commented out `clear_deletes()` and re-ran everything. The target repro is **byte-identical** (`['k1','k4']`, `claimed_but_missing: []`, both modes), the suite stays at 129, and the blip case now preserves the intent (`pending_deletes = ['k2']`, `'k2' in eb = False`).

Your stated rationale — grouped mode would inflate `affected_group_ids` (`utils.py:1204-1206`) — is true but harmless once the manifest is reset: the sidecar is empty, so the `for key, remote_val in remote_index.items()` pull loop (`utils.py:1240`) adds nothing, `pre_push_manifest.get(gid)` is `None` so no old generation is pulled or GC'd, and the group either repacks from local keys or lands in `emptied_gids` with nothing to delete. In per-key mode the post-commit `remote_session.delete_objects(list(deletes))` (`utils.py:1583`) targets keys that cannot exist in the new incarnation (the journal keeps `written` and `deletes` disjoint), then `clear_committed` clears them.

**Recommendation: drop `clear_deletes()` from the design entirely.** The stale deletes are self-cancelling no-ops on the legitimate path and load-bearing on the misfire path.

- (a) Executed: delete-intent script (per-key + grouped) with and without `clear_deletes()`; repro both modes; full suite.
- (b) Not checked: a *grouped* push where a stale delete's group id has no local members at all and `emptied_gids` interacts with a concurrent reader — I reasoned that from `utils.py:1382-1386` and `1640-1646`, did not construct it.

## 3. HIGH — the `meta_pending` rule is a sticky latch that clobbers another writer's newer metadata

**Confidence: high (verified end to end).**

`set_meta_pending(True)` is persistent and has no clearing path except a successful push. Once a misfired open sets it, `refresh_local_metadata` (`utils.py:449-450`) refuses to adopt the remote's metadata forever (the read-your-writes gate), and the next push takes the `journal.meta_pending` branch of `_build_meta_section_for_push` (`utils.py:1063-1071`), which *deliberately* advances the local timestamp past the remote's so the stale copy wins:

```
### CASE B: spurious 404 blip
  y meta: {'dataset_type': 'grid', 'v': 1}
  during blip meta_pending: True
  owner published v2
  y sees meta after owner update: {'dataset_type':'grid','v':1} | meta_pending: True
  FRESH reader meta after y pushed: {'dataset_type': 'grid', 'v': 1}
UserWarning: The unpushed local metadata edit carried a timestamp at or before the
remote's; its timestamp was advanced so the edit propagates to readers.
```

The owner's `v2` is gone. Note this also fires on `'c'`/`'w'` opens of any local file with a populated metadata slot against a not-yet-created remote — a much broader trigger surface than the incident.

Your goal (cfdb must attach, not re-create) does not need the latch. I replaced it with a push-time fallback: `_build_meta_section_for_push(..., remote_absent=not remote_session.initialized)`, and when `remote_absent and not replace_pending and remote_state.meta_section is None`, build the section from the local slot. Result:

```
### CASE A: genuine delete_remote then reopen
  meta_pending: False | local meta: {'dataset_type':'grid','v':1}
  FRESH reader meta on rebuilt remote: {'dataset_type':'grid','v':1}   <- cfdb attaches
### CASE B: spurious 404 blip
  y sees meta after owner update: {'dataset_type':'grid','v':2}
  FRESH reader meta after y pushed: {'dataset_type':'grid','v':2}      <- no clobber
```

Suite still 129. This also keeps the `replace_pending` doctrine intact (`utils.py:1092-1096` still wins for `'n'`) and does not touch the "fresh local file first push over a live remote" case the docstring protects, because that case has `remote_session.initialized == True`.

To answer Q6 directly: for cfdb, `create = open_blt.writable and meta is None` (`cfdb/edataset.py:143-147`) is decided from the **local** slot, which survives the deletion either way — so cfdb attaches with or without your latch. The latch is only needed so that *other* local files see a metadata-bearing remote, and the push-time fallback achieves that without persisting anything.

- (a) Executed: the two-case metadata script above, against the latch version and the fallback version; full suite after each.
- (b) Not checked: `set_metadata(None)` semantics through this path (`_resolve_missing`'s `main.py:1127` branch), and cfdb's own tests — I read `edataset.py` but did not run cfdb.

## 4. MEDIUM — the design rule your plan is missing

Findings 1–3 are one rule stated three times: **every effect of forgetting an incarnation must be re-derivable from the remote.** `remote_session.initialized == False` is a *guess*, not a fact, so the block must only discard things a later correct open can rebuild. Sidecar + manifest + `remote_ts`: yes, provided the freshness stamp goes with them. Journaled deletes and `meta_pending`: no. I'd put that sentence in the docstring of `forget_remote_incarnation`, because it's the invariant a future edit will otherwise break.

## 5. MEDIUM — Q2 answered: what actually reaches the `initialized == False` branch, and it is *not* the same trust boundary as `remote_gone`

**Confidence: high on the mechanism, medium on the provider taxonomy (read, not exercised).**

`_load_db_metadata` (`remote.py:193-223`) branches on the literal HTTP status from `head_object`. `s3func`'s `Response.__init__` (`response.py:134`) assigns `self.status = response.status` with no remapping, and `S3Session.head_object` (`s3.py:211-235`) passes the urllib3 response straight through. Anything that is not 200 or 404 **raises** `urllib3.exceptions.HTTPError` from `remote.py:222`. So:

- **Cannot** reach the branch: 403/expired or wrong credentials, 5xx, throttling, transport errors (they raise, or `offline='auto'` catches `TRANSPORT_ERRORS` and produces an `OfflineSession`, which `_check_offline_arg` at `main.py:1535-1540` restricts to `flag='r'` — so offline can never reach your block; that part of Q8 is settled).
- **Can** reach it, meaning "gone" is wrong: `NoSuchBucket` (bucket typo'd, or deleted and pending recreation) returns 404 on a HEAD of a key inside it; a typo'd `db_key`/prefix; an endpoint pointed at the wrong account or region that answers 404; and any S3-compatible provider that returns 404 where AWS would 403 or 5xx.

Your Q2 framing — "the trade is acceptable because it's the same boundary `_resolve_missing`'s `remote_gone` already accepts" — **does not hold as stated.** `remote_gone` (`main.py:1091-1130`) only deletes locally-materialized *values*, which the next successful fetch restores, and it explicitly refuses to touch journal-pending keys. Your block, as drafted, additionally destroys the deletes journal and latches `meta_pending`, neither of which any later open can rebuild. With findings 2 and 3 applied, the boundaries *do* become equivalent and the trade is fine. Placement is otherwise correct: the block sits after the post-lock `remote_session._load_db_metadata()` re-read (`main.py:398`), so the "another writer created the remote while I waited on the lock" race is already closed.

- (a) Executed: read `s3func/response.py`, `s3func/s3.py:211-235`, `remote.py:193-223`, `main.py:355-400`, `main.py:1535-1540`.
- (b) Not checked, accepted on reasoning: real provider status codes. I did not exercise a live endpoint; the "NoSuchBucket → 404" and "403-instead-of-404 without ListBucket" claims are from the S3 API contract, not from an observation here.

## 6. MEDIUM — Q8: the reader story is more broken than you describe, both in-session and across sessions

**Confidence: high (verified).** You wrote that readers "heal per key through `_resolve_missing`'s `remote_gone` path". Values heal. The *key listing never does*:

```
reader keys before:             ['k1','k2','k3']  | k1 = b'k1'
reader keys after delete:       ['k1','k2','k3']
reader get(k2) after delete:    None              <- value healed
reader keys after the heal:     ['k1','k2','k3']  <- still claimed
reader items() ->               ['k1']
reader keys after items():      ['k1','k2','k3']
COLD-REOPENED reader keys:      ['k1','k2','k3']  <- survives close/reopen
```

`_resolve_missing` deletes from `_local_file` only (`main.py:1121-1126`); it never removes the sidecar entry, and `_pull_remote_index` returns early because `fetch_remote_index` bails on `not initialized`. So `keys()` (`main.py:665-678`), `__contains__` (`main.py:762-766`) and `__len__` (`main.py:752-759`) keep the ghosts permanently and **disagree with `items()`** for the rest of that local file's life. Leaving `'r'` alone is still the right call — a reader that unlinked its sidecar on a misfired 404 would be destroying an offline cache with no lock and no writer's confidence — but say so in the docs section, because "readers heal" oversells it. The cheap safe improvement, if you want one, is a `logger.warning` on a `'r'` open where the sidecar is non-empty and the remote is uninitialised.

- (a) Executed: the reader script above, per-key and grouped.
- (b) Not checked: `prune()`/`load_items()` on very large ghost sets, and the `'r'`-open path when the local main file is absent (that raises `RemoteMissingError` at `utils.py:301-302`; read, not run).

## 7. LOW–MEDIUM — Q7: `_index_lock` is the right choice, but it does not cover iteration; neither would the handle swap

**Confidence: high (verified).** `keys()` at `main.py:665-678` iterates `self._remote_index` **without taking `_index_lock`** (contrast `_load_item` at `main.py:1208`, `load_items` at `main.py:873`, `_resolve_missing` at `main.py:1100`). So the in-place purge is safe against point reads and unsafe against a live iterator:

```
first key from live iterator: k1
iterator RAISED RuntimeError : booklet mutated during iteration
point read k5 after delete_remote: None
sidecar now: []
```

The handle-swap path is no better — it calls `self._remote_index.close()` (`main.py:1018`), which is why `Change.pull`'s docstring already carries the "iteration concurrent with a pull may observe a closed index or raise RuntimeError" caveat (`main.py:131-134`). Keep the purge idiom (a loud `RuntimeError` beats a closed handle) and copy that caveat onto `delete_remote()`. Two nits on the purge itself: mirror `clear()`'s `k not in utils.reserved_key_strs` filter (`main.py:1322-1323`) rather than push's unfiltered list, and note that `_index_lock` is an `RLock` (`main.py:600`) so `forget_remote_incarnation`'s `journal.persist` inside it is fine.

## 8. What I checked and found sound

- **Q1 — the `replace_pending` carve-out is correct, and I could not break it.** Crashed `'n'`, remote deleted in between, `'w'` reopen, both modes: warns, sidecar kept (`['k1','k2','k3']`), stale manifest survives into `pre_push_manifest` in grouped mode (`{0:…,1:…,3:…}`) — and never reaches the commit, because `replace_pending` forces `new_manifest = dict(new_gens)` (`utils.py:1531-1532`) and the pre-push purge (`main.py:273-284`) reduces the sidecar to `journal.written`. Rebuilt remote `['brand_new']`, `fsck` clean in both modes. I also confirmed there is no *other* state that must survive: a never-pushed local file cannot hold journaled deletes at all, because `__delitem__` only calls `record_delete` when the key is in `_remote_index` (`main.py:1268-1274`); RCG entries are ordinary keys via `record_write` (`main.py:1535-1536`) with nothing in the sidecar or slot 2; `self.type` is the class literal passed by `__init__` (`main.py:349`), not `remote_session.type`, so your six-field reset in `remote.py` is safe.
- **Q4 — moving `RemoteState.load` is inert.** Nothing between the old and new positions writes slot 2 (`get_remote_index_file`, `fetch_remote_index`, `open_remote_index` never touch `local_file`). And the forget block and the format-1 path are provably disjoint: `v1_remote` requires `remote_session.initialized` (`main.py:415-416`), which is exactly the negation of the block's guard. Verified by construction plus a green suite.
- **Q5 — unpushed writes survive correctly in both modes.** Overwrite of an existing remote key plus a brand-new key, remote deleted, reopen: `journal.written = ['k2','new']`, sidecar `[]`, read-your-writes holds (`k2 = b'EDITED'`), push clean, rebuilt remote `['k1','k2','new']` with `k2 = b'EDITED'`, `fsck` clean.
- **Your `flag='n'` claim is exactly right.** Pristine 0.10.4, both modes: `flag='n'` pre-push `keys()` = `['k1','k2','k3','new']` with sidecar `['k1','k2','k3']` while the commit is protected (`rebuilt: ['new']`); with the fix, `keys()` = `['new']`. One phrasing correction: the manifest at an `'n'` reopen is already `{}` in *both* modes, because `init_local_file` truncates the local booklet and destroys slot 2 — the `'n'` case is a sidecar-only problem.
- **Q3 — no resurrection within a session.** `__delitem__` removes the local value too (`main.py:1279-1280`) and removes the index entry, so a dropped delete cannot resurrect a materialized value in-session. The resurrection is purely cross-session via the re-fetched sidecar — which is finding 2.

## 9. Q9 — the test plan

Reverting the fix, these still pass and gate nothing: **(5)** crashed replacement keeps its sidecar, **(6)** `'r'` and offline unchanged, and **(8)** helper units (they only test new code). Those are worth keeping as over-firing guards, but label them as such — none of them fails on 0.10.4. **(1), (2), (3), (4)** are genuine gates; I confirmed (1)/(2)'s shape fails on 0.10.4 (`claimed_but_missing: ['k2','k3']` per-key; two missing group objects grouped) and (4)'s (`keys()` lists ghosts pre-push on 0.10.4).

The missing tests are all on the misfire side — the entire class of bug in findings 1–3:

- **spurious-404 round trip:** create, cold second local file, one open whose HEAD returns 404 while the store is untouched, reopen normally → the sidecar must be re-fetched, `keys()` must be complete, and a push must not shrink the remote. (Fails on the plan as drafted.)
- **deletion intent survives a misfire:** journal a delete, close, one 404 open, reopen → `pending_deletes` must still hold the key. (Fails on the plan as drafted.)
- **metadata precedence survives a misfire:** owner publishes `v2` between the blip and the reopen → the blipped writer must adopt `v2`, and its push must not republish `v1`. (Fails on the plan as drafted.)
- **grouped twin of test (5)** — your test 5 as written is per-key only; the grouped variant is where the stale `pre_push_manifest` actually exists, and it passes, so it's worth pinning.
- **`fsck` assertion on the orphan side, not just `claimed_but_missing`** — the new failure mode shows up as `orphans`, and in grouped mode not even there.

## 10. Smaller things

- `RemoteState.reset()` sets `_dirty = True` unconditionally, so every `'c'`/`'w'` open against a not-yet-created remote now writes slot 2 and (with the stamp fix) the header. Guard it the way `clear_written` does.
- The docs section should state plainly what the recreated remote contains: **only the keys this local file had materialized**. In the repro `k2`/`k3` are gone forever — correct, but a user re-creating from a cold cache will silently get a near-empty database. This bites `RemoteConnGroup` hardest, where the entries *are* the database and the local file is typically a cold catalogue copy.
- `delete_remote()` + `flag='w'` is documented at `main.py:420-422` as the way to change `num_groups`, but on a `'w'`/`'c'` reopen the journal still pins the old value and a differing `num_groups` raises (`main.py:531-537`). Pre-existing, unchanged by your fix, but the ops doc you're adding is the place to say "delete_remote then `flag='n'`".

## Accepted on trust (not verified here)

The production incident narrative and its eight-day timeline; `s3func`'s behaviour against real S3-compatible endpoints (everything above runs on `ebooklet/tests/fake_s3.py`, whose own docstring lists its fidelity limits — one version per key, no delete markers, no real lock tickets); booklet's tolerance of `_set_file_timestamp(0)` beyond the round-trip I observed; cfdb's runtime behaviour (I read `edataset.py:125-175`, ran nothing); and the live-remote test tier, which I could not run since the config file is not staged.

## Bottom line

The diagnosis is correct, the placement in `_init_common` is correct, the flag matrix is correct, and the `replace_pending` carve-out survived everything I threw at it. Three of the five things the block *does* are safe; two of them — `clear_deletes()` and the `meta_pending` latch — are irreversible, and a third (the sidecar unlink) is only reversible if you also reset the freshness stamp. As drafted, the fix trades a loud integrity failure for a silent one. With the one-line stamp reset, `clear_deletes()` removed, and the metadata carry-forward moved from a journal latch to a push-time fallback, I could not construct a case where it loses anything — and the original repro is still fixed in both storage modes.

<!-- finished: 2026-09-08T14:15:19+12:00 exit=0 -->
