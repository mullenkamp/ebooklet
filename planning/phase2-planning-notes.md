# Phase 2 planning handoff (written 2026-07-13, pre-compaction)

Restart package for planning/implementing **Phase 2 of the architecture-assessment
roadmap** — the final round before the ebooklet **0.10.0** release. Read this file,
`architecture-assessment-synthesis.md` (roadmap items 13–16), and `envlib/OPEN_WORK.md`
(current state + rulings), then draft the plan below into a reviewable brief.

## Where things stand (round state)

- Phase 1 is FULLY IMPLEMENTED AND VERIFIED: Stage −1 (ebooklet 0.9.6 hotfix,
  committed, NOT on PyPI), Stage 0 (booklet 0.12.7, RELEASED), Stage 1 (s3func 0.9.3,
  RELEASED), Stage 2 (journal + Seam-1, committed `a3d73eb`), Stages 3+4 (generational
  format 2 + fsck, **in the ebooklet working tree awaiting Mike's review/commit** —
  Phase-2 implementation MUST NOT touch ebooklet source until that commit lands).
- Final attestation: ebooklet **138/138 incl. live** via `uv run pytest -n 4 --dist
  loadfile` (8:20; pytest-xdist is a dev dep now; NO test-file restructuring — Mike's
  ruling; revisit only if one file dominates, likely test_push_integrity.py);
  cfdb **217/217** + envlib **143/143** on the format-2 overlay.
- **Release ruling (Mike, 2026-07-13): 0.10.0 releases only after Phase 2** (one
  release, per the original Phases-1+2-together ruling); the changelog's dated
  `## 0.10.0 (2026-07-13)` heading stays.
- **Review shape (Mike): dual-blind design review of the Phase-2 plan** — standing
  protocol: ONE shared neutral brief file in this repo (`phase2-design-brief.md`,
  modeled on `phase1-design-brief.md`), Claude = fresh-context Opus subagent (blind;
  /tmp-only writes; never open any tests/s3_config.toml; no live S3; may run
  experiments via `ebooklet/tests/fake_s3.py`), Gemini = Mike-driven `agy` on the same
  brief ("I'm the author reviewing my own plan" opener); author synthesizes;
  single-sourced CRITICALs get author re-verification; post-run `git status` audits of
  all five repos; both reports preserved in-repo as `phase2-review-{claude,gemini}.md`.
- Verification bar (unchanged): pre-fix proofs on a pre-change worktree for behavior
  changes; hermetic tier via fake_s3; full live suite (parallel) at round close;
  downstream cfdb+envlib on `--with` overlays with in-overlay version asserts
  (`uv run --frozen --with /home/mike/git/ebooklet --reinstall-package ebooklet ...`);
  sequential uv only (other repos' suites can run via their own `.venv/bin/python`).

## The Phase-2 plan draft (presented to Mike 2026-07-13; he approved proceeding to
## the dual review after compaction. BOTH former ⚖ items are now RULED — do not
## re-ask, and the brief should present them as settled with their rationale)

1. **Exception taxonomy** (~0.5d). New `ebooklet.Error` base; typed exceptions with
   LEGACY PARENTAGE KEPT via dual inheritance: `UUIDMismatchError(Error, ValueError)`
   for the two 'UUID' ValueErrors (utils.py L311 local/remote mismatch; remote.py
   L745 http/S3 mismatch); `ReadOnlyError(Error, ValueError)` for the ~15
   'read only'/'not writable' guards; `RemoteMissingError(Error, ValueError)` for
   "remote doesn't exist" (utils.py L293 open-read; main.py L1272 RCG add);
   re-parent `UnsupportedFormatError` + `GroupTooLargeError` under `Error` (keep
   ValueError); `RemoteIntegrityError` gains `Error`, KEEPS HTTPError parentage
   (Mike's prior ruling); NEW `LockLostError(Error)` for push-boundary verify
   failures (deliberately NOT HTTPError — not connectivity; envlib's offline catch
   is HTTPError-based); lock-acquire timeout stays builtin TimeoutError. Full raise
   inventory was collected (see "recon findings" below for consumer-facing sites;
   re-grep `raise ` in main/utils/remote at implementation for the sweep).
2. **MutableMapping/API pass** (~1d incl. test churn). `__delitem__` of a missing key
   raises KeyError (today: silent no-op, main.py ~L1068-1090); `update()` accepts
   mapping/iterable-of-pairs/kwargs (today: single mapping only, ~L663-671); dead
   `force_shutdown` params on close/sync removed (~L1121/L1133 — verify no callers
   in cfdb/envlib first); `Change.update()` renamed `build_changelog()` (name
   collision; only called internally by push/iter_changes — verified no external
   callers). **RULED (Mike, 2026-07-13): `clear()` becomes a TRUE clear** —
   journaled deletion of every key, applied at the next push, cancellable via
   discard() until then; cache eviction's replacement idiom is
   `prune(timestamp=now)` (pending writes journal-protected); NO new evict()
   method (minimal API). **RULED (Mike, 2026-07-13): `flag='n'` stays AS-IS — no
   `replace_remote=True` guard.** Rationale (record in the brief so reviewers do
   not relitigate): dbm's 'n' contract IS replacement — a confirmation parameter
   would make the flag not do what it says, the inverse of the clear() dishonesty
   being fixed; the assessment's real Seam-1 objection (implicit, stateful,
   non-atomic replacement) was fixed structurally in Phase 1 (journaled intent +
   loud warnings + atomic upload-then-commit; nothing is destroyed until a
   replacement push fully succeeds). This settles the synthesis's open Seam-1 API
   choice as "neither methods nor kwarg". Ops docs get: "use 'c' unless you mean
   to replace" + the nothing-lost-until-commit recovery note.
3. **`push()` → `PushResult`** (msgspec Struct: `updated: bool`, `failures: dict`,
   `__bool__` = updated-and-no-failures). Clean object, NOT a dict subclass —
   consumers migrate in the SAME sweep (they are only ours): envlib
   `_raise_on_push_failure` reads `.failures` (+ WRAP the currently-unwrapped
   `register()` push, catalogue.py L826); cfdb only documents/passes through
   (edataset.py L44-51 — also fix its docstring drift: says "list", code returns
   dict). Mechanical churn: every `push() is True` assertion in all three repos'
   tests becomes truthiness/explicit checks (sed-able).
4. **Offline read mode** (~0.5–1d). `open_ebooklet/open_rcg(..., offline=False|'auto'|True)`:
   True = never touch the remote (local files only; error if absent); 'auto' = fall
   back to local on CONNECTIVITY errors only (integrity/format/uuid errors still
   raise — the distinction envlib hand-rolls today); remote-requiring ops under
   offline raise typed `OfflineError(Error)`. envlib `refresh()` then retires
   `_read_cached_index` (catalogue.py L977-1005) in favor of
   `open_rcg(..., offline='auto')` and switches its dispatch to typed exceptions —
   FIXING a real bug: its `except ValueError` (L581-599, the 'UUID' in str(err)
   string-match) would swallow `UnsupportedFormatError` and mislabel it "RCG not
   readable yet". Design detail for the plan: how open constructs the offline
   session (stub session whose remote ops raise OfflineError; `initialized` False;
   check_local_remote_sync skipped) — reviewers should stress the read paths
   (unmaterialized keys under offline → clean typed error, not KeyError).
5. **Ops docs** (~0.5d). `docs/ops.md`: recovery recipes out of changelog prose —
   partial-failure retry, force_push after a failed commit, fsck usage, force_lock
   (2h age gate), the v1→v2 upgrade recipe, RemoteIntegrityError triage.
6. **Downstream riders (same round)**: envlib+cfdb pins → `ebooklet>=0.10.0`
   (+ lock refreshes AFTER the release: expect uv's stale-index trap —
   `uv sync --refresh-package ebooklet`); changelogs everywhere; envlib CHANGELOG
   0.1.1 section already says "companion to ebooklet 0.9.6" — update to 0.10.0.

## Key recon findings (verified 2026-07-13; line refs current as of that recon)

- **cfdb** (`cfdb/edataset.py`): push contract ONLY in the docstring (L44-51,
  delegates L51); NO isinstance(dict) check anywhere in cfdb; NO typed ebooklet
  exception handling anywhere (only `except BaseException: close(); raise` at L153
  in open_edataset). So the PushResult migration in cfdb = docstring + passthrough;
  exception taxonomy = optional adoption, nothing breaks.
- **envlib** (`envlib/catalogue.py`): `_raise_on_push_failure` L60-71
  (isinstance-dict → RuntimeError), call sites L808/L885/L909, UNWRAPPED push at
  L826 (register); uuid string-match L581-599; `_OFFLINE_ERRORS = (HTTPError,
  ConnectionError, TimeoutError)` L57 with the RemoteIntegrityError re-raise L601;
  `_read_cached_index` L977-1005 (direct booklet.open of the RCG local file).
- **ebooklet push() returns**: `Change.push` — partial dict return ~L217-221; True/
  False return ~L240/L247; docstring drift at ~L173 ("list" vs dict). Pre-push
  guards: ValueError (not-writable ~L177/L180), HTTPError (lock lost ~L187).
- **Unreachable-remote path** (for offline mode): open_ebooklet → check_remote_conn
  → utils.open_remote_conn → remote_conn.open(flag) → S3SessionReader.__init__ →
  `_load_db_metadata()` → head_object → urllib3 MaxRetryError/NewConnectionError
  (HTTPError subclasses) propagate; non-200/404 raises HTTPError at remote.py ~L222.
  Same choke point for every `_pull_remote_index`.
- Deferred implementation-time verifications: does cfdb ever `del` a possibly-absent
  key (KeyError change impact); any callers passing `force_shutdown`; any
  cfdb/envlib `clear()` callers.

## Suggested restart sequence (post-compaction)

1. Confirm Mike has committed the Stage 3+4 ebooklet diff (`git -C ~/git/ebooklet
   status` clean apart from this file and `phase2-*`); do not start Phase-2 edits
   before that.
2. Turn the plan draft above into `phase2-design-brief.md` (embed the plan; neutral
   wording; the standing ground rules + the ⚖ rulings marked as open questions for
   Mike at approval, unless he has ruled by then).
3. Get Mike's plan approval (+ the two ⚖ rulings) → launch the Claude reviewer
   (background) → hand Mike the agy opener → synthesize → implement → verify per the
   bar above → release close-out (0.10.0 to PyPI by Mike; then lock refreshes,
   OPEN_WORK/cross-project updates, the v1-remote upgrade + fsck live proof
   reminders, envlib 0.1.1 staging final check).
