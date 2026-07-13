# ebooklet Phase 2 — independent design review brief (pre-implementation)

I'm the author, reviewing my own implementation plan before writing any code, and I want it
independently stress-tested. You are a fresh-context reviewer performing a **critical design
review** of the plan below. Your job is NOT to summarize or to agree: verify the plan's claims
against the real source, probe for failure modes, and hunt for what the plan did **not**
consider. The numbered questions at the end are starting points, not the boundary — the most
valuable findings are the ones not on any list here.

## Ground rules

- Repos in scope (read them broadly — dependency source, callers, related modules):
  `~/git/ebooklet` (the subject, at commit `99ad450` — Phase 1 has landed; the tree is
  0.10.0-unreleased), `~/git/booklet` (local KV store, 0.12.7), `~/git/s3func` (S3 HTTP layer
  + distributed lock, 0.9.3). This round the two consumers are **part of the subject**, not
  just context: `~/git/cfdb-repos/cfdb` and `~/git/envlib-repos/envlib` migrate in the same
  sweep (their changes are in the plan).
- **Do not modify, create, or delete any files in any repo.** Throwaway scripts and
  experiments under `/tmp` are encouraged — booklet is pure-local (exercise it directly), and
  `ebooklet/tests/fake_s3.py` is an in-memory S3 session fake that drives the real ebooklet
  remote/push code with no credentials (see `test_push_integrity.py` for usage; push idiom is
  `eb.changes().push()`).
- Do not open machine-local git-ignored config files (e.g. `tests/s3_config.toml` in any
  repo — routine local test configuration, out of scope for this review). No network calls;
  no live S3.
- Background documents in `~/git/ebooklet`: `architecture-assessment-synthesis.md` (the
  adopted plan-of-record; this phase implements roadmap items 13–16), `CHANGELOG.md` (the
  0.10.0 section records what Phase 1 already changed), `phase1-design-brief.md` +
  `phase1-review-*.md` (the previous round, for process context only).

## Fixed constraints (rulings — verify the plan honors them; do not propose alternatives)

1. **One release**: Phases 1+2 ship together as ebooklet **0.10.0** — this is the final round
   before release. Public-API breaks are in scope *here* (this IS the API pass). booklet and
   s3func do **not** change this round.
2. Standing invariants: push is always explicit (nothing auto-pushes on close; `sync()` is
   local-only); no sidecar files; single writer (S3 bakery lock), lock-free readers; new
   ebooklet-owned formats use msgspec.
3. **RULED — `clear()` becomes a TRUE clear**: journaled deletion of every key, applied at
   the next push, cancellable via `discard()` until then. Local cache eviction's replacement
   idiom is `prune(timestamp=now)` (journal-pending writes are already prune-protected). NO
   new `evict()` method (minimal API). Do not propose an evict method or a confirmation
   parameter — review the *mechanics*, not the ruling.
4. **RULED — `flag='n'` stays as-is; no `replace_remote=True` confirmation guard.**
   Rationale (recorded so it isn't relitigated): dbm's 'n' contract IS replacement — a
   confirmation parameter would make the flag not do what it says, the inverse of the
   `clear()` dishonesty being fixed by ruling 3. The structural danger was fixed in Phase 1:
   replacement intent is journaled and survives sessions, reopens warn loudly, and the
   replacement push is upload-then-commit-then-sweep — nothing is destroyed until a
   replacement push fully commits. This settles the synthesis's open Seam-1 API choice as
   "neither methods nor kwarg". Ops docs get "use 'c' unless you mean to replace".
5. New exceptions keep legacy parentage via dual inheritance for this release (consumers
   migrate in the same sweep, but dual parentage keeps existing `except ValueError` /
   `except HTTPError` code working); `RemoteIntegrityError` KEEPS its HTTPError parentage.
6. No test-file restructuring (the pytest-xdist parallel run layout stays).
7. Previously assessed and dismissed (do not re-propose): CAS/conditional-write commit
   protocols; content-addressed objects; segmented/delta index; multi-writer merge semantics;
   sidecar files in any role; lock heartbeats.

Items marked ⊕ go beyond the adopted plan-of-record (author elaborations from a design
pass) — give those extra scrutiny; they have had less review than the rest.

---

# THE PLAN UNDER REVIEW

## Context

Phase 2 is the pre-1.0 API-surface pass, roadmap items 13–16: exception taxonomy,
MutableMapping/API conformance, `push()` → `PushResult`, offline read mode, ops docs, plus
the downstream cfdb/envlib migration in the same sweep. All file:line refs verified against
ebooklet `99ad450`, and the current cfdb/envlib trees, on 2026-07-13.

Downstream blast radius already verified (source AND tests of both consumers):
- `force_shutdown`: zero callers outside ebooklet.
- `__delitem__`-of-missing → KeyError: cfdb's four `del self._blt[...]` sites
  (`cfdb/main.py:98,104`, `cfdb/support_classes.py:1173,1209`) are all inside
  `try/except KeyError: pass`; envlib's one `del rcg[...]` (`catalogue.py:884`) is
  guaranteed-present (checked at L850-853).
- `clear()` / `update()`: zero downstream callers on ebooklet objects.
- push-return consumers: envlib `catalogue.py` (`_raise_on_push_failure` L60-71; call sites
  L808/L885/L909; one unwrapped push L826), cfdb `edataset.py:44-51` (docstring +
  passthrough), and 8 cfdb test assertions (`test_edataset.py:118, 221-222, 344, 356, 379,
  401, 432, 499` — `is True` / `is False` / truthiness).

## Item 1 — Exception taxonomy (~0.5d)

New module `ebooklet/errors.py`, everything re-exported at the package root:

- `Error(Exception)` — base for all ebooklet exceptions.
- `UUIDMismatchError(Error, ValueError)` — the two 'UUID' ValueErrors: `utils.py:311`
  (local file vs remote) and `remote.py:745` (http conn vs S3 conn).
- `ReadOnlyError(Error, ValueError)` — the ~15 'read only'/'not writable' guards across
  main/utils.
- `RemoteMissingError(Error, ValueError)` — "remote doesn't exist": `utils.py:293`
  (flag='r' open with neither remote nor local) and `main.py:1272` (RCG add).
- `UnsupportedFormatError` (today `utils.py:61`) and `GroupTooLargeError` (today
  `utils.py:71`) re-parent to `(Error, ValueError)`; `RemoteIntegrityError` (today
  `main.py:36`) re-parents to `(Error, urllib3.exceptions.HTTPError)` — HTTPError parentage
  KEPT (ruling). Classes move to `errors.py`; old attribute paths (`utils.UnsupportedFormatError`
  etc.) keep working via aliases.
- NEW `LockLostError(Error)` for the push-boundary lock-verify failures — the push-start
  verify (`main.py:183-192`, today a placeholder `urllib3.exceptions.HTTPError` raise) and
  the pre-commit verify inside `utils.update_remote`. **Deliberately NOT an HTTPError**: a
  lost lock is not connectivity, and downstream connectivity handling (envlib's
  `_OFFLINE_ERRORS`) must never treat it as such. The placeholder has never shipped
  (0.10.0-unreleased), so this re-typing breaks nobody.
- `OfflineError(Error)` — see Item 4.
- Lock-*acquire* timeout stays the builtin `TimeoutError` (it already communicates the right
  thing and is what callers catch today).
- Implementation includes a full `raise` re-grep sweep of `main.py`/`utils.py`/`remote.py`/
  `fsck.py`, classifying every consumer-facing raise into the taxonomy (internal
  assertion-style raises may stay builtin).

## Item 2 — MutableMapping/API pass (~1d incl. test churn)

1. `__delitem__` of a missing key raises `KeyError` (`main.py:1068`; today a silent no-op).
   Missing = not in the local index view (after Phase 1's journal-delete replay). Deleting a
   key that exists remotely but is locally unmaterialized still works (index entry present).
2. `update()` implements the full MutableMapping contract — mapping OR iterable-of-pairs OR
   kwargs (`main.py:663`; today: single positional mapping only). Reserved-key validation
   still applies to every incoming key.
3. Dead `force_shutdown` params removed from `close()`/`sync()` (`main.py:1121/1133`; zero
   callers verified in all repos).
4. `Change.update()` renamed `build_changelog()` (`main.py:89`) — name collision with
   MutableMapping.update; verified only called internally (push / iter_changes).
5. `clear()` becomes a true clear (ruling 3). Today (`main.py:1097-1118`) it is documented
   local cache eviction: booklet ftruncate + journal/remote-state re-persist + metadata
   restore. ⊕ New mechanics: snapshot all keys from the index (snapshot first — booklet
   raises on mutation during iteration) → add ALL to `journal.deletes`, clear
   `journal.written` and `meta_pending`, single `journal.persist` → remove the local index
   entries (Phase 1's replay-on-swap then makes the clear resurrection-proof across
   re-pulls) → truncate local value blocks as today (re-persisting the reserved slots in the
   same order today's code does). After: `len()==0`, `keys()` empty; `discard()` cancels
   (journal.deletes cleared + forced index re-pull restores entries — the existing
   discard-a-journaled-delete path, applied in bulk); next push applies the deletions
   (emptied groups drop from the manifest; the commit PUT flips readers atomically to an
   empty db).

## Item 3 — `push()` → `PushResult`

msgspec Struct (clean object, NOT a dict subclass):

```python
class PushResult(msgspec.Struct):
    updated: bool          # the remote actually changed (grouped: the commit PUT happened)
    failures: dict         # key/group → error description; empty = no failures
    def __bool__(self): return self.updated and not self.failures
```

- Legacy mapping: `True` → `(updated=True, {})` truthy; `False` (no-op) →
  `(updated=False, {})` falsy; partial-failure dict → `failures=<the same dict content>`,
  `updated=False` in grouped mode (a partial phase-B failure commits NOTHING — readers still
  see the old state) ⊕ / `updated=True` in per-key mode iff any object changed — falsy either
  way. ⊕ Deliberate semantic fix to document: the legacy partial dict was *truthy* (non-empty
  dict), so `if push():` misread partial failure as success; any failure is now falsy.
- Constructed at the `Change.push` boundary (`main.py:172-247`); `utils.update_remote`'s
  internal returns stay as-is and are adapted at the boundary. Fixes the docstring drift at
  `main.py:174` ("list" vs dict).
- Consumers migrate in the same sweep (Item 6); they are all ours.

## Item 4 — Offline read mode (~0.5–1d)

`open_ebooklet(..., offline=False)` and `open_rcg(..., offline=False)` with
`offline ∈ {False, 'auto', True}`:

- ⊕ `offline != False` requires `flag='r'` (offline is a READ mode; anything else raises
  `ValueError` at open). The session exposes an `.offline` property (bool).
- `offline=True`: never touch the remote. No remote session is constructed; a stub session
  object satisfying the reader-session interface is installed whose every remote-touching
  method raises `OfflineError`; `initialized` is False; `check_local_remote_sync` and the
  open-time `_load_db_metadata` are skipped entirely. The local file must exist — if absent,
  raise `OfflineError` ("offline=True and no local file at ..."). Reads of locally
  materialized keys serve normally (journal honored as usual); reads of **unmaterialized**
  keys (index entry present, value not local) raise `OfflineError` naming the key — NOT
  `KeyError`, which would falsely claim the key doesn't exist (`in`/`keys()` still report
  it; documented).
- `offline='auto'`: attempt a normal online open; on CONNECTIVITY failure only, fall back to
  the offline=True path ⊕ with a `UserWarning` ("remote unreachable; serving local data").
  Connectivity = transport-level failures (DNS/connect/timeout: urllib3
  MaxRetryError/NewConnectionError/ConnectTimeout/ReadTimeout/ProtocolError, OS
  ConnectionError). **Never** falls back on: `RemoteIntegrityError` (an HTTPError subclass —
  the classifier must check typed ebooklet errors FIRST), `UUIDMismatchError`,
  `UnsupportedFormatError`, or HTTP *status* errors (403/500 etc. — a misconfigured
  credential must raise, not silently serve stale data). This is exactly the distinction
  envlib hand-rolls today (`catalogue.py:57` + the RemoteIntegrityError re-raise at ~L601).
- Unreachable-remote choke point (for the classifier): `open_ebooklet` →
  `utils.open_remote_conn` → `remote_conn.open(flag)` → `S3SessionReader.__init__` →
  `_load_db_metadata()` → head_object → urllib3 transport errors propagate; non-200/404
  raises HTTPError at `remote.py:~222`. Same choke point for every `_pull_remote_index`.
- Scope note: 'auto' classifies at OPEN time; a connectivity loss mid-session (e.g. a lazy
  value fetch) raises normally — no silent mid-session degradation.

## Item 5 — Ops docs (~0.5d)

New `docs/ops.md`, pulling the recovery recipes out of changelog prose: partial-failure
retry; `force_push` after a failed commit; fsck usage (report vs delete_orphans + the age
gate); `force_lock` (2h age gate) and lock triage; the v1→v2 upgrade recipe; the 'n'-flag
guidance ("use 'c' unless you mean to replace" + nothing-lost-until-commit recovery);
`RemoteIntegrityError` triage. README points at it.

## Item 6 — Downstream migration (same round; commits ride each repo's own release)

**envlib** (`envlib/catalogue.py`):
- `_raise_on_push_failure` (L60-71) reads `result.failures` instead of isinstance-dict;
  call sites L808/L885/L909 unchanged otherwise. ⊕ WRAP the currently-unwrapped `register()`
  push at L826 with the same check.
- `refresh()` bootstrap dispatch (L578-605): the `except ValueError` + `'UUID' in str(err)`
  string-match becomes typed — `UUIDMismatchError` → the identity-mismatch warning;
  `RemoteMissingError` → the bootstrap "not readable yet" warning; anything else
  **propagates** — this FIXES a real latent bug: today `UnsupportedFormatError` (a
  ValueError) would be swallowed and mislabeled "RCG not readable yet".
- The offline-fallback path switches to `open_rcg(..., offline='auto')`;
  `_read_cached_index` (L979-1005, direct booklet read of the RCG local file) retires;
  `_OFFLINE_ERRORS` (L57) and its RemoteIntegrityError re-raise dance retire (the
  distinction now lives in ebooklet's 'auto' classifier).
- Changelog: the 0.1.1 section's "companion to ebooklet 0.9.6" → 0.10.0; pin
  `ebooklet>=0.10.0`; test churn for push-return mocks/asserts.

**cfdb**: `edataset.py` push docstring (L44-51) corrected to PushResult (the passthrough
code is unchanged); the 8 `test_edataset.py` assertions become explicit
(`result.updated is True/False`, `not result.failures`); pin `ebooklet>=0.10.0`; changelog.
Typed-exception *adoption* in cfdb: deliberately NOT this round (verified: cfdb has no
ebooklet exception handling beyond `except BaseException: close(); raise` at
`edataset.py:153`, which composes fine).

Lock refreshes (`uv sync --refresh-package ebooklet`) happen only AFTER the PyPI release.

## Verification plan

- **Pre-fix proofs on a `99ad450` worktree**: missing-key `del` is a silent no-op;
  `update()` rejects kwargs/iterable-of-pairs; `clear()` leaves keys re-pullable (eviction);
  `push()` returns True/False/dict and a non-empty partial dict is truthy; unreachable
  remote hard-fails `open_ebooklet`; the envlib swallow bug (UnsupportedFormatError
  mislabeled "not readable yet").
- **Hermetic tier via `fake_s3.py`**: taxonomy raise sites; KeyError semantics; update()
  forms; true-clear E2E (clear → push → fresh reader sees empty; clear → discard restores);
  PushResult mapping incl. partial-failure falsiness (fake_s3 fault injection); offline=True
  open/read/unmaterialized-key paths; 'auto' classification (injected transport error →
  fallback+warning; injected integrity/format error → raise).
- **Round close**: full ebooklet live suite `uv run pytest -n 4 --dist loadfile`; cfdb
  (217+) and envlib (143+) suites on `uv run --frozen --with ~/git/ebooklet
  --reinstall-package ebooklet ...` overlays with in-overlay version asserts.

---

# Specific review questions (starting points — go beyond them)

1. **Raise-inventory completeness**: sweep every `raise` in `main.py`/`utils.py`/
   `remote.py`/`fsck.py` and check the classification — which consumer-facing raises did the
   taxonomy miss? Conversely, which *internal* `except ValueError`/`except HTTPError` blocks
   inside ebooklet itself would now catch (or stop catching) the re-typed errors?
2. **Dual inheritance**: any MRO or `except`-resolution surprise with
   `(Error, ValueError)` / `(Error, HTTPError)` bases? Is moving the three existing classes
   to `errors.py` with aliases at their old paths import-cycle-safe
   (main↔utils↔remote all import each other's exceptions today)?
3. **LockLostError is not HTTPError**: audit every catch site in all five repos for
   HTTPError handling on PUSH paths that would have caught the Phase-1 placeholder raise
   (`main.py:186-192`) and would now miss `LockLostError`. Is there any path where a
   lost-lock raise escaping an HTTPError catch changes behavior for the worse?
4. **`__delitem__` KeyError**: does anything inside ebooklet rely on delete-of-missing being
   silent — the flag='n' purge (`main.py:146-155` area), `Change.discard`, the journal
   delete replay, RCG delegation? Do the MutableMapping-derived methods (`pop`, `popitem`,
   `setdefault`) behave correctly after the change (e.g. `pop(key, default)` must not leak
   the KeyError)?
5. **`update()` signature**: exact MutableMapping/dict semantics (positional-only `other`?);
   kwargs whose names collide with reserved key strings still get rejected?
6. **`clear()` mechanics** (the ruling is fixed; the mechanics are not): pressure-test the
   journal transition — (a) unpushed WRITES at clear time (written keys become deletes; is
   dropping their pending values correct-and-loud enough?); (b) interaction with
   `replace_pending` sessions (clear inside an unpushed 'n' session); (c) per-key mode;
   (d) discard-after-clear (does the bulk index re-pull actually restore every entry, given
   the ts-gated pull?); (e) reserved-slot re-persist ordering vs today's `main.py:1097-1118`;
   (f) push-after-clear (all groups emptied → manifest `{}` → atomic empty commit — is the
   emptied-group path exercised for ALL groups at once anywhere today?); (g) a reader-flag
   session calling clear (must still raise ReadOnlyError).
7. **PushResult**: is the `updated`/`failures`/`__bool__` mapping right for every push path
   (normal, no-op, partial grouped, partial per-key, `force_push`, replacement push)? Is
   `__bool__` on a msgspec Struct supported cleanly? Should `failures` values be structured
   (exception class + message) rather than strings — what do the consumers actually need for
   triage/retry?
8. **Offline stub-session completeness**: trace the `flag='r'` open path
   (`utils.py:139-208`, `utils.open_remote_conn`, `S3SessionReader.__init__`) and enumerate
   EVERY remote touch — does the stub/skip design cover each? What is the consequence of
   skipping `check_local_remote_sync` (serving stale local data silently — acceptable for a
   read cache, or does it need a warning with the local timestamp)? Do iteration paths
   (`load_items`, `values()`, `items()`) hit unmaterialized keys with a clean `OfflineError`,
   not a KeyError or a crash?
9. **'auto' classification**: is transport-vs-status the right cut (DNS/timeout falls back;
   403/500 raises)? Verify the exact exception surface s3func 0.9.3 produces after its
   retry layer (exhausted retries RETURN the final response — so what does
   `_load_db_metadata` actually raise per failure class?). Confirm the classifier tests
   typed ebooklet errors (RemoteIntegrityError!) before any HTTPError check.
10. **envlib migration equivalence**: build the dispatch table of current `refresh()`
    behavior (L578-605 + `_OFFLINE_ERRORS` + `_read_cached_index`) vs the new typed +
    offline='auto' design — any case handled today that the new design drops or reorders?
    The bootstrap case (remote RCG doesn't exist yet, no local file) under 'auto' must still
    produce envlib's warn-and-treat-as-empty, not a hard failure. Does retiring
    `_read_cached_index` lose anything (its hex-key filter; its skip of undecodable values)?
11. **cfdb**: anything beyond the docstring + the 8 assertions (sweep for push-return and
    exception assumptions; does any cfdb code path do `del` on ebooklet keys OUTSIDE the
    four guarded sites, e.g. via `pop`)?
12. **Phase-1 interaction**: do the Phase-2 changes disturb any Phase-1 invariant — journal
    persistence points vs the new clear(); `changes()`/`view_changelog` output after a
    clear; fsck's raises joining the taxonomy; anything in the generational push that
    assumed delete-of-missing silence or dict-typed push returns internally?
13. **What's missing**: the most valuable findings are the ones this brief didn't ask
    about — API-surface holes a pre-1.0 pass should fix now (cheap while we're breaking
    things), consistency gaps between the new exceptions and existing warnings, offline-mode
    interactions with RCG member opens, anything.

# Deliverable

1. A one-line **verdict**: implementable as specified / implementable with changes (list
   them) / not implementable (why).
2. **Findings**, numbered, each with a severity (CRITICAL = would corrupt data or block the
   design; MAJOR = wrong behavior or a hole that must be fixed in the plan; MINOR =
   improvement/doc), the **evidence** (file:line), and a concrete failure scenario.
3. Answers to the numbered questions above (brief where you simply concur).
4. A short **examined-and-held** list: load-bearing claims you checked that held, so the
   author knows what was verified rather than skipped.

Write the full report to `/tmp/phase2_review.md` (pick another name under `/tmp` if that
path already exists) and also present it in full in your reply.
