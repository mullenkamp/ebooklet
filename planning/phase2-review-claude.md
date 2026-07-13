<!-- Reviewer: Claude Opus 4.8 (fresh-context subagent, blind), 2026-07-13, on phase2-design-brief.md.
Body verbatim from the agent's final report. Process note: the reviewer session inherited the
author session's plan mode, so it was restricted to read-only source analysis — the brief's
encouraged /tmp+fake_s3 experiments could not run, and its report-file writes never landed
(report captured from the agent's reply). The empirically-deferred items (F14, all-groups-emptied
clear E2E, offline load_items surfacing) moved into the implementation verification plan.
Post-run git-status audit of all five repos: clean. -->

# ebooklet Phase 2 — critical design review

## Verdict

**Implementable with changes.** The design is sound and most claims hold against the source, but three issues must be fixed in the plan before implementation: (F1) the `__delitem__` "missing" definition is wrong as written and breaks an existing test; (F2) `PushResult.updated` for a non-replacement grouped *partial* failure is specified as `False` but the code commits the successful groups (self-contradiction with the field's own docstring); (F3) the `clear()` journal-persist ordering is ambiguous in a way that risks silently losing the journaled deletes. The remainder are MINOR consistency/robustness items.

---

## Findings

### F1 — MAJOR: `__delitem__` "missing = not in the local index view" is wrong; must be `__contains__`-based
**Evidence:** plan Item 2.1 ("Missing = not in the local index view"); `main.py:1068-1090`; `__contains__` at `main.py:646-650` = `remote_index OR local_file`; existing test `ebooklet/tests/test_delete_safety.py:218-227` (`test_delete_then_set_of_never_pushed_key`).

A freshly-`set` key that has never been pushed lives in `self._local_file` and `journal.written` but is **not** in `self._remote_index`. Under the literal "missing = not in the local index view", `del` of such a key would raise `KeyError`, even though `key in db` is `True`.

**Failure scenario:** the existing test does, in one `flag='n'` session, `eb['newkey']=b'v1'; del eb['newkey']; eb['newkey']=b'v2'`. `newkey` is never in `remote_index` (fresh 'n' session, empty index), so the plan's literal rule makes `del eb['newkey']` raise `KeyError` — the test fails. Worse in production: cfdb's four guarded sites (`cfdb/main.py:98,104`, `support_classes.py:1173,1209`) swallow `KeyError`, so a spurious `KeyError` means the intended local delete never runs — the just-written chunk stays in `local_file`/`journal.written` and gets **pushed**. cfdb's coordinate-shrink path (`support_classes.py:1173/1209`) deletes chunks that may have been written earlier in the same session.

**Fix:** guard on presence via `__contains__`: `if key not in self: raise KeyError(key)` (not in `remote_index` **and** not in `local_file`), then proceed with the existing branch logic. This preserves: present-remotely-but-unmaterialized → deletes; freshly-written-unpushed → cancels the pending write; truly-absent (incl. already-deleted-this-session) → `KeyError`. Verified this reconciles both `test_set_then_delete_still_deletes` and `test_delete_then_set_of_never_pushed_key`.

### F2 — MAJOR: `PushResult.updated` for a non-replacement grouped *partial* failure is specified `False` but the remote *did* change
**Evidence:** plan Item 3 field doc `updated: # the remote actually changed (grouped: the commit PUT happened)` vs. the prose "partial-failure dict → … updated=False in grouped mode (a partial phase-B failure commits NOTHING)"; `utils.update_remote` `main.py:1102-1109, 1169, 1180, 1262-1265`.

For a **non-replacement** grouped push, the commit is aborted *only* for replacements (`if replace_pending and failures: return failures`, `utils.py:1107-1109`). A normal grouped push with partial upload failures has `updated=True` (`utils.py:1029`), enters the commit block, and executes `put_db_object` (`utils.py:1169`) publishing a manifest with the successful groups repointed to new generations and failed groups left on old generations. Readers see the new data for the successful groups — the remote changed. "commits NOTHING" is true only for the *replacement* partial case.

**Failure scenario:** a caller reading `result.updated` on a partial grouped push gets `False` while successful groups are live on the remote. It contradicts per-key mode (correctly mapped to `updated=True` on partial) — an unjustified asymmetry. Also `update_remote` returns *either* `updated` *or* `failures`, never both (`utils.py:1262-1265`), so the `Change.push` boundary cannot know whether the commit happened if "internal returns stay as-is". `__bool__` masks this for `if push():` (always falsy when `failures` non-empty), so no truthiness consumer breaks — but the field is inaccurate.

**Fix (pick one):** (a) redefine/doc `updated` as "a clean full commit happened", set it `False` whenever `failures` is non-empty, and drop the "commit PUT happened" wording; or (b) change `update_remote` to return `(updated, failures)` and set `updated=True` for non-replacement partials, `False` for a replacement partial. (a) is the smaller change.

### F3 — MAJOR: `clear()` journal-persist ordering can lose the journaled deletes
**Evidence:** plan Item 2.5 steps ("… single `journal.persist` … → truncate local value blocks as today (re-persisting the reserved slots …)"); `booklet/utils.py:1029-1049` (`clear()` = `ftruncate(fileno, sub_index_init_pos)` — destroys both reserved slots, preserves the uuid header); current `main.py:1098-1119`.

`booklet.clear()` truncates to `sub_index_init_pos`, destroying reserved slots 1 (journal) and 2 (remote-state). A `journal.persist` performed in step 2 (before the step-4 truncate) is wiped by the truncate. The deletes survive only because they also live in the in-memory `JournalState`, and only if the journal is **re-persisted AFTER the truncate** (today's code: truncate → `clear_written` → `persist(force=True)` → `remote_state.persist(force=True)`). The "single journal.persist" phrasing in step 2 invites persisting once, before truncate.

**Failure scenario:** `clear()` → close (no push) → reopen: the journal slot has no deletes, so the cleared keys resurrect on the next index pull (clear silently became a no-op).

**Fix:** state the ordering explicitly: snapshot `remote_index.keys()` (minus reserved/metadata keys, F13); `del remote_index[k]` (separate sidecar, survives truncate); mutate the in-memory journal; **then** `local_file.clear()`; **then** `journal.persist(force=True)` and `remote_state.persist(force=True)` and `refresh_local_metadata(...)`. Drop the redundant pre-truncate persist.

### F4 — MINOR: raise-inventory misses `remote.py`'s not-writable / source-missing guards
**Evidence:** plan Item 1 says the ReadOnly guards are "across main/utils"; the sweep shows the `'Session is not writable.'` guards are in **remote.py** (`356,369,393,403,428,559,584`), plus `remote.py:439` `'target remote is not writable.'` and `remote.py:444` `'The source remote does not exist.'`. `utils.py` has zero such guards. The `~15` count matches main.py's 13 `'File is open for read only'` + `main.py:177`.

`remote.py:444` (`copy_remote` source missing) is semantically `RemoteMissingError` and is consumer-facing via `EVariableLengthValue.copy_remote`. **Fix:** classify `remote.py:444` under `RemoteMissingError`; decide the credential-based not-writable guards (F9); correct "main/utils" → "main/remote".

### F5 — MINOR: offline bulk reads must surface `OfflineError` cleanly, not via a raised worker future
**Evidence:** `load_items` does `error = f.result()` (`main.py:784-788`); the workers call `remote_session.get_object(...)` (`utils.py:506,567,617`). A stub that *raises* `OfflineError` inside a worker propagates through `f.result()` as an uncaught exception, not a per-key named marker.

The common case (fully-materialized cache) is clean — `check_local_vs_remote` returns `False` for every key (`utils.py:661-675`), no fetch dispatched. Only an unmaterialized entry triggers the raise. **Fix:** in offline mode, pre-check for unmaterialized keys and raise one `OfflineError` naming them *before* dispatching any fetch (point reads via `_load_item`/`get` already propagate acceptably). Grouped mode can't name the user key from the group generation — name the requested keys.

### F6 — MINOR: retiring `_read_cached_index` narrows offline robustness
**Evidence:** `envlib/catalogue.py:977-1005` reads the local `.rcg` directly with booklet (never touches the remote), applies the 24-hex filter, skips undecodable values, and requires `'remote_conn'`; the new path (`open_rcg(offline='auto')` + `rcg.items()`) routes through `load_items`, raising `OfflineError` on any unmaterialized entry (F5) and losing the undecodable-skip / presence defenses. `refresh()` always full-iterates online (`catalogue.py:580,613`), so the cache is normally fully materialized. **Fix:** document the "offline cache must be fully materialized" assumption; keep the 24-hex filter (it already does).

### F7 — MINOR: `PushResult.failures` value type is unspecified (retryability lost)
**Evidence:** the internal dict mixes `resp.error` (s3func dict), `MissingRemoteObject`, `GroupTooLargeError`, raw `HTTPError` (`utils.py:980,1031,1053,969`); the non-retryable `GroupTooLargeError` is only *logged* (`utils.py:1056-1058`). envlib only stringifies (`catalogue.py:68`); cfdb only checks truthiness. **Fix:** pin a stable value shape conveying at least message + retryable, cheap to do pre-1.0.

### F8 — MINOR: `offline='auto'` scope + partial-open re-open cleanup
**Evidence:** the plan names `_load_db_metadata → head_object` as the choke point, but the initial index fetch (`fetch_remote_index → get_object`, `utils.py:389`) is a second remote touch during open. A remote reachable for HEAD but failing on the index GET wouldn't fall back if the classifier wraps only `open_remote_conn`. `_init_common`'s `except BaseException` (`main.py:461-474`) cleans up a partial open, so an offline re-open is feasible — but state that 'auto' wraps the whole open and re-opens offline on transport failure from either touch.

### F9 — MINOR: `ReadOnlyError` conflates flag-based read-only with credential-based non-writability
**Evidence:** two modes fold into one name: (1) opened `flag='r'` → `'File is open for read only.'` (13 sites); (2) credentials lack write permission → `'Remote is not writable.'` (`main.py:177`) / `'Session is not writable.'` (remote.py). A user catching `ReadOnlyError` to mean "I opened read-only" also catches "no write credentials". **Fix:** document `ReadOnlyError` as "cannot write (read-only mode or no write credentials)", or split the credential case out. Not blocking.

### F10 — MINOR: `Change.update → build_changelog` rename rationale is inaccurate
**Evidence:** plan Item 2.4 cites a "name collision with MutableMapping.update", but `Change` (`main.py:63`) is a plain class, not a `MutableMapping` — no MRO collision. The real reason is semantic confusion with `EVariableLengthValue.update`. Callers verified internal only (`main.py:105,130,213`); no external caller (cfdb/envlib use `changes().push()`). Keep the rename, fix the rationale.

### F11 — MINOR: commit-PUT failure raises `HTTPError` instead of flowing through `PushResult`
**Evidence:** a partial *upload* failure returns a dict → `PushResult.failures`, but a **commit** PUT failure raises `urllib3.exceptions.HTTPError` (`utils.py:1171-1172`), as does lost-lock at commit (→ `LockLostError`). So `result = push()` yields a `PushResult` for one failure class and an exception for another. Document the two failure channels in the ops doc / push docstring.

### F12 — MINOR/observation: offline mode doesn't extend to RCG member (dataset) opens
**Evidence:** Item 4 adds `offline` to the factories and `refresh()` uses `offline='auto'` for the RCG, but a consumer that refreshed offline then opens a member dataset (`DatasetRef → open_edataset → open_ebooklet`) still does a normal online open and fails offline. Offline *browsing* works; offline *data access* does not. Decide whether member opens thread `offline` through, or document the limitation.

### F13 — MINOR: `clear()` snapshot should exclude reserved/metadata keys
**Evidence:** other paths skip `utils.metadata_key_str` (`main.py:743,545`; `utils.py:906`). A stale metadata entry in a `remote_index` added to `journal.deletes` would seed a spurious group into `affected_group_ids` (`utils.py:894,898`). Format-2 indexes shouldn't carry it, but filter `reserved_key_strs` defensively.

### F14 — MINOR/verify: `__bool__` on a msgspec Struct + mutable-default caveat
**Evidence:** msgspec Structs support methods, so `def __bool__` should work — but confirm in the hermetic tier that `bool(PushResult(...))` calls it and `PushResult == True` is `False` (relevant to the cfdb `== True` test migration). If defaults are ever added, use `msgspec.field(default_factory=dict)` for `failures`. The plan's definition has no defaults — fine.

### F15 — MINOR: internal `raise ValueError(local_init_bytes)` is an ugly assertion
**Evidence:** `utils.py:1120-1121` raises `ValueError` whose *argument is a bytearray* on an init-bytes mismatch. Give it a message or make it `AssertionError`.

---

## Answers to the numbered questions

1. **Raise-inventory.** Missed consumer-facing raises: `remote.py:444` (→ RemoteMissingError) and the credential-based `'Session is not writable.'` guards in **remote.py** (the plan put ReadOnly guards in "main/utils"; they're main/**remote**, utils has none) — F4. Internal catches: ebooklet has **no** `except ValueError` blocks, so re-typing the ValueErrors changes no internal catch; the only HTTPError catch (`remote.py:389`) never sees `RemoteIntegrityError` and, since RemoteIntegrityError keeps HTTPError parentage, still catches it. Safe.

2. **Dual inheritance.** No MRO surprise (valid C3 for both `(Error,ValueError)` and `(Error,HTTPError)`; `except ValueError`/`HTTPError`/`Error` all resolve). Cycle-safe **iff** `errors.py` is a leaf: `utils` imports nothing from ebooklet (verified leaf), `remote→utils`, `main→utils,remote,journal`; put classes in `errors.py` (stdlib/urllib3 only), alias at old paths (`utils.UnsupportedFormatError`, `main.RemoteIntegrityError`). No cycle.

3. **LockLostError not HTTPError.** Safe. The two placeholder raises (`main.py:187`, `utils.py:1163`) sit in push code wrapped by no `except HTTPError` in ebooklet. cfdb has no HTTPError catch and passes push through; envlib's only HTTPError catch is `_OFFLINE_ERRORS` in `refresh()` — a `flag='r'` read path with no lock/push, unreachable by LockLostError (and retiring this round). No push-path HTTPError catch in any repo would have caught the placeholder. Broad `except Exception` still catches it. "Never shipped" holds.

4. **`__delitem__` KeyError.** See **F1** (definition must be `__contains__`-based). Nothing internal relies on delete-of-missing silence: the flag='n' purge (`main.py:204-211`), `Change.discard` (`main.py:141`), journal replay (`main.py:418,861`), lost-keys drop (`utils.py:997-999`) all `del` **booklet** objects guarded by `if key in …`. MutableMapping `pop(key, default)` is safe (its `del` runs only after a successful `self[key]`); `popitem`/`setdefault` fine. (Caveat: `pop(k, default)` still propagates `RemoteIntegrityError` from `__getitem__` — pre-existing.)

5. **`update()` signature.** Route every key through `self[key]=value`/`self.set` so the reserved-key guard (`main.py:606-607,623-624`) applies; `MutableMapping.update(other=(), /, **kwds)` gives all four input forms. Reserved-key strings are booklet's non-identifier bytes, so **kwargs** can't express them anyway; a mapping/pairs input carrying one is rejected by `__setitem__`. Dropping the current override moves the writable guard to the first `__setitem__` (nothing applied on a read-only file). RCG inherits it correctly (`__setitem__ → RCG.set → add`).

6. **`clear()` mechanics.** (a) recommend a `UserWarning` when `journal.written` non-empty at clear (silent drop of unpushed edits). (b) replacement session: fine (replacement rebuilds from `new_gens`, ignores `journal.deletes`). (c) per-key: fine (`(deletes and num_groups is None)` commit condition `utils.py:1102`; `delete_objects(list(deletes))` `utils.py:1177`; watch one giant delete's scale). (d) discard-after-clear: **works** — `discard()` forces `_pull_remote_index(force=True)` (`main.py:164-165`), skipping the ts gate, restoring every entry. (e) reserved-slot order: **F3** (persist AFTER truncate). (f) push-after-clear all-groups-emptied: traced — every gid → `emptied_gids` (`utils.py:1010-1013`), `new_manifest={}` (`utils.py:1133-1136`), one commit PUT flips readers to empty; `num_groups` metadata retained; new path, must be E2E-tested. (g) reader-flag clear still raises (`main.py:1118-1119`). Filter reserved/metadata keys (F13). `booklet.clear()` preserves the uuid, so clear+push keeps existing readers' UUID match.

7. **PushResult.** Normal/no-op/force_push/successful-replacement: correct. Replacement partial: correct `(False, failures)`. **Non-replacement grouped partial: WRONG — F2.** Per-key partial `(True, failures)`. `__bool__` on a Struct: verify (F14). `failures` value type should convey retryability (F7). Commit-PUT failure raises rather than returning failures (F11).

8. **Offline stub.** The `flag='r'` open touches the remote only via `_load_db_metadata` (`remote.py:196`) and `fetch_remote_index→get_object` (`utils.py:389`), both gated on `initialized`/`uuid`. With the stub reporting `initialized=False`/`uuid=None`, `check_local_remote_sync` returns `False` (`utils.py:307`), `init_local_file` goes local-only, `fetch_remote_index` returns early — **no remote touch during open**. Skipping `check_local_remote_sync` serves possibly-stale local data silently (fine for explicit `offline=True`; 'auto' warns — add the local timestamp). Bulk iteration needs the F5 pre-check; point reads propagate cleanly. Absent `.remote_index` sidecar → empty offline index (edge). Offline can't verify the ebooklet type vs the remote (type guard skipped) — caller must use the right factory.

9. **'auto' classification.** Correct cut, confirmed against s3func 0.9.3: `http_url.py:46-63` sets `Retry(raise_on_status=False)` with `status_forcelist=(429,500,502,503,504)`, so exhausted **status** retries RETURN the response → `_load_db_metadata` raises a **bare** `HTTPError` (`remote.py:222`); a **transport** failure propagates from `request` through `get_object`/`head_object` (no try/except, `s3.py:191,217`) as a specific urllib3 subclass. **Critical:** match the specific transport types and check typed ebooklet errors FIRST — a broad `except HTTPError` for fallback would wrongly swallow the status bare `HTTPError`, `RemoteIntegrityError`, and `UnsupportedFormatError`. The plan's explicit type list does this. RemoteIntegrityError-before-HTTPError: required and specified.

10. **envlib migration equivalence.** All cases preserved; the UnsupportedFormatError mislabel-as-"not readable yet" is a **real latent bug** the typed dispatch fixes (v1 remote → `main.py:374` UnsupportedFormatError, today swallowed by `except ValueError`). Bootstrap (remote 404 + no local file) under 'auto' → clean 404, not connectivity → 'auto' doesn't fall back → `RemoteMissingError` propagates → `except RemoteMissingError` → warn-empty (preserved). Differences: connectivity+no-cache changes the propagated exception *type* (raw connectivity → `OfflineError`); the offline warning loses the cache-path detail. Retiring `_read_cached_index` loses undecodable-skip / presence defenses and partial-cache tolerance (F5/F6); 24-hex filter retained. `register()` push at `catalogue.py:826` genuinely needs wrapping.

11. **cfdb.** Nothing beyond the docstring + 8 assertions. Only push consumer is the passthrough (`edataset.py:51`); no source inspects the return. No `del`/`pop` on ebooklet keys outside the four guarded sites (the other `del`/`pop`/`clear`/`update` are on cfdb's own `_var_cache`/`_sys_meta`/`_data`). `except BaseException: close(); raise` (`edataset.py:153`) composes with any type. The 8 assertions (`test_edataset.py:118,221-222,344,356,379,401,432,499`) use `is True`/`is False`/`== True`/truthiness and **will** break under `PushResult` — migrate as planned.

12. **Phase-1 interaction.** No invariant disturbed except the clear()-ordering care (F3). Internal dict-typed return preserved: `Change.push` still checks `isinstance(update_remote(...), dict)` (`main.py:217`) and builds `PushResult` after. Internal `del`s stay booklet-guarded. `changes()`/`view_changelog` after a true clear: empty changelog, `pending_deletes` = every key (consistent). fsck's raises align with the taxonomy/rulings.

13. **What's missing.** F4, F7, F9, F11, F12, F13, F15 above. Also: ebooklet's **own** suite has many `assert …push() is True` (`test_rcg_entries.py`, `test_member_integrity.py`, `test_delete_safety.py`) that break under `PushResult` — the "test churn" scope is larger than the consumer tests. Consider a `clear()`-dropped-unpushed-writes warning (Q6a).

---

## Examined and held

- `force_shutdown`: zero callers in all five repos (only the two defs, `main.py:1121/1133`). Safe to remove.
- cfdb's four `del self._blt[...]` sites all inside `try/except KeyError: pass`; envlib's `del rcg[...]` (`catalogue.py:884`) guaranteed present by `get`-then-raise at `850-853`.
- envlib push sites: `808/885/909` wrapped; `826` unwrapped (needs wrapping) — confirmed. `_raise_on_push_failure` and the `_OFFLINE_ERRORS` + RemoteIntegrityError re-raise are exactly as described.
- The two lock-verify placeholders are bare `urllib3.exceptions.HTTPError` (`main.py:187`, `utils.py:1163`), never shipped; s3func lock `verify()` exists (`locking.py:438`).
- `booklet.clear()` preserves the uuid/header (truncate to `sub_index_init_pos`, `utils.py:1034`), destroys both reserved slots — clear+push is uuid-safe.
- `booklet.__delitem__` raises `KeyError` on a missing key (`main.py:699`); the MutableMapping mixins build on it correctly.
- Taxonomy target raise sites all exist as claimed (UUID `utils.py:311`/`remote.py:745`; RemoteMissing `utils.py:293`/`main.py:1272`; UnsupportedFormat `utils.py:61,103,110`/`remote.py:204`/`fsck.py:107`/`main.py:374`; GroupTooLarge `utils.py:71,217`; RemoteIntegrity `main.py:36`; lock-acquire stays `TimeoutError` `main.py:307`/`fsck.py:173`).
- Import DAG acyclic today → `errors.py` leaf + aliases is cycle-safe.
- s3func `raise_on_status=False` (`http_url.py:63`) underpins the 'auto' transport/status cut.

**Empirical tier note:** plan mode barred the encouraged `/tmp`+`fake_s3` experiments, so this is source-derived. F14 (`msgspec.Struct.__bool__`), the all-groups-emptied clear E2E, and the offline `load_items` surfacing (F5) are the items most worth confirming empirically once out of plan mode.
