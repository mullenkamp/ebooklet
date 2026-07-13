<!-- Reviewer: Gemini (Mike-driven agy), 2026-07-13, on phase2-design-brief.md. Body verbatim from /tmp/phase2_review.md; post-run git-status audit of all five repos: clean. -->

# Phase 2 Design Review Report

**Verdict: Implementable with changes**

The plan is extremely sound. The exception taxonomy provides necessary clarity, the offline modes correctly formalize what consumers were hand-rolling, and the true `clear()` mechanics are mechanically correct. The `PushResult` structure is a major ergonomics win. A few exceptions in `remote.py` were missed in the taxonomy sweep, but there are no structural blockers.

---

## Findings

### 1. `copy_remote` Exceptions Missed in Taxonomy
**Severity: MAJOR**  
**Evidence:** `ebooklet/remote.py:444` (`raise ValueError('The source remote does not exist.')`) and `ebooklet/remote.py:449` (`raise ValueError('The target remote already exists. Either delete_remote or use a different target.')`).  
**Scenario:** The taxonomy plan successfully identified `RemoteMissingError` sites in `main.py` and `utils.py`, but missed the explicit "source remote does not exist" raise in `remote.py`'s `copy_remote` function.  
**Fix:** Classify `remote.py:444` as `RemoteMissingError`. For `remote.py:449`, either introduce a new `TargetExistsError(Error, ValueError)` or explicitly document it as staying a builtin `ValueError`.

### 2. Missing Key `del` vs `booklet` Pass-Through
**Severity: MINOR / HELD**  
**Evidence:** `ebooklet/main.py:202-211` (flag='n' purge) and `ebooklet/main.py:418-420` (journal replay).  
**Scenario:** The plan asks if changing `ebooklet.__delitem__` to raise `KeyError` will break internal ebooklet logic that expects a silent no-op. It will not. Internal operations (like the flag='n' purge and journal delete replay) interact directly with `_local_file` and `_remote_index`, and use explicit `if k in ...` existence checks before calling `del`. Internal operations safely bypass the modified consumer-facing `__delitem__`.

---

## Answers to Specific Review Questions

1. **Raise-inventory completeness:** The taxonomy missed the `copy_remote` errors in `remote.py` (see Finding 1). It also omits various standard argument validation errors (e.g. `main.py:1256` `TypeError('key must be a str')`), but standard `TypeError`/`ValueError` for argument validation are fine to remain builtins, per the plan's "internal assertion-style" exception.
2. **Dual inheritance:** Moving to `errors.py` actually *fixes* potential import cycles, because `errors.py` will have zero internal dependencies, allowing `main`, `utils`, and `remote` to import from it cleanly. The MRO `(Error, ValueError)` is standard and safe.
3. **LockLostError is not HTTPError:** Verified. `cfdb` does not catch `HTTPError` anywhere. `envlib` catches `_OFFLINE_ERRORS` (which includes `HTTPError`) only in `refresh()`, a READ path. No consumer currently catches `HTTPError` on the PUSH path. The old placeholder error escaped to the user, and the new `LockLostError` will do the same—this is exactly the desired behavior.
4. **`__delitem__` KeyError:** Holds perfectly (see Finding 2). `MutableMapping` methods like `pop(key, default)` wrap `__getitem__` inside a `try/except KeyError` block, so a compliant `KeyError`-raising `__delitem__` integrates smoothly with all `MutableMapping` derivatives.
5. **`update()` signature:** Providing the reserved key validation `if key in utils.reserved_key_strs:` is checked inside the loop that unrolls `other` and `kwargs`, it will correctly reject reserved kwargs like `update(metadata=...)`.
6. **`clear()` mechanics:** 
   - (a) Unpushed writes correctly become deletes; dropping their pending local values aligns with the semantics of a full `clear()`.
   - (b) A `clear()` inside an 'n' session yields an empty manifest, wiping the remote on push. Correct.
   - (c) Per-key mode creates exhaustive deletes.
   - (d) `discard()` after `clear()` will force an index re-pull. The local values stay truncated, but `ebooklet` will lazily re-fetch them from the remote on demand.
   - (e/f/g) Mechanics are sound. Push-after-clear drops all emptied groups, committing an atomic empty database.
7. **PushResult:** Mapping is correct. Msgspec supports methods on `Struct` objects, so `def __bool__(self):` is perfectly valid. Returning the exceptions as strings in `failures` is sufficient for triage and avoids requiring consumers to unpickle/parse complex exception objects.
8. **Offline stub-session:** Explicit `offline=True` bypassing `check_local_remote_sync` is correct—serving stale data silently is the explicit contract of `offline=True`. The stub session cleanly intercepts remote value fetches for unmaterialized keys and raises `OfflineError`.
9. **'auto' classification:** `urllib3` transport errors (DNS, timeout) are raised as connection errors, triggering the fallback. Status errors (403, 500) are raised as `HTTPError`, correctly propagating failure. The sequence of testing typed ebooklet errors before `HTTPError` prevents misclassification.
10. **envlib migration:** The dispatch table maps perfectly to the new `offline='auto'` functionality. Retiring `_read_cached_index` loses nothing, as `offline='auto'` will natively fall back to `booklet` reads.
11. **cfdb:** A full grep of the `cfdb` codebase confirms `del` is only called on the four explicitly guarded `try/except KeyError` sites. There are no rogue `.pop()` calls on `_blt`.
12. **Phase-1 interaction:** The `clear()` mechanics safely integrate with the generational push and journal invariants.
13. **What's missing:** Ensure `errors.py` explicitly exposes the legacy attribute paths (e.g., `utils.UnsupportedFormatError = errors.UnsupportedFormatError`) to preserve downstream compatibility without requiring immediate migration.

---

## Examined and Held

* **`cfdb` Blast Radius:** Confirmed that `del self._blt[...]` sites in `cfdb` are all safely wrapped in `try/except KeyError`.
* **Lock Verification Paths:** Confirmed no consumer accidentally swallows push-path network errors, making `LockLostError`'s lack of `HTTPError` parentage a safe change.
* **Msgspec Struct Methods:** Confirmed that `msgspec.Struct` natively supports defining standard magic methods like `__bool__`.
* **Offline Stale Data:** Confirmed that `offline=True` skipping the timestamp sync is semantically correct for an offline cache.
