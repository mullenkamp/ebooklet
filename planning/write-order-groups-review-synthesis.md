# Synthesis — round ebooklet-wog-plan-1 (Fable 5.1, GLM-5.3, Gemini 3.1 Pro; plan review)

## Core

**What survived:** all three arms found the core mechanism sound, and each part was executed by at least
one arm:
- write-order allocation (`locations()` is last-write order across overwrite, prune and relocation);
- gid reuse (stale readers heal through `_resolve_missing`);
- group membership from the index;
- the scatter numbers (0.341 reproduced twice).

**What has to change** (verified by me, see the verdicts file):
1. **The catalogue migration would lose entries (F2).** `~/.envlib/commons/catalogue.rcg` is a 13 July
   snapshot, and publishes go through `~/.envlib/cache/<hash>.rcg` one entry at a time. Use a
   journaled replacement from a file that has loaded the whole live catalogue, with a `copy_remote`
   backup first. No delete.
2. **A stale sidecar plus the emptied-group rule would GC live groups (F1).** The stale state is
   reproduced on 0.10.5: the local stamp is written before the commit PUT, and a lost lock already
   loses another writer's keys today. Fix the commit path inside this plan.
3. **The release order breaks the public catalogue for days (F4/L1).** envlib 0.1.7 accepts any
   ebooklet ≥ 0.10.5. Do the migration on release candidates and publish the finals right after.
4. **`migration_check` is not sufficient (F3).** Replace it with a pre-repoint comparison of the old
   remote against the new one (key set, timestamps, metadata section), and require an empty journal
   first.
5. **Correctness details:**
   - an omitted `group_bytes` must inherit the remote's mode (G1);
   - never discard a sidecar offline (G2);
   - split the reader-cap constant from the writer stamp (L3);
   - bump the journal version only for grouped files (F8, which reverses simplification 5);
   - restate the append acceptance criterion (F7).

**Refuted:** G6 (`EDataset.push()` exists); L4's numbers (a band is ~142 MB, not 5.7 MB); GLM's "no
data-loss scenario" (F1, F2).

**Pre-existing bugs in 0.10.5, both reproduced:**
- the lost update after a broken lock (E4c);
- `del k; db[k] = v; discard()` deletes `k` remotely on the next push (E6).

---

## Before/after (proposed; nothing applied yet)

| Plan section | Before | After (proposed) | Source |
|---|---|---|---|
| API default | `group_bytes=None` means per-key | A sentinel default means "inherit": the remote's mode, then the journal's, then `DEFAULT_GROUP_BYTES` for a new grouped remote. Explicit `None` means per-key. A mismatch only matters (raises) on an absent or `'n'` remote | G1 (verified) |
| Commit path | (not addressed) | Stamp the local file only after the commit PUT succeeds. On open, if `remote_state.remote_ts != remote.timestamp`, fetch the full index and never refresh the manifest alone (`main.py:555-562`). Fixes E4 and E4c. Add mutants for "manifest refreshed without index" and "stamp before PUT" | F1 (verified, ran) |
| Emptied-group rule | Any manifest gid with no live members is dropped | Unchanged, but only valid once the commit-path fix holds. Add a belt: refuse to drop a gid that this push's index scan has never seen populated in the *fetched* index | F1 |
| Catalogue migration | `delete_remote()` (0.10.5), then push the producer file to the same key | 1. `copy_remote` to a backup key. 2. Build a complete local file: open the live catalogue `'w'` with 0.10.5 and `load_items()`. 3. Journaled replacement under 0.11: every key `record_write`, `replace_pending`, `meta_pending`, as Fable's E10-ii, a small helper. One commit, no gap, uuid kept. 4. fsck plus the old-vs-new comparison. Reverses simplification 3 | F2 (verified), L2 |
| Rollout | Release ebooklet, cfdb and envlib (step 3), then migrate (step 4) | Publish `0.11.0rc1` / cfdb rc / envlib rc (pip skips pre-releases by default). Run all of step 4 with the rcs. Publish the finals immediately after the catalogue switch. Old clients break at the switch whatever we do; fresh installs get no window | F4/L1 (verified) |
| Migration check | `migration_check`: local missing or older than the old index | Before migrating: require an empty journal and a local key set and timestamps equal to the old remote's. After pushing to the new key and before repointing: compare the old index with the new index (key set, timestamps) and the metadata sections. Run once more over the whole session at the end | F3 (verified), L5 |
| Journal | Bump `JOURNAL_VERSION` for every file | Write v2 only for grouped files; per-key files stay v1. Reverses simplification 5 | F8 (verified) |
| Sidecar discard | Unlink a mismatched sidecar before `get_remote_index_file` | Only when online and immediately before a successful fetch; never offline | G2 (verified) |
| Format stamp | Table says per-key = 2 | State it explicitly: `SUPPORTED_FORMAT_VERSION` is the reader cap only; the stamp is 2 for per-key and 3 for grouped (`utils.py:1611`) | L3 (verified) |
| Acceptance criterion | "A one-band append uploads at most the tail plus the new data" | "…the tail, plus the groups holding updated keys (the partly filled last band and the time-coordinate chunks), plus the new data." Note that updated partial-band chunks grow their groups past `group_bytes` | F7 (verified) |
| Lazy deletes | Documented as storage cost only | Also document: stale-index readers can still fetch a deleted key's bytes, and deleted bytes stay publicly downloadable until the group is repacked. fsck reports each group's dead fraction now; compaction stays in the backlog | F5 (accepted) |
| Site list | As listed | Add `main.py:436-437, 558`; `utils.py:1296, 1524, 1586, 1644, 1663-1665, 1674-1675, 1682, 1611`; `remote.py:214, 432, 651`; `fsck.py:106, 125, 136` | F9a |
| Tests | 213 | 223. Add: empty-manifest grouped `copy_remote` (L6); a 0.10 pre-opened writer against a format flip (F6, documentation); the stale-sidecar crash and lock-loss cases (F1) | F9c, L6, F6 |
| Backlog (step 0) | Coalescing, compaction | Also the pre-existing E6 discard bug as its own item. It is reproduced, and fixing it is separate from this plan | F9b |
| Memory | "producer file `~/.envlib/commons/catalogue.rcg`" | Correct it: that file is a stale July snapshot | F2 |

## Disagreements between arms
- **"Sound, no data-loss path" (GLM, Gemini) against two data-loss paths (Fable).** Fable is right on
  both, and I reproduced the state for F1 and checked the files for F2. GLM's migration experiment was
  correct for a complete producer file; its own clause (b) named exactly the assumption that fails.
- **Rollout fix:** Fable recommends a read-only legacy path in 0.11; I recommend release candidates
  (zero extra code). See the decision below.
