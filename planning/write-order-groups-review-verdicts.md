# Verdicts — round ebooklet-wog-plan-1 (plan frozen; no edits until all arms report)

## Gemini (gemini-3.1-pro-high), finished 20:59, ~11 min

| # | Claim | Verdict | How checked |
|---|---|---|---|
| G1 | The `group_bytes` default makes the plan's "mode conflict with the kwarg raises" fire against existing remotes | **verified as a plan defect**, though the failing direction differs from Gemini's | Read the plan. `group_bytes=None` means per-key, so an *omitted* kwarg opening a grouped remote (every reader, envlib's `Catalogue`) would raise. Gemini reasoned from a 32 MB default, which hits per-key remotes instead. Same root cause: "omitted" and "explicitly per-key" are the same value. The current code lets the remote win (`main.py:579-590`). |
| G2 | Unlinking a mismatched sidecar destroys an offline reader's cache | **verified (read)**, medium | The plan's discard rule has no online/offline condition. Offline there is no re-fetch, so `keys()` is lost while materialized values could still be served. The fix: discard only right before a successful online fetch. Gemini's booklet experiment (reopen ignores the passed `value_len`) is consistent with the agent map, and I did not re-run it. |
| G3 | delete_remote + push of the same local file sends every key and the metadata, with the uuid preserved | **verified (ran)** on 0.10.5 | `g3/g3.py`: 20 keys, grouped (5), `store.clear()`, then reopen `'w'` with no kwarg. Result: `num_groups` resolved 5 from the journal, the push succeeded, a fresh reader saw 20 keys, all values equal, and metadata `{'meta': 'data'}`. Caveat: in 0.11 the journal path changes (legacy hash resolves to grouped); the fixture test in the plan covers that. |
| G4 | Membership from the index alone plus lazy deletes is safe | accepted (reasoning; the arm did not execute it) | Consistent with the code: the index entry is removed locally before the push. |
| G5 | `loc_map` order survives the skew timestamp rewrite | **verified (read)** | `create_changelog` builds `loc_map` by insertion in `locations()` order and updates in place (`utils.py:1018-1020`, `1003`). Dicts keep insertion order on update. |
| G6 | "`open_edataset(...) + push` is wrong; it needs `.changes().push()`" | **refuted** | cfdb `edataset.py:44` defines `EDataset.push(self, force_push=False)`. |

## Fable (claude-fable-5-1), finished 21:10, ~22 min. It ran 196 tests and E1-E13, and used two sub-readers for downstream facts.

| # | Claim | Verdict | How checked |
|---|---|---|---|
| F1 | A sidecar older than the manifest (crash after the commit PUT; lock lost mid-push), combined with "drop any gid with no live members", GCs live groups | **verified (ran)**: both states reproduce on 0.10.5 | `f1/e4.py`. E4: `manifest==commit2: True, sidecar==commit1: True`. E4c: W2's five keys are absent after W1's retry, `final keys: k0..k5, w1_new`. **E4c is a lost update on HEAD today**, pre-existing. The consequence under planned code is reasoned (the code doesn't exist), but it follows directly from the plan's emptied-group rule. |
| F2 | The catalogue migration is delete-first, and the producer file may be incomplete | **verified (read + ls)**, decisive | `~/.envlib/commons/catalogue.rcg` is dated **Jul 13**, 72 KB, while the live catalogue is ~489 KB. Publishes go through `~/.envlib/cache/<blake2b(conn)>.rcg` (`catalogue.py:778-780`, `_upsert_entry` `:1108` opens with `'c'` and reads one entry). The live catalogue's timestamp also moved during the review (1791269608 → 1791273188), so another publisher was active. Delete-then-push from this machine's file would lose entries. The arm's journaled-replacement alternative (E10-ii) is accepted as executed by the arm; I have not re-run it. |
| F3 | `migration_check` misses metadata and pending journal state | **verified (read)** | The no-remote branch of `create_changelog` pushes every local key (`utils.py:1026-1029`). `_build_meta_section_for_push`'s `remote_absent` branch embeds the local metadata slot (`utils.py:1128-1132`). A local pending delete (key absent locally) is silently dropped. So a clean key/timestamp check can still publish different metadata or drafts. |
| F4 | The release order causes a public outage: envlib 0.1.7 pulls ebooklet 0.11, which refuses the legacy catalogue | **verified (ran)** | PyPI: `envlib 0.1.7 requires ebooklet>=0.10.5`, no cap. `catalogue.py:797-815` catches only UUIDMismatch and RemoteMissing. |
| F5 | Lazy deletes: stale readers stop converging, and deleted bytes stay publicly retrievable | accepted (reasoning, consistent with E5 case 4) | Convergence today relies on the generation being replaced, so a 404 triggers the re-check. Lazy deletes leave the generation in place. |
| F6 | A 0.10 writer session opened before the format flip pushes format 2 over it | accepted (the arm executed E8; I did not re-run it) | Relevant only to a same-key catalogue step. |
| F7 | "An append touches the tail plus new groups only" is false for cfdb | **verified (read)** | The last band is partly filled: 217/840 per the manifest, and my own simulation counted 644 keys for an 840-h append. cfdb `append()` rewrites the whole coordinate range (`support_classes.py:1251-1265`). Updated partial-band chunks grow in place, so their groups grow past `group_bytes` (reasoned). |
| F8 | Bumping `JOURNAL_VERSION` for every file breaks 0.10.5 on shared caches | **verified (read)** | `journal.py:92` raises `ValueError` on `v > 1`, before any offline handling. Realistic here: several venvs with different locks share `~/.envlib/cache`. This reverses simplification 5. |
| F9a | Site list incomplete (`main.py:558`, `436-437`, …) | accepted (read the list; consistent with the agent map) | |
| F9b | Pre-existing: `del k; db[k]=v; changes().discard()` → the next unrelated push deletes k remotely | **verified (ran)** | `f1/e6.py`: both modes, `remote keys after unrelated push: ['b', 'z']`. |
| F9c | 223 tests, not 213 | accepted (the arm ran collect) | |

## GLM-5.3 (z-ai/glm-5.3:exacto), finished 21:49, ~61 min, $1.64. It ran exp1-exp7 against fake S3 and 0.10.5.

| # | Claim | Verdict | How checked |
|---|---|---|---|
| L1 | Rollout window: fresh installs fail between the envlib release and the catalogue migration | **verified**, same as F4 | PyPI pins (F4). |
| L2 | If the re-push after `delete_remote` fails, cold caches see an EMPTY catalogue | accepted (consistent with Fable E10-i, which the arm executed) | Moot if the catalogue uses a replacement (F2). |
| L3 | The commit stamp is unconditional (`format_version = SUPPORTED_FORMAT_VERSION`), so a naive bump stamps per-key remotes '3' | **verified (read)** | `utils.py:1611`. The plan's table says per-key = 2, but never names the reader-cap / writer-stamp split. |
| L4 | Per-push cost is bounded by the tail; "a one-band append (5.7 MB) pays ~5.6×" | **principle accepted; numbers refuted** | A band is 322 × ~0.44 MB ≈ **142 MB** (temperature 51.8 GB / 117,852 chunks), not 5.7 MB; 5.7 MB is today's hash-group size. A one-band append pays at most 32 MB of tail, ≤ ~23 %. The small-frequent-push point stands (already raised with Mike). |
| L5 | An interrupted migration leaves consistent orphans that fsck cannot see; check completeness across the whole session | accepted (reasoned) | Overlaps F3. |
| L6 | `copy_remote` on an empty-manifest grouped remote falls into the per-key branch | accepted; **the plan already covers it** (it branches on `storage_kind`) | Add the empty-manifest case to the tests. |
| L7a | ingest-wrf-3k `publish.py:113` None-guard must switch to `group_bytes` | accepted | Already in step 4.5. |
| L7b | Journal v2 is a one-way door for local files | **verified**, same as F8 | |
| L7c | A naive bump's v1_remote refusal tells the user to recreate a good remote | **verified (arm ran; consistent with Fable E3)** | Supports `storage_kind`. |
| L-Q2 | Today a delete-only grouped push PUTs a repacked group, and the commit gate's grouped-deletes term is load-bearing | accepted (arm executed; consistent with the code at `utils.py:1556`) | |
| L-overall | "No scenario where the design as written loses data" | **refuted by F1 and F2** | GLM's exp3 assumed the producer file is complete (its own clause b), and it is not. |

## Reviewer table

| Arm | Wall | Commands (`arm_activity.py`) | Subagents | Cost |
|---|---|---|---|---|
| Fable 5.1 (high) | 22 min | 69 | 2 read-only sub-readers (downstream repos) | Claude allocation |
| Gemini 3.1 Pro (high) | 11 min | 148 (73 unique), 0 build-shaped | 0 | free |
| GLM-5.3 exacto (high) | 61 min | 82, 9 build-shaped | 0 | $1.64 |
