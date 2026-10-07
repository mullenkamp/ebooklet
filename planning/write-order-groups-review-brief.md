# Plan review — ebooklet: write-order groups (remote format 3) replacing hash grouping

## Framing

I am the author of the plan below and I am reviewing my own work. I want an independent technical
critique before writing any code. Be direct; if the reasoning is wrong, say so and show why.

ebooklet (scope `ebooklet`) syncs a local key/value file (the `booklet` package, a PyPI dependency)
to S3-compatible object storage. cfdb (scope `cfdb`, the package directory only) stores its array
chunks in it, and envlib (scope `envlib`, package directory only) is a public data catalogue built on
both. Its catalogue is itself an ebooklet `RemoteConnGroup`. The scope `envlib-ingest-wrf-3k` is a
producer repo whose 12 published datasets are the main grouped remotes to migrate.

Today grouped mode assigns keys to S3 group objects by `blake2b(key) % num_groups`. A push repacks and
re-uploads every touched group whole, so appending new chunks to a growing dataset re-uploads most of
it. The plan replaces this with groups the writer assigns at push time, in local-file write order,
filled to a byte target, with each key's group id stored in its remote-index entry. It also changes
deletes to index-only ("lazy") and migrates every live grouped remote, including the public
catalogue. This is ordinary storage/data tooling; nothing here targets a third party.

The plan is reproduced verbatim at the end of this brief. Line references in it are to `ebooklet` at
its current HEAD unless another repo is named.

## How I would like you to work

- **Check the claims, do not take them.** The plan rests on several claims about the current code
  (listed under the questions). Re-run anything a conclusion of yours depends on.
- **Execution means running the code under review.** The planned code does not exist yet, so the
  decisive experiments are on the *current* ebooklet and booklet: build the `ebooklet` scope's
  environment, then drive the real `open_ebooklet` / `open_rcg` / push paths against the in-repo fake
  S3 (`ebooklet/tests/fake_s3.py`: `FakeS3Connection`, `make_writer`, `make_reader`). For example,
  confirm or refute that:
  - `local_file.locations()` yields keys in write order across overwrites, `prune()` and index
    relocation;
  - an absent-remote push from a local file whose journal records a hash `num_groups` sends every
    key plus the metadata;
  - the `v1_remote` test would capture format-2 per-key remotes after a version bump;
  - a reader holding an old index and manifest behaves as the plan claims when a group's generation
    is replaced and GC'd.

  A re-implementation of the logic in a script of your own is a model of your reading, not a test of
  the code; claims from one are weighted as reasoning. Mutating your working copy to see whether a
  guard fires is encouraged. The existing fake-S3 suites run with `uv run pytest` in the `ebooklet`
  scope. The live suites need a local S3 config that is not shipped, so they skip or fail; ignore
  them.
- **Evidence for every experiment.** For each experiment a finding rests on, paste its command and raw
  output into your reply, whoever ran it.
- **Look for what I did not think to check.** The questions below are where *I* suspect I am weak,
  which is exactly the wrong place to stop.
- **Separate severity.** "Wrong, will lose or corrupt data" is a different class from "I would design
  this differently". Simplicity is a goal: if a part of the plan can be cut without losing a
  guarantee, say so.
- Do not open git-ignored local machine configuration (credentials files and the like); out of scope.

## Reproduction notes

The assessment numbers in the plan's Context came from this script, which uses ebooklet's own
`key_to_group_id` over real cfdb chunk-key strings (`cfdb.utils.make_var_chunk_key`). I ran it from
the cfdb repo's environment. In the container, any environment with both packages works.

```python
# scatter.py (abridged): groups touched by an append under hash grouping
from ebooklet.utils import key_to_group_id
from cfdb.utils import make_var_chunk_key
# WRF-3k temperature: G=8521, chunks (840, 24, 24) over (306_817 h, 315, 534); 322 spatial chunks/band
sp = [(y, x) for y in range(0, 315, 24) for x in range(0, 534, 24)]
keys = lambda t0, t1: [make_var_chunk_key('temperature', (t, y, x))
                       for t in range((t0 // 840) * 840, t1, 840) for y, x in sp]
G = 8521
touched = {key_to_group_id(k, G) for k in keys(306_817, 306_817 + 8760)}   # one-year append
print(len(touched) / G)   # I observed 0.341 (2,909 groups); upload 36.2 % of the final dataset, 12.4x amplification
```

The claim that the live public catalogue is hash-grouped comes from a read-only HEAD:

```
curl -sI https://b2.envlib.xyz/file/envlib/envlib-commons/catalogue | grep -i x-bz-info
# observed: x-bz-info-format_version: 2, x-bz-info-type: RemoteConnGroup, x-bz-info-num_groups: 13
```

## Questions I am least confident about

These are starting points, not boundaries.

1. **Write order.** Is the physical order of `booklet.locations()` really "the order keys were last
   written" in every path that matters? The paths I can think of:
   - overwrites;
   - `prune()`;
   - index relocation;
   - values materialized by a pull (appended to the file);
   - the in-place timestamp rewrite in `create_changelog`.

   Allocation only orders *new* keys, so does anything here matter for correctness, or only for
   locality? Is there a case where a key new to the remote has no `loc_map` entry?
2. **Group membership from the index alone, with lazy deletes.** Under the plan a repack's member list
   is the index members of that gid plus the keys allocated to it this push.
   - Is there any path (journal replay, a stale incarnation, `reconcile_local_with_index`, a failed
     earlier push, a replacement) where the index and the group objects disagree, such that a repack
     drops a live value or keeps a deleted one?
   - Are fully emptied groups detected correctly when the only change in a push is deletes?
   - The commit gate at `utils.py:1556` must fire for grouped deletes; what else assumes grouped
     deletes cause a repack?
3. **Group-id reuse (`max(manifest)+1`).** I claim reusing an emptied group's number is safe:
   generations are fresh random tokens, readers pair index and manifest from one commit, and fsck
   ages orphans by listing time. Check this against the reader re-check protocol (`_resolve_missing`),
   the cached `remote_state`, journal delete replay, `copy_remote`, and a reader mid-read during
   phase D GC.
4. **Migration.**
   - Datasets go to a new key through the ordinary absent-remote push.
   - The catalogue goes through `delete_remote()` and then a push to the same key from the producer
     file.
   - Does that path really push every key and carry the metadata when the local file's journal,
     cached `remote_state` and sidecar all belong to the old hash-grouped remote? Look at
     `main.py:488-537`, `utils.py:1087-1135`, and stale-incarnation handling (`forget_remote_incarnation`).
   - What happens to existing reader caches of the catalogue (same uuid, 15-byte sidecar) and to
     envlib's catalogue cache path?
   - Is `migration_check` (local copy missing or older than the old remote's index) the right and
     sufficient precondition?
5. **Classification and old clients.**
   - Can a format-2 per-key remote be misclassified as legacy or grouped?
   - Can a 0.10.x client *write* into a format-3 remote through any path that skips the
     `format_version` gate? Candidates: `RemoteConnGroup.add` reading member metadata,
     `get_user_metadata`, `copy_remote`, offline sessions.
   - Can a 0.11 client write a format-2 stamp with 19-byte entries?
6. **Failure semantics.** Consider a failed fresh group, a failed tail pull or a crash between upload
   and commit. Do the index, the journal and the manifest stay consistent? Does a retry re-allocate
   correctly? Does anything still filter by hash after the change (the plan lists the sites; is the
   list complete)?
7. **Design economics.**
   - Is filling the tail group (pulling it when not local) worth its cost?
   - Is a 32 MB default sensible given that a ranged read currently spans first-to-last wanted member
     in a group (read coalescing is out of scope)?
   - Is there anything simpler that keeps append cost proportional to new data?
8. **Rollout.** Consider the release order (ebooklet 0.11 → cfdb → envlib → ingest repos) and the
   migration order. Is there a window where a live dataset or the catalogue is unreadable by an
   up-to-date client, or writable into an inconsistent state? Does the `num_groups=None` shim cover
   every downstream caller?

## Deliverable

A ranked list of findings, most serious first: what it is, why it matters, your confidence, and the
evidence (the file and line, or the command and its actual output). For every finding you mark
verified, state **(a)** what you executed or derived, and **(b)** what it depends on that you did
*not* check: assumptions inherited from me, baselines taken on trust, alternatives not tried. Clause
(b) is the one I need most.

Do not report a version, a timing or an output you did not observe. "I reasoned this from the source"
is a good answer; invented provenance is worse than none. Refuting one of my claims with evidence is a
valuable outcome. If part of the plan is sound, say what you checked to conclude that, not that it
looks reasonable.

---

# The plan under review (verbatim)

(Host paths in the plan map to scopes: `~/git/ebooklet` → `ebooklet`,
`~/git/cfdb-repos/cfdb/cfdb` → `cfdb`, `~/git/envlib-repos/envlib/envlib` → `envlib`, and
`~/git/envlib-repos/ingest/envlib-ingest-wrf-3k` → `envlib-ingest-wrf-3k`. Paths not in a scope, such
as other ingest repos, wrf-model-eval and `~/.envlib`, are not available to you.)

# Plan: write-order groups in ebooklet (remote format 3), replacing hash grouping

## Core

**Purpose:** make appending to a grouped ebooklet remote upload only the new data, not most of the
dataset.

**Decision:** replace hash grouping (`blake2b(key) % num_groups`, `ebooklet/utils.py:202`) with groups the
WRITER assigns at push time, in local-file write order, filled to a byte target `group_bytes`.
- `group_bytes` defaults to 32 MB and replaces `num_groups`. It is a writer-side packing setting and is
  not stored on the remote.
- Each key's group id is stored in its remote-index entry, which grows from 15 to 19 bytes. Readers take
  the gid from the index, so ebooklet never learns cfdb's key format.

**Mechanism:** the push's existing `locations()` sweep already yields keys in physical (= write) order and
already marks keys new to the remote.
- New keys first fill the tail group (the highest gid, pulled if not local), then fresh groups numbered
  `max(manifest)+1`.
- Existing keys keep their gid for life, so an append touches the tail plus new groups only.
- Deletes are **lazy**: they remove the index entry only. Dead bytes stay in their group until it is next
  repacked, and a group with no live members is dropped.

**Scope:**
- Grouped remotes become format 3 and the hash-grouped path is dropped.
- Per-key remotes stay format 2, byte-identical, so the ECan datasets are untouched.
- Every grouped remote is migrated: 12 WRF-3k datasets, esa-sst and the two MEGA remotes go to new keys
  and are repointed. The **commons catalogue** (grouped: HEAD shows `num_groups: 13`) is deleted and
  re-pushed to the same key from the producer file. That leaves a gap of a few seconds; after it,
  pre-0.11 clients get "upgrade ebooklet".
- Read coalescing and compaction are out (backlog items).

**Top risks:**
1. Migration data loss: a local file missing keys the old remote has. `migration_check` refuses to
   migrate in that case. For the catalogue this check is the only guard, because its delete comes first.
2. Per-key remotes misclassified as legacy, which would break the live ECan datasets.
3. A repack or tail top-up whose member list does not come from the index alone. That could resurrect
   a deleted key or drop a live one.

---

## Context
Assessment this session, simulated with ebooklet's own hash over real cfdb keys:
- An append drags each new chunk's whole hash group into the upload, so the amplification is about the
  number of keys per group.
- For WRF-3k a yearly extension is 10–28× per dataset (~150 GB to add ~12 GB), and the 1980s prepend
  would re-upload ~100 %.
- C1 at G≈1000 would upload 22.5× its final size over 45 yearly pushes.
- Mike does the prepend *after* this lands.
- Backlog item: `envlib/OPEN_WORK.md` "[ebooklet] Group by a dataset dimension…".

Design choices Mike settled on 2026-10-06:
- write-order allocation, not a dimension rule;
- fill partial groups, pulling the tail if needed;
- a 32 MB byte target;
- migrate all grouped remotes;
- coalescing out of scope;
- lazy deletes;
- the six simplifications: gid = `max(manifest)+1`; `group_bytes` not stored; no in-place migration
  helper; no tail-pull fallback; journal version bumped for every file; readers' journal writes left
  as they are.

## Steps

### 0. Housekeeping (first, after approval)
In `envlib/OPEN_WORK.md`:
- Add **[ebooklet] Coalesce grouped ranged reads**. The problem: `get_remote_group_values`
  (`utils.py:835-897`) GETs from the first wanted member to the last, so a sparse read over-reads up to
  one group. Mike looked into this before and found it harder than expected. I found no record of that
  in `ebooklet/planning/`. It matters more with 32 MB write-order groups.
- Add **[ebooklet] Compaction of dead bytes in grouped remotes**. Lazy deletes leave dead bytes in partly
  deleted groups. fsck could report each group's dead fraction, and a pass could repack groups above a
  threshold. It is not needed for envlib's append-mostly data.
- Mark the "group by a dimension" item as in progress.

Then copy this plan to `~/git/ebooklet/planning/write-order-groups-plan.md`.

### 1. ebooklet 0.11.0: format 3

**API** (`main.py`: `open_ebooklet` 1633-1758, `open_rcg` 1767-1872, `_init_common` 370-680,
`RemoteConnGroup` 1519-1527)
- `group_bytes: int | None = None`. None means per-key; an int means grouped with that packing target.
- `ebooklet.DEFAULT_GROUP_BYTES = 32 * 2**20`.
- Validate `1 <= group_bytes <= utils._MAX_GROUP_BYTES`. Tiny values are allowed for tests.
- `num_groups`:
  - `None` is accepted silently for one release, because cfdb-ingest `forecast_archive.py:81` and
    ifs-download `archive.py:72` pass it explicitly.
  - An int raises `ValueError` naming `group_bytes`.
  - Removed in 0.12.
- A public `db.group_bytes` property replaces the private `_num_groups`. Two downstream readers depend
  on the private attribute: ECan `repair_precip.py:204` and cfdb `benchmarks/.../plan.py:8`.
- Delete `key_to_group_id`, `next_prime` and `is_prime_small`.

**Remote format** (`utils.py:51-65`, commit metadata `1606-1614`, `remote.py:193-223`)

| Kind | `format_version` | Group fields in S3 metadata | Index entry |
|---|---|---|---|
| per-key | 2 (unchanged) | none | 15 bytes |
| grouped | 3 | none | 19 bytes: ts7 + gid4 + off4 + len4 |
| legacy | 1, or 2 + `num_groups` | — | refused for r/w/c; `'n'` may proceed suppressed |

- `PAYLOAD_VERSION` stays 2; the index bytes carry their own `value_len`.
- New `remote_session.storage_kind`: None, `'per_key'`, `'grouped'` or `'legacy'`.
  - It replaces the `v1_remote` test at `main.py:434-435`, which would otherwise also catch format-2
    per-key remotes.
  - The suppressed path also resets `remote_state`.
- New helpers `encode_group_entry` and `decode_index_entry` (dispatch on length). Every slicing site
  switches to them: `utils.py:1313-1318, 1464, 1504`; `main.py:943-950, 1231-1233, 1265, 1282-1284`.
- Refusal messages name the migration route. Reword the "format-1/0.10" text at `main.py:518-523`,
  `utils.py:106-117`, `errors.py:61-66` and `utils.py:226-230, 1512`.

**Mode resolution** (rewrite `main.py:579-617, 1056-1063`)
- An initialized, unsuppressed remote decides grouped or per-key by its format. A mode conflict with the
  kwarg raises.
- Otherwise the journal's mode decides, then the kwarg.
- `group_bytes` comes from the kwarg or `DEFAULT_GROUP_BYTES`; it is never resolved against the remote.
- Why this changes: today a journal's `num_groups` outranks a remote that lacks it.

**Journal** (`journal.py`)
- Add `storage` ('per_key' | 'grouped' | None).
- Read the legacy `num_groups`/`num_groups_set` into it: an int means legacy hash, which resolves to
  grouped on an absent remote.
- `deletes` stays a set of key names.
- Bump `JOURNAL_VERSION` to 2 for every file, so 0.10 refuses any 0.11 local file with "upgrade
  ebooklet". Local files are not downgrade-safe (document this).

**Allocation** (a new pure, unit-testable `utils.plan_groups(...)`, called from `update_remote`; it
replaces the hash uses at 1257, 1261, 1272 and 1298)
- Changed keys that are already in the index use their entry gid, and their group is repacked (updates
  stay in place).
- New keys are taken in `loc_map` (write) order. Each goes into the current group while
  `size + entry <= group_bytes`; otherwise it opens gid `max(manifest ∪ this push's gids)+1`. A fresh
  group always takes at least one key, so an oversized value gets its own group.
- **Every repack's member list is the index members of that gid plus the keys allocated to it.** Local
  keys never join a group by themselves. This replaces the `loc_map` membership at `1268-1274`.
- The tail is `max(manifest)`.
  - Its projected size is the sum of its live index members, using updated values' local lengths.
  - It is pulled through the existing phase-A pull, only when new keys land in it.
  - If the pull fails, that group fails like any other and the retry tries again.
- Reusing an emptied group's number is safe. Object names are `{gid}.{generation}` with a fresh random
  generation each time, readers use index and manifest from one commit, and fsck ages orphans by
  listing time.
- A replacement (`'n'`) treats every key as new, has no tail and starts at gid 0.
- Staging:
  - Index entries come only from successful uploads.
  - Clearing journaled writes is filtered by each key's allocated or entry gid against `failed_gids`
    (`1664-1665`).
  - A failed fresh group leaves no index or manifest entry; its keys stay journaled and are
    re-allocated on retry.
- Belt: grouped mode requires a 19-byte sidecar before phase A.

**Lazy deletes** (grouped mode; per-key deletes stay eager object deletes)
- A delete only removes the index entry. That already happens locally before the push: `__delitem__`
  (`main.py:1331`), `clear()` (`1381`) and the journal replay (`567-569, 1090-1092`). No group is
  uploaded for it.
- The commit gate (`utils.py:1556`, today `deletes and per-key`) must also fire for grouped deletes.
  Deletes clear from the journal with the commit, so the `failed_gids` filter at `1525` goes.
- Dead bytes drop out whenever their group is repacked for another reason.
- The push counts live members per gid during its index scan. A manifest gid with none is dropped and
  its object GC'd: the existing emptied-group path (`1436-1439, 1715-1720`), with no upload.
- A deleted-then-re-set key is simply new and goes to the tail.

**Readers and sidecar**
- `load_items`, `_retry_fetch` and `_load_item` take gid, offset and length from `decode_index_entry`.
- Grouped mode is detected as sidecar `value_len == 19`, recomputed after each index swap.
- Before `get_remote_index_file` (`main.py:537`), a sidecar whose `value_len` does not match the mode is
  unlinked and re-fetched; it is a cache.
- `open_remote_index` takes `value_len`.
- A fetched index whose layout disagrees with `format_version` raises.

**fsck and copy_remote**
- fsck (`fsck.py:104-137`):
  - accepts per-key format 2 and grouped format 3, and refuses legacy with the migration message;
  - finds unmanifested gids from index entries;
  - new report fields `empty_groups` (a manifest gid with no live members, which should not survive a
    push) and `bad_entry_layout`.
- `copy_remote` (`remote.py:470`) branches on `storage_kind`, not `if src_manifest`.

**migration_check(conn, local_path)** (in `ebooklet/migrate.py`, the only migration code)
- Parses the legacy format-2 payload and its 15-byte index (`parse_db_payload` is unchanged).
- Lists keys whose local copy is missing or older than the old remote's, and returns a report.
- Migrations abort unless the report is clean.
- If it isn't, hydrate first with the old client: `uv run --with 'ebooklet==0.10.5' … load_items()`.

**Tests** (213 total, ~140 grouped)
- Mechanical sweep: replace `num_groups=5` / `NUM_GROUPS` with a `TEST_GB` constant in the `_seed`
  helpers.
- Replace the `key_to_group_id` / `_key_for_group` same-group constructions with write-order
  constructions. Add `gid_of` / `members_by_gid` conftest helpers that decode the sidecar.
- **Every rewritten test asserts its own precondition** (e.g. `gid_of(a) != gid_of(b)`), so it cannot
  go vacuous.
- New tests:
  - allocation follows write order;
  - an append touches only the tail plus new groups (old generations unchanged, upload bytes bounded);
  - tail top-up, including the exact-fit `<=` boundary;
  - non-local tail pull on a pruned second machine;
  - an oversized value gets its own group;
  - emptying the tail and appending reuses its number with a new generation, and old readers stay
    correct;
  - a lazy delete uploads no group, is committed, and the key reads as absent;
  - deleting every member of a group drops it and GCs the object with no upload;
  - a later update of a group with dead bytes repacks only live members;
  - delete then re-set puts the key in the tail, and readers get the new value;
  - a failed fresh group: no entries, keys still journaled, the retry succeeds;
  - a stale 15-byte sidecar is discarded;
  - legacy remotes are refused for r/w/c;
  - a per-key remote is stamped `'2'` with 15-byte entries, plus an opt-in cross-version read with
    0.10.5;
  - the journal v1 legacy load, and v2 refused by the 0.10 struct;
  - fsck fields;
  - `'n'` over format 3;
  - migration from a checked-in fixture generated under 0.10.5. Cover a push to a new key, and
    `delete_remote` followed by a push to the same key (same uuid, metadata kept, every key present).
    Include an old cache opening the result, and `migration_check` catching an incomplete local file;
  - the catalogue (RCG) in format 3;
  - `group_bytes` validation and the `num_groups` shim.
- **Mutation list** (each must turn a test red; mutate with `PYTHONDONTWRITEBYTECODE=1`):
  - allocate in changelog order, not write order;
  - skip the tail top-up;
  - top up without pulling the tail;
  - repack membership from `loc_map`, not the index;
  - the commit gate ignores grouped deletes;
  - an emptied group stays in the manifest;
  - stage entries for failed groups;
  - `<` instead of `<=`;
  - the projected tail size counts dead bytes;
  - an oversized value goes into the tail;
  - no sidecar layout check;
  - legacy classified as per-key;
  - format 3 stamped on a per-key remote;
  - fsck still hashes;
  - `migration_check` ignores keys that are older locally.

**Docs:** README "Grouped Storage" and storage format (68-85, 145-168); `docs/ops.md` (45-46, 68-100,
200-201, 225-237, plus a new "Migrating hash-grouped remotes" section); `CLAUDE.md` 57-89; a CHANGELOG
0.11.0 entry with an upgrade recipe.

### 2. Benchmark the default `group_bytes` (Mike's call to run; live uploads go to his B2)
- Push a real WRF-3k subset (a few bands of temperature) to a scratch prefix at 8, 16, 32, 64 and
  128 MB, each fresh. Record push MB/s, peak RSS, and the upload bytes of a one-band append.
- Time three reads at each size: a point series, a full field for one band, and a 2×2-tile sub-region
  (the over-read case).
- Keep 32 MB unless the numbers argue otherwise. Results go to `ebooklet/planning/`.

### 3. Downstream releases
- **cfdb:** `open_edataset(group_bytes=None)` (`edataset.py:89/122/140`, which also fixes the stale
  "required for flag='n'" text). Update `test_edataset.py`, `docs/guide/s3-remote.md`,
  `cfdb_summary_usage.md` (619-657) and the cfdb skill (809-865). Floor `ebooklet>=0.11`.
- **envlib:** `publish(group_bytes=...)` (`catalogue.py:992-1008`). Update `test_catalogue_live.py`.
  Drop the "prime" advice in `quickstart.md:119` and `publishing.md:9-16`, and update the envlib skill
  (49, 79-81). Floors for the new cfdb and ebooklet. Release this before the catalogue migration, so
  that `pip install -U envlib` is the fix for users.
- **ingest-base:** `tsortho.py:663/682`, `tsforecast.py:459/478`, README:397. The image-based repos need
  a base-image rebuild. Also ECan `repair_precip.py:204` → `.group_bytes`.
- Refresh the locks in all seven repos with `uv lock --upgrade-package`.
- Mike publishes every release; verify each by PyPI presence.

### 4. Migrate the live grouped remotes (each run is Mike's, supervised)
Ordered so the window in which new clients read the catalogue but not an unmigrated dataset stays
short:
1. **Rehearsal:** the private MEGA `era5_cfdb` and `sst_cfdb` go to new keys. Update
   `push_era5_remote.py`, `push_sst_remote.py` and `test_transfer_guards.py:36-40, 201-202`.
2. **Data pushes to new member keys, without repointing:** esa-sst and the 12 WRF-3k datasets.
   - Each is `migration_check`, then `open_edataset(new_conn, path, 'w', group_bytes=...)` + push, then
     fsck.
   - About 420 GB in total (the WRF-3k size is an estimate, G × 5.7 MB), so ~2.3–9 h at 13–50 MB/s.
3. **The commons catalogue at the same key:**
   - Run `migration_check` against `~/.envlib/commons/catalogue.rcg`, and back up that file.
   - Then `delete_remote()` (with 0.10.5), then push the producer file with 0.11, then fsck.
   - This leaves a few seconds with no catalogue. From then on, old clients get "upgrade" errors.
4. **Repoint** each entry with `cat.publish(path, new_conn, commons_conn)`: the data push is a no-op,
   then fsck, then the upsert under the same dvid. Read back through the public URL.
5. Config updates:
   - esa-sst `config.py:129-133`;
   - wrf-3k: per-dataset `group_bytes` in `config.py` (dropping the stale scatter comments),
     `build.py:177`, `publish.py:113-143`, `PROVENANCE.md:132-145`;
   - `member_key` gains a suffix for the new key.
6. After a grace period, delete the old keys with the 0.10.5 client.

## Verification
- **Step 1:**
  - `uv run pytest` in ebooklet with the fake-S3 suites; the live suites with `s3_config.toml` on Mike's
    go.
  - Run the mutation list: each mutant must go red, then the code is restored. Re-mutate after fixing
    any surviving mutant.
  - The cross-version per-key read against 0.10.5.
- **Step 3:** cfdb with `--ignore=test_edataset.py` locally, then live. envlib metadata + vocabulary
  tests, then the live catalogue tests on Mike's go.
- **Step 4, per remote:**
  - `migration_check` is clean.
  - fsck on the new key: 0 orphans, 0 missing, 0 empty groups.
  - Byte-equality read-back of sampled chunks through the public URL against the local file
    (`verify_remote.py` for WRF-3k, `verify.py` for esa-sst).
  - A catalogue query returns the new `db_url` under the same dvid.
- **The goal itself:** after migration, a one-band append to a scratch copy of a WRF-3k dataset uploads
  at most the tail group plus the new data, read from the `ebooklet.push` log byte totals.

## Review (to propose to Mike; nothing launched without his go)
- **Plan review:** dual blind is warranted. This is a storage format plus an irreversible migration of
  live public data.
  - Arms: Fable 5 + GLM-5.3. Add Gemini if it is free, and Sonnet 5.5 at xhigh if allocation allows.
  - Proposed scope: `~/git/ebooklet`, `~/git/cfdb-repos/cfdb/cfdb`, `~/git/envlib-repos/envlib/envlib`,
    `~/git/envlib-repos/ingest/envlib-ingest-wrf-3k`.
- **Code review after implementation:** triggers 1, 2 and 3 all fire. The migration is a gate, the data
  is public and single-copy, and one author writes the code and its tests.
