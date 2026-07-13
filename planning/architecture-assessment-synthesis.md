# ebooklet architecture assessment — synthesis of two independent reviews

**Process record (2026-07-12):** two independent, fresh-context reviewers — Claude (Fable 5)
and Gemini — ran the same brief (`architecture-assessment-task.md`) blind to each other,
against ebooklet 0.9.4 + booklet 0.12.6 + s3func 0.9.2 with cfdb/envlib as consumer context.
Both post-run file audits clean (repos byte-identical). The Claude report is the deeper
artifact (5 experiments through real code over a fake S3 session, 12 findings); the Gemini
report is compact but landed the same structural conclusions independently. Synthesis by the
author's assistant; the single-sourced CRITICAL was re-verified against source before
adoption.

> **Post-synthesis ruling (Mike, 2026-07-12): old-format compatibility is dropped from the
> plan entirely.** Very few ebooklet files exist and all are under the author's control, so:
> no legacy (15-byte-entry) read path, no per-remote format migration, no
> journal-seeding-from-timestamp-diff fallback. The generational implementation ships
> single-format; the upgrade procedure is "push all pending changes on the old version,
> upgrade, re-create/re-push the few existing remotes once." Old clients cannot read
> new-format remotes — acceptable, there are no external consumers yet. The Phase-0
> `format_version` header stays (cheap future headroom); Seam-3 drops to ~2–3 days and the
> Phase-1 total shrinks by roughly a day. This window (no backwards-compatibility burden)
> closes at the public commons launch — one more argument for doing the Phase-1 redesigns,
> and ideally the Phase-2 API-shaping decisions, before external consumers exist.

## Headline

1. **Both reviewers independently converged on the same Seam-3 redesign** — generation-
   numbered **immutable** group objects with the single-PUT db object as the atomic commit
   point, old generations deleted only after commit. Cross-model convergence on an unprompted
   design is the strongest signal this process produces: this is the design.
2. **One live CRITICAL data-loss bug found outside the seams** (Claude, F1; author-verified
   against source): S3 deletes are prefix-based. Deleting emptied group `1` also deletes
   groups `10`, `11`, `12`…; `delete_remote()` on db `mydb` also destroys sibling `mydb2`
   (and `flag='n'` pushes call it automatically). Demonstrated end-to-end. **Fix before
   anything else** (~2–4h): exact-key group deletes; bounded prefix set for `delete_remote`
   ({exact `db_key`, `db_key + '/'`, `db_key + '.lock.'}).
3. **Seam 2 is worse than the recorded clock-skew instance** (Claude, by experiment):
   cross-session **deletes are silently lost or resurrected with no clock skew at all**
   (`_deletes` is memory-only; the timestamp changelog cannot represent deletion), and a
   skewed local edit is **destroyed by reading it back** (remote re-pull overwrites the
   unpushed edit — read-your-writes violation). The 0.8.4 warning covers one of three
   sub-cases. This defeats the premise behind any "can wait" phasing.
4. **The foundations held under both reviews**: local-first + explicit push, the layering,
   grouped storage, the concurrency model's shape, the 0.9.3/0.9.4 fix correctness, and the
   **RCG entry schema v1 — judged good enough to freeze** for the commons launch.

## Per-seam verdicts

| Seam | Gemini | Claude | Synthesis |
|---|---|---|---|
| 1 session/lifecycle | Targeted refactor: deprecate `flag='n'`, explicit `create_remote()`/`delete_remote()`; persist num_groups in a hidden local key | Targeted refactor: keep dbm flags; internal `_replace_pending`/`_remote_initialized` decomposition; `replace_remote=True` opt-in for 'n'-over-existing; num_groups in a reserved local-booklet key or header bytes | **Targeted refactor.** Full agreement on substance: remote replacement must be explicit, and num_groups belongs INSIDE the local booklet file (both reviewers independently proposed the same hidden-key mechanism — no sidecar). API shape (new methods vs opt-in kwarg) is the one open choice → decide in the pre-1.0 API pass; do the non-breaking internal decomposition before the migration since it forecloses neither. |
| 2 change tracking | Redesign: 8-byte `modification_sequence` in booklet (header + per-block stamps), `iter_modified_since(seq)`; phase before-1.0 ("0.8.4 warning mitigates") | Redesign: persistent pending-change **journal** (writes+deletes+num_groups) in a reserved booklet key; changelog from journal; read-your-writes gate; NO format change; phase **before-migration** (warning demonstrated to cover 1 of 3 sub-cases; deletes lost skew-free) | **Redesign, before-migration, journal first.** Claude's experiments defeat the mitigation premise behind Gemini's softer phasing. The journal is incremental, format-stable, and carries num_groups for free. Gemini's sequence counter is the stronger long-term primitive (per-write crash safety — it closes the journal's own sync-window residual) → reserve it as the 1.0-era booklet format-bump option if the residual ever matters. |
| 3 push atomicity | **CRITICAL redesign**: generational immutable groups + manifest & user-metadata **embedded in the db object payload** (kills the separate `_metadata` object and its split-brain); torn pushes currently corrupt reader views permanently | **Targeted redesign** (re-framed: mutability, not atomicity — the atomic commit point already exists): generational groups via index-entry v2 (22 bytes with a generation field; `value_len` as the layout discriminator); publish-then-GC ordering; deleted-member recovery inconsistency (interim fix via `_resolve_missing`); the accepted mid-push window becomes false integrity errors for public consumers | **Redesign, before-migration — the convergent core is settled** (immutable generations, single-PUT commit, GC after commit; fsck becomes trivial; the 0.9.3 re-check starts healing instead of false-positiving). Container details differ and **compose**: adopt Gemini's embedded manifest+metadata (one object carries everything that must change together, fixing the `_metadata` split-brain and giving `copy_remote` its object list) with Claude's format-versioning/legacy-gating discipline and exact-key GC. Needs a 1-page design doc at implementation to pick per-entry-generation vs manifest-map. |

## Unique findings adopted (complementary coverage)

- **Claude:** F1 prefix-deletes (CRITICAL, verified); F5 `force_lock` breaks *live* writers +
  holders never re-verify their ticket (fix: age-gated break + re-verify at push boundary);
  F7 >4 GiB group = mid-push OverflowError, needs a pack-time guard; F4 deleted-member
  silent `in`/`get` inconsistency (interim fix now); F8 O(total-keys) push/refresh as a
  documented scale boundary (fine for migration, revisit post-1.0); F9 MutableMapping
  violations bundle (`del` missing key no-ops; `clear()` is eviction wearing clear's name);
  F10 missing format versioning on wire formats about to freeze; F11 exception taxonomy
  (envlib string-matches `'UUID' in str(err)` — the consumer proving the gap); F12 security
  notes (warn on non-HTTPS `db_url` pre-commons; `writable`-probe writes outside the
  namespace).
- **Gemini:** torn-push **permanent** reader corruption via the recovery fallback (sharpens
  Seam-3's criticality — readers materialize uncommitted data, violating snapshot
  isolation); `copy_remote` replicates orphaned garbage via blind listing (Claude explicitly
  did not exercise copy_remote — complementary, manifest-driven copy folds into Seam 3);
  `_metadata` split-brain (absorbed by the embedded-manifest choice).

## Unified roadmap

**Phase 0 — immediately / before commons launch (≈2 days):**
1. **F1 prefix-delete fix** — first, alone, small. (2–4h + tests)
2. F4 interim: deleted-member recovery → `_resolve_missing`. (2–4h)
3. `format_version` in db-object metadata + document the index `value_len` discriminator. (1–2h)
4. Interim delete-loss mitigation: `close()`-time warning on non-empty `_deletes`. (1–2h)
5. Declare RCG entry v1 frozen in docs. (1h)
6. Warn on non-HTTPS `db_url` (public-commons posture). (1h)

**Phase 1 — before the tethys migration (≈7–10 days):**
7. Seam 2 journal (booklet reserved-key support; changelog from journal; cross-session
   deletes; read-your-writes gate). Supersedes item 4.
8. Seam 1 internal decomposition + num_groups persistence (free rider on the journal key).
9. Seam 3 generational immutable groups (design doc → embedded manifest + entry/format
   gating + publish-then-GC + legacy read path). `copy_remote` becomes manifest-driven.
10. F7 group-size pack-time guard. (1–2h)
11. fsck/verify tool (orphan sweep + claimed-but-missing report; trivial after 9). (1d)
12. F5(a)(c) lock safety: age-gated `break_other_locks` default + holder ticket
    re-verification at the push boundary. (1d, s3func + ebooklet)

**Phase 2 — pre-1.0:**
13. Exception taxonomy (`ebooklet.Error` base; `UUIDMismatchError` etc.; keep
    `RemoteIntegrityError`'s HTTPError parentage). Downstream migration notes.
14. API pass: MutableMapping conformance bundle; explicit remote-replacement API (settle
    methods-vs-kwarg here); `push()` → `PushResult` object with `__bool__` compatibility —
    NOTE: cfdb (`edataset.py` push docstring/passthrough) and envlib
    (`_raise_on_push_failure`) currently detect partial failure via `isinstance(result,
    dict)`, so the PushResult design must either subclass dict or migrate both consumers in
    the same sweep.
    (Implementation aid: `architecture-assessment-harness-fake_s3.py` in this repo is the
    reviewer's in-memory S3-session fake that drove real ebooklet code offline — a starting
    point for giving `remote.py`/push paths hermetic tests during Phases 1–2.)
15. Offline read mode in ebooklet (retire envlib's direct-cache workaround).
16. Ops docs: recovery recipes out of changelog prose.

**Assessed and dismissed by both:** CAS/conditional-write commit protocols (no qualifying
provider); content-addressed objects (generations are cheaper); segmented/delta index at
current scale; multi-writer merge semantics; sidecar files in any role; lock heartbeats
before the simpler F5 measures prove insufficient.

## Residual caveats

Cost estimates are reviewer judgment, not prototyped. Claude's experiments used a faithful
in-memory S3 fake — provider listing/PUT semantics were not re-proven live (prior recorded
probes stand). Gemini's report cites fewer sources and did not carry an examined-and-held
list; where the two overlapped, no contradiction was found. Claude's F1, being
single-sourced and severe, was re-verified by the author's assistant against
`remote.py:346-376` and s3func's prefix-delete path before adoption.
