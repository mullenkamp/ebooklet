# ebooklet architecture assessment — Claude (Fable 5) reviewer report, 2026-07-12

(Verbatim final report of the independent Claude reviewer; experiments referenced live in
/tmp/arch-review-cf/ — harness: fake_s3.py over real unmodified ebooklet 0.9.4 code.)

**Subject:** ebooklet 0.9.4, with booklet 0.12.6, s3func 0.9.2, and downstream cfdb 0.9.1 /
envlib 0.1.1-staged in scope as dependency and consumers.

**Headline:** the layering and the local-first/explicit-push model are sound and should be
kept. Two of the three seam hypotheses are confirmed but UNDER-scoped — the change-tracking
seam is worse than the on-record clock-skew instance (deletes are lost with no clock skew at
all, and skewed edits are destroyed by reading them back), and the push-atomicity seam is
best re-framed as a mutability problem, not an atomicity problem. Outside the seams: one
CRITICAL, currently-live data-loss bug (prefix-based S3 deletes with collateral damage,
demonstrated end-to-end).

## Seam 1 — session/lifecycle state: TARGETED REFACTOR

The 0.9.4 fix mutating `_flag` at runtime is itself the proof a hidden state machine exists,
but the failure history doesn't justify an externally-visible state machine. What caused the
churn: two pieces of session state must outlive the process and have nowhere durable to live
(pending deletes; creation-time num_groups). Sketch: (1) freeze `_flag`; introduce
`_replace_pending` and `_remote_initialized` internals (~0.5d, no API impact); (2) 'n' over
an EXISTING remote requires explicit `replace_remote=True` (pre-1.0; a remote wipe should
never be one flag-typo away — README's only guard is prose); (3) persist num_groups INSIDE
the local booklet: a second reserved internal key (like metadata_key_bytes; half-day booklet
addition) or header reserved bytes 77-199. Kills the 0.9.1 warning workaround. Phases:
decomposition + num_groups before-migration; replace-acknowledgment pre-1.0.

## Seam 2 — change tracking by wall clock: REDESIGN (worse than recorded)

Experiments through real code:
(a) Cross-session deletes silently lost — NO clock skew required. `_deletes` is memory-only;
the changelog is a timestamp diff with no representation of deletion. Observed: delete,
close without push, reopen, push() → False; remote still serves the key to fresh readers.
If another writer pushes in between, the re-pull silently RESURRECTS the deleted key —
the delete is completely lost. cfdb's documented mid-session-push pattern makes this a trap.
(b) A clock-skewed local edit is destroyed by READING IT BACK: the read path sees
remote-newer and re-pulls the remote value over the fresh unpushed edit. Read-your-writes
violated before any push; no warning.
(c) The 0.8.4 loud warning covers one of three sub-cases (group mode + sibling repack only).

Timestamps are fine as a reader freshness heuristic; indefensible as the push membership
test. Sketch — persistent pending-change journal: persist `_written_keys`/`_deletes` (+
num_groups) under one reserved internal key in the local booklet (no sidecar); changelog
membership = journal; deletes gain cross-session semantics (update_remote already handles a
populated deletes set); reads skip the remote re-pull for pending-write keys (fixes (b)).
No on-disk format change; old files seed the journal once from the timestamp diff.
Crash-safety residual: entries since the last sync are lost on hard crash (still strictly
better than today); a header last-push-EOF watermark is the stronger variant if ever needed.
Cost ~2-2.5d incl. booklet reserved-key support. Phase: BEFORE-migration. Interim: warn on
close() with non-empty `_deletes` (~1-2h).

## Seam 3 — push atomicity: TARGETED REDESIGN (re-framed: mutability, not atomicity)

The push already has an atomic commit point (the db object PUT, which physically contains
the full index). Every inconsistency comes from index snapshots referencing MUTABLE shared
objects, rewritten or deleted before the new index publishes. Confirmed windows:
- Deleted-member silent inconsistency: stale reader's ranged read fails verification →
  full-object recovery → member absent → silently treated as clean absence, no index
  re-check ('in' says True, get raises KeyError). The whole-object-404 path gets the full
  0.9.3 protocol; the member path gets nothing — two consistency machineries selected by
  the accident of group co-membership.
- Mid-push false positive (documented, accepted): now surfaces as false RemoteIntegrityError
  to public consumers, since envlib deliberately re-raises it past its offline fallback.

Sketch — generational group objects: keys become `{gid}.{generation}` (push-start timestamp),
never overwritten under a live reference; index entry v2 = ts(7)+gen(7)+offset(4)+length(4)
= 22 bytes with the booklet value_len as the built-in layout discriminator (15=legacy,
22=generational; per-remote migration at first new-format push). Push order: PUT new
generations → PUT db object (commit) → delete exactly the replaced old generations. Crash
before commit = orphans only; after commit = harmless stale objects. The 0.9.3 re-check then
HEALS mid-operation 404s (fresh index → new generation) instead of false-positiving.
Empty groups: publish first, delete after. fsck falls out (list + parse index → orphans vs
real faults). Interim fix regardless: route deleted-member recovery through
_resolve_missing (~2-4h). Offline mode belongs in ebooklet (`offline=True` or documented
auto-degrade for 'r' with a complete local cache; ~0.5-1d).
Costs: interim ~0.5d; format headroom ~0.5d; generational impl + GC + legacy path + tests
2-4d; fsck 1d. Phases: headroom + interim before-commons-launch; generational + fsck
before-migration.

## Findings outside the seams

CRITICAL F1 — Prefix-based S3 deletes cause mass collateral data loss (LIVE, two variants,
demonstrated end-to-end): delete_object deletes by prefix (remote.py:346-354 →
s3func delete_objects(prefix=...)); grouped mode deletes emptied groups via
delete_object(str(group_id)) — with num_groups=13, emptying group 1 deleted groups 10 and 12
too (index entries survive → fresh readers raise RemoteIntegrityError on live keys); with
num_groups=127 one emptied group can destroy ~38 sibling groups. delete_remote() uses
prefix=db_key with NO separator (remote.py:367-376) — delete_remote() on `mydb` destroyed
sibling database `mydb2` completely; flag='n' pushes call it automatically. Fix (~2-4h):
exact-key group deletes; bounded prefix set {exact db_key, db_key+'/', db_key+'.lock.'}.
Phase: immediately, before anything else.

MAJOR: F2 cross-session delete loss (see Seam 2); F3 read-your-writes violation (Seam 2);
F4 deleted-member silent inconsistency (Seam 3); F5 lock lifecycle — force_lock=True breaks
LIVE writers (break_other_locks deletes every ticket incl. a healthy writer's; holders latch
_acquired and never re-verify; the TimeoutError message steers users to force_lock; fix:
age-gate breaks by default + holder re-verifies its ticket at the push boundary + optional
heartbeat post-1.0); F6 no offline read mode; F7 >4GiB group = mid-push OverflowError while
writing the index entry (pack-time len(packed) guard, ~1-2h); F8 scale envelope — push and
reader refresh are O(total keys) not O(delta) (fine for the migration's write-once pattern;
document as a boundary; the Seam-2 journal removes the local scan; segmented index =
not-worth-it now).

MINOR: F9 MutableMapping violations (del missing key no-ops; clear() is cache eviction
wearing clear's name — after clear() keys remain visible and re-materialize; update()
signature; push docstring says list, returns dict; dead force_shutdown params; Change.update
name collision) — bundle pre-1.0; F10 no format versioning on group objects or db-object
metadata (add format_version, absence = v1, ~1h) — before-commons-launch; F11 exception
taxonomy too coarse — envlib string-matches 'UUID' in str(err), the consumer proving the
gap (ebooklet.Error base + UUIDMismatchError/RemoteMissingError/ReadOnlyError, pre-1.0);
F12 security notes: S3Connection.to_dict() structurally cannot leak credentials (held);
db_url accepts plain http:// silently (warn on non-HTTPS pre-commons); RCG trust model
inherent + documented; the `writable` probe writes outside the db_key+'/' namespace (fold
into F1 review).

## Examined and held

The 0.9.4 flag-downgrade fix (incl. partial-failure-no-downgrade and post-flip uuid check);
the 0.9.3 re-check protocol at object level (correct clean-absence incl. metadata unset,
correct loud path, sound writer-side argument); index handle-swap discipline (fetch-first,
atomic replace, finalizer re-registration, _index_lock, stamp after the fetch gate);
push-integrity mechanics (post-lock metadata refresh; lock release on early-raise;
full-membership repack; never-upload-partial groups; retained deletes); ranged-read
verification (header echo means drifted offsets can never serve wrong bytes — the weakness
is availability semantics, never data correctness); pack/unpack round-trip; next_prime
edges; booklet 0.12.6 iteration contract (read-verified; interleaved reads safe); s3func
bakery election core (sound; the issue is lifecycle, not election); s3func delete_objects
response checking + retry policy; RCG entry schema v1 — GOOD ENOUGH TO FREEZE; the
single-writer/many-readers model (right shape; per-push locking remains a future option);
the layering; author positions (local-first + explicit push right for the use case; grouped
storage justified; July per-fix engineering correct — the seams needed the redesigns).

## Roadmap (as proposed by this reviewer)

Phase 0 before-commons-launch (~1.5-2d): F1 fix FIRST; F4 interim; F10 format headroom;
F2 close-warning; RCG v1 freeze declaration.
Phase 1 before-migration (~6-9d): Seam-2 journal; Seam-1 decomposition; Seam-3 generational
objects; F7 guard; fsck tool; F5(a)(c) lock safety.
Phase 2 pre-1.0: F11 taxonomy; API pass (F9 + replace_remote + PushResult); F6 offline
mode; ops docs.
Not-worth-it: CAS commit protocols; content-addressed objects; segmented/delta index;
multi-writer merge; sidecar files anywhere; heartbeat leases before F5(a)(c) prove
insufficient.

## Not examined

Anything live (no S3/network; provider semantics modeled, not proven); copy_remote
end-to-end; s3func b2.py/signer.py/http_url.py; booklet internals beyond ebooklet's paths;
cfdb interior beyond edataset.py; envlib beyond catalogue.py's ebooklet consumption;
performance at real scale (O(total-keys) claims are code-derived); Change.discard residue
(suspected minor inconsistency, flagged for the API pass, not verified).
