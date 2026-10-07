<!-- ai-review-harness
round:      ebooklet-wog-code-2
agent:      gemini
requested:  model=gemini-3.1-pro-high effort=high
resolved:   model=gemini-3.1-pro-high
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-code-2.md
scopes:     ebooklet cfdb envlib envlib-ingest-base
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-07T11:35:53+13:00
-->

<!-- staged cfdb: 1736 KiB of 5488 KiB (3752 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8912 KiB of 230564 KiB (221652 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1464 KiB (708 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-base: 4368 KiB of 369548 KiB (365180 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 327 agent turn(s); the report is whichever is substantive -->
Here is the ranked list of findings based on a r
eview of the
changes and execution o
f the codeba
se.
### 1. [Accuracy] A3: Th
e migration script flow
silently del
etes all hydrated data (
High Severity)
**What it
is:** When migrating a database, th
e 0.10.5 migration scrip
t correctly
calls `eb.delete_
remote()`, which deletes the remote
but delibera
tely leaves `remote_stat
e.remote_ts`
intact as a watermark.
When the user republishe
s
by opening
the file in 0.11 (`flag=
'w'`), the 15-byte legac
y sidecar mi
sm
atches the 19-byte group
ed layout, so 0.11 unlin
ks it and sets `overwrit
e_remote_ind
ex =
True`. Since the remote is absent, it fetches a
fresh (empt
y) index and runs `recon
cile_local
_with_index` with `prev_synced_ts =
remote_state.remote_ts`.
The hydrate
d keys are unjournaled
and older than the watermark, so re
conciliation deletes all
of them.
**Why it matte
rs:** 100% d
ata loss
of the local file's hydrated conten
ts upon migration to 0.1
1.
**Confide
nce:** Verified.
**Evide
nce:**
-
**(a) What I derived:**
By tracing the 0.11 `_i
nit_common`
(`ebooklet/main.py
:598` discarding the sid
ecar) and `_pull_remote_
index` logic
against the 0.10
.5 source of `delete_remote` and `forget_remote_
incarnation`
. I confirmed through a
mutant script that a loc
ally
-hydrated unjournaled ke
y gets deleted by `recon
cile_local_with_index` w
hen compared
against an empty index
and an
old `remote
_ts`.
- **(b) What I did
not check:*
* I did not run the exac
t
`hydrate_and_delete.py` script again
st live S3 (as requested
to skip liv
e credentials), assuming
that `delete_remote()`
behaves
precisely as it did in 0.10.5.

###
2. [Accurac
y] D2: Ab
orted reconciliation falsely claims
freshness, p
ermanently locking in st
ale keys
**W
hat it is:** In `_pull_r
emote_index
` (`ebooklet/main.py:1169`), `reconc
ile_local_with_index
` can abort due to concurrent writes
(`RuntimeError`). When
it aborts, i
t logs a warning and ret
urns `[]`. The `_pull_remote_index`
method then continues an
d executes `self._local_
file._set_fi
le_timestamp(self
._remote_session.timestamp)` at line
1189.
**Why it matters:
** The local
file
is stamped as fully up-to-date with
the remote despite reco
nciliation s
kipping. Future opens wi
ll see `local_file._file
_timestamp == remote_sess
ion.timestamp` and skip
the index pu
ll entirely. Stale, remo
tely-deleted
keys will be served and
re-pushed forever.
**Co
nfidence:**
Verified.
**Evidence:**
- **(a) What
I
derived:** Reading `main.py` lines
1169-1189 an
d `utils.py` lines
618-625. The fallback r
eturn of `[]` guarantees
the stamp e
xecutes.
- **(b)
What I did not check:** I assumed t
hat a `Runti
meError` during dictiona
ry iteration (booklet's
concurrent m
utation guard) is the only reason the abort trig
gers, and th
at no other mechanism re
pairs the ti
mestamp later.

### 3.
[Accuracy / Simplicity]
A1: Post-commit `_load_
db_metadata`
exposes the session to
eventual consistency
**What it is:** At the end
of `update_
remote` (`ebooklet/utils
.py:18
55`), the session calls `remote_session._load_db
_metadata()`
, issuing an S3 HEAD req
uest right a
fter
the commit PUT. 
**Why
it matters:** On S3-comp
atible store
s with eventual consiste
ncy, or during a transie
nt network
404, this HEAD can return stale meta
data or "Not Found". Thi
s causes `re
mote_session.initialized
` to revert to `False` (and `uuid` t
o `None`), b
reaking the session inva
riants. A second push in
the same
session will then mistakenly think
the remote i
s absent, journal every
key (A2), and re-upload
the entire d
atabase.
**Confidence:** Verified.
**Evidenc
e:**
- **(a)
What I executed:** I ra
n a mutation test
(`test_stale_head2.py`) interceptin
g `_load_db_metadata` to
simulate a
404 return after the fi
rst push. The subsequent
push in the
same session re-uploade
d all 3 keys as
a brand-new group, confirming the r
egression.
- **(b) What
I did not ch
eck:** I assume
the underlying object store can exhibit eventua
l consistenc
y on HEAD after PUT.
**S
impler version:** The co
mmit PUT (`p
ut_
db_object`) already knows exactly what metadata
it just wrot
e. Remove `_load_db_meta
data()` and
assign
the properties (`timestamp = time_i
nt_us`, `uuid = local_fi
le.uuid.hex`
, `group_bytes`,
etc.) directly to the `remote_sessi
on`. This gi
ves up nothing except an
unnecessary S3 roundtri
p.

### 4.
[Simplicity] D1: Empty failure-free
pushes leak `.changelog
` files on disk
**What i
t is:** In
`Change.push` (`ebookle
t/main.py:346`), the cha
ngelog is unlinked only
`if committe
d:`. When a push is comp
letely empty (no changes
), `update_remote` skips
the commit
and returns `committed
=False, failures={}`. The `unlink()`
is skipped.
**Why it matters:** App
lications ca
lling `push()` periodica
lly on unchanged databases will leave empt
y `.blt.chan
gelog` files permanently
residing on
the filesystem.
**Confi
dence:** Verified.
**
Evidence:**
- **(a) What I executed:
** I ran an empty push v
ia `test_emp
ty_push
.py` and observed the `.changelog` file remained
on disk aft
er the script finished.
- **(b) What
I did not
check:** I
assume leaving temporary files in th
e directory
is considered undesirabl
e behavior.
**Simpler ve
rsion:** Cha
nge the guard to
`if not failures:`. Thi
s ensures the changelog
is unlinked both when ch
anges commit
successfully AND when a
clean no-op
push completes.

### 5. [Accuracy]
G: Malformed remote `gro
up_bytes` br
icks all readers and
writers
**What it is:**
In `remote.py:237`, `se
lf.group_bytes = int
(meta['group_bytes'])` p
arses the remote's byte
target without validatio
n. 
**Why it
matters:** If
a string is written to S3 metadata
erroneously, `int()` rai
ses `ValueError`, comple
tely crashin
g reader and writer init
ialization.
Additionall
y, if the value parses as `<= 0`, it
propagates
to `plan_groups` and cau
ses packing
algorithms to
behave pathologically.
**Confidence
:** High.
**Evidence:**
- **(a) What I derived:*
* Code
inspection
of `remote.py`.
- **(b) What I did n
ot check:** I assume ext
ernal modifi
cation of S
3 metadata i
s possible or that defensive program
ming against
internal bugs/edge case
s is desired
here.

---

### Clarification on yo
ur specific doubt (A2)

**You asked:
** "Is any o
f the post-round-1 code
more complic
ated than the problem
needs? For example: the
A2 journaling versus an alternative
such as cle
aring `remote_state.remo
te_ts
` when the remote is absent."

**A2
is completely necessary.
** I attempt
ed to cut A2 by setting
`replace_pending = True
` for absent remotes instead of expl
icit journaling. However
, `Change.pu
sh` aggressively purges
the local file of all un
journal
ed keys when
`replace_pending` is true (`ebookle
t/main.py:302-307`). Sin
ce
a hydrated file consists *entirely*
of unjournaled keys, sk
ipping the A
2 explicit journaling ca
used `Change.push` to de
lete
every local value befor
e the push even started
(verified via mutant). T
he A2 code protects hydrated data through both t
he `replace_pending` pur
ge and the post-crash reconciliation. Keep it.

<!-- finished: 2026-10-07T11:44:47+13:00 exit=0 -->
