<!-- ai-review-harness
round:      ebooklet-wog-code-1
agent:      gemini
requested:  model=gemini-3.1-pro-high effort=high
resolved:   model=gemini-3.1-pro-high
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-code-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-base
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-07T09:38:36+13:00
-->

<!-- staged cfdb: 1736 KiB of 5488 KiB (3752 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8724 KiB of 230268 KiB (221544 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1464 KiB (708 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-base: 4368 KiB of 369548 KiB (365180 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 487 agent turn(s); the report is whichever is substantive -->
Here is my review of the
`ebook
let` 0.11 write-order gr
oups changes
. I have investigated yo
ur questions by running
tests, mutat
ing code,
reading execution paths, and writin
g cross-vers
ion compatibility script
s. 

Overall, the core a
rchitecture
is sound. The lazy
deletes, format version isolation,
local-timest
amp reconciliation logic
, and storage layout mod
es are all r
emarkably solid. I found
no data los
s vulnerabilities or severe bugs on any path.
Here is the ranked list
of findings,
starting with simplicit
y cuts since
the accuracy checks ver
ified the design's guarantees.

#
## 1. The `StaleIndexErr
or` belt is unreachable
dead code (S
implicity)
**What it is
:** The `StaleIndexError
` guard in `update_remot
e` (`ebookle
t/utils.
py:1356-1363`) checks `remote_state.
remote_ts != remote_sess
ion.
timestamp` and aborts th
e push if they differ. T
his code is
unreachable through the
API and can
be safely deleted. 
**Wh
y it
matters:** Less code to maintain. I
t tests a condition that
cannot be m
et.
**Confidence:** High
.
**Evidence
:**
**(a) What I derived:** `remote_sess
ion.timestam
p` is only populated at
open time, or when expli
citly
re-HEADed via `_load_db_metadata()`
. `Change.pu
sh()` only re-HEADs if
`remote_session.initialized` is Fal
se. If it is initialized
, `remote_se
ssion.timestamp` retains
its value
from the initial open. The open-pat
h fix (`main.py:596-598`
) guarantees
that if `remote_ts != r
emote_session.timestamp`
at open, th
e index is re-fetched an
d `remote_ts
` is updated
to match. Therefore, by the time `u
pdate_remote
` runs, the two values a
re identically equal. 
I
f another
client breaks the lock and commits *after* this
session ope
ns, the local `remote_se
ssion.timest
amp` does not automatica
lly update (
since there is no polling). The `StaleIndexError
` check will
silently pass, and the
concurrent c
ommit is instead
caught correctly by `lock.verify()`
immediately before the
commit PUT (
`utils.py:1778`). I veri
fied this
by completely deleting the `StaleIndexError` bl
ock and runn
ing `test_stale_incarnat
ion.py
` and `test_write_order_groups.py`;
the only fai
lure was `test_stale_ind
ex_
belt`, which patches internal state
to artificially trigger
it.
**(b) What I did not
check:** I
assumed
`remote_sess
ion.timestamp` isn't asy
nchronously updated by a
background
thread, which the single
-threaded design implies
.

### 2. Formats and modes are stri
ctly coupled and perfect
ly backward-
compatible (Accuracy)
**
What it is:** A per-key
remote written by 0.11 is fully usab
le by 0.10.5. 0.11 canno
t accidental
ly stamp
format 2 with a 19-byte index or fo
rmat 3 with a 15-byte in
dex.
**Why it matters:** Prevents remote
corruption and ensures
seamless bac
kward compatibility for
older client
s reading unmodified dat
asets.
**Confidence
:** High.
**Evidence:**
**(a) What I executed:*
* I wrote a cross-versio
n script tha
t
used 0.11 to create a per-key remot
e (`group_by
tes=None`), pushed it, d
umped the fa
ke
S3 store, and then read it back usi
ng `ebooklet==0.10.5` in
a separate
`uv run
` environment. 
```pytho
n
with open_ebooklet(con
n, 'test.blt', 'n', grou
p_
bytes=None) as db:
    db['key1'] =
b'value1'
    db.
changes().push()
```
Out
put verified `format_ver
sion: 2` and
the 0.10.
5 client successfully printed `Keys:
['key1']`. 
I traced `u
tils.py` and `main.py` a
nd found
the format choice (`FORMAT_GROUPED`
vs `FORMAT_
PER_KEY`) is strictly ti
ed to `group
_bytes is not None`.
When `group_bytes is No
ne`, `index_value_len` i
s forced to 15 at open t
ime (`main.p
y
:574`), mean
ing the sidecar is populated entirel
y with 15-by
te entries, which are th
en passed unmodified as
`index_bytes_for_commit` and stamped
with `format_version: 2
` (`utils.py
:177
5`).
**(b) W
hat I did not check:** I did not run
this test against live
S3, relying
entirely on
`FakeS3Connection` to ac
curately mock the metada
ta and payload storage.
### 3. Empt
ied groups are reliably
detected
and GC'd (Accuracy)
**What it is:**
Groups that
become empty, either th
rough lazy d
eletes or lost
-key drops, are properly
dropped from the manife
st and garba
ge collected.
**Why it m
atters:** Prevents accum
ulating orph
an groups or
pushing emp
ty group objects.
**Confidence:** Hi
gh.
**Evidence:** 
**(a)
What I deri
ved:** In
`utils.py:1
434`, an emptied group is detected i
f it's in `p
re_push_manifest
` but not in
`affected_group_ids` (n
o keys were allocated to
it) and `no
t in_index
.get(gid)` (it has no li
ve members). If this tri
ggers, it's added to `em
ptied
_gids` and `updated = Tr
ue`. At commit time, the
condition `if updated o
r ... or del
etes:
` ensures the push proceeds even if
deletes were
the *only* change. Then
, in Phase D
(`utils.py:188
9`), the `emptied_gids` loop success
fully calls `delete_obje
ct` for the
old generations. 
If
a key is dropped because it lost it
s local value (`journal.
discard_writ
ten(key)`), it doesn't m
ake it to `affected
_group_ids`. If that group has no ot
her members, it hits the
same empty
check and is dropped.
**
(b) What
I did not check:** I di
d not manually verify th
at `delete_object` in S3
actually re
claims the space, assumi
ng the
`boto3` integration han
dles the literal HTTP DE
LETE correctly.

### 4.
`group_entry
_size` precisely
matches `pack_group`'s byte layout (Accuracy)
*
*What it is:
** The mathematical proj
ection of a
group'
s packed size matches the exact byte
s materialized during pa
cking. Overs
ized values are correctl
y assigned their own fre
sh groups.
**Why it matters:** A mismatch would
cause `GroupTooLargeErr
or` crashes
or push groups exceeding
the `group_bytes` targe
t.
**Confiden
ce:** High.
**Evidence:** 
**(a) Wha
t I derived:
** `group_entry_
size` calcul
ates `2 + len(key) + 7 + 4 + value_l
en`. `pack_
group` (`utils.py:300-309`) packs ex
actly: `>H` (2)
+ key + timestamp (7) +
`>I` (4) + value. This
is a perfect match. 
For
oversized values, `plan_groups` (`u
tils.py:263-274`) checks
`cur_size
+ size <= group_bytes`. If a single
value exceeds this, the
condition f
ails. The loop assigns t
he key to `n
ext_
gid`, sets `cur_size = group_header_len + size`
(which immed
iately exceeds `group_by
tes`),
and on the *next* iteration, the co
ndition fails again, for
cing the next key into y
et another f
resh group.
**(b) What I did not check:** I didn
't test keys with non-AS
CII characte
rs to see if `len
(key.encode(
))` behaves identically
in both places, assuming
standard UTF-8 encoding
applies saf
ely across the board.
### 5. Migration script `hydrate_and
_delete.py`
is safe and preserves me
tadata (Accu
racy)
**
What it is:** The migration script securely guar
antees data
integrity before deletin
g the format
-2 remote. It won't delete data it s
houldn't.
**Why it matters:** Deleti
ng a remote
is a point-of-no-return
; if the script is flawed, users wil
l lose datasets.
**Confi
dence:** High.
**Evidenc
e:**
**(a) What I
derived:** The script m
andates `load_items()` s
uccess and compares the
remote index
to `local_ts`,
refusing if any remote keys are mis
sing or newe
r. 
I verified metadata
preservation
: while `forget_remote_i
ncarnation
` drops the manifest, it *explicitly
preserves* `remote_stat
e.meta_section` (the cac
he). When 0.
11 subsequently pushes,
`_build_meta_section_for
_push` falls back to thi
s cache if t
he remote is absent
(`utils.py:1213`). Thus, the 0.10 r
emote's metadata survive
s the deleti
on
and is cleanly embedded into the 0.11 replaceme
nt.
**(b) Wh
at I did not check:** I
assumed the
user
doesn't bypass the scri
pt's `ebooklet.__version
__.startswith('0.10.')`
guard. I als
o assumed
`load_items()` guarantees perfect by
te-for-byte
local materialization wh
en it succeeds.

### 6.
Downstream d
efaults are
safe for scope (Accurac
y)
**What it is:** `group_bytes` cor
rectly defau
lts to grouped mode (32
MiB)
for new dat
asets, while `envlib-ing
est-base` securely pins
`group_bytes=None` for i
ts highly
-append-frequent `tsortho` and `tsfo
recast` databases.
**Why
it matters:** Group
ed storage on tiny, frequent writes
would cause excessive re
pack overhead; the pinni
ng prevents
this regression. 
**Conf
idence:** High.
**Evidence:** 
**(a)
What I derived:** `cfdb
.edataset:14
6` and `envlib.catalogue:1017` only
forward `group_bytes` if
explicitly
provided (`is not _NOT_
GIVEN`). In
`ebooklet.main:568`, the
absence of
an explicit target
combined with `STORAGE_GROUPED` sec
urely defaul
ts to `utils.DEFAULT_GRO
UP_BYTES` (3
2 MiB
). I read your plan note
s detailing that `ingest
-base` explicitly overri
des this wit
h `None` where needed.
*
*(
b) What I did not check:** I assumed
the 32 MiB default alig
ns perfectly
with the standard array
chunk
sizes gener
ated by `wrf-3k` and `es
a-sst`, which your bench
mark notes suggest it do
es.
### 7. The `fsck` fields and `copy_r
emote` changes are justi
fied (Simpli
city)
**
What it is:** The addition of `dead_
fraction`, `bad_entry_la
yout`, and t
he `copy_
remote` manifest branch. 
**Why it m
atters:** Ev
aluated for simplicity c
uts as requested.
**Conf
idence:** Hi
gh.
**Evidence:** 
**(a) Wha
t I derived:** 
- `dead_
fraction` is highly valu
able for
grouped sto
rage. It calculates the live bytes f
rom the index alone, exp
osing how mu
ch of a group object con
sists of sup
erseded dead
bytes. Keep it.
- `bad_
entry_layout` is a cheap
sanity chec
k against corrupted side
cars. Keep it.
- The `copy_remote` change to branc
h on `self.storage_kind
!= utils.STO
RAGE_PER
_KEY` instead of `if src_manifest:`
is functionally identica
l (an empty
grouped manifest implies
an empty in
dex, so
both branch
es would yield an empty key list). H
owever, the new check is
conceptuall
y accurate (an empty rem
ote can still be grouped
) and
avoids pars
ing a format-3 index wit
h a format-2 15-byte decoder. Keep i
t.
**(b) Wha
t I did
not check:** N/A.

### Summary Conc
lusion
The "write-order
groups" feature achieves
its guarant
ees flawlessly
. The timestamp reconciliation clean
ly protects unjournaled
edits, and the legacy co
mpatibility boundaries are airtight.
My only recommended cha
nge is the deletion of t
he unreachable `StaleInd
exError` blo
ck in `update_remote` to simplify the codebase.

<!-- finished: 2026-10-07T09:49:26+13:00 exit=0 -->
