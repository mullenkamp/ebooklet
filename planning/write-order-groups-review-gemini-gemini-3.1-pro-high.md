<!-- ai-review-harness
round:      ebooklet-wog-plan-1
agent:      gemini
requested:  model=gemini-3.1-pro-high effort=high
resolved:   model=gemini-3.1-pro-high
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-plan-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-wrf-3k
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-06T20:48:28+13:00
-->

<!-- staged cfdb: 1732 KiB of 5196 KiB (3464 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8184 KiB of 228148 KiB (219964 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1448 KiB (692 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-wrf-3k: 3192 KiB of 292760 KiB (289568 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 301 agent turn(s); the report is whichever is substantive -->
Here is the plan review.
The
core mechanics of the plan are rema
rkably solid
, but there are two crit
ical flaws i
n how it handles mode re
solution and caching for
older and offline clients.

### 1. (High) Mode
conflict on
`group_bytes` default wi
ll break downstream
per-key readers
**What:** The plan
sets `group_
bytes` to default to 32
MB and states it is "nev
er
resolved against the re
mote", and t
hat "A mode conflict wit
h the kwarg raises."
**W
hy it matter
s:** E
Can datasets are format-2 per-key. I
f downstream read script
s (like `rep
air_precip.py` and
others) call `open_ebooklet()` with
out explicitly passing `
group_bytes=
None`, they will receive
the 32 MB
default. The remote wil
l identify as per-key, c
onflicting with the 32 M
B kwarg, and
raise a
mode conflict. This breaks backward
compatibili
ty for any existing read
er script in
production.
**Confidenc
e:** High. `
group_bytes` defaulting to 32 MB means the kwarg
effectively always conf
licts with p
er-key remotes unless ex
plicitly pas
sed as `None`.
To maintain backward co
mpatibility,
an omitted kwarg MUST i
nherit the i
nitialized remote's mode
.

### 2. (H
igh) Unlinking mismatched sidecars dest
roys offline reader cach
es
**What:**
The plan specifies that
before `get
_remote_index_file`,
"a sidecar whose value_len does not
match the m
ode is unlinked and re-f
etched; it i
s a
cache."
**Why it matters:** If a us
er opens a migrated data
set offline
(e.g., fallback `offline
='auto'`),
the remote is uninitialized. The jo
urnal's `num_groups=13`
resolves the mode to `gr
ouped
` (format 3, expecting `
value_len=19`). The clie
nt will unlink the exist
ing 15-byte
sidecar, but fail to re-fetch it be
cause it is
offline. The reader lose
s its cache
entirely and sees 0 keys
.
**Confidence:** High.
**Evidence:** I executed
a test show
ing `booklet.FixedLength
Value` happi
ly ignores
the passed `value_len`
and reads a 15-byte file
using the h
eader's length. Because
the plan introduces `de
code_index_entry` that d
ispatches on
length, the offline rea
der *could*
have successfully utiliz
ed the 15-byte sidecar (offline reads do n
ot require `
gid`). Unlinking it dest
roys this ca
pability unnecessarily.
```bash
cat << 'EOF' > test_booklet_valuelen.py
import os
i
mport
booklet

def test():
db_path = "test_val.bl
t"
    with
booklet.FixedLengthValue
(
db_path, 'n', value_len=
15, key_serializer='str'
) as db:
db['a'] = b'0' * 15
with booklet.FixedL
engthValue
(db_path, 'w', value_len
=19, key_serializer='str
') as db:
pri
nt("db._value_len =", db._value_len)
test()
EOF
export UV
_PROJECT_ENVIRONMENT=/opt/envs/ebook
let && uv run test_bookl
et_valuelen.
py
```
**Output:**
```
db._value_len =
15
```

### 3. (Verifie
d)
Migration push correctly pushes all
keys and metadata
**Wha
t I executed
:** I ran a script mimic
king the cat
alogue migration: creating a hash-grouped remote
, clearing the S3 mock s
tore (simula
ting `delete_remote()`),
and pushing
the
same local file to the same key wit
h `flag='w'`
.
```bash
cat << 'EOF' >
test_migration_push.py
import os
im
port booklet
from ebookl
et import op
en_ebooklet
from ebookle
t.tests.
fake_s3 import FakeS3Con
nection

def test_migrat
ion_push():
db_path = "
test_migrate.blt"
    st
ore = {}
    conn = Fake
S3Connection(store, 'mig
rate_key')
# 1. Create a
hash-grouped remote
with open_eb
ooklet(conn, db_path
, flag='n', num_groups=5
) as db:
        db['k1'
] = b'
v1'
        db['k2'] = b'v2'
db.set_metadata({'
meta': 'data'})
        db.changes()
.push()
        
    pri
nt("Remote index after f
irst push
:")
    print(list(store.keys()))
    
    # 2.
Simulate del
ete_remote()
    store.c
lear
()
# 3. Try pushing the same local file
to the SAME key!
    wi
th open
_ebooklet(conn, db_path, flag='w', n
um_groups=5)
as db:
db.changes().push()
# 4. Check if the
keys are act
ually in the
new remote
    with open_ebooklet(c
onn, "test_read.blt", fl
ag='r') as reader:
print("Keys
in remote:", list(reader
.keys()))
        print(
"Metadata in remote:", r
eader.get_
metadata())

test_migration_push()
E
OF
export UV_PROJECT_ENV
IRONMENT=/opt/envs/ebook
let && uv run test_migration_push.py
```
**Output:**
```
Rem
ote index after first
push:
['migrate_key/1.c
13f8dbbab274', 'migrate_
key/3.0d3c106a619e4', 'm
igrate_key']
Keys in remo
te: ['k1', 'k2']
Metadata in remote:
{'meta': 'data'}
```
**Why it works:** When the remot
e is absent, `forget_rem
ote_incarnat
ion` unlinks the
sidecar and clears `rem
ote_state.meta_section`.
However, `c
reate_changelog`'s
`else` branc
h safely captures *all* keys in `loc
al_file.loca
tions()`. `update_remote
` gracefully
falls back to `local_file.get_metadata()` for t
he payload.
The UUID is also preserv
ed because i
t is
sourced from `local_file.uuid`.
**What I did no
t check:** I
assumed the catalogue p
roducer file
holds
a complete local image of the datab
ase. If it i
s only a partial image,
keys would b
e lost, but `migration
_check` guards against this.

### 4.
(Verified)
Repack membership from t
he index alo
ne + lazy
deletes is safe
**What I derived:**
I reviewed
`__delitem__` and the jo
urnal replay
. Both remove deleted ke
ys directly from the
local `remo
te_index`. When the push
scans the `remote_index
` to build the repack me
mber list, t
he deleted keys are inherently gone. T
he repacked
group naturally excludes
them withou
t uploading a group, and
the empty-group GC will
cleanly drop the manifest entry.
**
What I did not check:**
I assumed th
e new index scan correct
ly aggregates all live keys for the
repacked groups without
performance issues on la
rge indices.

### 5. (Ve
rified) `loc
_map` physical
order is preserved despite timestam
p rewrites
**What I deri
ved:** `crea
te_changelog`'s skew nor
malization modifies
elements in `loc_map` in-place (`lo
c_map[key] = (new_ts, ..
.
)`). Diction
aries in Python 3.7+ preserve insert
ion order wh
en values are updated. T
herefore, wr
ite-order
allocation
is perfectly preserved d
espite the timestamp rew
rite.
**What
I did not check:** I as
sumed no paths completel
y remove and
re-insert keys into `loc_map` during the captur
e phase.

### 6. (Minor)
Typo in mig
ration command
**
What:** The
plan's step 4 states `open_edataset(
...) + push`. The interf
ace actually requires `.
changes().push()`. This will fail me
chanically a
nd isn't a design flaw, but is worth
noting for the rollout.

<!-- finished: 2026-10-06T20:59:45+13:00 exit=0 -->
