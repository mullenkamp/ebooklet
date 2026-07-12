#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Persistent pending-change journal (Seam 2 of the architecture assessment).

The journal records this local file's unpushed state - written keys, pending
deletes, the num_groups choice, a pending remote replacement, and pending
metadata - in booklet reserved slot 1, so it survives session close (and most
crashes) instead of dying with the process. Before the journal, deletes were
memory-only (silently lost at close) and clock-skewed local edits never
entered the timestamp-diff changelog.

The journal is the only source of pending DELETES; for writes it is one leg
of a union with the timestamp diff (booklet auto-flushes its write buffer
mid-bulk-load, so data blocks can be durable while the journal - persisted at
sync boundaries, not per-write - is stale; the timestamp diff catches that
crash window).

Serialization is msgspec-JSON, encoded/decoded entirely here: booklet's
reserved-slot API is bytes-in/bytes-out and never sees msgspec.
"""
import msgspec

## Reserved-slot assignments (booklet 0.12.7 slots).
JOURNAL_SLOT = 1
REMOTE_STATE_SLOT = 2   # manifest + remote user-metadata cache (storage format 2)

JOURNAL_VERSION = 1


class JournalRecord(msgspec.Struct):
    """
    The serialized form of the journal. Decode is non-strict (unknown fields
    are tolerated, missing fields take defaults - the blob may outlive this
    ebooklet version); a `v` beyond JOURNAL_VERSION refuses loudly instead of
    default-parsing state it cannot understand.
    """
    v: int = JOURNAL_VERSION
    written: list = []
    deletes: list = []
    num_groups: int | None = None
    ## Tri-state via the bool: num_groups=None + num_groups_set=True means
    ## per-key storage was CHOSEN; num_groups_set=False means never recorded.
    num_groups_set: bool = False
    replace_pending: bool = False
    meta_pending: bool = False


class JournalState:
    """
    Live journal state: mutable sets and flags with dirty tracking.

    Invariant maintained by the mutation methods: written and deletes are
    DISJOINT - a re-written key must never stay pending-delete (the push's
    delete pass would remove the index entry its upload pass just wrote), and
    a deleted key must never stay pending-write.

    Persistence is if-dirty: read paths and 'r' sessions never mutate the
    journal, so persist() is a no-op for them (this also keeps journal
    persistence from invalidating live iterators when nothing changed).
    """

    __slots__ = ('written', 'deletes', 'num_groups', 'num_groups_set',
                 'replace_pending', 'meta_pending', '_dirty')

    def __init__(self, record: JournalRecord = None):
        if record is None:
            record = JournalRecord()
        self.written = set(record.written)
        self.deletes = set(record.deletes)
        self.num_groups = record.num_groups
        self.num_groups_set = record.num_groups_set
        self.replace_pending = record.replace_pending
        self.meta_pending = record.meta_pending
        ## Belt for the invariant on load - the mutation methods keep the sets
        ## disjoint, so an intersection can only come from a foreign writer.
        self.deletes -= self.written
        self._dirty = False

    @classmethod
    def load(cls, local_file):
        """
        Load the journal from reserved slot 1 of the local booklet, or start a
        fresh one when the slot has never been written (pre-journal files, new
        files, and flag='n' recreations - the 'n' truncation destroys the slot,
        which is exactly right: a replacement starts with no pending history).
        """
        raw = local_file.get_reserved(JOURNAL_SLOT)
        if raw is None:
            return cls()
        record = msgspec.json.decode(raw, type=JournalRecord)
        if record.v > JOURNAL_VERSION:
            raise ValueError(
                f'This local file carries a journal with version {record.v}, but this '
                f'ebooklet only supports up to {JOURNAL_VERSION}. Upgrade ebooklet to open it.'
            )
        return cls(record)

    def persist(self, local_file, force: bool = False):
        """
        Write the journal to reserved slot 1 when dirty (or forced - used after
        clear(), which destroys the slot regardless of dirtiness). The local
        booklet is opened writable in every ebooklet mode, so this never hits
        a read-only file; 'r' sessions simply never dirty the journal.
        """
        if not (self._dirty or force):
            return
        record = JournalRecord(
            v=JOURNAL_VERSION,
            written=sorted(self.written),
            deletes=sorted(self.deletes),
            num_groups=self.num_groups,
            num_groups_set=self.num_groups_set,
            replace_pending=self.replace_pending,
            meta_pending=self.meta_pending,
            )
        local_file.set_reserved(JOURNAL_SLOT, msgspec.json.encode(record))
        self._dirty = False

    ## Mutation methods - every one maintains the disjointness invariant and
    ## marks the state dirty.

    def record_write(self, key):
        self.written.add(key)
        self.deletes.discard(key)
        self._dirty = True

    def record_delete(self, key):
        self.deletes.add(key)
        self.written.discard(key)
        self._dirty = True

    def discard_written(self, key):
        if key in self.written:
            self.written.discard(key)
            self._dirty = True

    def discard_delete(self, key):
        if key in self.deletes:
            self.deletes.discard(key)
            self._dirty = True

    def clear_committed(self, written_keys=(), delete_keys=()):
        """Remove entries a successful commit has made durable remotely."""
        if written_keys or delete_keys:
            self.written -= set(written_keys)
            self.deletes -= set(delete_keys)
            self._dirty = True

    def clear_written(self):
        """For ebooklet.clear(): local values are gone, so pending writes are
        moot; pending DELETES and the replacement intent survive."""
        if self.written:
            self.written.clear()
            self._dirty = True

    def set_num_groups(self, num_groups):
        if not self.num_groups_set or self.num_groups != num_groups:
            self.num_groups = num_groups
            self.num_groups_set = True
            self._dirty = True

    def set_replace_pending(self, value: bool):
        if self.replace_pending != value:
            self.replace_pending = value
            self._dirty = True

    def set_meta_pending(self, value: bool):
        if self.meta_pending != value:
            self.meta_pending = value
            self._dirty = True
