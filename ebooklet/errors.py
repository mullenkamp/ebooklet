"""
Typed exception taxonomy (0.10.0).

Every ebooklet-raised, consumer-facing error derives from `Error`, so
`except ebooklet.Error` catches exactly the errors ebooklet itself raises.
Exceptions that were builtin ValueErrors before 0.10.0 keep ValueError as a
second base for this release, and RemoteIntegrityError keeps urllib3's
HTTPError parentage, so pre-taxonomy `except ValueError` / `except HTTPError`
handlers continue to work while consumers migrate to the typed names.

Deliberately NOT typed (stay builtin): argument/configuration validation
(reserved-key and key-charset guards, num_groups conflicts, connection
parameter checks - TypeError/ValueError), the lock-acquire TimeoutError, and
copy_remote's target-already-exists ValueError. Transport/status failures
surface as urllib3.exceptions.HTTPError from the session layer.
"""
import urllib3


class Error(Exception):
    """Base class for all ebooklet exceptions."""


class ReadOnlyError(Error, ValueError):
    """
    A write was attempted on a session that cannot write - either the file
    was opened read-only (flag='r') or the remote session lacks write
    credentials. One name covers both: "you cannot write here", regardless
    of whether the mode or the credentials are the reason.
    """


class UUIDMismatchError(Error, ValueError):
    """
    Two things that must identify the same database don't: the local file
    carries a different UUID than the remote (stale cache of a deleted-and-
    recreated remote), or an http connection and an S3 connection given
    together point at different files.
    """


class RemoteMissingError(Error, ValueError):
    """
    The remote database does not exist where one was required: a read-only
    open with neither a remote database nor a local file, registering a
    nonexistent remote into a RemoteConnGroup, or copy_remote from a source
    that does not exist.
    """


class UnsupportedFormatError(Error, ValueError):
    """
    The remote database's storage format_version does not match what this
    ebooklet version supports (too new: upgrade ebooklet; too old: the remote
    must be re-created - 0.10 dropped the format-1 read path). A compatibility
    fault, deliberately distinct from RemoteIntegrityError (which means the
    store contradicts its own index).
    """


class GroupTooLargeError(Error, ValueError):
    """
    A group's packed size would exceed the 4-byte offset/length fields
    (2**32 - 1 bytes). Not retryable as-is: re-shard the database with a
    larger num_groups (flag='n' re-creation), or store smaller values.
    """


class RemoteIntegrityError(Error, urllib3.exceptions.HTTPError):
    """
    The remote store contradicts its own index: the index claims key(s) whose
    backing object is missing (404), and a fresh index re-pull confirmed the
    claim. Distinct from connectivity errors so callers can tell "the remote
    is inconsistent" apart from "the remote is unreachable"; subclasses
    urllib3's HTTPError so existing handlers keep working.
    """


class LockLostError(Error):
    """
    This session's write-lock ticket was broken by another client (their
    force_lock) - discovered by the pre-push or pre-commit verification.
    Mutual exclusion is gone, so the push aborts; all pending changes stay
    journaled. Re-open the file to re-acquire the lock and push again.
    Deliberately NOT an HTTPError: the remote is reachable - losing the lock
    is not a connectivity failure, and connectivity handlers must not treat
    it as one.
    """


class OfflineError(Error):
    """
    The operation requires the remote, but the session is in offline mode
    (opened with offline=True, or offline='auto' fell back after the remote
    was unreachable). Raised for remote-requiring operations and for reads of
    keys whose values are not materialized in the local cache.
    """


class PushInProgressError(Error, RuntimeError):
    """
    prune() or clear() was called while a push is running on this session.
    A compaction moves/destroys the local value bytes the push is reading
    (the push captures physical offsets up front), so it must wait for the
    push to finish.
    """


class ConcurrentCompactionError(Error, RuntimeError):
    """
    The push detected that a compaction (prune()/clear() on the underlying
    local booklet) ran after the push captured its value offsets - every
    captured offset is invalid, so the push aborts BEFORE its commit. Nothing
    was committed, staged, or journal-cleared; any group objects already
    uploaded are invisible orphans (fsck sweeps them). Re-run the push.
    Should be unreachable through the session API (PushInProgressError blocks
    prune/clear during a push); this is the belt for out-of-band compactions.
    """


## Transport-level failures that mean "the remote is unreachable" - the ONLY
## errors offline='auto' falls back on. HTTP *status* errors (403/500 etc.
## surface as bare HTTPError) and ebooklet's typed errors are deliberately
## NOT in this tuple: a misconfigured credential or an integrity/format/uuid
## fault must raise, never silently serve stale local data.
TRANSPORT_ERRORS = (
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ProtocolError,
    ConnectionError,
)
