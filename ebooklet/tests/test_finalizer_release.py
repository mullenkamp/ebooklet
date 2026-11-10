"""The finalizer and close() always release the S3 lock (2026-11-10; code review booklet-reindex-code-1).

A raise in an earlier teardown step used to skip ``lock.release()``, and a writer that died that way left an orphaned
lock ticket on the remote that stalled every later writer. These tests drive the real ``ebooklet_finalizer`` with
stand-ins whose steps raise, and record which steps ran.
"""
import pytest

from ebooklet import main as ebmain
from ebooklet.utils import ebooklet_finalizer


class Step:
    def __init__(self, log, name, exc=None):
        self.log, self.name, self.exc = log, name, exc

    def __call__(self, *args):
        self.log.append(self.name)
        if self.exc is not None:
            raise self.exc


def parts(log, raising=None, exc=OSError('injected')):
    class Obj:
        pass
    objs = {}
    for name, method in (('local_file', 'close'), ('remote_index', 'close'), ('remote_session', 'close'),
                         ('lock', 'release'), ('journal', 'persist')):
        o = Obj()
        setattr(o, method, Step(log, f'{name}.{method}', exc if name == raising else None))
        objs[name] = o
    return objs


@pytest.mark.parametrize('raising', ['local_file', 'remote_index', 'remote_session'])
def test_finalizer_releases_the_lock_when_a_close_raises(raising):
    log = []
    o = parts(log, raising)
    with pytest.raises(OSError):
        ebooklet_finalizer(o['local_file'], o['remote_index'], o['remote_session'], o['lock'], o['journal'])
    assert 'lock.release' in log, log


def test_finalizer_releases_the_lock_on_keyboard_interrupt_in_journal_persist():
    log = []
    o = parts(log, 'journal', exc=KeyboardInterrupt('injected'))
    with pytest.raises(KeyboardInterrupt):
        ebooklet_finalizer(o['local_file'], o['remote_index'], o['remote_session'], o['lock'], o['journal'])
    assert log == ['journal.persist', 'local_file.close', 'remote_index.close', 'remote_session.close',
                   'lock.release'], log


def test_finalizer_swallows_and_logs_an_ordinary_persist_failure():
    log = []
    o = parts(log, 'journal', exc=OSError('injected'))
    ebooklet_finalizer(o['local_file'], o['remote_index'], o['remote_session'], o['lock'], o['journal'])
    assert log[-1] == 'lock.release', log


def test_close_runs_the_finalizer_when_sync_raises():
    calls = []

    class Fake:
        def sync(self):
            calls.append('sync')
            raise OSError('injected sync failure')

        def _finalizer(self):
            calls.append('finalizer')

    with pytest.raises(OSError):
        ebmain.EVariableLengthValue.close(Fake())
    assert calls == ['sync', 'finalizer'], calls
