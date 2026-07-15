#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scale-tier live push regression (deselected by default; run with
`uv run pytest -m scale` at release cadence — NEVER in CI).

T3 from the 2026-07-15 transport review: the regime-faithful concurrency
shape — a real ebooklet push of ~10 large groups through 10 upload threads
over a real WAN. The pass condition that matters is the ZERO-RETRY assert:
the 0.10.0-era failure mode was a push that "succeeded" while silently
re-sending full multi-MB bodies through urllib3 retries (the run-B storm);
this test treats any urllib3 retry as a failure, so that shape can never
again pass unnoticed. On a link fast enough that no timeout is ever at risk
the test degrades to a plain full-push integrity check — still useful.

Transfer budget: ~480 MB per run. The test remote is deleted in finally.
NOTE: a crashed prior run can leave a young write-lock ticket on the fixed
db_key; break it with S3SessionWriter.break_other_locks(timestamp=<now>).
"""
import io
import logging
import os
import pathlib

import pytest

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from ebooklet import open_ebooklet, S3Connection

pytestmark = pytest.mark.scale

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with io.open(script_path.joinpath('s3_config.toml'), 'rb') as f:
        _cc = toml.load(f)['connection_config']
    endpoint_url = _cc['endpoint_url']
    access_key_id = _cc['access_key_id']
    access_key = _cc['access_key']
    _HAVE_CREDS = True
except Exception:
    _HAVE_CREDS = os.environ.get('access_key') is not None
    if _HAVE_CREDS:
        endpoint_url = os.environ['endpoint_url']
        access_key_id = os.environ['access_key_id']
        access_key = os.environ['access_key']

bucket = 'achelous'
_DB_KEY = 'scale-push-test'
_N_VALUES = 12
_VALUE_SIZE = 40 * 2**20     # ~40MiB/group at num_groups=11 - the failing shape


class _RetryCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@pytest.mark.skipif(not _HAVE_CREDS, reason='no s3 credentials available')
def test_concurrent_large_push_no_retries(tmp_path):
    conn = S3Connection(access_key_id=access_key_id, access_key=access_key,
                        bucket=bucket, endpoint_url=endpoint_url, db_key=_DB_KEY)
    values = {f'k{i}': os.urandom(_VALUE_SIZE) for i in range(_N_VALUES)}

    capture = _RetryCapture()
    urllib3_logger = logging.getLogger('urllib3.connectionpool')
    urllib3_logger.addHandler(capture)
    try:
        with open_ebooklet(conn, tmp_path / 'scale.blt', flag='n', num_groups=11) as eb:
            for k, v in values.items():
                eb[k] = v
            result = eb.changes().push()
            assert result, f'push failed: {result.failures}'

        retries = [m for m in capture.messages if 'Retrying' in m]
        assert not retries, (
            f'{len(retries)} silent urllib3 retr(ies) during the push - full bodies were '
            f're-sent (the run-B storm shape). First: {retries[0]}'
        )

        ## Fresh-reader spot check: content survived the trip.
        fresh_conn = S3Connection(access_key_id=access_key_id, access_key=access_key,
                                  bucket=bucket, endpoint_url=endpoint_url, db_key=_DB_KEY)
        with open_ebooklet(fresh_conn, tmp_path / 'fresh.blt', flag='r') as r:
            assert r['k0'] == values['k0']
            assert r[f'k{_N_VALUES - 1}'] == values[f'k{_N_VALUES - 1}']
    finally:
        urllib3_logger.removeHandler(capture)
        session = conn.open('w')
        session.delete_remote()
