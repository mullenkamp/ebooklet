"""
Table-driven tests of how a reader classifies a remote from its db object's metadata: the format gate
(too-new formats are refused) and the storage kind (per-key, grouped, legacy, or no remote).

Written BEFORE the rule was factored out of S3SessionReader into remote.classify_db_metadata /
remote.storage_kind_of (plan `mongo-read-path-plan.md`, Step 1), so the refactor is checked against
the behaviour it replaced. The MongoDB refresh applies the same helpers to a GET's metadata.
"""
import base64
import uuid as _uuid

import pytest

from ebooklet import remote, utils
from ebooklet.tests import fake_s3

_UUID = _uuid.uuid4()


def _meta(**over):
    meta = {
        'init_bytes': base64.urlsafe_b64encode(b'init').decode(),
        'timestamp': '1795420237763601',
        'uuid': _UUID.hex,
        'type': 'EVariableLengthValue',
    }
    meta.update(over)
    return {k: v for k, v in meta.items() if v is not None}


def _reader(meta):
    store = {}
    if meta is not None:
        store['db'] = (b'', meta)
    return fake_s3.make_reader(store, 'db')


CASES = [
    # (label, metadata or None for no remote, expected storage kind)
    ('no remote', None, None),
    ('format 1 (no stamp)', _meta(), utils.STORAGE_LEGACY),
    ('format 1 (explicit)', _meta(format_version='1'), utils.STORAGE_LEGACY),
    ('format 2 per-key', _meta(format_version='2'), utils.STORAGE_PER_KEY),
    ('format 2 hash-grouped', _meta(format_version='2', num_groups='16'), utils.STORAGE_LEGACY),
    ('format 3 grouped', _meta(format_version='3'), utils.STORAGE_GROUPED),
    ('format 3 with group_bytes', _meta(format_version='3', group_bytes='1048576'), utils.STORAGE_GROUPED),
]


@pytest.mark.parametrize('label,meta,expected', CASES, ids=[c[0] for c in CASES])
def test_storage_kind(label, meta, expected):
    reader = _reader(meta)
    assert reader.storage_kind == expected
    if meta is None:
        assert reader.uuid is None and reader.timestamp is None and not reader.initialized
    else:
        assert reader.initialized
        assert reader.uuid == _UUID
        assert reader.timestamp == 1795420237763601
        assert reader.type == 'EVariableLengthValue'


def test_recorded_group_bytes_is_parsed():
    assert _reader(_meta(format_version='3', group_bytes='1048576')).group_bytes == 1048576
    assert _reader(_meta(format_version='3')).group_bytes is None


@pytest.mark.parametrize('fv', ['4', '99'])
def test_too_new_format_is_refused(fv):
    with pytest.raises(utils.UnsupportedFormatError, match=f'format_version {fv}'):
        _reader(_meta(format_version=fv))


def test_helpers_agree_with_the_reader():
    """The pure helpers the MongoDB refresh uses give exactly what a reader gives, case by case."""
    for label, meta, expected in CASES:
        if meta is None:
            continue
        fields = remote.classify_db_metadata(meta)
        assert remote.storage_kind_of(fields['format_version'], fields['legacy_num_groups']) == expected, label
        reader = _reader(meta)
        for name in ('format_version', 'timestamp', 'uuid', 'type', 'legacy_num_groups', 'group_bytes'):
            assert fields[name] == getattr(reader, name), (label, name)
    with pytest.raises(utils.UnsupportedFormatError):
        remote.classify_db_metadata(_meta(format_version='4'))
