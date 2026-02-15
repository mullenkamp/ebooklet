import pytest
import pathlib
import booklet
from ebooklet import utils, remote
import uuid6 as uuid

def test_check_local_vs_remote(tmp_path):
    local_path = tmp_path / "local.blt"
    # Create a local booklet file
    with booklet.open(local_path, "n", key_serializer="str", value_serializer="pickle") as db:
        db["key1"] = "value1"
        ts = db.get_timestamp("key1")
    
    # Mock remote_time_bytes
    remote_ts_bytes = booklet.utils.int_to_bytes(ts + 1000, 7) # Future timestamp
    
    with booklet.open(local_path, "w") as db:
        # Remote is newer
        assert utils.check_local_vs_remote(db, remote_ts_bytes, "key1") is True
        
        # Remote is same
        remote_ts_same = booklet.utils.int_to_bytes(ts, 7)
        assert utils.check_local_vs_remote(db, remote_ts_same, "key1") is False
        
        # Remote is older
        remote_ts_older = booklet.utils.int_to_bytes(ts - 1000, 7)
        assert utils.check_local_vs_remote(db, remote_ts_older, "key1") is False
        
        # Key not in remote
        assert utils.check_local_vs_remote(db, None, "key1") is None

def test_create_changelog(tmp_path):
    local_path = tmp_path / "local.blt"
    ri_path = tmp_path / "remote_index.blt"
    
    # Setup local
    with booklet.open(local_path, "n", key_serializer="str", value_serializer="pickle") as db:
        db["new_key"] = "val"
        db["updated_key"] = "new_val"
        db["same_key"] = "same_val"
        
        ts_new = db.get_timestamp("new_key")
        ts_updated = db.get_timestamp("updated_key")
        ts_same = db.get_timestamp("same_key")

    # Setup remote index
    with booklet.FixedLengthValue(ri_path, "n", key_serializer="str", value_len=7) as ri:
        # updated_key is older in remote
        ri["updated_key"] = booklet.utils.int_to_bytes(ts_updated - 1000, 7)
        # same_key is same in remote
        ri["same_key"] = booklet.utils.int_to_bytes(ts_same, 7)
        # new_key is missing in remote

    class MockRemoteSession:
        uuid = "some-uuid"

    with booklet.open(local_path, "w") as db:
        with booklet.FixedLengthValue(ri_path, "r") as ri:
            cl_path = utils.create_changelog(local_path, db, ri, MockRemoteSession())
            
            assert cl_path.exists()
            with booklet.FixedLengthValue(cl_path, "r") as cl:
                assert "new_key" in cl
                assert "updated_key" in cl
                assert "same_key" not in cl
            
            cl_path.unlink()
