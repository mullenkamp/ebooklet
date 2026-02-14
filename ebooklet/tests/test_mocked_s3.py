import pytest
import boto3
from moto import mock_aws
import ebooklet
from ebooklet import remote, RemoteConnGroup
import pathlib
import uuid6 as uuid
import urllib3
from botocore.exceptions import ClientError

@pytest.fixture
def s3_setup():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

@pytest.fixture
def remote_conn(s3_setup):
    return remote.S3Connection(
        access_key_id="testing",
        access_key="testing",
        db_key="test.blt",
        bucket=s3_setup,
        endpoint_url=None
    )

def test_basic_mocked_flow(remote_conn, tmp_path):
    local_path = tmp_path / "test.blt"
    
    with ebooklet.open(remote_conn, local_path, flag="c", value_serializer="pickle") as db:
        db["a"] = 1
        db["b"] = 2
        db.changes().push()
    
    # New local file, pull from remote
    local_path_2 = tmp_path / "test2.blt"
    with ebooklet.open(remote_conn, local_path_2, flag="r", value_serializer="pickle") as db:
        assert db["a"] == 1
        assert db["b"] == 2

def test_uuid_mismatch(remote_conn, tmp_path):
    local_path_1 = tmp_path / "db1.blt"
    local_path_2 = tmp_path / "db2.blt"
    
    # Initialize remote with db1
    with ebooklet.open(remote_conn, local_path_1, flag="c") as db:
        db["x"] = 1
        db.changes().push()
    
    # Try to open same remote with a different local db (which will have a different UUID)
    with ebooklet.open(remote_conn, local_path_2, flag="c") as db:
        # This should fail during initialization because remote UUID != local UUID
        # However, if local doesn't exist, it might adopt remote UUID if it's coded that way.
        # Let's create local_path_2 first as a separate db.
        pass
    
    with ebooklet.open(remote.S3Connection(
        access_key_id="testing", access_key="testing", db_key="other.blt", bucket=remote_conn.bucket
    ), local_path_2, flag="c") as db:
        db["y"] = 2
        db.changes().push()
        
    with pytest.raises(ValueError, match="The local file has a different UUID than the remote"):
        with ebooklet.open(remote_conn, local_path_2, flag="w") as db:
            pass

def test_remote_lock_conflict(remote_conn, tmp_path, mocker):
    local_path_1 = tmp_path / "db1.blt"
    local_path_2 = tmp_path / "db2.blt"
    
    # Open for writing (acquires lock)
    db1 = ebooklet.open(remote_conn, local_path_1, flag="c")
    
    # Attempt to open another session for writing
    # In a real scenario, this would block or fail depending on how s3func.lock is implemented.
    # For moto, we might need to verify it actually tries to acquire.
    
    with pytest.raises(Exception): # Adjust to specific lock error if known
        # Depending on s3func, it might timeout.
        # Let's mock the lock to fail immediately to test handling
        mocker.patch("s3func.S3Session.lock", side_effect=RuntimeError("Lock acquired by another"))
        ebooklet.open(remote_conn, local_path_2, flag="w")
    
    db1.close()

def test_remote_conn_group_expanded(s3_setup, tmp_path):
    rcg_conn = remote.S3Connection("testing", "testing", "group.rcg", s3_setup)
    db_conn1 = remote.S3Connection("testing", "testing", "db1.blt", s3_setup)
    db_conn2 = remote.S3Connection("testing", "testing", "db2.blt", s3_setup)
    
    # Initialize members
    for conn in [db_conn1, db_conn2]:
        with ebooklet.open(conn, tmp_path / conn.db_key, flag="c") as db:
            db["init"] = True
            db.changes().push()
            
    # Test RCG
    rcg_path = tmp_path / "group.rcg"
    with ebooklet.open(rcg_conn, rcg_path, flag="n", remote_conn_group=True) as group:
        group.add(db_conn1)
        group.add(db_conn2)
        group.changes().push()
        
        assert len(group) == 2
        
    # Reopen and verify
    with ebooklet.open(rcg_conn, rcg_path, flag="r") as group:
        items = list(group.values())
        assert len(items) == 2
        types = [it["type"] for it in items]
        assert all(t == "EVariableLengthValue" for t in types)

def test_corrupt_remote_index(remote_conn, tmp_path):
    local_path = tmp_path / "test.blt"
    
    with ebooklet.open(remote_conn, local_path, flag="c") as db:
        db["a"] = 1
        db.changes().push()
    
    # Manually corrupt the remote index (the main object)
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=remote_conn.bucket, Key=remote_conn.db_key, Body=b"not a booklet file")
    
    # Opening should fail when trying to read the remote index
    # Depending on implementation, it might be a ValueError or similar from booklet
    with pytest.raises(Exception):
        with ebooklet.open(remote_conn, tmp_path / "test_fail.blt", flag="r") as db:
            _ = db["a"]
