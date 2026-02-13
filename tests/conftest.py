import json
import time
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--network",
        action="store_true",
        default=False,
        help="run tests that require network access",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--network"):
        # --network given: do not skip network tests
        return
    skip_network = pytest.mark.skip(reason="need --network option to run")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip_network)


@pytest.fixture(scope="session")
def container():
    import docker

    client = docker.from_env()
    port = 9000
    minio_container = client.containers.run(
        "quay.io/minio/minio",
        "server /data",
        detach=True,
        ports={f"{port}/tcp": port},
        environment={
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
        },
    )
    time.sleep(3)  # give it time to boot
    # enter
    yield {
        "port": port,
        "endpoint": f"http://localhost:{port}",
        "username": "minioadmin",
        "password": "minioadmin",
    }
    # exit
    minio_container.stop()
    minio_container.remove()


@pytest.fixture(scope="session")
def minio_bucket(container):
    # Setup with guidance from https://medium.com/@sant1/using-minio-with-docker-and-python-cbbad397cb5d
    from minio import Minio

    bucket = "my-bucket"
    filename = "test.nc"
    # Initialize MinIO client
    client = Minio(
        "localhost:9000",
        access_key=container["username"],
        secret_key=container["password"],
        secure=False,
    )
    client.make_bucket(bucket)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": "arn:aws:s3:::my-bucket",
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectRetention",
                    "s3:GetObjectLegalHold",
                ],
                "Resource": "arn:aws:s3:::my-bucket/*",
            },
        ],
    }
    client.set_bucket_policy(bucket, json.dumps(policy))
    yield {
        "port": container["port"],
        "endpoint": container["endpoint"],
        "username": container["username"],
        "password": container["password"],
        "bucket": bucket,
        "file": filename,
        "client": client,
    }


@pytest.fixture
def local_netcdf4_file(tmp_path: Path) -> str:
    """Create a NetCDF4 file with data in multiple groups."""
    import xarray as xr

    filepath = tmp_path / "test.nc"
    ds1 = xr.DataArray([1, 2, 3], name="foo").to_dataset()
    ds1.to_netcdf(filepath)
    return str(filepath)
