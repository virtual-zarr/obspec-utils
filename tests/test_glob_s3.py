"""
Compare obspec_utils.glob against fsspec.glob using S3 (MinIO).

Requires Docker to run MinIO container.
"""

from __future__ import annotations

import io

import pytest

from obspec_utils import glob
import s3fs

from obstore.store import S3Store


def _docker_available() -> bool:
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available for MinIO"
)


@pytest.fixture
def s3_glob_env(minio_bucket):
    """Add test files to MinIO bucket and return S3Store + s3fs."""
    client = minio_bucket["client"]
    bucket = minio_bucket["bucket"]
    endpoint = minio_bucket["endpoint"]
    username = minio_bucket["username"]
    password = minio_bucket["password"]

    files = [
        "data/file1.nc",
        "data/file2.nc",
        "data/2024/01/temp_data.nc",
        "data/2024/01/perm_data.nc",
        "data/2024/02/temp_data.nc",
        "data/2023/old_data.nc",
        "data/readme.txt",
        "other/file.nc",
        "root.nc",
        # Files for testing ] as first char in character class
        "data/file].nc",
        "data/file[.nc",
        "data/filea.nc",
    ]

    for path in files:
        content = f"content of {path}".encode()
        client.put_object(bucket, path, io.BytesIO(content), len(content))

    store = S3Store(
        bucket,
        config={
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ACCESS_KEY_ID": username,
            "AWS_SECRET_ACCESS_KEY": password,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
        },
    )

    fs = s3fs.S3FileSystem(
        endpoint_url=endpoint,
        key=username,
        secret=password,
    )

    return {"store": store, "fs": fs, "bucket": bucket}


def fsspec_glob(fs, bucket: str, pattern: str) -> set[str]:
    """Run fsspec glob, strip bucket prefix."""
    results = fs.glob(f"{bucket}/{pattern}")
    prefix = f"{bucket}/"
    return {r.removeprefix(prefix) for r in results}


def obspec_glob(store, pattern: str) -> set[str]:
    """Run obspec_utils glob."""
    return set(glob(store, pattern))


class TestVsFsspecS3:
    """Compare obspec_utils.glob against fsspec.glob on S3."""

    def test_single_star(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/*.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_double_star(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/**/*.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_double_star_prefix(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/**/temp_*.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_question_mark(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file?.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_character_class(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file[12].nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_no_matches(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "nonexistent/*.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern) == set()

    def test_literal_path(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file1.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_multiple_wildcards(self, s3_glob_env):
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/202?/**/*.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_bracket_literal_in_class(self, s3_glob_env):
        """[]] should match literal ] as first char in class."""
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file[]].*"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_negated_bracket_literal_in_class(self, s3_glob_env):
        """[!]] should match any char except ] as first char after !."""
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file[!]].nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)

    def test_unclosed_bracket_as_literal(self, s3_glob_env):
        """[ without closing ] should be treated as literal character."""
        store, fs, bucket = (
            s3_glob_env["store"],
            s3_glob_env["fs"],
            s3_glob_env["bucket"],
        )
        pattern = "data/file[.nc"
        assert obspec_glob(store, pattern) == fsspec_glob(fs, bucket, pattern)


class TestErrorHandling:
    """Test that glob returns useful errors for misconfigured stores."""

    def test_misconfigured_store_wrong_credentials(self, minio_bucket):
        """Glob with wrong credentials should raise an error with useful message."""
        bucket = minio_bucket["bucket"]
        endpoint = minio_bucket["endpoint"]

        # Create store with wrong credentials
        store = S3Store(
            bucket,
            config={
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ACCESS_KEY_ID": "wrong_key",
                "AWS_SECRET_ACCESS_KEY": "wrong_secret",
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
            },
        )

        with pytest.raises(Exception) as exc_info:
            list(glob(store, "**/*.nc"))

        # Verify the error message contains useful information
        error_msg = str(exc_info.value).lower()
        assert any(
            keyword in error_msg
            for keyword in ["access", "denied", "credentials", "signature", "forbidden"]
        ), f"Error message should indicate auth failure, got: {exc_info.value}"

    def test_misconfigured_store_wrong_endpoint(self, minio_bucket):
        """Glob with wrong endpoint should raise a connection error."""
        bucket = minio_bucket["bucket"]

        # Create store with wrong endpoint (port that's not listening)
        store = S3Store(
            bucket,
            config={
                "AWS_ENDPOINT_URL": "http://localhost:19999",
                "AWS_ACCESS_KEY_ID": minio_bucket["username"],
                "AWS_SECRET_ACCESS_KEY": minio_bucket["password"],
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
            },
        )

        with pytest.raises(Exception) as exc_info:
            list(glob(store, "**/*.nc"))

        # Verify the error indicates a connection issue
        error_msg = str(exc_info.value).lower()
        assert any(
            keyword in error_msg
            for keyword in ["connect", "connection", "refused", "error", "failed"]
        ), f"Error message should indicate connection failure, got: {exc_info.value}"

    def test_misconfigured_store_nonexistent_bucket(self, minio_bucket):
        """Glob with nonexistent bucket should raise a bucket not found error."""
        endpoint = minio_bucket["endpoint"]

        # Create store pointing to a bucket that doesn't exist
        store = S3Store(
            "nonexistent-bucket-12345",
            config={
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ACCESS_KEY_ID": minio_bucket["username"],
                "AWS_SECRET_ACCESS_KEY": minio_bucket["password"],
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
            },
        )

        with pytest.raises(Exception) as exc_info:
            list(glob(store, "**/*.nc"))

        # Verify the error indicates the bucket doesn't exist
        error_msg = str(exc_info.value).lower()
        assert any(
            keyword in error_msg
            for keyword in [
                "bucket",
                "nosuchbucket",
                "not found",
                "does not exist",
                "access denied",
            ]
        ), f"Error message should indicate bucket issue, got: {exc_info.value}"
