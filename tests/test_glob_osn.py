"""
Compare obspec_utils.glob against fsspec.glob using real-world S3 data.

These tests hit the USGS OSN public endpoint and require network access.

To run these tests:
    uv run --all-groups pytest tests/test_glob_osn.py -v --network
"""

from __future__ import annotations

import pytest
import s3fs

from obspec_utils import glob
from obstore.store import S3Store


# Skip all tests in this module unless --network is passed
pytestmark = pytest.mark.network


STORAGE_ENDPOINT = "https://usgs.osn.mghpcc.org"
STORAGE_BUCKET = "esip"


@pytest.fixture(scope="module")
def s3_store() -> S3Store:
    """Create an S3Store for anonymous access to the OSN endpoint."""
    return S3Store(
        STORAGE_BUCKET,
        config={
            "AWS_ENDPOINT_URL": STORAGE_ENDPOINT,
            "AWS_REGION": "not-used",
            "AWS_SKIP_SIGNATURE": "true",
        },
    )


@pytest.fixture(scope="module")
def s3fs_filesystem() -> s3fs.S3FileSystem:
    """Create an s3fs filesystem for anonymous access to the OSN endpoint."""
    return s3fs.S3FileSystem(
        anon=True,
        client_kwargs={"endpoint_url": STORAGE_ENDPOINT},
    )


def fsspec_glob(fs: s3fs.S3FileSystem, pattern: str) -> set[str]:
    """Run fsspec glob, strip bucket prefix."""
    results = fs.glob(f"{STORAGE_BUCKET}/{pattern}")
    prefix = f"{STORAGE_BUCKET}/"
    return {r.removeprefix(prefix) for r in results}


def obspec_glob(store: S3Store, pattern: str) -> set[str]:
    """Run obspec_utils glob."""
    return set(glob(store, pattern))


class TestRealWorldGlob:
    """Compare obspec_utils.glob against fsspec.glob on real S3 data."""

    def test_cordex_arctic_nc_files(
        self, s3_store: S3Store, s3fs_filesystem: s3fs.S3FileSystem
    ):
        """Test discovering NetCDF files in cordex/arctic directory."""
        pattern = "rsignell/cordex/arctic/*.nc"
        obspec_results = obspec_glob(s3_store, pattern)
        fsspec_results = fsspec_glob(s3fs_filesystem, pattern)

        assert obspec_results == fsspec_results
        # Sanity check that we found some files
        assert len(obspec_results) > 0

    def test_cordex_recursive(
        self, s3_store: S3Store, s3fs_filesystem: s3fs.S3FileSystem
    ):
        """Test recursive glob with double star."""
        pattern = "rsignell/cordex/**/*.nc"
        obspec_results = obspec_glob(s3_store, pattern)
        fsspec_results = fsspec_glob(s3fs_filesystem, pattern)

        assert obspec_results == fsspec_results
        assert len(obspec_results) > 0

    def test_single_char_wildcard(
        self, s3_store: S3Store, s3fs_filesystem: s3fs.S3FileSystem
    ):
        """Test ? wildcard pattern."""
        pattern = "rsignell/cordex/arctic/rasm.45.????.01.nc"
        obspec_results = obspec_glob(s3_store, pattern)
        fsspec_results = fsspec_glob(s3fs_filesystem, pattern)

        assert obspec_results == fsspec_results

    def test_no_matches(self, s3_store: S3Store, s3fs_filesystem: s3fs.S3FileSystem):
        """Test pattern that matches nothing."""
        pattern = "rsignell/nonexistent_directory_xyz/*.nc"
        obspec_results = obspec_glob(s3_store, pattern)
        fsspec_results = fsspec_glob(s3fs_filesystem, pattern)

        assert obspec_results == fsspec_results == set()

    def test_literal_path(self, s3_store: S3Store, s3fs_filesystem: s3fs.S3FileSystem):
        """Test literal path without wildcards."""
        # First find an actual file to test with
        pattern = "rsignell/cordex/arctic/*.nc"
        files = list(glob(s3_store, pattern))
        if files:
            literal_path = files[0]
            obspec_results = obspec_glob(s3_store, literal_path)
            fsspec_results = fsspec_glob(s3fs_filesystem, literal_path)
            assert obspec_results == fsspec_results
            assert len(obspec_results) == 1
