import pytest
import xarray as xr
from obspec_utils.obspec import (
    BufferedStoreReader,
    EagerStoreReader,
    ParallelStoreReader,
)
from obstore.store import LocalStore


ALL_READERS = [BufferedStoreReader, EagerStoreReader, ParallelStoreReader]


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_with_xarray(local_netcdf4_file, ReaderClass) -> None:
    """Test that all readers work with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ReaderClass(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


@pytest.mark.parametrize("ReaderClass", [EagerStoreReader, ParallelStoreReader])
def test_reader_interface_matches_buffered(local_netcdf4_file, ReaderClass) -> None:
    """Test that readers implement the same interface as BufferedStoreReader."""
    store = LocalStore()
    buffered_reader = BufferedStoreReader(store=store, path=local_netcdf4_file)
    other_reader = ReaderClass(store=store, path=local_netcdf4_file)

    assert buffered_reader.readall() == other_reader.readall()
    assert isinstance(other_reader.readall(), bytes)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_multiple_reads(local_netcdf4_file, ReaderClass) -> None:
    """Test that readers can perform multiple reads with seek/tell."""
    store = LocalStore()
    reader = ReaderClass(store=store, path=local_netcdf4_file)

    # Read the first 100 bytes
    chunk1 = reader.read(100)
    assert len(chunk1) == 100
    assert isinstance(chunk1, bytes)

    # Read the next 100 bytes
    chunk2 = reader.read(100)
    assert len(chunk2) == 100
    assert isinstance(chunk2, bytes)

    # The two chunks should be different (different parts of the file)
    assert chunk1 != chunk2

    # Test tell
    position = reader.tell()
    assert position == 200

    # Test seek
    reader.seek(0)
    assert reader.tell() == 0

    # Re-reading from the beginning should give us the same data
    chunk1_again = reader.read(100)
    assert chunk1 == chunk1_again
