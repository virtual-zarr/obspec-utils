import xarray as xr
from obspec_utils.obspec import BufferedStoreReader, EagerStoreReader
from obstore.store import LocalStore


def test_local_reader(local_netcdf4_file) -> None:
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = BufferedStoreReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_eager_reader(local_netcdf4_file) -> None:
    """Test that EagerStoreReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = EagerStoreReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_eager_reader_interface(local_netcdf4_file) -> None:
    """Test that EagerStoreReader implements the same interface as BufferedStoreReader."""
    store = LocalStore()
    buffered_reader = BufferedStoreReader(store=store, path=local_netcdf4_file)
    eager_reader = EagerStoreReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_buffered = buffered_reader.readall()
    data_eager = eager_reader.readall()
    assert data_buffered == data_eager
    assert isinstance(data_eager, bytes)


def test_eager_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that EagerStoreReader can perform multiple reads."""
    store = LocalStore()
    reader = EagerStoreReader(store=store, path=local_netcdf4_file)

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
