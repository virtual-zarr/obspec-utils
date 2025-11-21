import xarray as xr
from obspec_utils import ObstoreMemCacheReader, ObstoreReader
from obstore.store import LocalStore


def test_local_reader(local_netcdf4_file) -> None:
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ObstoreReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_memcache_reader(local_netcdf4_file) -> None:
    """Test that ObstoreMemCacheReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ObstoreMemCacheReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_memcache_reader_interface(local_netcdf4_file) -> None:
    """Test that ObstoreMemCacheReader implements the same interface as ObstoreReader."""
    store = LocalStore()
    regular_reader = ObstoreReader(store=store, path=local_netcdf4_file)
    memcache_reader = ObstoreMemCacheReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_regular = regular_reader.readall()
    data_memcache = memcache_reader.readall()
    assert data_regular == data_memcache
    assert isinstance(data_memcache, bytes)


def test_memcache_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that ObstoreMemCacheReader can perform multiple reads."""
    store = LocalStore()
    reader = ObstoreMemCacheReader(store=store, path=local_netcdf4_file)

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
