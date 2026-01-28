import pytest
import xarray as xr
from obspec_utils.readers import (
    BlockStoreReader,
    BufferedStoreReader,
    EagerStoreReader,
)
from obstore.store import LocalStore


ALL_READERS = [BufferedStoreReader, EagerStoreReader, BlockStoreReader]


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_with_xarray(local_netcdf4_file, ReaderClass) -> None:
    """Test that all readers work with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ReaderClass(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


@pytest.mark.network
def test_eager_reader_xarray_http_store():
    """
    Regression test: EagerStoreReader works with HTTPStore and xarray.

    This tests that:
    1. HTTPStore can fetch remote files
    2. EagerStoreReader correctly buffers the data
    3. The reader has the 'closed' property required by scipy/xarray
    4. xarray can open the dataset using the scipy engine
    """
    pytest.importorskip("xarray")
    import xarray as xr
    from obstore.store import HTTPStore

    store = HTTPStore.from_url(
        "https://raw.githubusercontent.com/pydata/xarray-data/refs/heads/master/"
    )
    with EagerStoreReader(store, "air_temperature.nc") as reader:
        # Verify reader has data
        assert len(reader._buffer.getvalue()) > 0

        # Verify closed property exists and is False
        assert reader.closed is False

        # Open with xarray (scipy engine for NetCDF Classic format)
        ds = xr.open_dataset(reader, engine="scipy")
        assert "air" in ds.data_vars
        ds.close()

    # Verify closed is True after context exit
    assert reader.closed is True
