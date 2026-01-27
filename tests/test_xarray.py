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
