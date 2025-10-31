import xarray as xr
from obspec_utils import ObstoreReader
from obstore.store import LocalStore


def test_local_reader(local_netcdf4_file) -> None:
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ObstoreReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)
