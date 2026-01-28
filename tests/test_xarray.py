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


@pytest.mark.network
def test_readme_example():
    """
    Test the example from the README frontpage.

    Uses ERA5 data from the NSF-NCAR public S3 bucket.
    Verifies output matches fsspec.
    """
    import s3fs
    from obstore.store import S3Store

    bucket = "nsf-ncar-era5"
    path = (
        "e5.oper.an.pl/202501/e5.oper.an.pl.128_060_pv.ll025sc.2025010100_2025010123.nc"
    )

    store = S3Store(
        bucket=bucket,
        aws_region="us-west-2",
        skip_signature=True,
    )
    fs = s3fs.S3FileSystem(anon=True)

    with (
        fs.open(f"{bucket}/{path}") as f,
        BlockStoreReader(store, path) as reader,
        xr.open_dataset(f, engine="h5netcdf") as ds_fsspec,
        xr.open_dataset(reader, engine="h5netcdf") as ds_obspec,
    ):
        # Compare indexes
        assert list(ds_fsspec.indexes) == list(ds_obspec.indexes)
        # Load just one point to verify data access
        var = list(ds_obspec.data_vars)[0]
        subset_fsspec = ds_fsspec[var].isel({d: 0 for d in ds_fsspec[var].dims})
        subset_obspec = ds_obspec[var].isel({d: 0 for d in ds_obspec[var].dims})
        xr.testing.assert_equal(subset_fsspec, subset_obspec)
