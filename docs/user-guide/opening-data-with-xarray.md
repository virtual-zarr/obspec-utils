# Opening Data with Xarray

This guide shows how to use `obspec-utils` readers to open cloud-hosted datasets with [xarray](https://docs.xarray.dev/).

## Overview

`obspec-utils` readers provide a file-like interface (`read`, `seek`, `tell`) that xarray can use directly with engines like `h5netcdf`. Combined with [obstore](https://developmentseed.org/obstore/latest/) for cloud storage access, this enables efficient reading of HDF5 and NetCDF files from S3, GCS, or Azure.

## Quick Start

This example opens a NetCDF file from the [NASA Earth Exchange (NEX) Data Collection](https://registry.opendata.aws/nasanex/) on AWS Open Data:

```python exec="on" source="above" session="xarray" result="code"
import xarray as xr
from obstore.store import S3Store
from obspec_utils.readers import EagerStoreReader

# Access public AWS Open Data (no credentials needed)
store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,  # Anonymous access
)

with EagerStoreReader(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp85_r1i1p1_inmcm4_2100.nc") as reader:
    ds = xr.open_dataset(reader, engine="h5netcdf")
    print(ds)
```
