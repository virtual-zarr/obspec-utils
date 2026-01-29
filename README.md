# obspec-utils

[![PyPI][pypi_badge]][pypi_link]
[![PyPI - Python Version][python_badge]][pypi_link]

[pypi_badge]: https://img.shields.io/pypi/v/obspec-utils.svg
[pypi_link]: https://pypi.org/project/obspec-utils/
[python_badge]: https://img.shields.io/pypi/pyversions/obspec-utils.svg

Utilities for cloud data access built on [obspec], fully compatible with [obstore]'s storage classes.

[obspec]: https://github.com/developmentseed/obspec
[obstore]: https://github.com/developmentseed/obstore

- **File-like readers** that work with xarray, rasterio, h5netcdf, and other libraries expecting file objects.
- **Composable store wrappers** for caching, request tracing, and splitting large requests.
- **Glob-style file discovery** with patterns like `data/**/*.nc`, using efficient prefix filtering.
- **AiohttpStore** for generic HTTPS access (e.g., THREDDS, NASA Earthdata).

## Installation

```sh
pip install obspec-utils
```

## Quick Example

```python
import xarray as xr
from obstore.store import S3Store
from obspec_utils.glob import glob
from obspec_utils.readers import EagerStoreReader

store = S3Store(
    bucket="its-live-data",
    aws_region="us-west-2",
    skip_signature=True,
)

# Find NetCDF files matching a pattern
files = glob(store, "NSIDC/velocity_image_pair_sample/landsatOLI/v02/N20E080/*.nc")
path = next(files)

with EagerStoreReader(store, path) as reader, xr.open_dataset(reader, engine="h5netcdf") as ds:
    print(ds)
```

## Documentation

Full documentation is available at [obspec-utils.readthedocs.io](https://obspec-utils.readthedocs.io).

## License

`obspec-utils` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
