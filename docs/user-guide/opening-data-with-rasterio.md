# Opening Data with Rasterio

This guide shows how to use `obspec-utils` readers to open cloud-hosted raster data with [rasterio](https://rasterio.readthedocs.io/).

## Opening a Cloud-Hosted GeoTIFF

Use [`EagerStoreReader`][obspec_utils.readers.EagerStoreReader] to provide a file-like interface that rasterio can read:

```python exec="on" source="above" session="rasterio" result="code"
import rasterio
from obstore.store import S3Store
from obspec_utils.readers import EagerStoreReader

# Access public Arctic DEM data (no credentials needed)
store = S3Store(
    bucket="pgc-opendata-dems",
    aws_region="us-west-2",
    skip_signature=True,
)

path = "arcticdem/mosaics/v4.1/2m_dem_tiles.vrt"

with EagerStoreReader(store, path) as reader:
    with rasterio.open(reader) as src:
        print(f"CRS: {src.crs}")
        print(f"Bounds: {src.bounds}")
        print(f"Shape: {src.width} x {src.height}")
```

## Using with Xarray and rioxarray

For analysis workflows, combine with [rioxarray](https://corteva.github.io/rioxarray/) to load raster data as xarray datasets:

```python exec="on" source="above" session="rasterio" result="code"
import xarray as xr
from obstore.store import S3Store
from obspec_utils.readers import EagerStoreReader

store = S3Store(
    bucket="pgc-opendata-dems",
    aws_region="us-west-2",
    skip_signature=True,
)

path = "arcticdem/mosaics/v4.1/2m_dem_tiles.vrt"

with EagerStoreReader(store, path) as reader:
    ds = xr.open_dataset(reader, engine="rasterio")
    print(ds)
```

## Choosing a Reader

For raster data, the choice of reader depends on your access pattern:

| Reader | Best for |
|--------|----------|
| [`EagerStoreReader`][obspec_utils.readers.EagerStoreReader] | Small files or when you need the entire file (metadata parsing, VRT files) |
| [`BlockStoreReader`][obspec_utils.readers.BlockStoreReader] | Large files with sparse access patterns (reading specific tiles/windows) |

For most rasterio use cases, [`EagerStoreReader`][obspec_utils.readers.EagerStoreReader] works well since rasterio typically needs to read file headers and metadata which requires random access across the file.
