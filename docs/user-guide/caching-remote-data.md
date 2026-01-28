# Caching Remote Data

This guide shows how to reduce network requests when you need to read the same remote data multiple times.

## The Problem

When working with cloud-hosted data, every read operation can trigger a network request. If you're accessing the same data repeatedly, this can be slow and wasteful. Repeatedly accessing the same data can happen evening when reading only one file, and also happens when reading a file multiple times.

## The Solution

Wrap your store with [`CachingReadableStore`][obspec_utils.wrappers.CachingReadableStore] to cache files after the first access:

```python exec="on" source="above" session="cache" result="code"
from obstore.store import S3Store
from obspec_utils.wrappers import CachingReadableStore

# Create the underlying store
store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,
)

# Wrap with caching (256 MB default cache size)
cached_store = CachingReadableStore(store)

# First access fetches from network
path = "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp85_r1i1p1_inmcm4_2100.nc"
data1 = cached_store.get_range(path, start=0, length=1000)
print(f"After first read: {cached_store.cache_size / 1e6:.1f} MB cached")

# Second access served from cache (no network request)
data2 = cached_store.get_range(path, start=1000, length=1000)
print(f"After second read: {cached_store.cache_size / 1e6:.1f} MB cached")
print(f"Cached files: {len(cached_store.cached_paths)}")
```

## Sizing Your Cache

Set `max_size` based on your available memory and the files you're working with:

```python exec="on" source="above" session="cache2" result="code"
from obstore.store import S3Store
from obspec_utils.wrappers import CachingReadableStore

store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,
)

# 512 MB cache for larger workloads
cached_store = CachingReadableStore(store, max_size=512 * 1024 * 1024)

# Cache multiple files
paths = [
    "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp85_r1i1p1_inmcm4_2099.nc",
    "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp85_r1i1p1_inmcm4_2100.nc",
]

for path in paths:
    cached_store.get_range(path, start=0, length=100)

print(f"Cache size: {cached_store.cache_size / 1e6:.1f} MB")
print(f"Cached files ({len(cached_store.cached_paths)}):")
for p in cached_store.cached_paths:
    print(f"  {p.split('/')[-1]}")
```

When the cache exceeds [`max_size`][obspec_utils.wrappers.CachingReadableStore], the least recently used files are evicted automatically.

## Using with Xarray

Combine caching with readers for xarray workflows:

```python exec="on" source="above" session="cache3" result="code"
import xarray as xr
from obstore.store import HTTPStore
from obspec_utils.wrappers import CachingReadableStore
from obspec_utils.readers import EagerStoreReader

# Access sample NetCDF files over HTTP
store = HTTPStore.from_url("https://github.com/pydata/xarray-data/raw/refs/heads/master/")
cached_store = CachingReadableStore(store)

path = "air_temperature.nc"

# First open: fetches from network
with EagerStoreReader(cached_store, path) as reader:
    ds1 = xr.open_dataset(reader, engine="scipy")
    var_names = list(ds1.data_vars)

print(f"Variables: {var_names}")
print(f"Cache size after first open: {cached_store.cache_size / 1e6:.2f} MB")

# Second open: served entirely from cache
with EagerStoreReader(cached_store, path) as reader:
    ds2 = xr.open_dataset(reader, engine="scipy")

print(f"Cache size after second open: {cached_store.cache_size / 1e6:.2f} MB (unchanged)")
```

## Cleaning Up

Use the context manager for automatic cleanup, or call [`clear_cache()`][obspec_utils.wrappers.CachingReadableStore.clear_cache] explicitly:

```python exec="on" source="above" session="cache4" result="code"
from obstore.store import S3Store
from obspec_utils.wrappers import CachingReadableStore

store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,
)
path = "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp85_r1i1p1_inmcm4_2100.nc"

# Option 1: Context manager (cache cleared on exit)
with CachingReadableStore(store) as cached_store:
    cached_store.get_range(path, start=0, length=100)
    print(f"Inside context: {cached_store.cache_size / 1e6:.1f} MB cached")
# Cache automatically cleared when exiting the context
print(f"Outside context: {cached_store.cache_size / 1e6:.1f} MB cached")

# Option 2: Explicit cleanup
cached_store = CachingReadableStore(store)
cached_store.get_range(path, start=0, length=100)
print(f"Before clear: {cached_store.cache_size / 1e6:.1f} MB cached")
cached_store.clear_cache()
print(f"After clear: {cached_store.cache_size / 1e6:.1f} MB cached")
```

## When Caching Helps

Caching is most effective when:

- You read the same files multiple times (parsing metadata, then reading data)
- Multiple operations access overlapping files
- Files are small enough to fit in your cache budget
