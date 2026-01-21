# Obspec Utils

Utilities for interacting with object storage, based on [obspec](https://github.com/developmentseed/obspec).

## Background

`obspec-utils` provides helpful utilities for working with object storage in Python, built on top of obspec and obstore. The library includes:

1. **ObjectStoreRegistry**: A registry for managing multiple object stores, allowing you to resolve URLs to the appropriate store and path. This is particularly useful when working with datasets that span multiple storage backends or buckets.

2. **ReadableStore Protocol**: A minimal protocol defining the read-only interface required for object storage access. This allows alternative backends (like aiohttp) to be used instead of obstore.

3. **File Handlers**: Wrappers around obstore's file reading capabilities that provide a familiar file-like interface.

## Design Philosophy

The library is designed around **protocols rather than concrete classes**. The `ObjectStoreRegistry` accepts any object that implements the `ReadableStore` protocol, which means:

- **obstore classes** (S3Store, HTTPStore, GCSStore, etc.) work out of the box
- **Custom implementations** (like the included `AiohttpStore`) can be used as alternatives
- **The Zarr/VirtualiZarr layer doesn't care** which backend you use - it just needs something satisfying the protocol

This is particularly useful when:

- obstore's HTTPStore (designed for WebDAV/S3-like semantics) isn't ideal for your use case
- You need generic HTTPS access to THREDDS, NASA data servers, or other HTTP endpoints
- You want to use a different HTTP library like aiohttp

## Getting started

The library can be installed from PyPI:

```bash
python -m pip install obspec-utils
```

## Usage

### ObjectStoreRegistry

The `ObjectStoreRegistry` allows you to register object stores and resolve URLs to the appropriate store:

```python
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry

# Create and register stores
s3store = S3Store(bucket="my-bucket", prefix="my-data/")
registry = ObjectStoreRegistry({"s3://my-bucket": s3store})

# Resolve a URL to get the store and path
store, path = registry.resolve("s3://my-bucket/my-data/file.nc")
# path == "file.nc"
```

### Using Alternative HTTP Backends

For generic HTTPS access where obstore's HTTPStore may not be ideal, you can use the `AiohttpStore`:

```python
from obspec_utils.registry import ObjectStoreRegistry
from obspec_utils.aiohttp import AiohttpStore

# Create an aiohttp-based store for a THREDDS server
store = AiohttpStore(
    "https://thredds.example.com/data",
    headers={"Authorization": "Bearer <token>"},  # Optional auth
    timeout=60.0,
)

registry = ObjectStoreRegistry({"https://thredds.example.com/data": store})

# Use it just like any other store
store, path = registry.resolve("https://thredds.example.com/data/file.nc")
data = await store.get_range_async(path, start=0, end=1000)
```

### File Handlers

The file handlers provide file-like interfaces (read, seek, tell) for reading from object stores.

#### Protocol-based readers (recommended)

These work with **any** ReadableStore implementation:

```python
from obspec_utils.obspec import StoreReader, StoreMemCacheReader

# Works with obstore
from obstore.store import S3Store
store = S3Store(bucket="my-bucket")
reader = StoreReader(store, "path/to/file.bin", buffer_size=1024*1024)

# Also works with AiohttpStore or any ReadableStore
from obspec_utils.aiohttp import AiohttpStore
store = AiohttpStore("https://example.com/data")
reader = StoreReader(store, "file.bin")

# Standard reader with buffered reads
data = reader.read(100)  # Read 100 bytes
reader.seek(0)           # Seek back to start

# Memory-cached reader for repeated access
cached_reader = StoreMemCacheReader(store, "file.bin")
data = cached_reader.readall()
```

#### Obstore-specific readers

For maximum performance with obstore, use the obstore-specific readers which leverage obstore's native `ReadableFile`:

```python
from obstore.store import S3Store
from obspec_utils.obstore import ObstoreReader, ObstoreMemCacheReader

store = S3Store(bucket="my-bucket")

# Uses obstore's optimized buffered reader
reader = ObstoreReader(store, "path/to/file.bin", buffer_size=1024*1024)
data = reader.read(100)

# Uses obstore's MemoryStore for caching
cached_reader = ObstoreMemCacheReader(store, "path/to/file.bin")
data = cached_reader.readall()
```

## Contributing

1. Clone the repository: `git clone https://github.com/virtual-zarr/obspec-utils.git`
2. Install development dependencies: `uv sync --all-groups`
3. Run the test suite: `uv run --all-groups pytest`

## License

`obspec-utils` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
