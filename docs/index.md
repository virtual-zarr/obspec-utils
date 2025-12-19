# Obspec Utils

Utilities for interacting with object storage, based on [obspec](https://github.com/developmentseed/obspec).

## Background

`obspec-utils` provides helpful utilities for working with object storage in Python, built on top of obspec and obstore. The library includes:

1. **ObjectStoreRegistry**: A registry for managing multiple object stores, allowing you to resolve URLs to the appropriate store and path. This is particularly useful when working with datasets that span multiple storage backends or buckets.

2. **File Handlers**: Wrappers around obstore's file reading capabilities that provide a familiar file-like interface, making it easy to integrate with libraries that expect standard Python file objects.

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
from obspec_utils import ObjectStoreRegistry

# Create and register stores
s3store = S3Store(bucket="my-bucket", prefix="my-data/")
registry = ObjectStoreRegistry({"s3://my-bucket": s3store})

# Resolve a URL to get the store and path
store, path = registry.resolve("s3://my-bucket/my-data/file.nc")
# path == "file.nc"
```

### File Handlers

The file handlers provide file-like interfaces for reading from object stores:

```python
from obstore.store import S3Store
from obspec_utils import ObstoreReader, ObstoreMemCacheReader

store = S3Store(bucket="my-bucket")

# Standard reader with buffered reads
reader = ObstoreReader(store, "path/to/file.bin", buffer_size=1024*1024)
data = reader.read(100)  # Read 100 bytes

# Memory-cached reader for repeated access
cached_reader = ObstoreMemCacheReader(store, "path/to/file.bin")
data = cached_reader.readall()  # Read entire file from memory cache
```

## Contributing

1. Clone the repository: `git clone https://github.com/virtual-zarr/obspec-utils.git`
2. Install development dependencies: `uv sync --all-groups`
3. Run the test suite: `uv run --all-groups pytest`

## License

`obspec-utils` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
