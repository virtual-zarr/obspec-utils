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

The file handlers provide file-like interfaces (read, seek, tell) for reading from object stores. They work with **any** ReadableStore implementation:

```python
from obspec_utils.obspec import BufferedStoreReader, EagerStoreReader, ParallelStoreReader

# Works with obstore
from obstore.store import S3Store
store = S3Store(bucket="my-bucket")
reader = BufferedStoreReader(store, "path/to/file.bin", buffer_size=1024*1024)

# Also works with AiohttpStore or any ReadableStore
from obspec_utils.aiohttp import AiohttpStore
store = AiohttpStore("https://example.com/data")
reader = BufferedStoreReader(store, "file.bin")

# Buffered reader with on-demand reads
data = reader.read(100)  # Read 100 bytes
reader.seek(0)           # Seek back to start

# Eager reader loads entire file into memory
eager_reader = EagerStoreReader(store, "file.bin")
data = eager_reader.readall()

# Parallel reader uses get_ranges() for efficient multi-chunk fetching with LRU cache
parallel_reader = ParallelStoreReader(store, "file.bin", chunk_size=256*1024)
data = parallel_reader.read(1000)
```

## Contributing

1. Clone the repository: `git clone https://github.com/virtual-zarr/obspec-utils.git`
2. Install development dependencies: `uv sync --all-groups`
3. Run the test suite: `uv run --all-groups pytest`

### Code standards - using prek

!!! note
    These instructions are replicated from [zarr-python](https://github.com/zarr-developers/zarr-python).

All code must conform to the PEP8 standard. Regarding line length, lines up to 100 characters are allowed, although please try to keep under 90 wherever possible.

`Obspec-utils` uses a set of git hooks managed by [`prek`](https://github.com/j178/prek), a fast, Rust-based pre-commit hook manager that is fully compatible with `.pre-commit-config.yaml` files. `prek` can be installed locally by running:

```bash
uv tool install prek
```

or:

```bash
pip install prek
```

The hooks can be installed locally by running:

```bash
prek install
```

This would run the checks every time a commit is created locally. The checks will by default only run on the files modified by a commit, but the checks can be triggered for all the files by running:

```bash
prek run --all-files
```

You can also run hooks only for files in a specific directory:

```bash
prek run --directory src/obspec_utils
```

Or run hooks for files changed in the last commit:

```bash
prek run --last-commit
```

To list all available hooks:

```bash
prek list
```

If you would like to skip the failing checks and push the code for further discussion, use the `--no-verify` option with `git commit`.

## License

`obspec-utils` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
