"""obspec-utils: Utilities for working with object stores via obspec/obstore.

This package provides:

- `obspec_utils.protocols`: Type definitions for store interfaces
- `obspec_utils.readers`: File-like readers for object stores
- `obspec_utils.wrappers`: Caching, tracing, and request splitting
- `obspec_utils.stores`: Concrete store implementations
- `obspec_utils.registry`: URL-to-store mapping
- `obspec_utils.glob`: Glob pattern matching for object stores

Example
-------
```python
from obspec_utils.wrappers import CachingReadableStore
from obspec_utils.readers import BufferedStoreReader
from obspec_utils.registry import ObjectStoreRegistry

# Create a cached store and register it
cached = CachingReadableStore(s3_store)
registry = ObjectStoreRegistry({"s3://bucket": cached})

# Resolve URLs to stores
store, path = registry.resolve("s3://bucket/file.nc")

# Create a file-like reader
reader = BufferedStoreReader(store, path)
data = reader.read(1024)
```
"""

from obspec_utils._version import __version__
from obspec_utils.glob import glob, glob_async, glob_objects, glob_objects_async

__all__ = [
    "__version__",
    "glob",
    "glob_objects",
    "glob_async",
    "glob_objects_async",
]
