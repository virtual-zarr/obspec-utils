from typing import Protocol, TypeAlias, runtime_checkable

from obspec import (
    Get,
    GetAsync,
    GetRange,
    GetRangeAsync,
    GetRanges,
    GetRangesAsync,
)

Url: TypeAlias = str
"""A URL string (e.g., 's3://bucket/path' or 'https://example.com/file')."""

Path: TypeAlias = str
"""A path string within an object store."""


@runtime_checkable
class ReadableStore(
    Get,
    GetAsync,
    GetRange,
    GetRangeAsync,
    GetRanges,
    GetRangesAsync,
    Protocol,
):
    """
    A minimal protocol for read-only object storage access.

    This protocol defines the intersection of obspec protocols required for
    read-only operations like those used by VirtualiZarr. Any object that
    implements these methods can be used with ObjectStoreRegistry.

    The protocol includes:
    - `get` / `get_async`: Download entire files
    - `get_range` / `get_range_async`: Download a single byte range
    - `get_ranges` / `get_ranges_async`: Download multiple byte ranges efficiently

    This allows backends like obstore (S3Store, HTTPStore, etc.), aiohttp wrappers,
    or any custom implementation to be used interchangeably.

    Examples
    --------

    Using with obstore:

    ```python
    from obstore.store import S3Store
    from obspec_utils import ObjectStoreRegistry

    # S3Store implements ReadableStore protocol
    store = S3Store(bucket="my-bucket")
    registry = ObjectStoreRegistry({"s3://my-bucket": store})
    ```

    Using with a custom aiohttp wrapper:

    ```python
    from obspec_utils import ObjectStoreRegistry
    from obspec_utils.aiohttp import AiohttpStore

    # AiohttpStore implements ReadableStore protocol
    store = AiohttpStore("https://example.com/data")
    registry = ObjectStoreRegistry({"https://example.com/data": store})
    ```
    """

    pass


__all__ = ["Url", "Path", "ReadableStore"]
