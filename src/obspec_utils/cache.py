"""Caching utilities for obspec-utils.

This module provides a caching wrapper for ReadableStore implementations,
useful for reducing network requests when files are accessed multiple times.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from obstore.store import MemoryStore

from obspec_utils.obspec import ReadableStore

if TYPE_CHECKING:
    from collections.abc import Buffer

    from obspec import GetOptions, GetResult, GetResultAsync


class CachingReadableStore(ReadableStore):
    """
    A wrapper that caches full objects in a MemoryStore on first access.

    This wrapper implements the ReadableStore protocol and caches entire
    objects when they are first accessed. Subsequent accesses (including
    range requests) are served from the cache.

    The cache uses LRU (Least Recently Used) eviction when it exceeds
    the maximum size.

    Parameters
    ----------
    store
        The underlying store to wrap.
    max_size
        Maximum cache size in bytes. When exceeded, least recently used
        entries are evicted. Default: 256 MB (256 * 1024 * 1024).

    Notes
    -----
    **Thread Safety**: This class is thread-safe and works correctly with
    multi-threaded executors (e.g., ``ThreadPoolExecutor``).

    **Distributed Limitations**: The cache is local to each process. In
    distributed settings (Dask distributed, ProcessPoolExecutor, Lithops),
    each worker maintains its own independent cache with no sharing:

    - Workers accessing the same files will each fetch independently
    - Memory is duplicated across workers
    - This is typically acceptable when workloads are partitioned by file
      (each worker processes different files)

    For workloads where multiple workers repeatedly access the same files,
    consider external caching solutions (Redis, shared filesystem) or
    restructuring the workload to minimize cross-worker file access.

    Examples
    --------
    With context manager (cache cleared on exit):

    ```python
    from obstore.store import S3Store
    from obspec_utils.cache import CachingReadableStore
    from obspec_utils.registry import ObjectStoreRegistry

    s3_store = S3Store(bucket="my-bucket")

    with CachingReadableStore(s3_store, max_size=512*1024*1024) as cached:
        registry = ObjectStoreRegistry({"s3://my-bucket": cached})
        # Use registry - first access fetches from S3, subsequent from cache
        store, path = registry.resolve("s3://my-bucket/file.nc")
        data = store.get_range(path, start=0, end=1000)
    # Cache cleared automatically
    ```

    With explicit cleanup:

    ```python
    cached = CachingReadableStore(s3_store)
    registry = ObjectStoreRegistry({"s3://my-bucket": cached})
    # ... use registry ...
    cached.clear_cache()  # Explicit cleanup when done
    ```
    """

    def __init__(self, store: ReadableStore, max_size: int = 256 * 1024 * 1024) -> None:
        """
        Create a caching wrapper around a store.

        Parameters
        ----------
        store
            The underlying store to wrap.
        max_size
            Maximum cache size in bytes. Default: 256 MB.
        """
        self._store = store
        self._cache = MemoryStore()
        self._max_size = max_size
        self._current_size = 0
        self._path_sizes: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to the underlying store.

        This ensures CachingReadableStore is transparent for any additional
        public methods or attributes the underlying store may have.

        Note: Private attributes (starting with '_') are not forwarded.
        """
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        if "_store" not in self.__dict__:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )
        return getattr(self._store, name)

    def __reduce__(self):
        """Support pickling for multiprocessing and distributed frameworks.

        Returns a fresh instance with an empty cache. This is intentional:
        serializing the full cache contents would be inefficient for distributed
        workloads where each worker typically processes different files.

        The underlying store and max_size configuration are preserved.
        """
        return (
            self.__class__,
            (self._store, self._max_size),
        )

    def _add_to_cache(self, path: str, data: bytes) -> None:
        """Add data to cache, evicting LRU entries if needed.

        Must be called with self._lock held.
        """
        size = len(data)

        # Evict LRU entries until we have room
        while self._current_size + size > self._max_size and self._path_sizes:
            oldest_path, oldest_size = self._path_sizes.popitem(last=False)
            self._cache.delete(oldest_path)
            self._current_size -= oldest_size

        # Add to cache
        self._cache.put(path, data)
        self._path_sizes[path] = size
        self._current_size += size

    def _ensure_cached(self, path: str) -> None:
        """Ensure path is in cache, fetching full object if not.

        Must be called with self._lock held.
        """
        if path in self._path_sizes:
            # Cache hit - move to end (most recently used)
            self._path_sizes.move_to_end(path)
        else:
            # Cache miss - fetch and cache
            result = self._store.get(path)
            data = bytes(result.buffer())
            self._add_to_cache(path, data)

    async def _ensure_cached_async(self, path: str) -> None:
        """Async version of _ensure_cached.

        Note: The lock is released during the async fetch to avoid blocking.
        """
        with self._lock:
            if path in self._path_sizes:
                self._path_sizes.move_to_end(path)
                return
            # Path not in cache - need to fetch

        # Fetch without holding the lock
        result = await self._store.get_async(path)
        data = bytes(await result.buffer_async())

        with self._lock:
            # Check again in case another coroutine cached it
            if path not in self._path_sizes:
                self._add_to_cache(path, data)
            else:
                self._path_sizes.move_to_end(path)

    def clear_cache(self) -> None:
        """Clear all cached objects."""
        with self._lock:
            self._cache = MemoryStore()
            self._path_sizes.clear()
            self._current_size = 0

    @property
    def cache_size(self) -> int:
        """Current cache size in bytes."""
        with self._lock:
            return self._current_size

    @property
    def cached_paths(self) -> list[str]:
        """List of currently cached paths (in LRU order, oldest first)."""
        with self._lock:
            return list(self._path_sizes.keys())

    def __enter__(self) -> "CachingReadableStore":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager, clearing the cache."""
        self.clear_cache()

    # Implement ReadableStore protocol

    def get(self, path: str, *, options: GetOptions | None = None) -> GetResult:
        """Get entire file, using cache if available."""
        with self._lock:
            self._ensure_cached(path)
        return self._cache.get(path, options=options)

    async def get_async(
        self, path: str, *, options: GetOptions | None = None
    ) -> GetResultAsync:
        """Get entire file async, using cache if available."""
        await self._ensure_cached_async(path)
        return await self._cache.get_async(path, options=options)

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Get a byte range, caching the full object first if needed."""
        with self._lock:
            self._ensure_cached(path)
        return self._cache.get_range(path, start=start, end=end, length=length)

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Get a byte range async, caching the full object first if needed."""
        await self._ensure_cached_async(path)
        return await self._cache.get_range_async(
            path, start=start, end=end, length=length
        )

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[Buffer]:
        """Get multiple byte ranges, caching the full object first if needed."""
        with self._lock:
            self._ensure_cached(path)
        return self._cache.get_ranges(path, starts=starts, ends=ends, lengths=lengths)

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[Buffer]:
        """Get multiple byte ranges async, caching the full object first if needed."""
        await self._ensure_cached_async(path)
        return await self._cache.get_ranges_async(
            path, starts=starts, ends=ends, lengths=lengths
        )


__all__ = ["CachingReadableStore"]
