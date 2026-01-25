"""Request splitting utilities for obspec-utils.

This module provides a store wrapper that splits large get() requests into
parallel get_ranges() calls for faster fetching of large files.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from obstore.store import MemoryStore

from obspec_utils.obspec import ReadableStore

if TYPE_CHECKING:
    from collections.abc import Buffer

    from obspec import GetOptions, GetResult, GetResultAsync, ObjectMeta


class SplittingReadableStore(ReadableStore):
    """
    Wraps a store to split large get() requests into parallel get_ranges().

    This accelerates fetching large files by dividing them into chunks and
    fetching in parallel via get_ranges(). The splitting is transparent to
    callers - they see a normal get() interface.

    Designed to compose with CachingReadableStore:

    ```python
    from obstore.store import S3Store
    from obspec_utils.splitting import SplittingReadableStore
    from obspec_utils.cache import CachingReadableStore

    store = S3Store(bucket="my-bucket")
    store = SplittingReadableStore(store)  # Fast parallel fetches
    store = CachingReadableStore(store)    # Cache the results

    # get() is now: parallel fetch -> cache
    result = store.get("large-file.nc")
    ```

    Parameters
    ----------
    store
        The underlying store to wrap.
    request_size
        Target size for each parallel range request in bytes. Default: 12 MB.
        Tuned for cloud storage throughput.
    max_concurrent_requests
        Maximum number of parallel requests. Default: 18. If a file would
        require more requests than this, request sizes are increased to fit.

    Notes
    -----
    This wrapper only affects get() and get_async(). Range requests
    (get_range, get_ranges) pass through unchanged since they're already
    appropriately sized by the caller.

    The parallel fetching strategy is based on Icechunk's approach:
    https://github.com/earth-mover/icechunk/blob/main/icechunk/src/storage/mod.rs

    Examples
    --------
    Basic usage:

    ```python
    from obstore.store import S3Store
    from obspec_utils.splitting import SplittingReadableStore

    store = S3Store(bucket="my-bucket")
    fast_store = SplittingReadableStore(store)

    # Large file fetched via parallel requests
    result = fast_store.get("large-file.nc")
    ```

    With caching (recommended pattern):

    ```python
    from obspec_utils.cache import CachingReadableStore

    store = S3Store(bucket="my-bucket")
    store = SplittingReadableStore(store)
    store = CachingReadableStore(store)

    # First access: parallel fetch, then cached
    result1 = store.get("file.nc")
    # Second access: served from cache (no fetch)
    result2 = store.get("file.nc")
    ```

    Custom chunk sizes:

    ```python
    # Larger chunks for high-bandwidth connections
    store = SplittingReadableStore(
        s3_store,
        request_size=32 * 1024 * 1024,  # 32 MB chunks
        max_concurrent_requests=8,
    )
    ```
    """

    def __init__(
        self,
        store: ReadableStore,
        request_size: int = 12 * 1024 * 1024,
        max_concurrent_requests: int = 18,
    ) -> None:
        """
        Create a splitting wrapper around a store.

        Parameters
        ----------
        store
            Any object implementing the full read interface: [Get][obspec.Get],
            [GetAsync][obspec.GetAsync], [GetRange][obspec.GetRange],
            [GetRangeAsync][obspec.GetRangeAsync], [GetRanges][obspec.GetRanges],
            [GetRangesAsync][obspec.GetRangesAsync], [Head][obspec.Head],
            and [HeadAsync][obspec.HeadAsync].
        request_size
            Target size for each parallel range request. Default: 12 MB.
        max_concurrent_requests
            Maximum number of parallel requests. Default: 18.
        """
        self._store = store
        self._request_size = request_size
        self._max_concurrent_requests = max_concurrent_requests

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to the underlying store.

        This ensures SplittingReadableStore is transparent for any additional
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

    def _compute_ranges(self, file_size: int) -> tuple[list[int], list[int]] | None:
        """Compute start positions and lengths for parallel fetching.

        Returns None if splitting isn't beneficial (single request sufficient).
        """
        if file_size == 0:
            return None

        request_size = self._request_size
        num_requests = (file_size + request_size - 1) // request_size

        # Single request - no benefit from splitting
        if num_requests == 1:
            return None

        # Cap at max_concurrent_requests by increasing request size
        if num_requests > self._max_concurrent_requests:
            num_requests = self._max_concurrent_requests
            request_size = (file_size + num_requests - 1) // num_requests

        starts = []
        lengths = []
        for i in range(num_requests):
            start = i * request_size
            length = min(request_size, file_size - start)
            starts.append(start)
            lengths.append(length)

        return starts, lengths

    def _wrap_as_get_result(self, path: str, data: bytes) -> GetResult:
        """Wrap raw bytes as a GetResult using a temporary MemoryStore."""
        temp = MemoryStore()
        temp.put(path, data)
        return temp.get(path)

    async def _wrap_as_get_result_async(self, path: str, data: bytes) -> GetResultAsync:
        """Async version of _wrap_as_get_result."""
        temp = MemoryStore()
        temp.put(path, data)
        return await temp.get_async(path)

    def get(self, path: str, *, options: GetOptions | None = None) -> GetResult:
        """Get file, using parallel fetching if beneficial.

        If the file is large enough to benefit from splitting, fetches via
        parallel get_ranges(). Otherwise falls back to a single get() request.
        """
        file_size = self.head(path)["size"]
        ranges = self._compute_ranges(file_size)

        if ranges is not None:
            starts, lengths = ranges
            results = self._store.get_ranges(path, starts=starts, lengths=lengths)
            data = b"".join(bytes(part) for part in results)
            return self._wrap_as_get_result(path, data)

        # Fall back to regular get (file too small for splitting)
        return self._store.get(path, options=options)

    async def get_async(
        self, path: str, *, options: GetOptions | None = None
    ) -> GetResultAsync:
        """Async get, using parallel fetching if beneficial."""
        file_size = (await self.head_async(path))["size"]
        ranges = self._compute_ranges(file_size)

        if ranges is not None:
            starts, lengths = ranges
            results = await self._store.get_ranges_async(
                path, starts=starts, lengths=lengths
            )
            data = b"".join(bytes(part) for part in results)
            return await self._wrap_as_get_result_async(path, data)

        # Fall back to regular get_async (file too small for splitting)
        return await self._store.get_async(path, options=options)

    # Pass through range methods unchanged - caller already sized appropriately

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Get a byte range (passed through to underlying store)."""
        return self._store.get_range(path, start=start, end=end, length=length)

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Async get range (passed through to underlying store)."""
        return await self._store.get_range_async(
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
        """Get multiple byte ranges (passed through to underlying store)."""
        return self._store.get_ranges(path, starts=starts, ends=ends, lengths=lengths)

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[Buffer]:
        """Async get ranges (passed through to underlying store)."""
        return await self._store.get_ranges_async(
            path, starts=starts, ends=ends, lengths=lengths
        )

    def head(self, path: str) -> ObjectMeta:
        """Get file metadata (delegates to underlying store)."""
        return self._store.head(path)

    async def head_async(self, path: str) -> ObjectMeta:
        """Get file metadata async (delegates to underlying store)."""
        return await self._store.head_async(path)


__all__ = ["SplittingReadableStore"]
