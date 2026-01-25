"""Eager store reader that loads entire file into memory."""

from __future__ import annotations

import io
from typing import Protocol

from obspec import Get, GetRanges, Head


class EagerStoreReader:
    """
    A file-like reader that eagerly loads the entire file into memory.

    This reader fetches the complete file on first access and then serves all
    subsequent reads from the in-memory cache. Useful for files that will be
    read multiple times or when seeking is frequent.

    By default, the file is fetched using parallel range requests via
    [`get_ranges()`][obspec.GetRanges], which can significantly improve load time for large files.
    The defaults (12 MB request size, max 18 concurrent requests) are tuned for
    cloud storage. The file size is determined automatically via a HEAD request.

    The parallel fetching strategy is based on Icechunk's approach:
    https://github.com/earth-mover/icechunk/blob/main/icechunk/src/storage/mod.rs

    When to Use
    -----------
    Use EagerStoreReader when:

    - **Reading the entire file**: When you know you'll need most or all of the
      file's contents.
    - **Repeated random access**: After the initial load, any byte is accessible
      with no network latency.
    - **Small to medium files**: Files that fit comfortably in memory.
    - **Parallel initial fetch**: The default settings use parallel requests
      for faster download on cloud storage.

    Consider alternatives when:

    - You only need a small portion of a large file → use [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader]
    - Memory is constrained → use [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader] (bounded cache)
      or [BufferedStoreReader][obspec_utils.readers.BufferedStoreReader]
    - You're streaming sequentially and won't revisit data → use [BufferedStoreReader][obspec_utils.readers.BufferedStoreReader]

    See Also
    --------

    - [BufferedStoreReader][obspec_utils.readers.BufferedStoreReader] : On-demand reads with read-ahead buffering.
    - [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader] : Uses parallel requests with LRU caching for sparse access.
    """

    class Store(Get, GetRanges, Head, Protocol):
        """
        Store protocol required by EagerStoreReader.

        Combines [Get][obspec.Get], [GetRanges][obspec.GetRanges], and
        [Head][obspec.Head] from obspec.
        """

        pass

    def __init__(
        self,
        store: EagerStoreReader.Store,
        path: str,
        request_size: int = 12 * 1024 * 1024,
        file_size: int | None = None,
        max_concurrent_requests: int = 18,
    ) -> None:
        """
        Create an eager reader that loads the entire file into memory.

        The file is fetched immediately and cached in memory.

        Parameters
        ----------
        store
            Any object implementing [Get][obspec.Get], [GetRanges][obspec.GetRanges],
            and [Head][obspec.Head].
        path
            The path to the file within the store.
        request_size
            Target size for each parallel range request in bytes. Default is 12 MB,
            tuned for cloud storage throughput. The file will be divided into
            parts of this size and fetched using [`get_ranges()`][obspec.GetRanges].
        file_size
            File size in bytes. If not provided, the size is determined via
            `store.head()`. Pass this to skip the HEAD request if you already
            know the file size.
        max_concurrent_requests
            Maximum number of parallel range requests. Default is 18. If the file
            would require more requests than this, request sizes are increased to
            fit within this limit.
        """
        self._store = store
        self._path = path

        # Determine file size if not provided
        if file_size is None:
            file_size = store.head(path)["size"]

        # Handle empty files
        if file_size == 0:
            self._buffer = io.BytesIO(b"")
            return

        # Calculate number of requests needed
        num_requests = (file_size + request_size - 1) // request_size

        # Cap at max_concurrent_requests by increasing request size
        if num_requests > max_concurrent_requests:
            num_requests = max_concurrent_requests
            request_size = (file_size + num_requests - 1) // num_requests

        # Skip concurrency overhead for single request
        if num_requests == 1:
            result = store.get(path)
            data = bytes(result.buffer())
        else:
            # Parallel range requests
            starts = []
            lengths = []
            for i in range(num_requests):
                start = i * request_size
                length = min(request_size, file_size - start)
                starts.append(start)
                lengths.append(length)

            # Fetch all parts in parallel
            results = store.get_ranges(path, starts=starts, lengths=lengths)

            # Concatenate into single buffer
            data = b"".join(bytes(part) for part in results)

        self._buffer = io.BytesIO(data)

    def read(self, size: int = -1, /) -> bytes:
        """Read up to `size` bytes from the cached file."""
        return self._buffer.read(size)

    def readall(self) -> bytes:
        """Read the entire cached file."""
        pos = self._buffer.tell()
        self._buffer.seek(0)
        data = self._buffer.read()
        self._buffer.seek(pos)
        return data

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """Move the file position within the cached data."""
        return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        """Return the current position in the cached file."""
        return self._buffer.tell()

    def close(self) -> None:
        """Close the reader and release the in-memory buffer."""
        self._buffer = io.BytesIO(b"")

    def __enter__(self) -> "EagerStoreReader":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and close the reader."""
        self.close()


__all__ = ["EagerStoreReader"]
