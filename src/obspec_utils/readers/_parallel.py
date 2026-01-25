"""Parallel store reader with chunk-based LRU caching."""

from __future__ import annotations

import io
from collections import OrderedDict
from typing import Protocol

from obspec import Get, GetRanges, Head


class ParallelStoreReader:
    """
    A file-like reader that uses parallel range requests for efficient chunk fetching.

    This reader divides the file into fixed-size chunks and uses [`get_ranges()`][obspec.GetRanges]
    to fetch multiple chunks in parallel. An LRU cache stores recently accessed chunks
    to avoid redundant fetches.

    This is particularly efficient for workloads that access multiple non-contiguous
    regions of a file.

    When to Use
    -----------
    Use ParallelStoreReader when:

    - **Sparse access patterns**: Reading many non-contiguous regions of a file.
    - **Large files with partial reads**: When you only need portions of a large
      file and don't want to load it all into memory.
    - **Memory-constrained environments**: The LRU cache has bounded memory usage
      (`chunk_size * max_cached_chunks`), regardless of file size.
    - **Unknown access patterns**: When you don't know upfront which parts of the
      file you'll need.

    Consider alternatives when:

    - You'll read the entire file anyway → use [EagerStoreReader][obspec_utils.readers.EagerStoreReader]
    - Access is purely sequential → use [BufferedStoreReader][obspec_utils.readers.BufferedStoreReader]
    - You need repeated access to more data than fits in the cache → use
      [EagerStoreReader][obspec_utils.readers.EagerStoreReader] to avoid re-fetching evicted chunks

    See Also
    --------

    - [BufferedStoreReader][obspec_utils.readers.BufferedStoreReader] : On-demand reads with read-ahead buffering.
    - [EagerStoreReader][obspec_utils.readers.EagerStoreReader] : Loads entire file into memory for fast random access.
    """

    class Store(Get, GetRanges, Head, Protocol):
        """
        Store protocol required by ParallelStoreReader.

        Combines [Get][obspec.Get], [GetRanges][obspec.GetRanges], and
        [Head][obspec.Head] from obspec.
        """

        pass

    def __init__(
        self,
        store: ParallelStoreReader.Store,
        path: str,
        chunk_size: int = 1024 * 1024,
        max_cached_chunks: int = 64,
    ) -> None:
        """
        Create a parallel reader with chunk-based caching.

        Parameters
        ----------
        store
            Any object implementing [Get][obspec.Get] and [GetRanges][obspec.GetRanges].
        path
            The path to the file within the store.
        chunk_size
            Size of each chunk in bytes. Default is 1 MB, tuned for cloud object
            stores where HTTP request overhead is significant. Smaller chunks mean
            more granular caching but more requests.
        max_cached_chunks
            Maximum number of chunks to keep in the LRU cache. Default is 64,
            giving a 64 MB cache with the default chunk size.
        """
        self._store = store
        self._path = path
        self._chunk_size = chunk_size
        self._max_cached_chunks = max_cached_chunks
        self._position = 0
        self._size: int | None = None
        # LRU cache: OrderedDict with chunk_index -> bytes
        self._cache: OrderedDict[int, bytes] = OrderedDict()

    def _get_size(self) -> int:
        """Lazily fetch the file size via a head() call."""
        if self._size is None:
            self._size = self._store.head(self._path)["size"]
        return self._size

    def _get_chunks(self, chunk_indices: list[int]) -> dict[int, bytes]:
        """Fetch multiple chunks in parallel using get_ranges()."""
        # Filter out already cached chunks
        needed = [i for i in chunk_indices if i not in self._cache]

        if needed:
            file_size = self._get_size()
            starts = []
            lengths = []

            for chunk_idx in needed:
                start = chunk_idx * self._chunk_size
                # Handle last chunk which may be smaller
                end = min(start + self._chunk_size, file_size)
                starts.append(start)
                lengths.append(end - start)

            # Fetch all chunks in parallel
            results = self._store.get_ranges(self._path, starts=starts, lengths=lengths)

            # Store in cache
            for chunk_idx, data in zip(needed, results):
                self._cache[chunk_idx] = bytes(data)

        # Mark all requested chunks as recently used
        for i in chunk_indices:
            self._cache.move_to_end(i)

        # Build return dict before eviction
        result = {i: self._cache[i] for i in chunk_indices}

        # Evict oldest if over capacity
        while len(self._cache) > self._max_cached_chunks:
            self._cache.popitem(last=False)

        return result

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to `size` bytes from the file.

        Parameters
        ----------
        size
            Number of bytes to read. If -1, read from current position to end.

        Returns
        -------
        bytes
            The data read from the file.
        """
        file_size = self._get_size()

        if size == -1:
            # Read from current position to end
            size = file_size - self._position
            if size <= 0:
                return b""

        # Clamp to remaining bytes
        remaining = file_size - self._position
        if size > remaining:
            size = remaining
        if size <= 0:
            return b""

        # Determine which chunks we need
        start_chunk = self._position // self._chunk_size
        end_pos = self._position + size
        end_chunk = (end_pos - 1) // self._chunk_size

        chunk_indices = list(range(start_chunk, end_chunk + 1))
        chunks = self._get_chunks(chunk_indices)

        # Assemble the result
        result = io.BytesIO()
        for chunk_idx in chunk_indices:
            chunk_data = chunks[chunk_idx]
            chunk_start = chunk_idx * self._chunk_size

            # Calculate slice within this chunk
            local_start = max(0, self._position - chunk_start)
            local_end = min(len(chunk_data), end_pos - chunk_start)

            result.write(chunk_data[local_start:local_end])

        data = result.getvalue()
        self._position += len(data)
        return data

    def readall(self) -> bytes:
        """
        Read the entire file.

        Returns
        -------
        bytes
            The complete file contents.
        """
        result = self._store.get(self._path)
        data = bytes(result.buffer())
        self._size = len(data)
        self._position = len(data)
        return data

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Move the file position.

        Parameters
        ----------
        offset
            Position offset.
        whence
            Reference point: 0=start (SEEK_SET), 1=current (SEEK_CUR), 2=end (SEEK_END).

        Returns
        -------
        int
            The new absolute position.
        """
        if whence == 0:  # SEEK_SET
            self._position = offset
        elif whence == 1:  # SEEK_CUR
            self._position += offset
        elif whence == 2:  # SEEK_END
            self._position = self._get_size() + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")

        if self._position < 0:
            self._position = 0

        return self._position

    def tell(self) -> int:
        """
        Return the current file position.

        Returns
        -------
        int
            Current position in bytes from start of file.
        """
        return self._position

    def close(self) -> None:
        """Close the reader and release the chunk cache."""
        self._cache.clear()

    def __enter__(self) -> "ParallelStoreReader":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and close the reader."""
        self.close()


__all__ = ["ParallelStoreReader"]
