from __future__ import annotations

import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import obstore as obs

if TYPE_CHECKING:
    from obstore import ReadableFile
    from obstore.store import ObjectStore

from obstore.store import MemoryStore


class ObstoreReader:
    _reader: ReadableFile

    def __init__(
        self, store: ObjectStore, path: str, buffer_size: int = 1024 * 1024
    ) -> None:
        """
        Create an obstore file reader that implements the read, readall, seek, and tell methods, which
        can be used in libraries that expect file-like objects.

        This wrapper is necessary in order to return Python bytes types rather than obstore Bytes buffers.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        buffer_size
            The minimum number of bytes to read in a single request. Up to buffer_size bytes will be buffered in memory.
        """
        self._reader = obs.open_reader(store, path, buffer_size=buffer_size)

    def read(self, size: int, /) -> bytes:
        return self._reader.read(size).to_bytes()

    def readall(self) -> bytes:
        return self._reader.read().to_bytes()

    def seek(self, offset: int, whence: int = 0, /):
        # TODO: Check on default for whence
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()


class ObstoreEagerReader(ObstoreReader):
    """
    A file reader that eagerly loads the entire file into memory.

    This reader loads the complete file contents into a MemoryStore before
    any reads occur. This is beneficial for files that will be read multiple
    times or when you want to avoid repeated network requests.
    """

    _reader: ReadableFile
    _memstore: MemoryStore

    def __init__(self, store: ObjectStore, path: str) -> None:
        """
        Create an obstore file reader that eagerly loads the file into memory.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        """
        self._memstore = MemoryStore()
        buffer = store.get(path).bytes()
        self._memstore.put(path, buffer)

        self._reader = obs.open_reader(self._memstore, path)


class ObstorePrefetchReader:
    """
    A file reader that prefetches upcoming byte ranges in the background.

    This reader anticipates sequential read patterns and fetches data ahead of
    the current position using background threads. This can significantly reduce
    latency for sequential reads from remote object stores.

    The prefetch buffer uses an LRU cache to manage memory, automatically evicting
    older chunks when the cache reaches capacity.
    """

    def __init__(
        self,
        store: ObjectStore,
        path: str,
        *,
        prefetch_size: int = 4 * 1024 * 1024,
        chunk_size: int = 1024 * 1024,
        max_workers: int = 2,
        max_cached_chunks: int = 8,
    ) -> None:
        """
        Create an obstore file reader with background prefetching.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        prefetch_size
            Total number of bytes to prefetch ahead of the current position.
            Default is 4 MB.
        chunk_size
            Size of each prefetch chunk in bytes. Smaller chunks provide finer
            granularity but more overhead. Default is 1 MB.
        max_workers
            Maximum number of concurrent prefetch threads. Default is 2.
        max_cached_chunks
            Maximum number of chunks to keep in the LRU cache. Oldest chunks
            are evicted when this limit is exceeded. Default is 8.
        """
        self._store = store
        self._path = path
        self._prefetch_size = prefetch_size
        self._chunk_size = chunk_size
        self._max_cached_chunks = max_cached_chunks

        # Get file size
        meta = obs.head(store, path)
        self._size: int = meta["size"]

        # Current position in the file
        self._pos = 0

        # LRU cache: chunk_index -> bytes
        # Using OrderedDict for LRU behavior
        self._cache: OrderedDict[int, bytes] = OrderedDict()
        self._cache_lock = threading.Lock()

        # Track which chunks are currently being fetched
        self._pending_chunks: set[int] = set()
        self._pending_lock = threading.Lock()

        # Thread pool for background prefetching
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # Flag to stop prefetching on close
        self._closed = False

    def _chunk_index(self, pos: int) -> int:
        """Get the chunk index for a given byte position."""
        return pos // self._chunk_size

    def _chunk_range(self, chunk_idx: int) -> tuple[int, int]:
        """Get the (start, end) byte range for a chunk index."""
        start = chunk_idx * self._chunk_size
        end = min(start + self._chunk_size, self._size)
        return start, end

    def _fetch_chunk(self, chunk_idx: int) -> bytes | None:
        """Fetch a single chunk from the store."""
        if self._closed:
            return None

        start, end = self._chunk_range(chunk_idx)
        if start >= self._size:
            return None

        data = obs.get_range(self._store, self._path, start=start, end=end)
        return data.to_bytes()

    def _prefetch_chunk_background(self, chunk_idx: int) -> None:
        """Background task to prefetch a chunk."""
        if self._closed:
            return

        try:
            data = self._fetch_chunk(chunk_idx)
            if data is not None:
                with self._cache_lock:
                    if chunk_idx not in self._cache:
                        self._cache[chunk_idx] = data
                        # Evict oldest if over capacity
                        while len(self._cache) > self._max_cached_chunks:
                            self._cache.popitem(last=False)
        finally:
            with self._pending_lock:
                self._pending_chunks.discard(chunk_idx)

    def _get_chunk(self, chunk_idx: int) -> bytes | None:
        """Get a chunk, fetching synchronously if not cached."""
        # Check cache first
        with self._cache_lock:
            if chunk_idx in self._cache:
                # Move to end for LRU
                self._cache.move_to_end(chunk_idx)
                return self._cache[chunk_idx]

        # Fetch synchronously
        data = self._fetch_chunk(chunk_idx)
        if data is not None:
            with self._cache_lock:
                self._cache[chunk_idx] = data
                self._cache.move_to_end(chunk_idx)
                while len(self._cache) > self._max_cached_chunks:
                    self._cache.popitem(last=False)

        return data

    def _trigger_prefetch(self) -> None:
        """Trigger prefetching of upcoming chunks."""
        if self._closed:
            return

        current_chunk = self._chunk_index(self._pos)
        prefetch_end = self._pos + self._prefetch_size
        end_chunk = self._chunk_index(min(prefetch_end, self._size - 1))

        for chunk_idx in range(current_chunk, end_chunk + 1):
            # Skip if already cached or being fetched
            with self._cache_lock:
                if chunk_idx in self._cache:
                    continue

            with self._pending_lock:
                if chunk_idx in self._pending_chunks:
                    continue
                self._pending_chunks.add(chunk_idx)

            # Submit prefetch task
            self._executor.submit(self._prefetch_chunk_background, chunk_idx)

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to size bytes from the file.

        Parameters
        ----------
        size
            Maximum number of bytes to read. If -1, read until end of file.

        Returns
        -------
        bytes
            The data read from the file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            size = self._size - self._pos

        if size <= 0 or self._pos >= self._size:
            return b""

        # Clamp to remaining bytes
        size = min(size, self._size - self._pos)

        # Trigger prefetch for upcoming data
        self._trigger_prefetch()

        # Collect data from chunks
        result = bytearray()
        bytes_remaining = size

        while bytes_remaining > 0 and self._pos < self._size:
            chunk_idx = self._chunk_index(self._pos)
            chunk_data = self._get_chunk(chunk_idx)

            if chunk_data is None:
                break

            # Calculate offset within chunk
            chunk_start, _ = self._chunk_range(chunk_idx)
            offset_in_chunk = self._pos - chunk_start

            # Calculate how much to read from this chunk
            available = len(chunk_data) - offset_in_chunk
            to_read = min(bytes_remaining, available)

            result.extend(chunk_data[offset_in_chunk : offset_in_chunk + to_read])
            self._pos += to_read
            bytes_remaining -= to_read

        return bytes(result)

    def readall(self) -> bytes:
        """Read and return all remaining bytes until end of file."""
        return self.read(-1)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change the stream position.

        Parameters
        ----------
        offset
            Position offset.
        whence
            Reference point for offset:
            - 0: Start of file (default)
            - 1: Current position
            - 2: End of file

        Returns
        -------
        int
            The new absolute position.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if whence == 0:
            new_pos = offset
        elif whence == 1:
            new_pos = self._pos + offset
        elif whence == 2:
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")

        self._pos = max(0, min(new_pos, self._size))
        return self._pos

    def tell(self) -> int:
        """Return the current stream position."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._pos

    def close(self) -> None:
        """Close the reader and release resources."""
        self._closed = True
        self._executor.shutdown(wait=False)
        with self._cache_lock:
            self._cache.clear()

    def __enter__(self) -> "ObstorePrefetchReader":
        return self

    def __exit__(self, *args) -> None:
        self.close()


class ObstoreParallelReader:
    """
    A file reader that fetches multiple byte ranges in parallel.

    This reader batches range requests and fetches them concurrently using
    obstore's get_ranges API. This is particularly effective for workloads
    that read multiple non-contiguous regions of a file, such as loading
    array chunks from HDF5/NetCDF files.

    Unlike the prefetch reader which speculatively fetches ahead, this reader
    fetches exactly what's needed but does so in parallel batches.
    """

    def __init__(
        self,
        store: ObjectStore,
        path: str,
        *,
        chunk_size: int = 1024 * 1024,
        max_cached_chunks: int = 32,
        batch_size: int = 16,
    ) -> None:
        """
        Create an obstore file reader with parallel range fetching.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        chunk_size
            Size of each chunk in bytes. Reads are aligned to chunk boundaries
            and multiple chunks are fetched in parallel. Default is 1 MB.
        max_cached_chunks
            Maximum number of chunks to keep in the LRU cache. Default is 32.
        batch_size
            Maximum number of ranges to fetch in a single parallel request.
            Default is 16.
        """
        self._store = store
        self._path = path
        self._chunk_size = chunk_size
        self._max_cached_chunks = max_cached_chunks
        self._batch_size = batch_size

        # Get file size
        meta = obs.head(store, path)
        self._size: int = meta["size"]

        # Current position in the file
        self._pos = 0

        # LRU cache: chunk_index -> bytes
        self._cache: OrderedDict[int, bytes] = OrderedDict()
        self._cache_lock = threading.Lock()

        self._closed = False

    def _chunk_index(self, pos: int) -> int:
        """Get the chunk index for a given byte position."""
        return pos // self._chunk_size

    def _chunk_range(self, chunk_idx: int) -> tuple[int, int]:
        """Get the (start, end) byte range for a chunk index."""
        start = chunk_idx * self._chunk_size
        end = min(start + self._chunk_size, self._size)
        return start, end

    def _fetch_chunks_parallel(self, chunk_indices: list[int]) -> dict[int, bytes]:
        """Fetch multiple chunks in parallel using get_ranges."""
        if not chunk_indices or self._closed:
            return {}

        # Build range requests
        starts = []
        ends = []
        valid_indices = []

        for idx in chunk_indices:
            start, end = self._chunk_range(idx)
            if start < self._size:
                starts.append(start)
                ends.append(end)
                valid_indices.append(idx)

        if not starts:
            return {}

        # Fetch all ranges in parallel
        results = obs.get_ranges(self._store, self._path, starts=starts, ends=ends)

        # Map results back to chunk indices
        return {idx: data.to_bytes() for idx, data in zip(valid_indices, results)}

    def _get_chunks(self, chunk_indices: list[int]) -> dict[int, bytes]:
        """Get multiple chunks, fetching missing ones in parallel."""
        result = {}
        missing = []

        # Check cache for each chunk
        with self._cache_lock:
            for idx in chunk_indices:
                if idx in self._cache:
                    self._cache.move_to_end(idx)
                    result[idx] = self._cache[idx]
                else:
                    missing.append(idx)

        # Fetch missing chunks in batches
        for i in range(0, len(missing), self._batch_size):
            batch = missing[i : i + self._batch_size]
            fetched = self._fetch_chunks_parallel(batch)

            # Add to cache and result
            with self._cache_lock:
                for idx, data in fetched.items():
                    self._cache[idx] = data
                    self._cache.move_to_end(idx)
                    result[idx] = data

                # Evict oldest chunks if over capacity
                while len(self._cache) > self._max_cached_chunks:
                    self._cache.popitem(last=False)

        return result

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to size bytes from the file.

        Parameters
        ----------
        size
            Maximum number of bytes to read. If -1, read until end of file.

        Returns
        -------
        bytes
            The data read from the file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            size = self._size - self._pos

        if size <= 0 or self._pos >= self._size:
            return b""

        # Clamp to remaining bytes
        size = min(size, self._size - self._pos)

        # Determine which chunks we need
        start_chunk = self._chunk_index(self._pos)
        end_chunk = self._chunk_index(self._pos + size - 1)
        chunk_indices = list(range(start_chunk, end_chunk + 1))

        # Fetch all needed chunks in parallel
        chunks = self._get_chunks(chunk_indices)

        # Assemble result from chunks
        result = bytearray()
        bytes_remaining = size

        while bytes_remaining > 0 and self._pos < self._size:
            chunk_idx = self._chunk_index(self._pos)
            chunk_data = chunks.get(chunk_idx)

            if chunk_data is None:
                break

            # Calculate offset within chunk
            chunk_start, _ = self._chunk_range(chunk_idx)
            offset_in_chunk = self._pos - chunk_start

            # Calculate how much to read from this chunk
            available = len(chunk_data) - offset_in_chunk
            to_read = min(bytes_remaining, available)

            result.extend(chunk_data[offset_in_chunk : offset_in_chunk + to_read])
            self._pos += to_read
            bytes_remaining -= to_read

        return bytes(result)

    def readall(self) -> bytes:
        """Read and return all remaining bytes until end of file."""
        return self.read(-1)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change the stream position.

        Parameters
        ----------
        offset
            Position offset.
        whence
            Reference point for offset:
            - 0: Start of file (default)
            - 1: Current position
            - 2: End of file

        Returns
        -------
        int
            The new absolute position.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if whence == 0:
            new_pos = offset
        elif whence == 1:
            new_pos = self._pos + offset
        elif whence == 2:
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")

        self._pos = max(0, min(new_pos, self._size))
        return self._pos

    def tell(self) -> int:
        """Return the current stream position."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._pos

    def close(self) -> None:
        """Close the reader and release resources."""
        self._closed = True
        with self._cache_lock:
            self._cache.clear()

    def __enter__(self) -> "ObstoreParallelReader":
        return self

    def __exit__(self, *args) -> None:
        self.close()


class ObstoreHybridReader:
    """
    A file reader combining exponential readahead with parallel chunk fetching.

    This reader uses two complementary caching strategies:

    1. **Exponential readahead cache**: For sequential reads from the file start
       (typical for HDF5/NetCDF metadata parsing). Fetches grow exponentially
       (e.g., 32KB → 64KB → 128KB) to minimize round-trips while avoiding
       over-fetching for small files.

    2. **Parallel chunk cache**: For random access to data chunks. Uses
       `get_ranges` to fetch multiple chunks in parallel with LRU eviction.

    The reader automatically selects the appropriate strategy based on access
    patterns, making it effective for both metadata-heavy operations (opening
    files) and data-heavy operations (loading array slices).
    """

    def __init__(
        self,
        store: ObjectStore,
        path: str,
        *,
        initial_readahead: int = 32 * 1024,
        readahead_multiplier: float = 2.0,
        max_readahead: int = 16 * 1024 * 1024,
        chunk_size: int = 1024 * 1024,
        max_cached_chunks: int = 32,
        batch_size: int = 16,
    ) -> None:
        """
        Create an obstore file reader with hybrid caching.

        Parameters
        ----------
        store
            [ObjectStore][obstore.store.ObjectStore] for reading the file.
        path
            The path to the file within the store. This should not include the prefix.
        initial_readahead
            Initial readahead size in bytes for sequential reads. Default is 32 KB.
        readahead_multiplier
            Multiplier for subsequent readahead sizes. Default is 2.0 (doubling).
        max_readahead
            Maximum readahead size in bytes. Default is 16 MB.
        chunk_size
            Size of each chunk for random access reads. Default is 1 MB.
        max_cached_chunks
            Maximum number of chunks in the LRU cache. Default is 32.
        batch_size
            Maximum number of ranges to fetch in a single parallel request.
            Default is 16.
        """
        self._store = store
        self._path = path
        self._initial_readahead = initial_readahead
        self._readahead_multiplier = readahead_multiplier
        self._max_readahead = max_readahead
        self._chunk_size = chunk_size
        self._max_cached_chunks = max_cached_chunks
        self._batch_size = batch_size

        # Get file size
        meta = obs.head(store, path)
        self._size: int = meta["size"]

        # Current position in the file
        self._pos = 0

        # Sequential readahead cache (contiguous from offset 0)
        self._seq_buffers: list[bytes] = []
        self._seq_len = 0
        self._last_readahead_size = 0

        # LRU chunk cache for random access
        self._chunk_cache: OrderedDict[int, bytes] = OrderedDict()
        self._cache_lock = threading.Lock()

        self._closed = False

    def _next_readahead_size(self) -> int:
        """Calculate the next readahead size using exponential growth."""
        if self._last_readahead_size == 0:
            return self._initial_readahead
        next_size = int(self._last_readahead_size * self._readahead_multiplier)
        return min(next_size, self._max_readahead)

    def _seq_contains(self, start: int, end: int) -> bool:
        """Check if the range is fully contained in the sequential cache."""
        return start >= 0 and end <= self._seq_len

    def _seq_slice(self, start: int, end: int) -> bytes:
        """Extract a slice from the sequential cache."""
        if start >= end:
            return b""

        result = bytearray()
        remaining_start = start
        remaining_end = end

        for buf in self._seq_buffers:
            buf_len = len(buf)

            # Skip buffers before our range
            if remaining_start >= buf_len:
                remaining_start -= buf_len
                remaining_end -= buf_len
                continue

            # Extract from this buffer
            chunk_start = remaining_start
            chunk_end = min(remaining_end, buf_len)
            result.extend(buf[chunk_start:chunk_end])

            remaining_start = 0
            remaining_end -= buf_len

            if remaining_end <= 0:
                break

        return bytes(result)

    def _extend_sequential_cache(self, needed_end: int) -> None:
        """Extend the sequential cache to cover at least needed_end."""
        while self._seq_len < needed_end and self._seq_len < self._size:
            fetch_size = self._next_readahead_size()
            # Ensure we fetch at least enough to cover the needed range
            fetch_size = max(fetch_size, needed_end - self._seq_len)
            fetch_end = min(self._seq_len + fetch_size, self._size)

            if fetch_end <= self._seq_len:
                break

            data = obs.get_range(
                self._store, self._path, start=self._seq_len, end=fetch_end
            )
            buf = data.to_bytes()

            self._seq_buffers.append(buf)
            self._seq_len += len(buf)
            self._last_readahead_size = len(buf)

    def _chunk_index(self, pos: int) -> int:
        """Get the chunk index for a given byte position."""
        return pos // self._chunk_size

    def _chunk_range(self, chunk_idx: int) -> tuple[int, int]:
        """Get the (start, end) byte range for a chunk index."""
        start = chunk_idx * self._chunk_size
        end = min(start + self._chunk_size, self._size)
        return start, end

    def _fetch_chunks_parallel(self, chunk_indices: list[int]) -> dict[int, bytes]:
        """Fetch multiple chunks in parallel using get_ranges."""
        if not chunk_indices or self._closed:
            return {}

        starts = []
        ends = []
        valid_indices = []

        for idx in chunk_indices:
            start, end = self._chunk_range(idx)
            if start < self._size:
                starts.append(start)
                ends.append(end)
                valid_indices.append(idx)

        if not starts:
            return {}

        results = obs.get_ranges(self._store, self._path, starts=starts, ends=ends)
        return {idx: data.to_bytes() for idx, data in zip(valid_indices, results)}

    def _get_chunks(self, chunk_indices: list[int]) -> dict[int, bytes]:
        """Get multiple chunks, fetching missing ones in parallel."""
        result = {}
        missing = []

        with self._cache_lock:
            for idx in chunk_indices:
                if idx in self._chunk_cache:
                    self._chunk_cache.move_to_end(idx)
                    result[idx] = self._chunk_cache[idx]
                else:
                    missing.append(idx)

        # Fetch missing chunks in batches
        for i in range(0, len(missing), self._batch_size):
            batch = missing[i : i + self._batch_size]
            fetched = self._fetch_chunks_parallel(batch)

            with self._cache_lock:
                for idx, data in fetched.items():
                    self._chunk_cache[idx] = data
                    self._chunk_cache.move_to_end(idx)
                    result[idx] = data

                while len(self._chunk_cache) > self._max_cached_chunks:
                    self._chunk_cache.popitem(last=False)

        return result

    def _read_via_chunks(self, start: int, end: int) -> bytes:
        """Read a range using the chunk cache with parallel fetching."""
        start_chunk = self._chunk_index(start)
        end_chunk = self._chunk_index(end - 1) if end > start else start_chunk
        chunk_indices = list(range(start_chunk, end_chunk + 1))

        chunks = self._get_chunks(chunk_indices)

        result = bytearray()
        pos = start
        remaining = end - start

        while remaining > 0 and pos < self._size:
            chunk_idx = self._chunk_index(pos)
            chunk_data = chunks.get(chunk_idx)

            if chunk_data is None:
                break

            chunk_start, _ = self._chunk_range(chunk_idx)
            offset_in_chunk = pos - chunk_start
            available = len(chunk_data) - offset_in_chunk
            to_read = min(remaining, available)

            result.extend(chunk_data[offset_in_chunk : offset_in_chunk + to_read])
            pos += to_read
            remaining -= to_read

        return bytes(result)

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to size bytes from the file.

        Uses exponential readahead for sequential reads from the file start,
        and parallel chunk fetching for random access patterns.

        Parameters
        ----------
        size
            Maximum number of bytes to read. If -1, read until end of file.

        Returns
        -------
        bytes
            The data read from the file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            size = self._size - self._pos

        if size <= 0 or self._pos >= self._size:
            return b""

        size = min(size, self._size - self._pos)
        start = self._pos
        end = start + size

        # Decide which cache strategy to use:
        # - If reading from within or just past the sequential cache, extend it
        # - Otherwise, use chunk-based parallel fetching
        use_sequential = start <= self._seq_len

        if use_sequential:
            # Extend sequential cache if needed
            if end > self._seq_len:
                self._extend_sequential_cache(end)

            # Read from sequential cache
            data = self._seq_slice(start, min(end, self._seq_len))

            # If we still need more (sequential cache hit file end), that's all we get
            self._pos += len(data)
            return data
        else:
            # Use parallel chunk fetching for random access
            data = self._read_via_chunks(start, end)
            self._pos += len(data)
            return data

    def readall(self) -> bytes:
        """Read and return all remaining bytes until end of file."""
        return self.read(-1)

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Change the stream position.

        Parameters
        ----------
        offset
            Position offset.
        whence
            Reference point for offset:
            - 0: Start of file (default)
            - 1: Current position
            - 2: End of file

        Returns
        -------
        int
            The new absolute position.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if whence == 0:
            new_pos = offset
        elif whence == 1:
            new_pos = self._pos + offset
        elif whence == 2:
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")

        self._pos = max(0, min(new_pos, self._size))
        return self._pos

    def tell(self) -> int:
        """Return the current stream position."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._pos

    def close(self) -> None:
        """Close the reader and release resources."""
        self._closed = True
        self._seq_buffers.clear()
        with self._cache_lock:
            self._chunk_cache.clear()

    def __enter__(self) -> "ObstoreHybridReader":
        return self

    def __exit__(self, *args) -> None:
        self.close()
