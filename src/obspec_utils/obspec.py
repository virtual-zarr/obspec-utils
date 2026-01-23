from __future__ import annotations

import io
from collections import OrderedDict
from typing import Protocol, runtime_checkable

from obspec import (
    Get,
    GetAsync,
    GetRange,
    GetRangeAsync,
    GetRanges,
    GetRangesAsync,
)


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
    from obspec_utils.registry import ObjectStoreRegistry

    # S3Store implements ReadableStore protocol
    store = S3Store(bucket="my-bucket")
    registry = ObjectStoreRegistry({"s3://my-bucket": store})
    ```

    Using with a custom aiohttp wrapper:

    ```python
    from obspec_utils.registry import ObjectStoreRegistry
    from obspec_utils.aiohttp import AiohttpStore

    # AiohttpStore implements ReadableStore protocol
    store = AiohttpStore("https://example.com/data")
    registry = ObjectStoreRegistry({"https://example.com/data": store})
    ```
    """

    pass


@runtime_checkable
class ReadableFile(Protocol):
    """
    Protocol for read-only file-like objects.

    This protocol defines the minimal interface needed to read from a file-like
    object, compatible with libraries that expect file handles (e.g., h5py, zarr).

    The readers in this module (`BufferedStoreReader`, `EagerStoreReader`,
    `ParallelStoreReader`) all implement this protocol, allowing them to be used
    interchangeably wherever a `ReadableFile` is expected.

    Examples
    --------

    Using as a type hint for configurable readers:

    ```python
    from typing import Callable
    from obspec_utils.obspec import ReadableStore, ReadableFile, BufferedStoreReader

    # Type alias for reader factories
    ReaderFactory = Callable[[ReadableStore, str], ReadableFile]

    def read_hdf5(
        store: ReadableStore,
        path: str,
        reader_factory: ReaderFactory = BufferedStoreReader,
    ):
        reader = reader_factory(store, path)
        # reader implements ReadableFile protocol
        with h5py.File(reader, mode="r") as f:
            ...
    ```

    Runtime checking:

    ```python
    from obspec_utils.obspec import ReadableFile, BufferedStoreReader

    reader = BufferedStoreReader(store, "file.nc")
    assert isinstance(reader, ReadableFile)  # True
    ```
    """

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to `size` bytes from the file.

        Parameters
        ----------
        size
            Number of bytes to read. If -1, read until EOF.

        Returns
        -------
        bytes
            The data read from the file.
        """
        ...

    def seek(self, offset: int, whence: int = 0, /) -> int:
        """
        Move to a new file position.

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
        ...

    def tell(self) -> int:
        """
        Return the current file position.

        Returns
        -------
        int
            Current position in bytes from start of file.
        """
        ...


# Protocol-based readers (work with any ReadableStore implementation)


class BufferedStoreReader:
    """
    A file-like reader with buffered on-demand reads.

    This class provides a file-like interface (read, seek, tell) on top of any
    object that implements the ReadableStore protocol, including obstore classes,
    AiohttpStore, or custom implementations.

    The reader uses `get_range()` calls to fetch data on-demand, with optional
    read-ahead buffering for efficiency.
    """

    def __init__(
        self, store: ReadableStore, path: str, buffer_size: int = 1024 * 1024
    ) -> None:
        """
        Create a file-like reader for any ReadableStore.

        Parameters
        ----------
        store
            Any object implementing the [ReadableStore][obspec_utils.obspec.ReadableStore] protocol.
        path
            The path to the file within the store.
        buffer_size
            Read-ahead buffer size in bytes. When reading, up to this many bytes
            may be fetched ahead to reduce the number of requests.
        """
        self._store = store
        self._path = path
        self._buffer_size = buffer_size
        self._position = 0
        self._size: int | None = None
        # Read-ahead buffer
        self._buffer = b""
        self._buffer_start = 0

    def _get_size(self) -> int:
        """Lazily fetch the file size via a get() call."""
        if self._size is None:
            result = self._store.get(self._path)
            self._size = result.meta["size"]
        return self._size

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to `size` bytes from the file.

        Parameters
        ----------
        size
            Number of bytes to read. If -1, read the entire file.

        Returns
        -------
        bytes
            The data read from the file.
        """
        if size == -1:
            return self.readall()

        # Check if we can satisfy from buffer
        buffer_end = self._buffer_start + len(self._buffer)
        if self._buffer_start <= self._position < buffer_end:
            # Some or all data is in buffer
            buffer_offset = self._position - self._buffer_start
            available = len(self._buffer) - buffer_offset
            if available >= size:
                # Fully satisfied from buffer
                data = self._buffer[buffer_offset : buffer_offset + size]
                self._position += len(data)
                return data

        # Need to fetch from store
        fetch_size = max(size, self._buffer_size)
        data = bytes(
            self._store.get_range(self._path, start=self._position, length=fetch_size)
        )

        # Update buffer
        self._buffer = data
        self._buffer_start = self._position

        # Return requested amount
        result = data[:size]
        self._position += len(result)
        return result

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


class EagerStoreReader:
    """
    A file-like reader that eagerly loads the entire file into memory.

    This reader fetches the complete file on first access and then serves all
    subsequent reads from the in-memory cache. Useful for files that will be
    read multiple times or when seeking is frequent.

    Works with any ReadableStore protocol implementation.
    """

    def __init__(self, store: ReadableStore, path: str) -> None:
        """
        Create an eager reader that loads the entire file into memory.

        The file is fetched immediately and cached in memory.

        Parameters
        ----------
        store
            Any object implementing the [ReadableStore][obspec_utils.obspec.ReadableStore] protocol.
        path
            The path to the file within the store.
        """
        result = store.get(path)
        data = bytes(result.buffer())
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


class ParallelStoreReader:
    """
    A file-like reader that uses parallel range requests for efficient chunk fetching.

    This reader divides the file into fixed-size chunks and uses `get_ranges()` to
    fetch multiple chunks in parallel. An LRU cache stores recently accessed chunks
    to avoid redundant fetches.

    This is particularly efficient for workloads that access multiple non-contiguous
    regions of a file, such as reading Zarr/HDF5 datasets.

    Works with any ReadableStore protocol implementation.
    """

    def __init__(
        self,
        store: ReadableStore,
        path: str,
        chunk_size: int = 256 * 1024,
        max_cached_chunks: int = 64,
    ) -> None:
        """
        Create a parallel reader with chunk-based caching.

        Parameters
        ----------
        store
            Any object implementing the [ReadableStore][obspec_utils.obspec.ReadableStore] protocol.
        path
            The path to the file within the store.
        chunk_size
            Size of each chunk in bytes. Smaller chunks mean more granular caching
            but potentially more requests.
        max_cached_chunks
            Maximum number of chunks to keep in the LRU cache.
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
        """Lazily fetch the file size via a get() call."""
        if self._size is None:
            result = self._store.get(self._path)
            self._size = result.meta["size"]
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
                # Move to end (most recently used)
                self._cache.move_to_end(chunk_idx)

                # Evict oldest if over capacity
                while len(self._cache) > self._max_cached_chunks:
                    self._cache.popitem(last=False)

        # Return requested chunks from cache
        return {i: self._cache[i] for i in chunk_indices}

    def read(self, size: int = -1, /) -> bytes:
        """
        Read up to `size` bytes from the file.

        Parameters
        ----------
        size
            Number of bytes to read. If -1, read the entire file.

        Returns
        -------
        bytes
            The data read from the file.
        """
        if size == -1:
            return self.readall()

        file_size = self._get_size()

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


__all__: list[str] = [
    "ReadableFile",
    "ReadableStore",
    "BufferedStoreReader",
    "EagerStoreReader",
    "ParallelStoreReader",
]
