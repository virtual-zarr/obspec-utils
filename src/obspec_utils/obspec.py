from __future__ import annotations

import io


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


__all__: list[str] = [
    "ReadableStore",
    "BufferedStoreReader",
    "EagerStoreReader",
]
