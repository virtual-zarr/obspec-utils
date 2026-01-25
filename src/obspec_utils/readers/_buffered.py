"""Buffered store reader with on-demand reads."""

from __future__ import annotations

from typing import Protocol

from obspec import Get, GetRange, Head


class BufferedStoreReader:
    """
    A file-like reader with buffered on-demand reads.

    This class provides a file-like interface (read, seek, tell) on top of any
    object store. The reader uses [`get_range()`][obspec.GetRange] calls to fetch data on-demand,
    with optional read-ahead buffering for efficiency.

    When to Use
    -----------
    Use BufferedStoreReader when:

    - **Sequential reading with rare backward seeks**: Best for workloads that
      mostly read forward through a file with rare backward seeks.
    - **Simple use cases**: When you need a basic file-like interface without
      caching or parallel fetching.
    - **Streaming data**: Processing data as it arrives without loading the full
      file into memory.

    Consider alternatives when:

    - You need to read the entire file anyway → use [EagerStoreReader][obspec_utils.readers.EagerStoreReader]
    - You have many non-contiguous reads → use [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader]
    - You'll repeatedly access the same regions → use [EagerStoreReader][obspec_utils.readers.EagerStoreReader]
      or [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader]

    See Also
    --------

    - [EagerStoreReader][obspec_utils.readers.EagerStoreReader] : Loads entire file into memory for fast random access.
    - [ParallelStoreReader][obspec_utils.readers.ParallelStoreReader] : Uses parallel requests with LRU caching for sparse access.
    """

    class Store(Get, GetRange, Head, Protocol):
        """
        Store protocol required by BufferedStoreReader.

        Combines [Get][obspec.Get], [GetRange][obspec.GetRange], and
        [Head][obspec.Head] from obspec.
        """

        pass

    def __init__(
        self,
        store: BufferedStoreReader.Store,
        path: str,
        buffer_size: int = 1024 * 1024,
    ) -> None:
        """
        Create a file-like reader for any object store.

        Parameters
        ----------
        store
            Any object implementing [Get][obspec.Get] and [GetRange][obspec.GetRange].
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
        """Lazily fetch the file size via a head() call."""
        if self._size is None:
            self._size = self._store.head(self._path)["size"]
        return self._size

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
        if size == -1:
            # Read from current position to end
            file_size = self._get_size()
            size = file_size - self._position
            if size <= 0:
                return b""

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

        # Check if we're at or past EOF
        file_size = self._get_size()
        if self._position >= file_size:
            return b""

        # Need to fetch from store - clamp to remaining bytes
        remaining = file_size - self._position
        fetch_size = min(max(size, self._buffer_size), remaining)
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

    def close(self) -> None:
        """Close the reader and release the read-ahead buffer."""
        self._buffer = b""
        self._buffer_start = 0

    def __enter__(self) -> "BufferedStoreReader":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager and close the reader."""
        self.close()


__all__ = ["BufferedStoreReader"]
