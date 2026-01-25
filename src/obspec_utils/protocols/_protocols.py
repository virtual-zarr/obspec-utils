"""Core protocol definitions for object store interfaces."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from obspec import (
    Get,
    GetAsync,
    GetRange,
    GetRangeAsync,
    GetRanges,
    GetRangesAsync,
    Head,
    HeadAsync,
)


@runtime_checkable
class ReadableStore(
    Get,
    GetAsync,
    GetRange,
    GetRangeAsync,
    GetRanges,
    GetRangesAsync,
    Head,
    HeadAsync,
    Protocol,
):
    """
    Full read interface for transparent store wrappers.

    This protocol combines the obspec protocols needed for stores that support
    complete read operations. It's used by transparent proxy wrappers like
    [CachingReadableStore][obspec_utils.wrappers.CachingReadableStore],
    [TracingReadableStore][obspec_utils.wrappers.TracingReadableStore], and
    [SplittingReadableStore][obspec_utils.wrappers.SplittingReadableStore].

    The protocol includes:

    - [Get][obspec.Get] / [GetAsync][obspec.GetAsync]: Download entire files
    - [GetRange][obspec.GetRange] / [GetRangeAsync][obspec.GetRangeAsync]: Download byte ranges
    - [GetRanges][obspec.GetRanges] / [GetRangesAsync][obspec.GetRangesAsync]: Download multiple ranges
    - [Head][obspec.Head] / [HeadAsync][obspec.HeadAsync]: Get file metadata (size, etag, etc.)

    !!! Warning
        It's recommended to define your own protocols. This protocol may change without warning.
    """

    pass


@runtime_checkable
class ReadableFile(Protocol):
    """
    Protocol for read-only file-like objects.

    This protocol defines the minimal interface needed to read from a file-like
    object, compatible with libraries that expect file handles (e.g., h5py, zarr).

    The `obspec_utils` readers ([`BufferedStoreReader`][obspec_utils.readers.BufferedStoreReader],
    [`EagerStoreReader`][obspec_utils.readers.EagerStoreReader],
    [`ParallelStoreReader`][obspec_utils.readers.EagerStoreReader]) all implement this protocol,
    allowing them to be used interchangeably wherever a [`ReadableFile`][obspec_utils.protocols.ReadableFile] is expected.

    !!! Warning
        It's recommended to define your own protocols. This protocol may change without warning.

    Examples
    --------

    Using as a type hint for configurable readers:

    ```python
    from typing import Callable
    from obspec_utils.protocols import ReadableStore, ReadableFile
    from obspec_utils.readers import BufferedStoreReader

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
    from obspec_utils.protocols import ReadableFile
    from obspec_utils.readers import BufferedStoreReader

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


__all__ = ["ReadableStore", "ReadableFile"]
