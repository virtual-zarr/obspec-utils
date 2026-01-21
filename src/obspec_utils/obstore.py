from __future__ import annotations

from typing import TYPE_CHECKING

try:
    import obstore as obs
    from obstore.store import MemoryStore
except ImportError as e:
    raise ImportError(
        "obstore is required for ObstoreReader. Install it with: pip install obstore"
    ) from e


if TYPE_CHECKING:
    from obstore import ReadableFile
    from obstore.store import ObjectStore


class ObstoreReader:
    """
    A file-like reader using obstore's native ReadableFile.

    This class uses obstore's optimized `open_reader()` which provides efficient
    buffered reading. It requires an actual [obstore.store.ObjectStore][] instance.

    For a generic reader that works with any ReadableStore, use
    [StoreReader][obspec_utils.obspec.StoreReader] instead.
    """

    _reader: ReadableFile

    def __init__(
        self, store: ObjectStore, path: str, buffer_size: int = 1024 * 1024
    ) -> None:
        """
        Create an obstore file reader.

        Parameters
        ----------
        store
            An obstore [ObjectStore][obstore.store.ObjectStore] instance.
        path
            The path to the file within the store.
        buffer_size
            The minimum number of bytes to read in a single request.
        """
        self._reader = obs.open_reader(store, path, buffer_size=buffer_size)

    def read(self, size: int, /) -> bytes:
        return self._reader.read(size).to_bytes()

    def readall(self) -> bytes:
        return self._reader.read().to_bytes()

    def seek(self, offset: int, whence: int = 0, /) -> int:
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()


class ObstoreMemCacheReader(ObstoreReader):
    """
    A file-like reader using obstore's MemoryStore for caching.

    This class fetches the entire file into obstore's MemoryStore, then uses
    obstore's native ReadableFile for efficient cached reads.

    For a generic cached reader that works with any ReadableStore, use
    [StoreMemCacheReader][obspec_utils.obspec.StoreMemCacheReader] instead.
    """

    _reader: ReadableFile
    _memstore: MemoryStore

    def __init__(self, store: ObjectStore, path: str) -> None:
        """
        Create an obstore memory-cached reader.

        Parameters
        ----------
        store
            An obstore [ObjectStore][obstore.store.ObjectStore] instance.
        path
            The path to the file within the store.
        """
        self._memstore = MemoryStore()
        buffer = store.get(path).bytes()
        self._memstore.put(path, buffer)
        self._reader = obs.open_reader(self._memstore, path)


__all__ = ["ObstoreReader", "ObstoreMemCacheReader"]
