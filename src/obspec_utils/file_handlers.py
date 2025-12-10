from __future__ import annotations

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


class ObstoreMemCacheReader(ObstoreReader):
    _reader: ReadableFile
    _memstore: MemoryStore

    def __init__(self, store: ObjectStore, path: str) -> None:
        """
        Create an obstore file reader that caches the specified path
        in a MemoryStore then performs reads from the file in memory.

        This reader loads the entire file into memory first, which can be beneficial
        for files that will be read multiple times or when you want to avoid repeated
        network requests to the original store.

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
