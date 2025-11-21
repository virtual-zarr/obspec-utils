from __future__ import annotations

from typing import TYPE_CHECKING

import obstore as obs

if TYPE_CHECKING:
    from obstore import ReadableFile
    from obstore.store import ObjectStore


class ObstoreReader:
    _reader: ReadableFile

    def __init__(self, store: ObjectStore, path: str) -> None:
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
        """
        self._reader = obs.open_reader(store, path)

    def read(self, size: int, /) -> bytes:
        return self._reader.read(size).to_bytes()

    def readall(self) -> bytes:
        return self._reader.read().to_bytes()

    def seek(self, offset: int, whence: int = 0, /):
        # TODO: Check on default for whence
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()
