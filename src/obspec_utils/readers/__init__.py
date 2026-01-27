"""File-like readers for object stores.

This module provides readers that wrap object stores with a file-like interface
(read, seek, tell), enabling use with libraries that expect file handles.
"""

from obspec_utils.readers._buffered import BufferedStoreReader
from obspec_utils.readers._eager import EagerStoreReader
from obspec_utils.readers._block import BlockStoreReader, ParallelStoreReader

__all__ = [
    "BlockStoreReader",
    "BufferedStoreReader",
    "EagerStoreReader",
    "ParallelStoreReader",
]
