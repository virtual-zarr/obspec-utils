"""Backward compatibility re-exports (deprecated).

New code should import from:
- `obspec_utils.protocols` for ReadableStore, ReadableFile
- `obspec_utils.readers` for BufferedStoreReader, EagerStoreReader, ParallelStoreReader
"""

import warnings

from obspec_utils.protocols import ReadableFile, ReadableStore
from obspec_utils.readers import (
    BufferedStoreReader,
    EagerStoreReader,
    ParallelStoreReader,
)

warnings.warn(
    "Importing from obspec_utils.obspec is deprecated. "
    "Please use 'from obspec_utils.protocols import ReadableStore, ReadableFile' "
    "and 'from obspec_utils.readers import BufferedStoreReader, EagerStoreReader, ParallelStoreReader' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "ReadableFile",
    "ReadableStore",
    "BufferedStoreReader",
    "EagerStoreReader",
    "ParallelStoreReader",
]
