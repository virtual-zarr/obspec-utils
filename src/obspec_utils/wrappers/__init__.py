"""Store wrappers that add functionality to underlying stores.

This module provides transparent wrapper classes that add caching, tracing,
and request splitting capabilities to any ReadableStore.
"""

from obspec_utils.wrappers._cache import CachingReadableStore
from obspec_utils.wrappers._splitting import SplittingReadableStore
from obspec_utils.wrappers._tracing import (
    RequestRecord,
    RequestTrace,
    TracingReadableStore,
)

__all__ = [
    "CachingReadableStore",
    "SplittingReadableStore",
    "TracingReadableStore",
    "RequestTrace",
    "RequestRecord",
]
