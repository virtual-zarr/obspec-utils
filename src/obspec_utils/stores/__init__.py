"""Object store implementations.

This module provides concrete store implementations that can be used
directly or registered with ObjectStoreRegistry.
"""

from obspec_utils.stores._aiohttp import (
    AiohttpGetResult,
    AiohttpGetResultAsync,
    AiohttpStore,
)

__all__ = [
    "AiohttpStore",
    "AiohttpGetResult",
    "AiohttpGetResultAsync",
]
