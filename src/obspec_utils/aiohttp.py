"""Backward compatibility re-exports (deprecated).

New code should import from `obspec_utils.stores`.
"""

import warnings

from obspec_utils.stores import AiohttpGetResult, AiohttpGetResultAsync, AiohttpStore

warnings.warn(
    "Importing from obspec_utils.aiohttp is deprecated. "
    "Please use 'from obspec_utils.stores import AiohttpStore' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["AiohttpStore", "AiohttpGetResult", "AiohttpGetResultAsync"]
