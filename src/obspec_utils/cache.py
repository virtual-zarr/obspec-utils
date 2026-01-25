"""Backward compatibility re-exports (deprecated).

New code should import from `obspec_utils.wrappers`.
"""

import warnings

from obspec_utils.wrappers import CachingReadableStore

warnings.warn(
    "Importing from obspec_utils.cache is deprecated. "
    "Please use 'from obspec_utils.wrappers import CachingReadableStore' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["CachingReadableStore"]
