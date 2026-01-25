"""Backward compatibility re-exports (deprecated).

New code should import from `obspec_utils.wrappers`.
"""

import warnings

from obspec_utils.wrappers import SplittingReadableStore

warnings.warn(
    "Importing from obspec_utils.splitting is deprecated. "
    "Please use 'from obspec_utils.wrappers import SplittingReadableStore' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["SplittingReadableStore"]
