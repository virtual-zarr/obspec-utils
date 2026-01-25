"""Backward compatibility re-exports (deprecated).

New code should import from `obspec_utils.wrappers`.
"""

import warnings

from obspec_utils.wrappers import RequestRecord, RequestTrace, TracingReadableStore

warnings.warn(
    "Importing from obspec_utils.tracing is deprecated. "
    "Please use 'from obspec_utils.wrappers import TracingReadableStore, RequestTrace, RequestRecord' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "RequestRecord",
    "RequestTrace",
    "TracingReadableStore",
]
