"""Protocols for object store interfaces.

This module defines the core protocols used throughout obspec-utils.
"""

from obspec_utils.protocols._protocols import ReadableFile, ReadableStore

__all__ = ["ReadableStore", "ReadableFile"]
