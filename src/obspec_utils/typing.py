from typing import TypeAlias

Url: TypeAlias = str
"""A URL string (e.g., 's3://bucket/path' or 'https://example.com/file')."""

Path: TypeAlias = str
"""A path string within an object store."""

__all__ = ["Url", "Path"]
