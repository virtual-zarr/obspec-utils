"""Tests for deprecated import paths.

These tests verify that importing from the old module paths still works
but raises DeprecationWarning with guidance to use the new paths.
"""

import pytest


class TestDeprecatedObspecImports:
    """Tests for deprecated obspec_utils.obspec imports."""

    def test_import_raises_deprecation_warning(self):
        """Importing from obspec_utils.obspec raises DeprecationWarning."""
        with pytest.warns(
            DeprecationWarning, match="obspec_utils.obspec is deprecated"
        ):
            from obspec_utils.obspec import ReadableFile  # noqa: F401


class TestDeprecatedCacheImports:
    """Tests for deprecated obspec_utils.cache imports."""

    def test_import_raises_deprecation_warning(self):
        """Importing from obspec_utils.cache raises DeprecationWarning."""
        with pytest.warns(DeprecationWarning, match="obspec_utils.cache is deprecated"):
            from obspec_utils.cache import CachingReadableStore  # noqa: F401


class TestDeprecatedTracingImports:
    """Tests for deprecated obspec_utils.tracing imports."""

    def test_import_raises_deprecation_warning(self):
        """Importing from obspec_utils.tracing raises DeprecationWarning."""
        with pytest.warns(
            DeprecationWarning, match="obspec_utils.tracing is deprecated"
        ):
            from obspec_utils.tracing import TracingReadableStore  # noqa: F401


class TestDeprecatedSplittingImports:
    """Tests for deprecated obspec_utils.splitting imports."""

    def test_import_raises_deprecation_warning(self):
        """Importing from obspec_utils.splitting raises DeprecationWarning."""
        with pytest.warns(
            DeprecationWarning, match="obspec_utils.splitting is deprecated"
        ):
            from obspec_utils.splitting import SplittingReadableStore  # noqa: F401


class TestDeprecatedAiohttpImports:
    """Tests for deprecated obspec_utils.aiohttp imports."""

    def test_import_raises_deprecation_warning(self):
        """Importing from obspec_utils.aiohttp raises DeprecationWarning."""
        with pytest.warns(
            DeprecationWarning, match="obspec_utils.aiohttp is deprecated"
        ):
            from obspec_utils.aiohttp import AiohttpStore  # noqa: F401
