"""Shared tests for store wrapper classes.

These parameterized tests ensure consistent behavior across all store wrappers
(CachingReadableStore, SplittingReadableStore, etc.).
"""

import pickle

import pytest

from obspec_utils.cache import CachingReadableStore
from obspec_utils.splitting import SplittingReadableStore

from .mocks import PicklableStore


def make_caching_wrapper(store):
    """Factory for CachingReadableStore."""
    return CachingReadableStore(store, max_size=256 * 1024 * 1024)


def make_splitting_wrapper(store):
    """Factory for SplittingReadableStore."""
    return SplittingReadableStore(store, request_size=1024)


ALL_WRAPPER_FACTORIES = [
    pytest.param(make_caching_wrapper, id="CachingReadableStore"),
    pytest.param(make_splitting_wrapper, id="SplittingReadableStore"),
]


# =============================================================================
# __getattr__ behavior tests
# =============================================================================


class TestStoreWrapperGetattr:
    """Tests for __getattr__ behavior on all store wrappers."""

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_getattr_forwards_public_attributes(self, make_wrapper):
        """Public attributes are forwarded to the underlying store."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        # PicklableStore has a 'head' method - should be accessible
        assert hasattr(wrapper, "head")
        assert callable(wrapper.head)

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_getattr_raises_for_private_attributes(self, make_wrapper):
        """Private attributes (underscore-prefixed) raise AttributeError.

        This prevents __getattr__ from forwarding _store lookups during
        unpickling, which would cause infinite recursion.
        """
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        with pytest.raises(AttributeError, match="has no attribute '_nonexistent'"):
            _ = wrapper._nonexistent

        with pytest.raises(AttributeError, match="has no attribute '_private_attr'"):
            _ = wrapper._private_attr

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_getattr_raises_for_nonexistent_public_attributes(self, make_wrapper):
        """Non-existent public attributes raise AttributeError."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        with pytest.raises(AttributeError):
            _ = wrapper.nonexistent_method

    @pytest.mark.parametrize(
        "WrapperClass", [CachingReadableStore, SplittingReadableStore]
    )
    def test_getattr_raises_when_store_not_initialized(self, WrapperClass):
        """AttributeError raised when _store not yet in __dict__.

        This can happen during unpickling before __init__ runs.
        We simulate this by creating an object without calling __init__.
        """
        # Create instance without calling __init__
        wrapper = object.__new__(WrapperClass)

        # Accessing any attribute should raise AttributeError, not recurse
        with pytest.raises(AttributeError):
            _ = wrapper.some_attribute

        with pytest.raises(AttributeError):
            _ = wrapper._private


# =============================================================================
# Pickling tests
# =============================================================================


class TestStoreWrapperPickling:
    """Pickle tests that apply to all store wrappers."""

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_pickle_roundtrip(self, make_wrapper):
        """Store wrapper can be pickled and unpickled."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        pickled = pickle.dumps(wrapper)
        restored = pickle.loads(pickled)

        assert type(restored) is type(wrapper)

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_pickle_getattr_no_recursion(self, make_wrapper):
        """__getattr__ doesn't cause infinite recursion during unpickling.

        Store wrappers define __getattr__ to forward to self._store. During
        unpickling, _store doesn't exist yet, which can cause infinite recursion:

            RecursionError: maximum recursion depth exceeded
            File "obspec_utils/splitting.py", line 135, in __getattr__
                return getattr(self._store, name)

        The fix is for __getattr__ to raise AttributeError for underscore-prefixed
        attributes instead of forwarding them.
        """
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        # This will raise RecursionError if __getattr__ isn't handling
        # underscore-prefixed attributes correctly during unpickling
        pickled = pickle.dumps(wrapper)
        restored = pickle.loads(pickled)

        # Verify the wrapper is functional after unpickling
        result = restored.get("file.txt")
        assert bytes(result.buffer()) == b"hello world"

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_pickle_restored_is_functional(self, make_wrapper):
        """Restored wrapper can fetch data correctly."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        restored = pickle.loads(pickle.dumps(wrapper))

        result = restored.get("file.txt")
        assert bytes(result.buffer()) == b"hello world"

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_pickle_multiple_protocols(self, make_wrapper):
        """Pickling works with different pickle protocols."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            pickled = pickle.dumps(wrapper, protocol=protocol)
            restored = pickle.loads(pickled)
            assert type(restored) is type(wrapper)

    @pytest.mark.parametrize("make_wrapper", ALL_WRAPPER_FACTORIES)
    def test_pickle_preserves_store(self, make_wrapper):
        """Unpickled wrapper preserves the underlying store."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        wrapper = make_wrapper(store)

        restored = pickle.loads(pickle.dumps(wrapper))

        # Should be able to fetch data through the restored store
        result = restored.get("file.txt")
        assert bytes(result.buffer()) == b"hello world"


# =============================================================================
# Wrapper-specific config preservation tests
# =============================================================================


class TestCachingReadableStorePickleConfig:
    """Config preservation tests specific to CachingReadableStore."""

    def test_pickle_preserves_max_size(self):
        """Unpickled CachingReadableStore preserves max_size."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")

        custom_max_size = 64 * 1024 * 1024
        wrapper = CachingReadableStore(store, max_size=custom_max_size)

        restored = pickle.loads(pickle.dumps(wrapper))
        assert restored._max_size == custom_max_size

    def test_pickle_creates_empty_cache(self):
        """Unpickled CachingReadableStore has a fresh empty cache."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")
        store.put("file2.txt", b"more data")

        wrapper = CachingReadableStore(store)
        wrapper.get("file.txt")
        wrapper.get("file2.txt")
        assert wrapper.cache_size > 0

        restored = pickle.loads(pickle.dumps(wrapper))
        assert restored.cache_size == 0
        assert len(restored.cached_paths) == 0


class TestSplittingReadableStorePickleConfig:
    """Config preservation tests specific to SplittingReadableStore."""

    def test_pickle_preserves_request_size(self):
        """Unpickled SplittingReadableStore preserves request_size."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")

        custom_request_size = 8 * 1024 * 1024
        wrapper = SplittingReadableStore(store, request_size=custom_request_size)

        restored = pickle.loads(pickle.dumps(wrapper))
        assert restored._request_size == custom_request_size

    def test_pickle_preserves_max_concurrent_requests(self):
        """Unpickled SplittingReadableStore preserves max_concurrent_requests."""
        store = PicklableStore()
        store.put("file.txt", b"hello world")

        wrapper = SplittingReadableStore(store, max_concurrent_requests=10)

        restored = pickle.loads(pickle.dumps(wrapper))
        assert restored._max_concurrent_requests == 10
