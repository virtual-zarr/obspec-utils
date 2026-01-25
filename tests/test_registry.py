"""Tests for ObjectStoreRegistry and related utilities."""

import pytest
from obstore.store import MemoryStore

from obspec_utils.registry import (
    ObjectStoreRegistry,
    PathEntry,
    UrlKey,
    get_url_key,
    path_segments,
)

from .mocks import MockReadableStoreWithoutHead


# =============================================================================
# Basic registry tests
# =============================================================================


def test_registry():
    registry = ObjectStoreRegistry()
    memstore = MemoryStore()
    registry.register("s3://bucket1", memstore)
    url = "s3://bucket1/path/to/object"
    ret, path = registry.resolve(url)
    assert path == "path/to/object"
    assert ret is memstore


def test_register_raises():
    registry = ObjectStoreRegistry()
    with pytest.raises(
        ValueError,
        match=r"Urls are expected to contain a scheme \(e\.g\., `file://` or `s3://`\), received .* which parsed to ParseResult\(scheme='.*', netloc='.*', path='.*', params='.*', query='.*', fragment='.*'\)",
    ):
        url = "bucket1/path/to/object"
        registry.register(url, MemoryStore())


def test_resolve_raises():
    registry = ObjectStoreRegistry()
    with pytest.raises(
        ValueError,
        match="Could not find an ObjectStore matching the url `s3://bucket1/path/to/object`",
    ):
        url = "s3://bucket1/path/to/object"
        registry.resolve(url)


# =============================================================================
# Protocol satisfaction tests
# =============================================================================


def test_obstore_has_get_methods():
    """Verify that obstore classes have Get protocol methods."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"test")
    # Verify it has the expected methods
    assert hasattr(memstore, "get")
    assert hasattr(memstore, "get_async")
    # Verify it works
    result = memstore.get("test.txt")
    assert bytes(result.buffer()) == b"test"


def test_caching_store_has_get_method():
    """Verify that CachingReadableStore has the Get protocol methods."""
    from obspec_utils.cache import CachingReadableStore

    memstore = MemoryStore()
    memstore.put("test.txt", b"test")
    cached = CachingReadableStore(memstore)
    # Verify it has the expected methods
    assert hasattr(cached, "get")
    assert hasattr(cached, "get_async")
    # Verify it works
    result = cached.get("test.txt")
    assert bytes(result.buffer()) == b"test"


def test_splitting_store_has_get_method():
    """Verify that SplittingReadableStore has the Get protocol methods."""
    from obspec_utils.splitting import SplittingReadableStore

    memstore = MemoryStore()
    memstore.put("test.txt", b"test")
    splitting = SplittingReadableStore(memstore)
    # Verify it has the expected methods
    assert hasattr(splitting, "get")
    assert hasattr(splitting, "get_async")
    # Verify it works
    result = splitting.get("test.txt")
    assert bytes(result.buffer()) == b"test"


def test_store_wrappers_compose():
    """Verify that store wrappers can be composed."""
    from obspec_utils.cache import CachingReadableStore
    from obspec_utils.splitting import SplittingReadableStore
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    memstore = MemoryStore()
    memstore.put("file.txt", b"hello world")

    store = SplittingReadableStore(memstore)
    store = CachingReadableStore(store)
    trace = RequestTrace()
    store = TracingReadableStore(store, trace)

    # Verify composed store has expected methods
    assert hasattr(store, "get")
    assert hasattr(store, "get_range")
    assert hasattr(store, "get_ranges")

    registry = ObjectStoreRegistry({"mem://test": store})
    resolved_store, path = registry.resolve("mem://test/file.txt")

    result = resolved_store.get(path)
    assert bytes(result.buffer()) == b"hello world"


def test_registry_with_custom_readable_store():
    """Test that registry works with any ReadableStore protocol implementation."""
    mock_store = MockReadableStoreWithoutHead(b"hello world")
    registry = ObjectStoreRegistry({"https://example.com": mock_store})

    store, path = registry.resolve("https://example.com/data/file.txt")
    assert store is mock_store
    assert path == "data/file.txt"

    assert store.get_range(path, start=0, end=5) == b"hello"
    assert store.get_range(path, start=6, length=5) == b"world"


@pytest.mark.asyncio
async def test_registry_with_async_operations():
    """Test async operations with registry."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"async test data")

    registry = ObjectStoreRegistry({"mem://test": memstore})
    store, path = registry.resolve("mem://test/test.txt")

    result = await store.get_range_async(path, start=0, end=5)
    assert bytes(result) == b"async"


# =============================================================================
# path_segments tests
# =============================================================================


def test_path_segments_simple():
    """Simple path splits correctly."""
    assert list(path_segments("a/b/c")) == ["a", "b", "c"]


def test_path_segments_leading_slash():
    """Leading slash is handled."""
    assert list(path_segments("/a/b/c")) == ["a", "b", "c"]


def test_path_segments_trailing_slash():
    """Trailing slash is handled."""
    assert list(path_segments("a/b/c/")) == ["a", "b", "c"]


def test_path_segments_multiple_slashes():
    """Multiple consecutive slashes are handled."""
    assert list(path_segments("a//b///c")) == ["a", "b", "c"]


def test_path_segments_empty():
    """Empty path returns empty list."""
    assert list(path_segments("")) == []


def test_path_segments_only_slashes():
    """Path of only slashes returns empty list."""
    assert list(path_segments("///")) == []


# =============================================================================
# get_url_key tests
# =============================================================================


def test_get_url_key_s3():
    """S3 URL parses correctly."""
    key = get_url_key("s3://bucket")
    assert key == UrlKey("s3", "bucket")


def test_get_url_key_https():
    """HTTPS URL parses correctly, path is ignored."""
    key = get_url_key("https://example.com/path/to/file")
    assert key == UrlKey("https", "example.com")


def test_get_url_key_no_scheme_raises():
    """URL without scheme raises ValueError."""
    with pytest.raises(ValueError, match="Urls are expected to contain a scheme"):
        get_url_key("bucket/path")


# =============================================================================
# PathEntry tests
# =============================================================================


def test_path_entry_lookup_root():
    """Lookup returns root store with depth 0."""
    entry = PathEntry()
    entry.store = MemoryStore()

    result = entry.lookup("/some/path")
    assert result is not None
    store, depth = result
    assert store is entry.store
    assert depth == 0


def test_path_entry_lookup_nested():
    """Lookup finds nested store with correct depth."""
    root = PathEntry()
    root.children["foo"] = PathEntry()
    root.children["foo"].children["bar"] = PathEntry()
    root.children["foo"].children["bar"].store = MemoryStore()

    result = root.lookup("/foo/bar/baz")
    assert result is not None
    store, depth = result
    assert store is root.children["foo"].children["bar"].store
    assert depth == 2


def test_path_entry_lookup_longest_match():
    """Lookup returns deepest matching store."""
    root = PathEntry()
    root.store = MemoryStore()
    root.children["foo"] = PathEntry()
    root.children["foo"].store = MemoryStore()
    root.children["foo"].children["bar"] = PathEntry()
    root.children["foo"].children["bar"].store = MemoryStore()

    result = root.lookup("/foo/bar/baz")
    assert result is not None
    store, depth = result
    assert store is root.children["foo"].children["bar"].store
    assert depth == 2


def test_path_entry_lookup_no_match():
    """Lookup returns None when no store found."""
    root = PathEntry()
    root.children["foo"] = PathEntry()

    result = root.lookup("/bar/baz")
    assert result is None


def test_path_entry_iter_stores():
    """iter_stores yields all stores in tree."""
    root = PathEntry()
    store1 = MemoryStore()
    store2 = MemoryStore()
    store3 = MemoryStore()

    root.store = store1
    root.children["foo"] = PathEntry()
    root.children["foo"].store = store2
    root.children["bar"] = PathEntry()
    root.children["bar"].store = store3

    stores = list(root.iter_stores())
    assert len(stores) == 3
    assert store1 in stores
    assert store2 in stores
    assert store3 in stores


# =============================================================================
# Nested path tests
# =============================================================================


def test_registry_nested_paths():
    """Registry resolves to correct store based on path."""
    store_root = MemoryStore()
    store_foo = MemoryStore()
    store_foo_bar = MemoryStore()

    registry = ObjectStoreRegistry()
    registry.register("s3://bucket", store_root)
    registry.register("s3://bucket/foo", store_foo)
    registry.register("s3://bucket/foo/bar", store_foo_bar)

    ret1, path1 = registry.resolve("s3://bucket/other/file.txt")
    assert ret1 is store_root
    assert path1 == "other/file.txt"

    ret2, path2 = registry.resolve("s3://bucket/foo/file.txt")
    assert ret2 is store_foo
    assert path2 == "foo/file.txt"

    ret3, path3 = registry.resolve("s3://bucket/foo/bar/file.txt")
    assert ret3 is store_foo_bar
    assert path3 == "foo/bar/file.txt"


def test_registry_longest_match():
    """Registry uses longest path match."""
    store_foo = MemoryStore()
    store_foo_bar = MemoryStore()

    registry = ObjectStoreRegistry()
    registry.register("s3://bucket/foo", store_foo)
    registry.register("s3://bucket/foo/bar", store_foo_bar)

    # /foo/bar/baz should match /foo/bar, not /foo
    ret, path = registry.resolve("s3://bucket/foo/bar/baz")
    assert ret is store_foo_bar


def test_registry_partial_segment_no_match():
    """Partial path segment doesn't match."""
    store_foo = MemoryStore()

    registry = ObjectStoreRegistry()
    registry.register("s3://bucket/foo", store_foo)

    # /foobar should NOT match /foo (partial segment match)
    with pytest.raises(ValueError, match="Could not find an ObjectStore"):
        registry.resolve("s3://bucket/foobar/file.txt")


# =============================================================================
# Store prefix handling tests
# =============================================================================


class MockStoreWithPrefix:
    """Mock store with a prefix attribute."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    def get(self, path, *, options=None):
        pass

    async def get_async(self, path, *, options=None):
        pass

    def get_range(self, path, *, start, end=None, length=None):
        pass

    async def get_range_async(self, path, *, start, end=None, length=None):
        pass

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        pass

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        pass


class MockStoreWithUrl:
    """Mock store with a url attribute."""

    def __init__(self, url: str):
        self.url = url

    def get(self, path, *, options=None):
        pass

    async def get_async(self, path, *, options=None):
        pass

    def get_range(self, path, *, start, end=None, length=None):
        pass

    async def get_range_async(self, path, *, start, end=None, length=None):
        pass

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        pass

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        pass


def test_resolve_with_store_prefix():
    """Store with .prefix attr strips prefix from path."""
    store = MockStoreWithPrefix("data/prefix")

    registry = ObjectStoreRegistry({"s3://bucket": store})

    ret, path = registry.resolve("s3://bucket/data/prefix/file.txt")
    assert ret is store
    assert path == "file.txt"


def test_resolve_with_store_url():
    """Store with .url attr strips URL path from path."""
    store = MockStoreWithUrl("https://example.com/data/prefix")

    registry = ObjectStoreRegistry({"https://example.com": store})

    ret, path = registry.resolve("https://example.com/data/prefix/file.txt")
    assert ret is store
    assert path == "file.txt"


def test_resolve_without_prefix_attrs():
    """Store without prefix attrs returns full path."""
    store = MemoryStore()

    registry = ObjectStoreRegistry({"s3://bucket": store})

    ret, path = registry.resolve("s3://bucket/full/path/file.txt")
    assert ret is store
    assert path == "full/path/file.txt"


def test_register_replaces_existing():
    """Registering at the same URL replaces the store."""
    store1 = MemoryStore()
    store2 = MemoryStore()

    registry = ObjectStoreRegistry()
    registry.register("s3://bucket", store1)
    registry.register("s3://bucket", store2)

    ret, _ = registry.resolve("s3://bucket/file.txt")
    assert ret is store2
    assert ret is not store1


# =============================================================================
# Async context manager tests
# =============================================================================


class MockAsyncContextStore:
    """Mock store with async context manager."""

    def __init__(self):
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exited = True

    def get(self, path, *, options=None):
        pass

    async def get_async(self, path, *, options=None):
        pass

    def get_range(self, path, *, start, end=None, length=None):
        pass

    async def get_range_async(self, path, *, start, end=None, length=None):
        pass

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        pass

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        pass


@pytest.mark.asyncio
async def test_registry_async_context_manager():
    """Registry calls __aenter__/__aexit__ on stores that support it."""
    store = MockAsyncContextStore()

    registry = ObjectStoreRegistry({"s3://bucket": store})

    assert not store.entered
    assert not store.exited

    async with registry:
        assert store.entered
        assert not store.exited

    assert store.exited


@pytest.mark.asyncio
async def test_registry_async_context_manager_mixed_stores():
    """Registry handles stores with and without async context manager."""
    async_store = MockAsyncContextStore()
    regular_store = MemoryStore()

    registry = ObjectStoreRegistry(
        {
            "s3://bucket1": async_store,
            "s3://bucket2": regular_store,
        }
    )

    async with registry:
        assert async_store.entered

    assert async_store.exited


# =============================================================================
# _iter_stores tests
# =============================================================================


def test_iter_stores_empty():
    """Empty registry yields nothing."""
    registry = ObjectStoreRegistry()
    stores = list(registry._iter_stores())
    assert stores == []


def test_iter_stores_multiple():
    """_iter_stores returns all registered stores."""
    store1 = MemoryStore()
    store2 = MemoryStore()
    store3 = MemoryStore()

    registry = ObjectStoreRegistry(
        {
            "s3://bucket1": store1,
            "s3://bucket2": store2,
            "https://example.com": store3,
        }
    )

    stores = list(registry._iter_stores())
    assert len(stores) == 3
    assert store1 in stores
    assert store2 in stores
    assert store3 in stores
