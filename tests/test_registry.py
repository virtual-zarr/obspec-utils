from io import BytesIO
import pytest
from obstore.store import MemoryStore

from obspec_utils.registry import (
    ObjectStoreRegistry,
    PathEntry,
    UrlKey,
    get_url_key,
    path_segments,
)
from obspec_utils.obspec import (
    ReadableStore,
    BufferedStoreReader,
    EagerStoreReader,
    ParallelStoreReader,
)

ALL_READERS = [BufferedStoreReader, EagerStoreReader, ParallelStoreReader]


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
        ret, path = registry.register(url, MemoryStore())


def test_resolve_raises():
    registry = ObjectStoreRegistry()
    with pytest.raises(
        ValueError,
        match="Could not find an ObjectStore matching the url `s3://bucket1/path/to/object`",
    ):
        url = "s3://bucket1/path/to/object"
        ret, path = registry.resolve(url)


def test_obstore_satisfies_readable_store_protocol():
    """Verify that obstore classes satisfy the ReadableStore protocol."""
    memstore = MemoryStore()
    # Runtime check using isinstance with @runtime_checkable protocol
    assert isinstance(memstore, ReadableStore)


def test_registry_with_custom_readable_store():
    """Test that registry works with any ReadableStore protocol implementation."""
    from collections.abc import Sequence

    class MockReadableStore:
        """A minimal mock that satisfies the ReadableStore protocol."""

        def __init__(self, data: bytes = b"test data"):
            self._data = data

        def get(self, path, *, options=None):
            # Return a mock GetResult
            return MockGetResult(self._data)

        async def get_async(self, path, *, options=None):
            return MockGetResultAsync(self._data)

        def get_range(self, path, *, start, end=None, length=None):
            if end is None:
                end = start + length
            return self._data[start:end]

        async def get_range_async(self, path, *, start, end=None, length=None):
            if end is None:
                end = start + length
            return self._data[start:end]

        def get_ranges(
            self, path, *, starts, ends=None, lengths=None
        ) -> Sequence[bytes]:
            if ends is None:
                ends = [s + ln for s, ln in zip(starts, lengths)]
            return [self._data[s:e] for s, e in zip(starts, ends)]

        async def get_ranges_async(
            self, path, *, starts, ends=None, lengths=None
        ) -> Sequence[bytes]:
            if ends is None:
                ends = [s + ln for s, ln in zip(starts, lengths)]
            return [self._data[s:e] for s, e in zip(starts, ends)]

    class MockGetResult:
        def __init__(self, data):
            self._data = data

        @property
        def attributes(self):
            return {}

        def buffer(self):
            return self._data

        @property
        def meta(self):
            return {
                "path": "",
                "last_modified": None,
                "size": len(self._data),
                "e_tag": None,
                "version": None,
            }

        @property
        def range(self):
            return (0, len(self._data))

        def __iter__(self):
            yield self._data

    class MockGetResultAsync:
        def __init__(self, data):
            self._data = data

        @property
        def attributes(self):
            return {}

        async def buffer_async(self):
            return self._data

        @property
        def meta(self):
            return {
                "path": "",
                "last_modified": None,
                "size": len(self._data),
                "e_tag": None,
                "version": None,
            }

        @property
        def range(self):
            return (0, len(self._data))

        async def __aiter__(self):
            yield self._data

    # Create a mock store and register it
    mock_store = MockReadableStore(b"hello world")
    registry = ObjectStoreRegistry({"https://example.com": mock_store})

    # Resolve and use the store
    store, path = registry.resolve("https://example.com/data/file.txt")
    assert store is mock_store
    assert path == "data/file.txt"

    # Verify the protocol methods work
    assert store.get_range(path, start=0, end=5) == b"hello"
    assert store.get_range(path, start=6, length=5) == b"world"


@pytest.mark.asyncio
async def test_registry_with_async_operations():
    """Test async operations with registry."""
    memstore = MemoryStore()
    # Put some test data
    memstore.put("test.txt", b"async test data")

    registry = ObjectStoreRegistry({"mem://test": memstore})
    store, path = registry.resolve("mem://test/test.txt")

    # Test async get_range
    result = await store.get_range_async(path, start=0, end=5)
    assert bytes(result) == b"async"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_basic_operations(ReaderClass):
    """Test basic read, seek, tell operations for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world from store reader")

    reader = ReaderClass(memstore, "test.txt")

    # Test read
    assert reader.read(5) == b"hello"
    assert reader.tell() == 5

    # Test seek and read
    reader.seek(6)
    assert reader.read(5) == b"world"

    # Test seek from current (SEEK_CUR)
    reader.seek(-5, 1)
    assert reader.read(5) == b"world"

    # Test readall
    reader.seek(0)
    assert reader.readall() == b"hello world from store reader"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_end(ReaderClass):
    """Test SEEK_END functionality for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789")

    reader = ReaderClass(memstore, "test.txt")

    # Seek to 2 bytes before end
    reader.seek(-2, 2)  # SEEK_END
    assert reader.read(2) == b"89"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_all_seek_modes(ReaderClass):
    """Test all seek modes for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ReaderClass(memstore, "test.txt")

    # SEEK_SET
    reader.seek(5)
    assert reader.tell() == 5
    assert reader.read(3) == b"567"

    # SEEK_CUR
    reader.seek(-3, 1)
    assert reader.tell() == 5
    assert reader.read(3) == b"567"

    # SEEK_END
    reader.seek(-4, 2)
    assert reader.read(4) == b"CDEF"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_past_end(ReaderClass):
    """Test reading past end of file for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"short")

    reader = ReaderClass(memstore, "test.txt")

    # For EagerStoreReader, read() returns what's available
    # For others, they clamp to file size
    data = reader.read(100)
    assert data == b"short"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_minus_one(ReaderClass):
    """Test read(-1) reads entire file for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    assert reader.read(-1) == b"hello world"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_minus_one_from_middle(ReaderClass):
    """Test read(-1) reads from current position to end."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    reader.seek(6)
    assert reader.read(-1) == b"world"


def test_buffered_reader_buffering():
    """Test that BufferedStoreReader buffering works correctly."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    # Small buffer size to test buffering behavior
    reader = BufferedStoreReader(memstore, "test.txt", buffer_size=8)

    # First read should fetch buffer_size bytes
    assert reader.read(2) == b"01"
    # Second read should come from buffer
    assert reader.read(2) == b"23"


def test_parallel_reader_cross_chunk_read():
    """Test ParallelStoreReader reading across chunk boundaries."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    # Small chunk size to test cross-chunk reads
    reader = ParallelStoreReader(memstore, "test.txt", chunk_size=4)

    # Read across chunk boundary (chunks are 0-3, 4-7, 8-11, 12-15)
    reader.seek(2)
    assert reader.read(6) == b"234567"  # Spans chunks 0 and 1

    # Read spanning multiple chunks
    reader.seek(0)
    assert reader.read(10) == b"0123456789"  # Spans chunks 0, 1, and 2


def test_parallel_reader_caching():
    """Test that ParallelStoreReader chunks are cached correctly."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=2
    )

    # Read first chunk
    reader.seek(0)
    assert reader.read(4) == b"0123"

    # Read second chunk
    reader.seek(4)
    assert reader.read(4) == b"4567"

    # Read third chunk - should evict first chunk from cache
    reader.seek(8)
    assert reader.read(4) == b"89AB"

    # Reading first chunk again should still work (refetched)
    reader.seek(0)
    assert reader.read(4) == b"0123"


# --- EagerStoreReader tests with TracingReadableStore ---


class MockReadableStoreWithHead:
    """A mock store that supports the Head protocol."""

    def __init__(self, data: bytes = b"test data"):
        self._data = data

    def head(self, path):
        """Return metadata including file size."""
        return {
            "path": path,
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    def get(self, path, *, options=None):
        from tests.test_registry import _MockGetResult

        return _MockGetResult(self._data)

    async def get_async(self, path, *, options=None):
        from tests.test_registry import _MockGetResultAsync

        return _MockGetResultAsync(self._data)

    def get_range(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    async def get_range_async(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]


class MockReadableStoreWithoutHead:
    """A mock store without the Head protocol."""

    def __init__(self, data: bytes = b"test data"):
        self._data = data

    def get(self, path, *, options=None):
        return _MockGetResult(self._data)

    async def get_async(self, path, *, options=None):
        return _MockGetResultAsync(self._data)

    def get_range(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    async def get_range_async(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]


class _MockGetResult:
    def __init__(self, data):
        self._data = data

    @property
    def attributes(self):
        return {}

    def buffer(self):
        return self._data

    @property
    def meta(self):
        return {
            "path": "",
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    @property
    def range(self):
        return (0, len(self._data))

    def __iter__(self):
        yield self._data


class _MockGetResultAsync:
    def __init__(self, data):
        self._data = data

    @property
    def attributes(self):
        return {}

    async def buffer_async(self):
        return self._data

    @property
    def meta(self):
        return {
            "path": "",
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    @property
    def range(self):
        return (0, len(self._data))

    async def __aiter__(self):
        yield self._data


def test_eager_reader_with_request_size_and_file_size():
    """Test EagerStoreReader uses get_ranges when request_size and file_size provided."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (16 bytes)
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithoutHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with request_size and file_size
    reader = EagerStoreReader(
        traced_store, "test.txt", request_size=4, file_size=len(data)
    )

    # Verify the data is correct
    assert reader.read() == data

    # Verify get_ranges was used (not get)
    summary = trace.summary()
    assert summary["total_requests"] == 4  # 16 bytes / 4 byte requests = 4 requests
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert summary["total_bytes"] == len(data)


def test_eager_reader_uses_head():
    """Test EagerStoreReader uses head() to get file size when available."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (16 bytes)
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with request_size but no file_size
    # Store has head() method so it should be used
    reader = EagerStoreReader(traced_store, "test.txt", request_size=4)

    # Verify the data is correct
    assert reader.read() == data

    # Verify get_ranges was used (head() call isn't traced, only data requests)
    summary = trace.summary()
    assert summary["total_requests"] == 4  # 16 bytes / 4 byte requests
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert summary["total_bytes"] == len(data)


def test_eager_reader_falls_back_to_single_get():
    """Test EagerStoreReader falls back to get() when head not available."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithoutHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader without file_size and no head()
    # Should fall back to single get() request
    reader = EagerStoreReader(traced_store, "test.txt", request_size=4)

    # Verify the data is correct
    assert reader.read() == data

    # Verify single get() was used (fallback)
    summary = trace.summary()
    assert summary["total_requests"] == 1
    assert trace.requests[0].method == "get"
    assert summary["total_bytes"] == len(data)


def test_eager_reader_small_file_uses_single_get():
    """Test EagerStoreReader uses single get() when file fits in one request."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data smaller than default request_size (12 MB)
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with default settings - file is smaller than request_size
    reader = EagerStoreReader(traced_store, "test.txt")

    # Verify the data is correct
    assert reader.read() == data

    # Verify single get() was used (skips concurrency overhead)
    summary = trace.summary()
    assert summary["total_requests"] == 1
    assert trace.requests[0].method == "get"


def test_eager_reader_empty_file():
    """Test EagerStoreReader handles empty file correctly."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create empty data
    data = b""
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with file_size=0
    reader = EagerStoreReader(traced_store, "test.txt", request_size=4, file_size=0)

    # Verify the data is empty
    assert reader.read() == b""

    # No requests should be made for empty file
    assert trace.total_requests == 0


def test_eager_reader_request_boundaries():
    """Test EagerStoreReader handles non-aligned request boundaries."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (10 bytes, not evenly divisible by request_size=4)
    data = b"0123456789"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with request_size=4, file_size=10
    reader = EagerStoreReader(
        traced_store, "test.txt", request_size=4, file_size=len(data)
    )

    # Verify the data is correct
    assert reader.read() == data

    # Should be 3 requests: 0-3 (4 bytes), 4-7 (4 bytes), 8-9 (2 bytes)
    summary = trace.summary()
    assert summary["total_requests"] == 3
    assert summary["total_bytes"] == len(data)

    # Verify request sizes
    lengths = [r.length for r in trace.requests]
    assert lengths == [4, 4, 2]


def test_eager_reader_max_concurrent_requests():
    """Test EagerStoreReader caps requests at max_concurrent_requests."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (100 bytes)
    data = b"x" * 100
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # With request_size=10, would need 10 requests
    # But max_concurrent_requests=4, so should redistribute to 4 requests
    reader = EagerStoreReader(
        traced_store,
        "test.txt",
        request_size=10,
        file_size=len(data),
        max_concurrent_requests=4,
    )

    # Verify the data is correct
    assert reader.read() == data

    # Should be capped at 4 requests
    summary = trace.summary()
    assert summary["total_requests"] == 4
    assert summary["total_bytes"] == len(data)


def test_eager_reader_redistribution_even_split():
    """Test EagerStoreReader redistributes evenly when capping requests."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (100 bytes)
    data = b"x" * 100
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # With request_size=10, would need 10 requests
    # With max_concurrent_requests=4, should get 4 requests of 25 bytes each
    reader = EagerStoreReader(
        traced_store,
        "test.txt",
        request_size=10,
        file_size=len(data),
        max_concurrent_requests=4,
    )

    assert reader.read() == data

    # Verify redistributed request sizes (25, 25, 25, 25)
    lengths = [r.length for r in trace.requests]
    assert lengths == [25, 25, 25, 25]


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_context_manager(ReaderClass):
    """Test that readers work as context managers and release resources."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    with ReaderClass(memstore, "test.txt") as reader:
        assert reader.read(5) == b"hello"
        assert reader.tell() == 5

    # After exiting context, internal buffers should be cleared
    if hasattr(reader, "_buffer"):
        if isinstance(reader._buffer, bytes):
            assert reader._buffer == b""
        else:
            # BytesIO - check it's empty
            assert reader._buffer.getvalue() == b""
    if hasattr(reader, "_cache"):
        assert len(reader._cache) == 0


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_close(ReaderClass):
    """Test that readers can be explicitly closed."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    assert reader.read(5) == b"hello"

    reader.close()

    # After close, internal buffers should be cleared
    if hasattr(reader, "_buffer"):
        if isinstance(reader._buffer, bytes):
            assert reader._buffer == b""
        else:
            assert reader._buffer.getvalue() == b""
    if hasattr(reader, "_cache"):
        assert len(reader._cache) == 0


# --- path_segments Function Tests ---


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


# --- get_url_key Function Tests ---


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


# --- PathEntry Tests ---


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
    root.store = MemoryStore()  # store1 at root
    root.children["foo"] = PathEntry()
    root.children["foo"].store = MemoryStore()  # store2 at /foo
    root.children["foo"].children["bar"] = PathEntry()
    root.children["foo"].children["bar"].store = MemoryStore()  # store3 at /foo/bar

    # Lookup /foo/bar/baz should return store3 at depth 2
    result = root.lookup("/foo/bar/baz")
    assert result is not None
    store, depth = result
    assert store is root.children["foo"].children["bar"].store
    assert depth == 2


def test_path_entry_lookup_no_match():
    """Lookup returns None when no store found."""
    root = PathEntry()
    # No store at root, only children
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


# --- Registry Nested Path Tests ---


def test_registry_nested_paths():
    """Registry resolves to correct store based on path."""
    store_root = MemoryStore()
    store_foo = MemoryStore()
    store_foo_bar = MemoryStore()

    registry = ObjectStoreRegistry()
    registry.register("s3://bucket", store_root)
    registry.register("s3://bucket/foo", store_foo)
    registry.register("s3://bucket/foo/bar", store_foo_bar)

    # Resolve paths
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


# --- Store Prefix Handling Tests ---


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


# --- Registry Replace Store Test ---


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


# --- Async Context Manager Tests ---


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
    regular_store = MemoryStore()  # Doesn't have __aenter__/__aexit__

    registry = ObjectStoreRegistry(
        {
            "s3://bucket1": async_store,
            "s3://bucket2": regular_store,
        }
    )

    async with registry:
        assert async_store.entered

    assert async_store.exited


# --- _iter_stores Method Tests ---


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


# --- BytesIO Consistency Tests ---
# These tests verify that readers behave consistently with Python's BytesIO


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_matches_bytesio(ReaderClass):
    """Reader read(n) matches BytesIO behavior."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(5) == ref.read(5)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_zero_matches_bytesio(ReaderClass):
    """Reader read(0) returns empty bytes like BytesIO."""
    data = b"hello world"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(0) == ref.read(0)
    assert reader.read(0) == b""
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_all_matches_bytesio(ReaderClass):
    """Reader read(-1) matches BytesIO.read(-1)."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(-1) == ref.read(-1)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_no_arg_matches_bytesio(ReaderClass):
    """Reader read() with no argument matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_sequential_reads_match_bytesio(ReaderClass):
    """Multiple consecutive reads match BytesIO behavior."""
    data = b"0123456789ABCDEF"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    for _ in range(4):
        assert reader.read(4) == ref.read(4)
        assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_set_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_SET) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(5) == ref.seek(5)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_cur_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_CUR) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    # Move forward first
    reader.read(5)
    ref.read(5)

    # Then seek relative
    assert reader.seek(3, 1) == ref.seek(3, 1)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_end_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_END) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(-5, 2) == ref.seek(-5, 2)
    assert reader.tell() == ref.tell()
    assert reader.read() == ref.read()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_returns_position_matches_bytesio(ReaderClass):
    """Reader seek() return value matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(10) == ref.seek(10)
    assert reader.seek(5, 1) == ref.seek(5, 1)
    assert reader.seek(-3, 2) == ref.seek(-3, 2)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_tell_matches_bytesio(ReaderClass):
    """Reader tell() matches BytesIO after various operations."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.tell() == ref.tell()
    reader.read(5)
    ref.read(5)
    assert reader.tell() == ref.tell()
    reader.seek(10)
    ref.seek(10)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_past_eof_matches_bytesio(ReaderClass):
    """Reading past EOF matches BytesIO behavior."""
    data = b"short"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(100) == ref.read(100)
    assert reader.tell() == ref.tell()
    # Reading again at EOF should return empty
    assert reader.read(10) == ref.read(10)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_negative_cur_matches_bytesio(ReaderClass):
    """Reader seek(-n, SEEK_CUR) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    # Move forward first
    reader.read(10)
    ref.read(10)

    # Then seek backward
    assert reader.seek(-5, 1) == ref.seek(-5, 1)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_empty_file_matches_bytesio(ReaderClass):
    """Empty file behavior matches BytesIO."""
    data = b""

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()
    assert reader.read(10) == ref.read(10)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_read_sequence_matches_bytesio(ReaderClass):
    """Interleaved seek/read operations match BytesIO."""
    data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    # Complex sequence of operations
    assert reader.read(10) == ref.read(10)
    assert reader.seek(5) == ref.seek(5)
    assert reader.read(5) == ref.read(5)
    assert reader.seek(-3, 1) == ref.seek(-3, 1)
    assert reader.read(10) == ref.read(10)
    assert reader.seek(-5, 2) == ref.seek(-5, 2)
    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_invalid_whence_raises(ReaderClass):
    """Reader raises ValueError for invalid whence like BytesIO."""
    data = b"hello world"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    # Verify BytesIO raises ValueError for invalid whence
    with pytest.raises(ValueError):
        ref.seek(0, 3)

    # Reader should match
    with pytest.raises(ValueError):
        reader.seek(0, 3)
