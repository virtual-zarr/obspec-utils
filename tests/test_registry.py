import pytest
from obstore.store import MemoryStore

from obspec_utils.registry import ObjectStoreRegistry
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


def test_eager_reader_with_chunk_size_and_file_size():
    """Test EagerStoreReader uses get_ranges when chunk_size and file_size provided."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (16 bytes)
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithoutHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with chunk_size and file_size
    reader = EagerStoreReader(
        traced_store, "test.txt", chunk_size=4, file_size=len(data)
    )

    # Verify the data is correct
    assert reader.read() == data

    # Verify get_ranges was used (not get)
    summary = trace.summary()
    assert summary["total_requests"] == 4  # 16 bytes / 4 byte chunks = 4 requests
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert summary["total_bytes"] == len(data)


def test_eager_reader_with_chunk_size_uses_head():
    """Test EagerStoreReader uses head() to get file size when available."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (16 bytes)
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with chunk_size but no file_size
    # Store has head() method so it should be used
    reader = EagerStoreReader(traced_store, "test.txt", chunk_size=4)

    # Verify the data is correct
    assert reader.read() == data

    # Verify get_ranges was used (head() call isn't traced, only data requests)
    summary = trace.summary()
    assert summary["total_requests"] == 4  # 16 bytes / 4 byte chunks
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

    # Create reader with chunk_size but no file_size and no head()
    # Should fall back to single get() request
    reader = EagerStoreReader(traced_store, "test.txt", chunk_size=4)

    # Verify the data is correct
    assert reader.read() == data

    # Verify single get() was used (fallback)
    summary = trace.summary()
    assert summary["total_requests"] == 1
    assert trace.requests[0].method == "get"
    assert summary["total_bytes"] == len(data)


def test_eager_reader_no_chunk_size():
    """Test EagerStoreReader uses single get() when no chunk_size specified."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader without chunk_size
    reader = EagerStoreReader(traced_store, "test.txt")

    # Verify the data is correct
    assert reader.read() == data

    # Verify single get() was used
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

    # Create reader with chunk_size and file_size=0
    reader = EagerStoreReader(traced_store, "test.txt", chunk_size=4, file_size=0)

    # Verify the data is empty
    assert reader.read() == b""

    # No requests should be made for empty file
    assert trace.total_requests == 0


def test_eager_reader_chunk_boundaries():
    """Test EagerStoreReader handles non-aligned chunk boundaries."""
    from obspec_utils.tracing import TracingReadableStore, RequestTrace

    # Create test data (10 bytes, not evenly divisible by chunk_size=4)
    data = b"0123456789"
    mock_store = MockReadableStoreWithHead(data)

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    # Create reader with chunk_size=4, file_size=10
    reader = EagerStoreReader(
        traced_store, "test.txt", chunk_size=4, file_size=len(data)
    )

    # Verify the data is correct
    assert reader.read() == data

    # Should be 3 chunks: 0-3 (4 bytes), 4-7 (4 bytes), 8-9 (2 bytes)
    summary = trace.summary()
    assert summary["total_requests"] == 3
    assert summary["total_bytes"] == len(data)

    # Verify chunk sizes
    lengths = [r.length for r in trace.requests]
    assert lengths == [4, 4, 2]


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
