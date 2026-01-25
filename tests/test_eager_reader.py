"""Tests specific to EagerStoreReader."""

from obspec_utils.obspec import EagerStoreReader
from obspec_utils.tracing import TracingReadableStore, RequestTrace

from .mocks import MockReadableStoreWithHead, MockReadableStoreWithoutHead


def test_eager_reader_with_request_size_and_file_size():
    """Test EagerStoreReader uses get_ranges when request_size and file_size provided."""
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithoutHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(
        traced_store, "test.txt", request_size=4, file_size=len(data)
    )

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 4
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert summary["total_bytes"] == len(data)


def test_eager_reader_uses_head():
    """Test EagerStoreReader uses head() to get file size when available."""
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(traced_store, "test.txt", request_size=4)

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 4
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert summary["total_bytes"] == len(data)


def test_eager_reader_falls_back_to_single_get():
    """Test EagerStoreReader falls back to get() when head not available."""
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithoutHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(traced_store, "test.txt", request_size=4)

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 1
    assert trace.requests[0].method == "get"
    assert summary["total_bytes"] == len(data)


def test_eager_reader_small_file_uses_single_get():
    """Test EagerStoreReader uses single get() when file fits in one request."""
    data = b"0123456789ABCDEF"
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(traced_store, "test.txt")

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 1
    assert trace.requests[0].method == "get"


def test_eager_reader_empty_file():
    """Test EagerStoreReader handles empty file correctly."""
    data = b""
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(traced_store, "test.txt", request_size=4, file_size=0)

    assert reader.read() == b""
    assert trace.total_requests == 0


def test_eager_reader_request_boundaries():
    """Test EagerStoreReader handles non-aligned request boundaries."""
    data = b"0123456789"
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(
        traced_store, "test.txt", request_size=4, file_size=len(data)
    )

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 3
    assert summary["total_bytes"] == len(data)

    lengths = [r.length for r in trace.requests]
    assert lengths == [4, 4, 2]


def test_eager_reader_max_concurrent_requests():
    """Test EagerStoreReader caps requests at max_concurrent_requests."""
    data = b"x" * 100
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(
        traced_store,
        "test.txt",
        request_size=10,
        file_size=len(data),
        max_concurrent_requests=4,
    )

    assert reader.read() == data

    summary = trace.summary()
    assert summary["total_requests"] == 4
    assert summary["total_bytes"] == len(data)


def test_eager_reader_redistribution_even_split():
    """Test EagerStoreReader redistributes evenly when capping requests."""
    data = b"x" * 100
    mock_store = MockReadableStoreWithHead(data)

    trace = RequestTrace()
    traced_store = TracingReadableStore(mock_store, trace)

    reader = EagerStoreReader(
        traced_store,
        "test.txt",
        request_size=10,
        file_size=len(data),
        max_concurrent_requests=4,
    )

    assert reader.read() == data

    lengths = [r.length for r in trace.requests]
    assert lengths == [25, 25, 25, 25]
