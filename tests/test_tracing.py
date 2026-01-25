"""Tests for TracingReadableStore, RequestTrace, and RequestRecord."""

import time

import pytest

from obspec_utils.wrappers import RequestRecord, RequestTrace, TracingReadableStore

from .mocks import MockReadableStore as MockStore


class FailingStore:
    """A mock store that raises exceptions."""

    def get(self, path, *, options=None):
        raise IOError("Store error")

    async def get_async(self, path, *, options=None):
        raise IOError("Store error")

    def get_range(self, path, *, start, end=None, length=None):
        raise IOError("Store error")

    async def get_range_async(self, path, *, start, end=None, length=None):
        raise IOError("Store error")

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        raise IOError("Store error")

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        raise IOError("Store error")

    def head(self, path):
        raise IOError("Store error")

    async def head_async(self, path):
        raise IOError("Store error")


# --- RequestRecord Tests ---


def test_request_record_fields():
    """Verify all fields are set correctly."""
    record = RequestRecord(
        path="test.txt",
        start=100,
        length=50,
        end=150,
        timestamp=1234567890.0,
        duration=0.5,
        method="get_range",
        range_style="length",
    )

    assert record.path == "test.txt"
    assert record.start == 100
    assert record.length == 50
    assert record.end == 150
    assert record.timestamp == 1234567890.0
    assert record.duration == 0.5
    assert record.method == "get_range"
    assert record.range_style == "length"


def test_request_record_end_computed():
    """Verify end = start + length when created via RequestTrace.add()."""
    trace = RequestTrace()
    trace.add(
        path="test.txt",
        start=100,
        length=50,
        timestamp=time.time(),
    )

    record = trace.requests[0]
    assert record.end == record.start + record.length
    assert record.end == 150


# --- RequestTrace Tests ---


def test_trace_add():
    """Adding records works correctly."""
    trace = RequestTrace()
    trace.add(
        path="file1.txt",
        start=0,
        length=100,
        timestamp=1000.0,
        duration=0.1,
        method="get_range",
        range_style="length",
    )
    trace.add(
        path="file2.txt",
        start=50,
        length=200,
        timestamp=1001.0,
        duration=0.2,
        method="get_ranges",
        range_style="end",
    )

    assert len(trace.requests) == 2
    assert trace.requests[0].path == "file1.txt"
    assert trace.requests[1].path == "file2.txt"


def test_trace_clear():
    """clear() removes all requests."""
    trace = RequestTrace()
    trace.add(path="test.txt", start=0, length=100, timestamp=time.time())
    trace.add(path="test.txt", start=100, length=100, timestamp=time.time())

    assert len(trace.requests) == 2
    trace.clear()
    assert len(trace.requests) == 0


def test_trace_total_bytes():
    """Property sums lengths correctly."""
    trace = RequestTrace()
    trace.add(path="test.txt", start=0, length=100, timestamp=time.time())
    trace.add(path="test.txt", start=100, length=200, timestamp=time.time())
    trace.add(path="test.txt", start=300, length=50, timestamp=time.time())

    assert trace.total_bytes == 350


def test_trace_total_requests():
    """Property counts requests."""
    trace = RequestTrace()
    assert trace.total_requests == 0

    trace.add(path="test.txt", start=0, length=100, timestamp=time.time())
    assert trace.total_requests == 1

    trace.add(path="test.txt", start=100, length=100, timestamp=time.time())
    assert trace.total_requests == 2


def test_trace_summary_empty():
    """Summary for empty trace."""
    trace = RequestTrace()
    summary = trace.summary()

    assert summary["total_requests"] == 0
    assert summary["total_bytes"] == 0
    assert summary["unique_files"] == 0
    assert "min_request_size" not in summary
    assert "max_request_size" not in summary


def test_trace_summary_with_requests():
    """Summary statistics are computed correctly."""
    trace = RequestTrace()
    trace.add(path="file1.txt", start=0, length=100, timestamp=time.time())
    trace.add(path="file1.txt", start=100, length=200, timestamp=time.time())
    trace.add(path="file2.txt", start=0, length=50, timestamp=time.time())

    summary = trace.summary()

    assert summary["total_requests"] == 3
    assert summary["total_bytes"] == 350
    assert summary["unique_files"] == 2
    assert summary["min_request_size"] == 50
    assert summary["max_request_size"] == 200
    assert summary["mean_request_size"] == 350 / 3


def test_trace_to_dataframe_empty():
    """Returns DataFrame with correct columns when empty."""
    pytest.importorskip("pandas")
    trace = RequestTrace()
    df = trace.to_dataframe()

    expected_columns = [
        "path",
        "start",
        "length",
        "end",
        "timestamp",
        "duration",
        "method",
        "range_style",
    ]
    assert list(df.columns) == expected_columns
    assert len(df) == 0


def test_trace_to_dataframe():
    """Returns DataFrame with all request data."""
    pytest.importorskip("pandas")
    trace = RequestTrace()
    trace.add(
        path="test.txt",
        start=0,
        length=100,
        timestamp=1000.0,
        duration=0.1,
        method="get_range",
        range_style="length",
    )

    df = trace.to_dataframe()

    assert len(df) == 1
    assert df.iloc[0]["path"] == "test.txt"
    assert df.iloc[0]["start"] == 0
    assert df.iloc[0]["length"] == 100
    assert df.iloc[0]["end"] == 100
    assert df.iloc[0]["timestamp"] == 1000.0
    assert df.iloc[0]["duration"] == 0.1
    assert df.iloc[0]["method"] == "get_range"
    assert df.iloc[0]["range_style"] == "length"


# --- TracingReadableStore - Sync Methods ---


def test_tracing_get():
    """Records path, method='get', start=0, length=file_size."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = traced.get("test.txt")
    assert result.buffer() == b"hello world"

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "get"
    assert record.start == 0
    assert record.length == 11  # len("hello world")


def test_tracing_get_range_with_length():
    """Records range_style='length'."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = traced.get_range("test.txt", start=0, length=5)
    assert bytes(result) == b"hello"

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "get_range"
    assert record.start == 0
    assert record.length == 5
    assert record.range_style == "length"


def test_tracing_get_range_with_end():
    """Records range_style='end'."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = traced.get_range("test.txt", start=6, end=11)
    assert bytes(result) == b"world"

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "get_range"
    assert record.start == 6
    assert record.length == 5
    assert record.range_style == "end"


def test_tracing_get_range_missing_params():
    """Raises ValueError when neither end nor length provided."""
    mock_store = MockStore()
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    with pytest.raises(ValueError, match="Either 'end' or 'length' must be provided"):
        traced.get_range("test.txt", start=0)


def test_tracing_get_ranges_with_lengths():
    """Creates multiple records, range_style='length'."""
    mock_store = MockStore(b"0123456789")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    results = traced.get_ranges("test.txt", starts=[0, 5], lengths=[3, 3])
    assert [bytes(r) for r in results] == [b"012", b"567"]

    assert len(trace.requests) == 2
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert all(r.range_style == "length" for r in trace.requests)
    assert trace.requests[0].start == 0
    assert trace.requests[0].length == 3
    assert trace.requests[1].start == 5
    assert trace.requests[1].length == 3


def test_tracing_get_ranges_with_ends():
    """Creates multiple records, range_style='end'."""
    mock_store = MockStore(b"0123456789")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    results = traced.get_ranges("test.txt", starts=[0, 5], ends=[3, 8])
    assert [bytes(r) for r in results] == [b"012", b"567"]

    assert len(trace.requests) == 2
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert all(r.range_style == "end" for r in trace.requests)


def test_tracing_get_ranges_missing_params():
    """Raises ValueError when neither ends nor lengths provided."""
    mock_store = MockStore()
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    with pytest.raises(ValueError, match="Either 'ends' or 'lengths' must be provided"):
        traced.get_ranges("test.txt", starts=[0, 5])


# --- TracingReadableStore - Async Methods ---


@pytest.mark.asyncio
async def test_tracing_get_async():
    """Records correctly for async get."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = await traced.get_async("test.txt")
    assert await result.buffer_async() == b"hello world"

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "get"


@pytest.mark.asyncio
async def test_tracing_get_range_async_with_length():
    """Records range_style='length' for async."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = await traced.get_range_async("test.txt", start=0, length=5)
    assert bytes(result) == b"hello"

    assert len(trace.requests) == 1
    assert trace.requests[0].range_style == "length"


@pytest.mark.asyncio
async def test_tracing_get_range_async_with_end():
    """Records range_style='end' for async."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = await traced.get_range_async("test.txt", start=6, end=11)
    assert bytes(result) == b"world"

    assert len(trace.requests) == 1
    assert trace.requests[0].range_style == "end"


@pytest.mark.asyncio
async def test_tracing_get_ranges_async_with_lengths():
    """Creates multiple records for async get_ranges with lengths."""
    mock_store = MockStore(b"0123456789")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    results = await traced.get_ranges_async("test.txt", starts=[0, 5], lengths=[3, 3])
    assert [bytes(r) for r in results] == [b"012", b"567"]

    assert len(trace.requests) == 2
    assert all(r.method == "get_ranges" for r in trace.requests)
    assert all(r.range_style == "length" for r in trace.requests)


@pytest.mark.asyncio
async def test_tracing_get_ranges_async_with_ends():
    """Creates multiple records for async get_ranges with ends."""
    mock_store = MockStore(b"0123456789")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    results = await traced.get_ranges_async("test.txt", starts=[0, 5], ends=[3, 8])
    assert [bytes(r) for r in results] == [b"012", b"567"]

    assert len(trace.requests) == 2
    assert all(r.range_style == "end" for r in trace.requests)


@pytest.mark.asyncio
async def test_tracing_get_range_async_missing_params():
    """Raises ValueError when neither end nor length provided for async."""
    mock_store = MockStore()
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    with pytest.raises(ValueError, match="Either 'end' or 'length' must be provided"):
        await traced.get_range_async("test.txt", start=0)


@pytest.mark.asyncio
async def test_tracing_get_ranges_async_missing_params():
    """Raises ValueError when neither ends nor lengths provided for async."""
    mock_store = MockStore()
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    with pytest.raises(ValueError, match="Either 'ends' or 'lengths' must be provided"):
        await traced.get_ranges_async("test.txt", starts=[0, 5])


# --- TracingReadableStore - Callback & Forwarding ---


def test_on_request_callback():
    """Callback invoked for each request."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    callback_records = []

    def on_request(record):
        callback_records.append(record)

    traced = TracingReadableStore(mock_store, trace, on_request=on_request)

    traced.get_range("test.txt", start=0, length=5)
    traced.get_range("test.txt", start=5, length=6)

    assert len(callback_records) == 2
    assert callback_records[0].start == 0
    assert callback_records[1].start == 5


def test_on_request_callback_for_get_ranges():
    """Callback invoked for each individual range in get_ranges."""
    mock_store = MockStore(b"0123456789")
    trace = RequestTrace()
    callback_records = []

    def on_request(record):
        callback_records.append(record)

    traced = TracingReadableStore(mock_store, trace, on_request=on_request)

    traced.get_ranges("test.txt", starts=[0, 5], lengths=[3, 3])

    # Should be called twice, once for each range
    assert len(callback_records) == 2


def test_tracing_head():
    """Records path, method='head', start=0, length=0."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = traced.head("test.txt")
    assert result["size"] == 11

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "head"
    assert record.start == 0
    assert record.length == 0  # HEAD requests don't transfer data


@pytest.mark.asyncio
async def test_tracing_head_async():
    """Records path, method='head', start=0, length=0 for async."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    result = await traced.head_async("test.txt")
    assert result["size"] == 11

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "head"
    assert record.start == 0
    assert record.length == 0  # HEAD requests don't transfer data


def test_getattr_forwards_to_store():
    """Unknown attributes forwarded to underlying store."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    # Access a non-overridden attribute from the underlying store
    assert traced._data == b"hello world"


# --- TracingReadableStore - Timing & Error Handling ---


def test_duration_recorded():
    """Duration is non-negative float."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    traced.get_range("test.txt", start=0, length=5)

    record = trace.requests[0]
    assert record.duration is not None
    assert record.duration >= 0


def test_timestamp_recorded():
    """Timestamp is recorded and reasonable."""
    mock_store = MockStore(b"hello world")
    trace = RequestTrace()
    traced = TracingReadableStore(mock_store, trace)

    before = time.time()
    traced.get_range("test.txt", start=0, length=5)
    after = time.time()

    record = trace.requests[0]
    assert before <= record.timestamp <= after


def test_records_on_exception():
    """Requests recorded even when store raises."""
    failing_store = FailingStore()
    trace = RequestTrace()
    traced = TracingReadableStore(failing_store, trace)

    with pytest.raises(IOError):
        traced.get_range("test.txt", start=0, length=5)

    # Request should still be recorded
    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "get_range"
    assert record.duration is not None


@pytest.mark.asyncio
async def test_records_on_exception_async():
    """Requests recorded even when async store raises."""
    failing_store = FailingStore()
    trace = RequestTrace()
    traced = TracingReadableStore(failing_store, trace)

    with pytest.raises(IOError):
        await traced.get_range_async("test.txt", start=0, length=5)

    assert len(trace.requests) == 1
    assert trace.requests[0].path == "test.txt"


def test_records_head_on_exception():
    """HEAD requests recorded even when store raises."""
    failing_store = FailingStore()
    trace = RequestTrace()
    traced = TracingReadableStore(failing_store, trace)

    with pytest.raises(IOError):
        traced.head("test.txt")

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "head"
    assert record.duration is not None


@pytest.mark.asyncio
async def test_records_head_async_on_exception():
    """HEAD async requests recorded even when store raises."""
    failing_store = FailingStore()
    trace = RequestTrace()
    traced = TracingReadableStore(failing_store, trace)

    with pytest.raises(IOError):
        await traced.head_async("test.txt")

    assert len(trace.requests) == 1
    record = trace.requests[0]
    assert record.path == "test.txt"
    assert record.method == "head"
    assert record.duration is not None
