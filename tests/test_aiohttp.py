"""Tests for AiohttpStore, AiohttpGetResult, and AiohttpGetResultAsync."""

import io
from datetime import datetime

import pytest

from obspec_utils.stores import AiohttpGetResult, AiohttpGetResultAsync, AiohttpStore

# Check if docker is available for MinIO tests
try:
    import docker

    docker.from_env()
    HAS_DOCKER = True
except Exception:
    HAS_DOCKER = False

requires_minio = pytest.mark.skipif(
    not HAS_DOCKER, reason="Docker not available for MinIO"
)


# --- Test Fixtures ---


@pytest.fixture(scope="module")
def minio_test_file(minio_bucket):
    """Upload a test file to MinIO and return its URL and content."""
    content = b"0123456789ABCDEF"
    filename = "test_aiohttp.bin"

    # Upload the file
    minio_bucket["client"].put_object(
        minio_bucket["bucket"],
        filename,
        io.BytesIO(content),
        len(content),
        content_type="application/octet-stream",
    )

    return {
        "url": f"{minio_bucket['endpoint']}/{minio_bucket['bucket']}/{filename}",
        "base_url": f"{minio_bucket['endpoint']}/{minio_bucket['bucket']}",
        "path": filename,
        "content": content,
    }


# --- AiohttpGetResult Tests ---


def test_get_result_buffer():
    """buffer() returns data."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResult(_data=data, _meta=meta)

    assert result.buffer() == data


def test_get_result_meta():
    """meta property returns ObjectMeta."""
    data = b"hello"
    meta = {
        "path": "test.txt",
        "size": 5,
        "last_modified": None,
        "e_tag": "abc123",
        "version": None,
    }
    result = AiohttpGetResult(_data=data, _meta=meta)

    assert result.meta == meta
    assert result.meta["path"] == "test.txt"
    assert result.meta["e_tag"] == "abc123"


def test_get_result_attributes():
    """attributes property returns attributes dict."""
    data = b"hello"
    meta = {
        "path": "test.txt",
        "size": 5,
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    attrs = {"Content-Type": "text/plain"}
    result = AiohttpGetResult(_data=data, _meta=meta, _attributes=attrs)

    assert result.attributes == {"Content-Type": "text/plain"}


def test_get_result_range_default():
    """Range defaults to (0, len(data))."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResult(_data=data, _meta=meta)

    assert result.range == (0, 11)


def test_get_result_range_custom():
    """Custom range is preserved."""
    data = b"world"
    meta = {
        "path": "test.txt",
        "size": 11,
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResult(_data=data, _meta=meta, _range=(6, 11))

    assert result.range == (6, 11)


def test_get_result_iter():
    """__iter__ yields data."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResult(_data=data, _meta=meta)

    chunks = list(result)
    assert chunks == [data]


# --- AiohttpGetResultAsync Tests ---


@pytest.mark.asyncio
async def test_get_result_async_buffer():
    """buffer_async() returns data."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResultAsync(_data=data, _meta=meta)

    assert await result.buffer_async() == data


def test_get_result_async_meta():
    """meta property returns ObjectMeta."""
    data = b"hello"
    meta = {
        "path": "test.txt",
        "size": 5,
        "last_modified": None,
        "e_tag": "abc123",
        "version": None,
    }
    result = AiohttpGetResultAsync(_data=data, _meta=meta)

    assert result.meta == meta


def test_get_result_async_range_default():
    """Range defaults to (0, len(data))."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResultAsync(_data=data, _meta=meta)

    assert result.range == (0, 11)


@pytest.mark.asyncio
async def test_get_result_async_aiter():
    """__aiter__ yields data."""
    data = b"hello world"
    meta = {
        "path": "test.txt",
        "size": len(data),
        "last_modified": None,
        "e_tag": None,
        "version": None,
    }
    result = AiohttpGetResultAsync(_data=data, _meta=meta)

    chunks = [chunk async for chunk in result]
    assert chunks == [data]


# --- AiohttpStore - Initialization ---


def test_store_base_url_trailing_slash():
    """Trailing slash is stripped from base_url."""
    store = AiohttpStore("https://example.com/data/")
    assert store.base_url == "https://example.com/data"


def test_store_headers_default():
    """Empty headers by default."""
    store = AiohttpStore("https://example.com")
    assert store.headers == {}


def test_store_headers_custom():
    """Custom headers are stored."""
    headers = {"Authorization": "Bearer token123"}
    store = AiohttpStore("https://example.com", headers=headers)
    assert store.headers == headers


def test_store_timeout_default():
    """Default timeout is 30 seconds."""
    store = AiohttpStore("https://example.com")
    assert store.timeout.total == 30.0


def test_store_timeout_custom():
    """Custom timeout is stored."""
    store = AiohttpStore("https://example.com", timeout=60.0)
    assert store.timeout.total == 60.0


# --- AiohttpStore - URL Building ---


def test_build_url_simple_path():
    """Simple path is appended to base_url."""
    store = AiohttpStore("https://example.com/data")
    assert store._build_url("file.txt") == "https://example.com/data/file.txt"


def test_build_url_leading_slash():
    """Leading slash is stripped from path."""
    store = AiohttpStore("https://example.com/data")
    assert store._build_url("/file.txt") == "https://example.com/data/file.txt"


def test_build_url_empty_path():
    """Empty path returns base_url."""
    store = AiohttpStore("https://example.com/data")
    assert store._build_url("") == "https://example.com/data"


def test_build_url_nested_path():
    """Nested path works correctly."""
    store = AiohttpStore("https://example.com/data")
    assert (
        store._build_url("subdir/file.txt")
        == "https://example.com/data/subdir/file.txt"
    )


# --- AiohttpStore - Context Manager ---


@pytest.mark.asyncio
async def test_context_manager_creates_session():
    """Session is created on entering context manager."""
    store = AiohttpStore("https://example.com")
    assert store._session is None

    async with store:
        assert store._session is not None


@pytest.mark.asyncio
async def test_context_manager_closes_session():
    """Session is closed on exiting context manager."""
    store = AiohttpStore("https://example.com")

    async with store:
        session = store._session
        assert session is not None

    assert store._session is None


# --- AiohttpStore - Async Methods (with MinIO) ---


@requires_minio
@pytest.mark.asyncio
async def test_get_async(minio_test_file):
    """Fetches file and returns correct data."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        result = await store.get_async(minio_test_file["path"])
        data = await result.buffer_async()

    assert data == minio_test_file["content"]


@requires_minio
@pytest.mark.asyncio
async def test_get_async_with_tuple_range_option(minio_test_file):
    """Fetches byte range using options with tuple range."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        # Use options={"range": (start, end)} to fetch bytes 0-4
        result = await store.get_async(
            minio_test_file["path"],
            options={"range": (0, 5)},
        )
        data = await result.buffer_async()

    assert data == b"01234"
    # Verify the range is recorded correctly
    assert result.range == (0, 5)


@requires_minio
@pytest.mark.asyncio
async def test_get_async_with_offset_range_option(minio_test_file):
    """Fetches from offset to end using options with offset dict."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        # Use options={"range": {"offset": n}} to fetch from offset to end
        result = await store.get_async(
            minio_test_file["path"],
            options={"range": {"offset": 10}},
        )
        data = await result.buffer_async()

    # File content is b"0123456789ABCDEF", offset 10 to end is b"ABCDEF"
    assert data == b"ABCDEF"


@requires_minio
@pytest.mark.asyncio
async def test_get_async_with_suffix_range_option(minio_test_file):
    """Fetches last N bytes using options with suffix dict."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        # Use options={"range": {"suffix": n}} to fetch last n bytes
        result = await store.get_async(
            minio_test_file["path"],
            options={"range": {"suffix": 6}},
        )
        data = await result.buffer_async()

    # File content is b"0123456789ABCDEF", last 6 bytes is b"ABCDEF"
    assert data == b"ABCDEF"


@requires_minio
@pytest.mark.asyncio
async def test_get_range_async_with_end(minio_test_file):
    """Fetches byte range using end parameter."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        # Get bytes 0-4 (exclusive end, so "01234")
        data = await store.get_range_async(minio_test_file["path"], start=0, end=5)

    assert bytes(data) == b"01234"


@requires_minio
@pytest.mark.asyncio
async def test_get_range_async_with_length(minio_test_file):
    """Fetches byte range using length parameter."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        # Get 5 bytes starting at offset 5
        data = await store.get_range_async(minio_test_file["path"], start=5, length=5)

    assert bytes(data) == b"56789"


@pytest.mark.asyncio
async def test_get_range_async_missing_params():
    """Raises ValueError when neither end nor length provided."""
    store = AiohttpStore("https://example.com")

    with pytest.raises(ValueError, match="Either 'end' or 'length' must be provided"):
        await store.get_range_async("file.txt", start=0)


@requires_minio
@pytest.mark.asyncio
async def test_get_ranges_async_with_ends(minio_test_file):
    """Fetches multiple byte ranges using ends parameter."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        results = await store.get_ranges_async(
            minio_test_file["path"],
            starts=[0, 10],
            ends=[5, 16],
        )

    assert [bytes(r) for r in results] == [b"01234", b"ABCDEF"]


@requires_minio
@pytest.mark.asyncio
async def test_get_ranges_async_with_lengths(minio_test_file):
    """Fetches multiple byte ranges using lengths parameter."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        results = await store.get_ranges_async(
            minio_test_file["path"],
            starts=[0, 10],
            lengths=[5, 6],
        )

    assert [bytes(r) for r in results] == [b"01234", b"ABCDEF"]


@pytest.mark.asyncio
async def test_get_ranges_async_missing_params():
    """Raises ValueError when neither ends nor lengths provided."""
    store = AiohttpStore("https://example.com")

    with pytest.raises(ValueError, match="Either 'ends' or 'lengths' must be provided"):
        await store.get_ranges_async("file.txt", starts=[0, 5])


# --- AiohttpStore - Sync Methods (with MinIO) ---


@requires_minio
def test_get_sync(minio_test_file):
    """Synchronous get returns AiohttpGetResult."""
    store = AiohttpStore(minio_test_file["base_url"])
    result = store.get(minio_test_file["path"])

    assert isinstance(result, AiohttpGetResult)
    assert result.buffer() == minio_test_file["content"]


@requires_minio
def test_get_range_sync(minio_test_file):
    """Synchronous get_range returns bytes."""
    store = AiohttpStore(minio_test_file["base_url"])
    data = store.get_range(minio_test_file["path"], start=0, length=5)

    assert bytes(data) == b"01234"


@requires_minio
def test_get_ranges_sync(minio_test_file):
    """Synchronous get_ranges returns sequence of bytes."""
    store = AiohttpStore(minio_test_file["base_url"])
    results = store.get_ranges(minio_test_file["path"], starts=[0, 10], lengths=[5, 6])

    assert [bytes(r) for r in results] == [b"01234", b"ABCDEF"]


# --- AiohttpStore - Session Management ---


@requires_minio
@pytest.mark.asyncio
async def test_without_context_manager(minio_test_file):
    """Without context manager, creates temp session per request."""
    store = AiohttpStore(minio_test_file["base_url"])
    assert store._session is None

    # Should still work - creates temporary session
    result = await store.get_async(minio_test_file["path"])
    data = await result.buffer_async()

    assert data == minio_test_file["content"]
    assert store._session is None  # No persistent session


# --- Header Parsing (unit tests) ---


def test_parse_meta_invalid_last_modified():
    """Malformed Last-Modified header falls back to current time."""
    from datetime import timezone

    store = AiohttpStore("https://example.com")
    before = datetime.now(timezone.utc)

    # Provide an invalid Last-Modified header
    meta = store._parse_meta_from_headers(
        "test.txt",
        {"Last-Modified": "not-a-valid-date"},
        content_length=100,
    )

    after = datetime.now(timezone.utc)

    # Should fall back to current time (approximately)
    assert meta["last_modified"] is not None
    assert isinstance(meta["last_modified"], datetime)
    # The fallback time should be between before and after
    assert before <= meta["last_modified"] <= after


def test_parse_meta_missing_last_modified():
    """Missing Last-Modified header falls back to current time."""
    from datetime import timezone

    store = AiohttpStore("https://example.com")
    before = datetime.now(timezone.utc)

    meta = store._parse_meta_from_headers(
        "test.txt",
        {},  # No Last-Modified header
        content_length=100,
    )

    after = datetime.now(timezone.utc)

    assert meta["last_modified"] is not None
    assert before <= meta["last_modified"] <= after


def test_parse_meta_content_range_with_total():
    """Content-Range header with numeric total extracts file size."""
    store = AiohttpStore("https://example.com")

    meta = store._parse_meta_from_headers(
        "test.txt",
        {"Content-Range": "bytes 0-999/5000"},
        content_length=1000,  # This is the chunk size, not total
    )

    # Size should be extracted from Content-Range total (5000), not content_length
    assert meta["size"] == 5000


def test_parse_meta_content_range_with_unknown_total():
    """Content-Range header with '*' total does not override size."""
    store = AiohttpStore("https://example.com")

    meta = store._parse_meta_from_headers(
        "test.txt",
        {"Content-Range": "bytes 0-999/*"},
        content_length=1000,
    )

    # Size should fall back to content_length since total is unknown
    assert meta["size"] == 1000


# --- Metadata from Real Server ---


@requires_minio
@pytest.mark.asyncio
async def test_meta_has_size(minio_test_file):
    """Response meta includes file size."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        result = await store.get_async(minio_test_file["path"])

    assert result.meta["size"] == len(minio_test_file["content"])


@requires_minio
@pytest.mark.asyncio
async def test_meta_has_etag(minio_test_file):
    """Response meta includes ETag from MinIO."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        result = await store.get_async(minio_test_file["path"])

    # MinIO always returns an ETag
    assert result.meta["e_tag"] is not None


@requires_minio
@pytest.mark.asyncio
async def test_meta_has_last_modified(minio_test_file):
    """Response meta includes last modified time."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        result = await store.get_async(minio_test_file["path"])

    assert result.meta["last_modified"] is not None
    assert isinstance(result.meta["last_modified"], datetime)


@requires_minio
@pytest.mark.asyncio
async def test_attributes_content_type(minio_test_file):
    """Response attributes include Content-Type."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        result = await store.get_async(minio_test_file["path"])

    assert "Content-Type" in result.attributes
    assert result.attributes["Content-Type"] == "application/octet-stream"


# --- AiohttpStore - Head Methods ---


@requires_minio
@pytest.mark.asyncio
async def test_head_async(minio_test_file):
    """head_async returns ObjectMeta with file size."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        meta = await store.head_async(minio_test_file["path"])

    assert meta["size"] == len(minio_test_file["content"])
    assert meta["path"] == minio_test_file["path"]


@requires_minio
@pytest.mark.asyncio
async def test_head_async_has_etag(minio_test_file):
    """head_async returns ObjectMeta with ETag."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        meta = await store.head_async(minio_test_file["path"])

    # MinIO always returns an ETag
    assert meta["e_tag"] is not None


@requires_minio
@pytest.mark.asyncio
async def test_head_async_has_last_modified(minio_test_file):
    """head_async returns ObjectMeta with last modified time."""
    async with AiohttpStore(minio_test_file["base_url"]) as store:
        meta = await store.head_async(minio_test_file["path"])

    assert meta["last_modified"] is not None
    assert isinstance(meta["last_modified"], datetime)


@requires_minio
@pytest.mark.asyncio
async def test_head_async_without_context_manager(minio_test_file):
    """head_async works without context manager (creates temp session)."""
    store = AiohttpStore(minio_test_file["base_url"])
    assert store._session is None

    meta = await store.head_async(minio_test_file["path"])

    assert meta["size"] == len(minio_test_file["content"])
    assert store._session is None  # No persistent session


@requires_minio
def test_head_sync(minio_test_file):
    """Synchronous head returns ObjectMeta."""
    store = AiohttpStore(minio_test_file["base_url"])
    meta = store.head(minio_test_file["path"])

    assert meta["size"] == len(minio_test_file["content"])
    assert meta["path"] == minio_test_file["path"]
    assert meta["e_tag"] is not None
    assert meta["last_modified"] is not None


# --- Nested Event Loop Handling (Jupyter compatibility) ---


@requires_minio
@pytest.mark.asyncio
async def test_sync_methods_from_running_loop(minio_test_file):
    """
    Sync methods work when called from within a running event loop.

    This simulates the Jupyter notebook environment where an event loop
    is already running. The per-store event loop design handles this by
    creating a dedicated thread with its own event loop for sync operations.
    """
    store = AiohttpStore(minio_test_file["base_url"])

    # We're inside an async function, so there's a running event loop.
    # Calling sync methods would fail with asyncio.run() but should
    # work with the per-store event loop implementation.
    try:
        # Test head (sync)
        meta = store.head(minio_test_file["path"])
        assert meta["size"] == len(minio_test_file["content"])

        # Test get (sync)
        result = store.get(minio_test_file["path"])
        assert result.buffer() == minio_test_file["content"]

        # Test get_range (sync)
        data = store.get_range(minio_test_file["path"], start=0, length=5)
        assert bytes(data) == b"01234"

        # Test get_ranges (sync)
        results = store.get_ranges(
            minio_test_file["path"], starts=[0, 10], lengths=[5, 6]
        )
        assert [bytes(r) for r in results] == [b"01234", b"ABCDEF"]

    finally:
        store.close()


def test_sync_loop_not_created_outside_async():
    """Sync loop is not created when not inside a running event loop."""
    store = AiohttpStore("https://example.com")

    # Before any sync call
    assert store._sync_loop is None
    assert store._sync_thread is None

    # close() should be safe even if loop was never created
    store.close()
    assert store._sync_loop is None


@requires_minio
@pytest.mark.asyncio
async def test_sync_loop_created_inside_async(minio_test_file):
    """Sync loop is lazily created when sync method called from async context."""
    store = AiohttpStore(minio_test_file["base_url"])

    # Before sync call
    assert store._sync_loop is None
    assert store._sync_thread is None

    # Call sync method from async context
    _ = store.head(minio_test_file["path"])

    # Sync loop should now exist
    assert store._sync_loop is not None
    assert store._sync_thread is not None
    assert store._sync_thread.is_alive()

    # Cleanup
    store.close()
    assert store._sync_loop is None
    assert store._sync_thread is None
