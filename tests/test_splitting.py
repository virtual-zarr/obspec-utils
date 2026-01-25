"""Tests for SplittingReadableStore."""

import pytest
from obstore.store import MemoryStore

from obspec_utils.wrappers import CachingReadableStore, SplittingReadableStore


class TestSplittingReadableStore:
    """Tests for basic splitting functionality."""

    def test_small_file_no_splitting(self):
        """Small files use single get() without splitting."""
        source = MemoryStore()
        source.put("small.txt", b"hello world")

        # Request size larger than file - no splitting
        splitter = SplittingReadableStore(source, request_size=1024)
        result = splitter.get("small.txt")

        assert bytes(result.buffer()) == b"hello world"

    def test_large_file_splits_requests(self):
        """Large files are split into parallel requests."""
        source = MemoryStore()
        data = b"x" * 10000  # 10 KB
        source.put("large.txt", data)

        # Small request size to force splitting
        splitter = SplittingReadableStore(
            source,
            request_size=1000,  # 1 KB chunks
            max_concurrent_requests=20,
        )
        result = splitter.get("large.txt")

        assert bytes(result.buffer()) == data

    def test_max_concurrent_requests_limits_chunks(self):
        """Splitting respects max_concurrent_requests."""
        source = MemoryStore()
        data = b"x" * 10000  # 10 KB
        source.put("file.txt", data)

        splitter = SplittingReadableStore(
            source,
            request_size=100,  # Would need 100 requests
            max_concurrent_requests=5,  # But limited to 5
        )

        # Should still work, with larger chunks
        result = splitter.get("file.txt")
        assert bytes(result.buffer()) == data

    def test_empty_file(self):
        """Empty files are handled correctly."""
        source = MemoryStore()
        source.put("empty.txt", b"")

        splitter = SplittingReadableStore(source)
        result = splitter.get("empty.txt")

        assert bytes(result.buffer()) == b""

    def test_range_requests_pass_through(self):
        """Range requests pass through unchanged."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        splitter = SplittingReadableStore(source)

        # Single range
        data = splitter.get_range("file.txt", start=0, end=5)
        assert bytes(data) == b"hello"

        # Multiple ranges
        ranges = splitter.get_ranges("file.txt", starts=[0, 6], ends=[5, 11])
        assert bytes(ranges[0]) == b"hello"
        assert bytes(ranges[1]) == b"world"

    def test_forwards_attributes(self):
        """Unknown attributes are forwarded to underlying store."""
        source = MemoryStore()
        splitter = SplittingReadableStore(source)

        # MemoryStore has put/delete methods
        assert hasattr(splitter, "put")
        assert hasattr(splitter, "delete")


class TestSplittingWithCaching:
    """Tests for composition with CachingReadableStore."""

    def test_splitting_then_caching(self):
        """SplittingReadableStore composes with CachingReadableStore."""
        source = MemoryStore()
        data = b"x" * 10000
        source.put("file.txt", data)

        # Compose: source -> splitting -> caching
        store = SplittingReadableStore(source, request_size=1000)
        store = CachingReadableStore(store)

        # First access: split fetch, then cached
        result1 = store.get("file.txt")
        assert bytes(result1.buffer()) == data
        assert store.cache_size == len(data)

        # Second access: from cache
        result2 = store.get("file.txt")
        assert bytes(result2.buffer()) == data

    def test_caching_then_splitting_order_matters(self):
        """Order matters: caching should wrap splitting, not vice versa."""
        source = MemoryStore()
        data = b"x" * 10000
        source.put("file.txt", data)

        # Correct order: source -> splitting -> caching
        store_correct = SplittingReadableStore(source, request_size=1000)
        store_correct = CachingReadableStore(store_correct)

        result = store_correct.get("file.txt")
        assert bytes(result.buffer()) == data

        # The cache stores the combined result from splitting
        assert store_correct.cache_size == len(data)


class TestSplittingAsync:
    """Tests for async operations."""

    @pytest.mark.asyncio
    async def test_get_async_small_file(self):
        """Async get works for small files."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        splitter = SplittingReadableStore(source, request_size=1024)
        result = await splitter.get_async("file.txt")

        assert bytes(await result.buffer_async()) == b"hello world"

    @pytest.mark.asyncio
    async def test_get_async_large_file(self):
        """Async get splits large files."""
        source = MemoryStore()
        data = b"x" * 10000
        source.put("file.txt", data)

        splitter = SplittingReadableStore(source, request_size=1000)
        result = await splitter.get_async("file.txt")

        assert bytes(await result.buffer_async()) == data

    @pytest.mark.asyncio
    async def test_range_async_pass_through(self):
        """Async range requests pass through."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        splitter = SplittingReadableStore(source)

        data = await splitter.get_range_async("file.txt", start=0, end=5)
        assert bytes(data) == b"hello"

        ranges = await splitter.get_ranges_async(
            "file.txt", starts=[0, 6], ends=[5, 11]
        )
        assert bytes(ranges[0]) == b"hello"
        assert bytes(ranges[1]) == b"world"
