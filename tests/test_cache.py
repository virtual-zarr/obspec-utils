"""Tests for CachingReadableStore."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from obstore.store import MemoryStore

from obspec_utils.cache import CachingReadableStore
from obspec_utils.registry import ObjectStoreRegistry


class TestCachingReadableStore:
    """Tests for basic caching functionality."""

    def test_cache_miss_fetches_from_store(self):
        """First access fetches from underlying store."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)
        result = cached.get("file.txt")

        assert bytes(result.buffer()) == b"hello world"
        assert "file.txt" in cached.cached_paths

    def test_cache_hit_returns_cached_data(self):
        """Second access returns from cache."""
        source = MemoryStore()
        source.put("file.txt", b"original data")

        cached = CachingReadableStore(source)

        # First access - caches the data
        result1 = cached.get("file.txt")
        assert bytes(result1.buffer()) == b"original data"

        # Modify source - cache should still return original
        source.put("file.txt", b"modified data")

        # Second access - should return cached data
        result2 = cached.get("file.txt")
        assert bytes(result2.buffer()) == b"original data"

    def test_get_range_caches_full_object(self):
        """Range request caches the full object."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)

        # Request a range - should cache the full object
        data = cached.get_range("file.txt", start=0, end=5)
        assert bytes(data) == b"hello"

        # Full object should be cached
        assert cached.cache_size == len(b"hello world")

        # Subsequent range from cache
        data2 = cached.get_range("file.txt", start=6, end=11)
        assert bytes(data2) == b"world"

    def test_get_ranges_caches_full_object(self):
        """Multiple range request caches the full object."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)

        ranges = cached.get_ranges("file.txt", starts=[0, 6], ends=[5, 11])
        assert bytes(ranges[0]) == b"hello"
        assert bytes(ranges[1]) == b"world"

        assert cached.cache_size == len(b"hello world")

    def test_clear_cache(self):
        """clear_cache() empties the cache."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)
        cached.get("file.txt")

        assert cached.cache_size > 0
        assert len(cached.cached_paths) == 1

        cached.clear_cache()

        assert cached.cache_size == 0
        assert len(cached.cached_paths) == 0

    def test_context_manager_clears_cache(self):
        """Context manager clears cache on exit."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)

        with cached:
            cached.get("file.txt")
            assert cached.cache_size > 0

        assert cached.cache_size == 0

    def test_context_manager_clears_on_exception(self):
        """Context manager clears cache even on exception."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)

        with pytest.raises(ValueError, match="test error"):
            with cached:
                cached.get("file.txt")
                assert cached.cache_size > 0
                raise ValueError("test error")

        assert cached.cache_size == 0


class TestLRUEviction:
    """Tests for LRU cache eviction."""

    def test_lru_eviction_when_full(self):
        """Oldest entries evicted when cache exceeds max_size."""
        source = MemoryStore()
        source.put("file1.txt", b"a" * 100)
        source.put("file2.txt", b"b" * 100)
        source.put("file3.txt", b"c" * 100)

        # Cache can hold ~200 bytes (2 files)
        cached = CachingReadableStore(source, max_size=200)

        # Cache file1 and file2
        cached.get("file1.txt")
        cached.get("file2.txt")
        assert cached.cache_size == 200
        assert cached.cached_paths == ["file1.txt", "file2.txt"]

        # Cache file3 - should evict file1
        cached.get("file3.txt")
        assert cached.cache_size == 200
        assert cached.cached_paths == ["file2.txt", "file3.txt"]

    def test_lru_access_updates_order(self):
        """Accessing cached item moves it to end (most recent)."""
        source = MemoryStore()
        source.put("file1.txt", b"a" * 100)
        source.put("file2.txt", b"b" * 100)
        source.put("file3.txt", b"c" * 100)

        cached = CachingReadableStore(source, max_size=200)

        # Cache file1 and file2
        cached.get("file1.txt")
        cached.get("file2.txt")
        assert cached.cached_paths == ["file1.txt", "file2.txt"]

        # Access file1 again - moves to end
        cached.get("file1.txt")
        assert cached.cached_paths == ["file2.txt", "file1.txt"]

        # Cache file3 - should evict file2 (now oldest)
        cached.get("file3.txt")
        assert cached.cached_paths == ["file1.txt", "file3.txt"]

    def test_lru_range_access_updates_order(self):
        """Range access also updates LRU order."""
        source = MemoryStore()
        source.put("file1.txt", b"a" * 100)
        source.put("file2.txt", b"b" * 100)

        cached = CachingReadableStore(source, max_size=200)

        cached.get("file1.txt")
        cached.get("file2.txt")
        assert cached.cached_paths == ["file1.txt", "file2.txt"]

        # Range access to file1 should update order
        cached.get_range("file1.txt", start=0, end=10)
        assert cached.cached_paths == ["file2.txt", "file1.txt"]


class TestRegistryIntegration:
    """Tests for integration with ObjectStoreRegistry."""

    def test_works_with_registry(self):
        """CachingReadableStore works as a registry store."""
        source = MemoryStore()
        source.put("data/file.txt", b"hello world")

        cached = CachingReadableStore(source)
        registry = ObjectStoreRegistry({"mem://bucket": cached})

        store, path = registry.resolve("mem://bucket/data/file.txt")
        assert store is cached
        assert path == "data/file.txt"

        result = store.get(path)
        assert bytes(result.buffer()) == b"hello world"

    def test_context_manager_with_registry(self):
        """Context manager pattern with registry."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        with CachingReadableStore(source) as cached:
            registry = ObjectStoreRegistry({"mem://bucket": cached})
            store, path = registry.resolve("mem://bucket/file.txt")

            data = bytes(store.get(path).buffer())
            assert data == b"hello world"
            assert cached.cache_size > 0

        # Cache cleared after context
        assert cached.cache_size == 0


class TestThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_access_same_file(self):
        """Multiple threads accessing same file safely."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)
        results = []
        errors = []

        def read_file():
            try:
                result = cached.get("file.txt")
                results.append(bytes(result.buffer()))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_file) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r == b"hello world" for r in results)
        assert cached.cache_size == len(b"hello world")

    def test_concurrent_access_different_files(self):
        """Multiple threads accessing different files safely."""
        source = MemoryStore()
        for i in range(10):
            source.put(f"file{i}.txt", f"data{i}".encode())

        cached = CachingReadableStore(source)
        results = {}
        lock = threading.Lock()

        def read_file(i):
            result = cached.get(f"file{i}.txt")
            data = bytes(result.buffer())
            with lock:
                results[i] = data

        with ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(read_file, range(10)))

        assert len(results) == 10
        for i in range(10):
            assert results[i] == f"data{i}".encode()


class TestAsyncOperations:
    """Tests for async operations."""

    @pytest.mark.asyncio
    async def test_get_async(self):
        """Async get caches data."""
        source = MemoryStore()
        source.put("file.txt", b"async data")

        cached = CachingReadableStore(source)
        result = await cached.get_async("file.txt")
        data = bytes(await result.buffer_async())

        assert data == b"async data"
        assert "file.txt" in cached.cached_paths

    @pytest.mark.asyncio
    async def test_get_range_async(self):
        """Async range get caches full object."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)
        data = await cached.get_range_async("file.txt", start=0, end=5)

        assert bytes(data) == b"hello"
        assert cached.cache_size == len(b"hello world")

    @pytest.mark.asyncio
    async def test_get_ranges_async(self):
        """Async multiple range get caches full object."""
        source = MemoryStore()
        source.put("file.txt", b"hello world")

        cached = CachingReadableStore(source)
        ranges = await cached.get_ranges_async("file.txt", starts=[0, 6], ends=[5, 11])

        assert bytes(ranges[0]) == b"hello"
        assert bytes(ranges[1]) == b"world"

    @pytest.mark.asyncio
    async def test_async_cache_hit_updates_lru(self):
        """Async access to already-cached file updates LRU order."""
        source = MemoryStore()
        source.put("file1.txt", b"a" * 100)
        source.put("file2.txt", b"b" * 100)
        source.put("file3.txt", b"c" * 100)

        cached = CachingReadableStore(source, max_size=200)

        # Cache file1 and file2 (sync or async doesn't matter)
        cached.get("file1.txt")
        cached.get("file2.txt")
        assert cached.cached_paths == ["file1.txt", "file2.txt"]

        # Async access to file1 - should hit cache and update LRU order
        result = await cached.get_async("file1.txt")
        assert bytes(await result.buffer_async()) == b"a" * 100
        assert cached.cached_paths == ["file2.txt", "file1.txt"]

        # Cache file3 - should evict file2 (now oldest)
        await cached.get_async("file3.txt")
        assert cached.cached_paths == ["file1.txt", "file3.txt"]

    @pytest.mark.asyncio
    async def test_concurrent_async_fetch_race_condition(self):
        """When two coroutines fetch same file, second finds it already cached."""
        import asyncio

        source = MemoryStore()
        source.put("file.txt", b"hello world")

        # Wrap store to add delay during fetch, creating race window
        class SlowStore:
            def __init__(self, store):
                self._store = store
                self.fetch_count = 0

            async def get_async(self, path):
                self.fetch_count += 1
                # Delay to ensure both coroutines start fetching before either finishes
                await asyncio.sleep(0.05)
                return await self._store.get_async(path)

            def __getattr__(self, name):
                return getattr(self._store, name)

        slow_source = SlowStore(source)
        cached = CachingReadableStore(slow_source)

        # Launch two concurrent fetches for the same file
        results = await asyncio.gather(
            cached.get_async("file.txt"),
            cached.get_async("file.txt"),
        )

        # Both should return correct data
        assert bytes(await results[0].buffer_async()) == b"hello world"
        assert bytes(await results[1].buffer_async()) == b"hello world"

        # Both coroutines should have started fetching (race condition)
        assert slow_source.fetch_count == 2

        # But file should only be cached once
        assert cached.cached_paths == ["file.txt"]
        assert cached.cache_size == len(b"hello world")


class TestAttributeForwarding:
    """Tests for attribute forwarding to underlying store."""

    def test_forwards_unknown_attributes(self):
        """Unknown attributes are forwarded to underlying store."""
        source = MemoryStore()
        cached = CachingReadableStore(source)

        # MemoryStore should have certain attributes/methods
        # This tests that __getattr__ forwards correctly
        assert hasattr(cached, "put")  # MemoryStore has put
        assert hasattr(cached, "delete")  # MemoryStore has delete
