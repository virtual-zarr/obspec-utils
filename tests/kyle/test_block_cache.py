from __future__ import annotations

import obstore as obs

from obspec_utils.kyle._block_cache import MemoryCache, SyncBlockCache


class TestMemoryCache:
    def test_block_index(self) -> None:
        cache = MemoryCache(block_size=1024)
        assert cache._block_index(0) == 0
        assert cache._block_index(1023) == 0
        assert cache._block_index(1024) == 1
        assert cache._block_index(2048) == 2

    def test_store_and_retrieve_single_block(self) -> None:
        cache = MemoryCache(block_size=1024)
        data = b"x" * 500
        cache.store("test.bin", 0, data)

        # Should be able to retrieve exact data
        result = cache.get("test.bin", 0, 500)
        assert result == data

        # Should be able to retrieve a slice
        result = cache.get("test.bin", 100, 200)
        assert result == b"x" * 100

    def test_store_and_retrieve_multiple_blocks(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Store 3 full blocks
        data = b"a" * 1024 + b"b" * 1024 + b"c" * 1024
        cache.store("test.bin", 0, data)

        # Retrieve spanning blocks
        result = cache.get("test.bin", 1000, 2048)
        assert result == b"a" * 24 + b"b" * 1024

        # Retrieve from middle of one block to middle of another
        result = cache.get("test.bin", 512, 2560)
        assert result == b"a" * 512 + b"b" * 1024 + b"c" * 512

    def test_get_returns_missing_ranges_for_uncached(self) -> None:
        cache = MemoryCache(block_size=1024)
        result = cache.get("test.bin", 0, 100)
        assert isinstance(result, list)
        assert result == [(0, 1024)]

    def test_get_returns_missing_ranges_spanning_blocks(self) -> None:
        cache = MemoryCache(block_size=1024)
        result = cache.get("test.bin", 0, 2000)
        assert isinstance(result, list)
        assert result == [(0, 2048)]

    def test_get_returns_missing_for_partial_cache(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Store first block only
        cache.store("test.bin", 0, b"x" * 1024)

        # Request spanning into uncached block
        result = cache.get("test.bin", 0, 2000)
        assert isinstance(result, list)
        assert result == [(1024, 2048)]

    def test_partial_block_at_eof(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Store a partial block (simulating EOF)
        data = b"x" * 500
        cache.store("test.bin", 0, data)

        # Should retrieve the partial data
        result = cache.get("test.bin", 0, 1000)
        assert result == data

    def test_lru_eviction(self) -> None:
        cache = MemoryCache(block_size=1024, max_blocks=2)

        cache.store("a.bin", 0, b"a" * 1024)
        cache.store("b.bin", 0, b"b" * 1024)

        # Both should be cached
        assert isinstance(cache.get("a.bin", 0, 1024), bytes)
        assert isinstance(cache.get("b.bin", 0, 1024), bytes)

        # Add a third, should evict least recently used
        cache.store("c.bin", 0, b"c" * 1024)

        assert isinstance(cache.get("c.bin", 0, 1024), bytes)
        # One of a or b should be evicted
        assert len(cache._blocks) == 2

    def test_coalesce_adjacent_missing_blocks(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Request 10 blocks worth of data - should coalesce into one range
        result = cache.get("test.bin", 0, 10 * 1024)
        assert isinstance(result, list)
        assert result == [(0, 10 * 1024)]

    def test_coalesce_splits_on_cached_block(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Cache blocks 0 and 5, leave 1-4 missing and 6 missing
        cache.store("test.bin", 0, b"x" * 1024)
        cache.store("test.bin", 5 * 1024, b"y" * 1024)

        # Request blocks 1-4 and 6 (missing)
        result = cache.get("test.bin", 1024, 7 * 1024)
        assert isinstance(result, list)
        # Blocks 1-4 are missing (consecutive), block 5 is cached, block 6 is missing
        # Should split into two ranges to avoid re-fetching block 5
        assert result == [(1024, 5 * 1024), (6 * 1024, 7 * 1024)]

    def test_coalesce_with_single_cached_block_gap(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Cache block 3, leave 0-2 and 4+ missing
        cache.store("test.bin", 3 * 1024, b"x" * 1024)

        # Request blocks 0-10
        result = cache.get("test.bin", 0, 11 * 1024)
        assert isinstance(result, list)
        # Blocks 0-2 missing, 3 cached, 4-10 missing
        # Should split into two ranges
        assert result == [(0, 3 * 1024), (4 * 1024, 11 * 1024)]

    def test_coalesce_multiple_separate_ranges(self) -> None:
        cache = MemoryCache(block_size=1024)
        # Create a pattern with cached blocks: cache blocks 0, 10, 20
        cache.store("test.bin", 0, b"a" * 1024)
        cache.store("test.bin", 10 * 1024, b"b" * 1024)
        cache.store("test.bin", 20 * 1024, b"c" * 1024)

        # Request everything from 0 to 25*1024
        result = cache.get("test.bin", 0, 25 * 1024)
        assert isinstance(result, list)
        # Missing: 1-9, 11-19, 21-24
        # Each group of consecutive missing blocks becomes a separate range
        assert len(result) == 3
        assert result[0] == (1 * 1024, 10 * 1024)  # blocks 1-9
        assert result[1] == (11 * 1024, 20 * 1024)  # blocks 11-19
        assert result[2] == (21 * 1024, 25 * 1024)  # blocks 21-24


class TestSyncBlockCache:
    def test_basic_get_range(self) -> None:
        store = obs.store.MemoryStore()
        data = b"hello world, this is test data!"
        obs.put(store, "test.txt", data)

        cache = SyncBlockCache(backend=store, cache=MemoryCache(block_size=16))
        result = cache.get_range("test.txt", start=0, length=5)
        assert result == b"hello"

    def test_caching_avoids_refetch(self) -> None:
        store = obs.store.MemoryStore()
        data = b"x" * 100
        obs.put(store, "test.bin", data)

        cache = SyncBlockCache(backend=store, cache=MemoryCache(block_size=64))

        # First fetch
        result1 = cache.get_range("test.bin", start=0, length=10)
        assert result1 == b"x" * 10

        # Modify the underlying store
        obs.put(store, "test.bin", b"y" * 100)

        # Should still get cached data
        result2 = cache.get_range("test.bin", start=0, length=10)
        assert result2 == b"x" * 10

        # But a different file should fetch fresh
        obs.put(store, "other.bin", b"z" * 100)
        result3 = cache.get_range("other.bin", start=0, length=10)
        assert result3 == b"z" * 10

    def test_range_spanning_blocks(self) -> None:
        store = obs.store.MemoryStore()
        data = b"a" * 32 + b"b" * 32 + b"c" * 32
        obs.put(store, "test.bin", data)

        cache = SyncBlockCache(backend=store, cache=MemoryCache(block_size=32))

        # Fetch spanning first two blocks
        result = cache.get_range("test.bin", start=16, end=48)
        assert result == b"a" * 16 + b"b" * 16

    def test_eof_handling(self) -> None:
        store = obs.store.MemoryStore()
        data = b"short"
        obs.put(store, "test.bin", data)

        cache = SyncBlockCache(backend=store, cache=MemoryCache(block_size=1024))

        # Request more than file size - should return what's available
        result = cache.get_range("test.bin", start=0, length=100)
        assert result == b"short"

    def test_end_vs_length(self) -> None:
        store = obs.store.MemoryStore()
        data = b"0123456789"
        obs.put(store, "test.bin", data)

        cache = SyncBlockCache(backend=store, cache=MemoryCache(block_size=1024))

        # Using end
        result1 = cache.get_range("test.bin", start=2, end=5)
        assert result1 == b"234"

        # Using length
        result2 = cache.get_range("test.bin", start=2, length=3)
        assert result2 == b"234"

    def test_shared_cache(self) -> None:
        store = obs.store.MemoryStore()
        obs.put(store, "test.bin", b"x" * 100)

        shared_cache = MemoryCache(block_size=64)
        cache1 = SyncBlockCache(backend=store, cache=shared_cache)
        cache2 = SyncBlockCache(backend=store, cache=shared_cache)

        # Fetch via cache1
        cache1.get_range("test.bin", start=0, length=10)

        # Modify store
        obs.put(store, "test.bin", b"y" * 100)

        # cache2 should see cached data from cache1
        result = cache2.get_range("test.bin", start=0, length=10)
        assert result == b"x" * 10

    def test_multiple_ranges_fetch(self) -> None:
        store = obs.store.MemoryStore()
        # Create data spanning many blocks
        data = b"".join(bytes([i] * 32) for i in range(20))  # 20 blocks of 32 bytes
        obs.put(store, "test.bin", data)

        shared_cache = MemoryCache(block_size=32)
        cache = SyncBlockCache(backend=store, cache=shared_cache)

        # Pre-cache blocks 0, 10 to create gaps
        cache.get_range("test.bin", start=0, length=32)
        cache.get_range("test.bin", start=10 * 32, length=32)

        # Now request a range spanning the gap - should use get_ranges
        result = cache.get_range("test.bin", start=0, end=15 * 32)

        # Verify we got correct data
        expected = b"".join(bytes([i] * 32) for i in range(15))
        assert result == expected
