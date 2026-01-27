"""Tests specific to BlockStoreReader."""

from obstore.store import MemoryStore

from obspec_utils.readers import BlockStoreReader
from obspec_utils.wrappers import RequestTrace, TracingReadableStore


def test_block_reader_cross_block_read():
    """Test BlockStoreReader reading across block boundaries."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4)

    reader.seek(2)
    assert reader.read(6) == b"234567"

    reader.seek(0)
    assert reader.read(10) == b"0123456789"


def test_block_reader_caching():
    """Test that BlockStoreReader blocks are cached correctly."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4, max_cached_blocks=2)

    reader.seek(0)
    assert reader.read(4) == b"0123"

    reader.seek(4)
    assert reader.read(4) == b"4567"

    # Third block evicts first from cache
    reader.seek(8)
    assert reader.read(4) == b"89AB"

    # First block refetched
    reader.seek(0)
    assert reader.read(4) == b"0123"


def test_block_reader_read_spanning_more_blocks_than_cache():
    """Read spanning more blocks than max_cached_blocks should succeed."""
    memstore = MemoryStore()
    # 20 bytes = 5 blocks of 4 bytes each
    memstore.put("test.txt", b"0123456789ABCDEFGHIJ")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4, max_cached_blocks=2)

    assert reader.read(20) == b"0123456789ABCDEFGHIJ"


def test_block_reader_lru_eviction_order():
    """LRU eviction should evict least recently used, not oldest inserted."""
    memstore = MemoryStore()
    # 16 bytes = 4 blocks of 4 bytes each
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=4, max_cached_blocks=2)

    # Read block 0
    reader.seek(0)
    reader.read(4)  # fetches block 0

    # Read block 1
    reader.seek(4)
    reader.read(4)  # fetches block 1, cache = [0, 1]

    # Re-read block 0 (makes it most recently used)
    reader.seek(0)
    reader.read(4)  # cache hit, cache = [1, 0]

    trace.clear()

    # Read block 2 - should evict block 1 (LRU), not block 0
    reader.seek(8)
    reader.read(4)  # fetches block 2, cache = [0, 2]

    # Block 0 should still be cached (no request)
    trace.clear()
    reader.seek(0)
    reader.read(4)
    assert trace.total_requests == 0, "Block 0 should still be in cache"

    # Block 1 should have been evicted (needs request)
    trace.clear()
    reader.seek(4)
    reader.read(4)
    assert trace.total_requests == 1, "Block 1 should have been evicted"


def test_block_reader_cache_hit_no_requests():
    """Re-reading cached data should not make new store requests."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=4, max_cached_blocks=4)

    # Initial read
    reader.read(8)  # fetches blocks 0 and 1

    # Re-read same data
    trace.clear()
    reader.seek(0)
    reader.read(8)

    assert trace.total_requests == 0, f"Expected 0 requests, got {trace.total_requests}"


def test_block_reader_partial_cache_hit():
    """Read spanning cached and uncached blocks should only fetch uncached."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=4, max_cached_blocks=4)

    # Read block 0 only
    reader.read(4)
    trace.clear()

    # Read blocks 0 and 1 - should only fetch block 1
    reader.seek(0)
    reader.read(8)

    # Should have 1 get_ranges request for block 1 only
    assert trace.total_requests == 1
    assert trace.requests[0].start == 4  # block 1 starts at byte 4
    assert trace.requests[0].length == 4


def test_block_reader_read_within_single_block():
    """Multiple reads within the same block should reuse cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=8, max_cached_blocks=2)

    # First read fetches block 0
    reader.read(2)  # "01"
    assert trace.total_requests == 2  # get (size) + get_ranges (block)
    trace.clear()

    # Subsequent reads within same block
    reader.read(2)  # "23"
    assert trace.total_requests == 0

    reader.seek(6)
    reader.read(2)  # "67"
    assert trace.total_requests == 0


def test_block_reader_read_at_block_boundary():
    """Read starting exactly at block boundary."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4, max_cached_blocks=4)

    # Read exactly at boundaries
    reader.seek(4)
    assert reader.read(4) == b"4567"

    reader.seek(8)
    assert reader.read(4) == b"89AB"

    reader.seek(12)
    assert reader.read(4) == b"CDEF"


def test_block_reader_last_block_smaller():
    """Last block smaller than block_size is handled correctly."""
    memstore = MemoryStore()
    # 10 bytes with block_size=4: blocks are [0-3], [4-7], [8-9]
    memstore.put("test.txt", b"0123456789")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4, max_cached_blocks=4)

    # Read the partial last block
    reader.seek(8)
    assert reader.read(4) == b"89"  # only 2 bytes available

    # Read spanning into partial block
    reader.seek(6)
    assert reader.read(10) == b"6789"  # 4 bytes available


def test_block_reader_read_zero_no_cache_effect():
    """read(0) should not fetch or modify cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=4, max_cached_blocks=2)

    # Prepopulate cache with block 0
    reader.read(4)
    trace.clear()

    # read(0) should do nothing
    result = reader.read(0)
    assert result == b""
    assert trace.total_requests == 0
    assert len(reader._cache) == 1  # cache unchanged


def test_block_reader_seek_preserves_cache():
    """Seeking should not invalidate the cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = BlockStoreReader(traced, "test.txt", block_size=4, max_cached_blocks=4)

    # Read blocks 0 and 1
    reader.read(8)
    trace.clear()

    # Seek around without reading
    reader.seek(0)
    reader.seek(100)
    reader.seek(4)
    reader.seek(0, 2)  # SEEK_END

    assert trace.total_requests == 0, "Seeking should not make requests"

    # Read from cached region
    reader.seek(0)
    reader.read(8)
    assert trace.total_requests == 0, "Cached data should still be available"


def test_block_reader_cache_cleared_on_close():
    """Cache should be cleared after close()."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = BlockStoreReader(memstore, "test.txt", block_size=4, max_cached_blocks=4)

    reader.read(8)  # populate cache
    assert len(reader._cache) == 2

    reader.close()
    assert len(reader._cache) == 0
