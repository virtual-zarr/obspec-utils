"""Tests specific to ParallelStoreReader."""

from obstore.store import MemoryStore

from obspec_utils.readers import ParallelStoreReader
from obspec_utils.wrappers import RequestTrace, TracingReadableStore


def test_parallel_reader_cross_chunk_read():
    """Test ParallelStoreReader reading across chunk boundaries."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ParallelStoreReader(memstore, "test.txt", chunk_size=4)

    reader.seek(2)
    assert reader.read(6) == b"234567"

    reader.seek(0)
    assert reader.read(10) == b"0123456789"


def test_parallel_reader_caching():
    """Test that ParallelStoreReader chunks are cached correctly."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=2
    )

    reader.seek(0)
    assert reader.read(4) == b"0123"

    reader.seek(4)
    assert reader.read(4) == b"4567"

    # Third chunk evicts first from cache
    reader.seek(8)
    assert reader.read(4) == b"89AB"

    # First chunk refetched
    reader.seek(0)
    assert reader.read(4) == b"0123"


def test_parallel_reader_read_spanning_more_chunks_than_cache():
    """Read spanning more chunks than max_cached_chunks should succeed."""
    memstore = MemoryStore()
    # 20 bytes = 5 chunks of 4 bytes each
    memstore.put("test.txt", b"0123456789ABCDEFGHIJ")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=2
    )

    assert reader.read(20) == b"0123456789ABCDEFGHIJ"


def test_parallel_reader_lru_eviction_order():
    """LRU eviction should evict least recently used, not oldest inserted."""
    memstore = MemoryStore()
    # 16 bytes = 4 chunks of 4 bytes each
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=4, max_cached_chunks=2)

    # Read chunk 0
    reader.seek(0)
    reader.read(4)  # fetches chunk 0

    # Read chunk 1
    reader.seek(4)
    reader.read(4)  # fetches chunk 1, cache = [0, 1]

    # Re-read chunk 0 (makes it most recently used)
    reader.seek(0)
    reader.read(4)  # cache hit, cache = [1, 0]

    trace.clear()

    # Read chunk 2 - should evict chunk 1 (LRU), not chunk 0
    reader.seek(8)
    reader.read(4)  # fetches chunk 2, cache = [0, 2]

    # Chunk 0 should still be cached (no request)
    trace.clear()
    reader.seek(0)
    reader.read(4)
    assert trace.total_requests == 0, "Chunk 0 should still be in cache"

    # Chunk 1 should have been evicted (needs request)
    trace.clear()
    reader.seek(4)
    reader.read(4)
    assert trace.total_requests == 1, "Chunk 1 should have been evicted"


def test_parallel_reader_cache_hit_no_requests():
    """Re-reading cached data should not make new store requests."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=4, max_cached_chunks=4)

    # Initial read
    reader.read(8)  # fetches chunks 0 and 1

    # Re-read same data
    trace.clear()
    reader.seek(0)
    reader.read(8)

    assert trace.total_requests == 0, f"Expected 0 requests, got {trace.total_requests}"


def test_parallel_reader_partial_cache_hit():
    """Read spanning cached and uncached chunks should only fetch uncached."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=4, max_cached_chunks=4)

    # Read chunk 0 only
    reader.read(4)
    trace.clear()

    # Read chunks 0 and 1 - should only fetch chunk 1
    reader.seek(0)
    reader.read(8)

    # Should have 1 get_ranges request for chunk 1 only
    assert trace.total_requests == 1
    assert trace.requests[0].start == 4  # chunk 1 starts at byte 4
    assert trace.requests[0].length == 4


def test_parallel_reader_read_within_single_chunk():
    """Multiple reads within the same chunk should reuse cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=8, max_cached_chunks=2)

    # First read fetches chunk 0
    reader.read(2)  # "01"
    assert trace.total_requests == 2  # get (size) + get_ranges (chunk)
    trace.clear()

    # Subsequent reads within same chunk
    reader.read(2)  # "23"
    assert trace.total_requests == 0

    reader.seek(6)
    reader.read(2)  # "67"
    assert trace.total_requests == 0


def test_parallel_reader_read_at_chunk_boundary():
    """Read starting exactly at chunk boundary."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=4
    )

    # Read exactly at boundaries
    reader.seek(4)
    assert reader.read(4) == b"4567"

    reader.seek(8)
    assert reader.read(4) == b"89AB"

    reader.seek(12)
    assert reader.read(4) == b"CDEF"


def test_parallel_reader_last_chunk_smaller():
    """Last chunk smaller than chunk_size is handled correctly."""
    memstore = MemoryStore()
    # 10 bytes with chunk_size=4: chunks are [0-3], [4-7], [8-9]
    memstore.put("test.txt", b"0123456789")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=4
    )

    # Read the partial last chunk
    reader.seek(8)
    assert reader.read(4) == b"89"  # only 2 bytes available

    # Read spanning into partial chunk
    reader.seek(6)
    assert reader.read(10) == b"6789"  # 4 bytes available


def test_parallel_reader_read_zero_no_cache_effect():
    """read(0) should not fetch or modify cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=4, max_cached_chunks=2)

    # Prepopulate cache with chunk 0
    reader.read(4)
    trace.clear()

    # read(0) should do nothing
    result = reader.read(0)
    assert result == b""
    assert trace.total_requests == 0
    assert len(reader._cache) == 1  # cache unchanged


def test_parallel_reader_seek_preserves_cache():
    """Seeking should not invalidate the cache."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    trace = RequestTrace()
    traced = TracingReadableStore(memstore, trace)
    reader = ParallelStoreReader(traced, "test.txt", chunk_size=4, max_cached_chunks=4)

    # Read chunks 0 and 1
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


def test_parallel_reader_cache_cleared_on_close():
    """Cache should be cleared after close()."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ParallelStoreReader(
        memstore, "test.txt", chunk_size=4, max_cached_chunks=4
    )

    reader.read(8)  # populate cache
    assert len(reader._cache) == 2

    reader.close()
    assert len(reader._cache) == 0
