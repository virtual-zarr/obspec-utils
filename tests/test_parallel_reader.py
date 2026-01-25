"""Tests specific to ParallelStoreReader."""

from obstore.store import MemoryStore

from obspec_utils.obspec import ParallelStoreReader


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
