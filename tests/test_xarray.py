import xarray as xr
from obspec_utils.obstore import (
    ObstoreReader,
    ObstoreHybridReader,
    ObstoreParallelReader,
    ObstorePrefetchReader,
    ObstoreEagerReader,
)
from obstore.store import LocalStore, MemoryStore


def test_local_reader(local_netcdf4_file) -> None:
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ObstoreReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_eager_reader(local_netcdf4_file) -> None:
    """Test that ObstoreEagerReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    reader = ObstoreEagerReader(store=LocalStore(), path=local_netcdf4_file)
    ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
    xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_eager_reader_interface(local_netcdf4_file) -> None:
    """Test that ObstoreEagerReader implements the same interface as ObstoreReader."""
    store = LocalStore()
    regular_reader = ObstoreReader(store=store, path=local_netcdf4_file)
    eager_reader = ObstoreEagerReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_regular = regular_reader.readall()
    data_memcache = eager_reader.readall()
    assert data_regular == data_memcache
    assert isinstance(data_memcache, bytes)


def test_eager_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that ObstoreEagerReader can perform multiple reads."""
    store = LocalStore()
    reader = ObstoreEagerReader(store=store, path=local_netcdf4_file)

    # Read the first 100 bytes
    chunk1 = reader.read(100)
    assert len(chunk1) == 100
    assert isinstance(chunk1, bytes)

    # Read the next 100 bytes
    chunk2 = reader.read(100)
    assert len(chunk2) == 100
    assert isinstance(chunk2, bytes)

    # The two chunks should be different (different parts of the file)
    assert chunk1 != chunk2

    # Test tell
    position = reader.tell()
    assert position == 200

    # Test seek
    reader.seek(0)
    assert reader.tell() == 0

    # Re-reading from the beginning should give us the same data
    chunk1_again = reader.read(100)
    assert chunk1 == chunk1_again


def test_prefetch_reader(local_netcdf4_file) -> None:
    """Test that ObstorePrefetchReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    with ObstorePrefetchReader(store=LocalStore(), path=local_netcdf4_file) as reader:
        ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
        xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_prefetch_reader_interface(local_netcdf4_file) -> None:
    """Test that ObstorePrefetchReader implements the same interface as ObstoreReader."""
    store = LocalStore()
    regular_reader = ObstoreReader(store=store, path=local_netcdf4_file)
    prefetch_reader = ObstorePrefetchReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_regular = regular_reader.readall()
    data_prefetch = prefetch_reader.readall()
    assert data_regular == data_prefetch
    assert isinstance(data_prefetch, bytes)

    prefetch_reader.close()


def test_prefetch_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that ObstorePrefetchReader can perform multiple reads."""
    store = LocalStore()
    reader = ObstorePrefetchReader(store=store, path=local_netcdf4_file)

    # Read the first 100 bytes
    chunk1 = reader.read(100)
    assert len(chunk1) == 100
    assert isinstance(chunk1, bytes)

    # Read the next 100 bytes
    chunk2 = reader.read(100)
    assert len(chunk2) == 100
    assert isinstance(chunk2, bytes)

    # The two chunks should be different (different parts of the file)
    assert chunk1 != chunk2

    # Test tell
    position = reader.tell()
    assert position == 200

    # Test seek
    reader.seek(0)
    assert reader.tell() == 0

    # Re-reading from the beginning should give us the same data
    chunk1_again = reader.read(100)
    assert chunk1 == chunk1_again

    reader.close()


def test_prefetch_reader_seek_whence() -> None:
    """Test seek with different whence values."""
    store = MemoryStore()
    data = b"0123456789" * 100  # 1000 bytes
    store.put("test.bin", data)

    reader = ObstorePrefetchReader(
        store=store, path="test.bin", chunk_size=100, prefetch_size=200
    )

    # whence=0 (SEEK_SET): from start
    pos = reader.seek(500)
    assert pos == 500
    assert reader.tell() == 500

    # whence=1 (SEEK_CUR): from current position
    pos = reader.seek(100, 1)
    assert pos == 600
    assert reader.tell() == 600

    # whence=2 (SEEK_END): from end
    pos = reader.seek(-100, 2)
    assert pos == 900
    assert reader.tell() == 900

    # Read from position 900
    chunk = reader.read(50)
    assert chunk == data[900:950]

    reader.close()


def test_prefetch_reader_chunked_reads() -> None:
    """Test reading across chunk boundaries."""
    store = MemoryStore()
    data = bytes(range(256)) * 40  # 10240 bytes
    store.put("test.bin", data)

    # Use small chunks to test boundary handling
    reader = ObstorePrefetchReader(
        store=store, path="test.bin", chunk_size=100, prefetch_size=300
    )

    # Read across chunk boundary (chunk 0 ends at 100)
    chunk = reader.read(150)
    assert chunk == data[:150]
    assert reader.tell() == 150

    # Read more, crossing another boundary
    chunk = reader.read(100)
    assert chunk == data[150:250]
    assert reader.tell() == 250

    reader.close()


def test_prefetch_reader_context_manager() -> None:
    """Test context manager properly closes resources."""
    store = MemoryStore()
    store.put("test.bin", b"hello world")

    with ObstorePrefetchReader(store=store, path="test.bin") as reader:
        data = reader.readall()
        assert data == b"hello world"

    # After closing, operations should raise
    try:
        reader.read(1)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_prefetch_reader_read_beyond_eof() -> None:
    """Test reading beyond end of file."""
    store = MemoryStore()
    store.put("test.bin", b"short")

    reader = ObstorePrefetchReader(store=store, path="test.bin", chunk_size=100)

    # Read more than available
    data = reader.read(1000)
    assert data == b"short"
    assert reader.tell() == 5

    # Read at EOF returns empty
    data = reader.read(100)
    assert data == b""

    reader.close()


# =============================================================================
# ObstoreParallelReader Tests
# =============================================================================


def test_parallel_reader(local_netcdf4_file) -> None:
    """Test that ObstoreParallelReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    with ObstoreParallelReader(store=LocalStore(), path=local_netcdf4_file) as reader:
        ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
        xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_parallel_reader_interface(local_netcdf4_file) -> None:
    """Test that ObstoreParallelReader implements the same interface as ObstoreReader."""
    store = LocalStore()
    regular_reader = ObstoreReader(store=store, path=local_netcdf4_file)
    parallel_reader = ObstoreParallelReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_regular = regular_reader.readall()
    data_parallel = parallel_reader.readall()
    assert data_regular == data_parallel
    assert isinstance(data_parallel, bytes)

    parallel_reader.close()


def test_parallel_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that ObstoreParallelReader can perform multiple reads."""
    store = LocalStore()
    reader = ObstoreParallelReader(store=store, path=local_netcdf4_file)

    # Read the first 100 bytes
    chunk1 = reader.read(100)
    assert len(chunk1) == 100
    assert isinstance(chunk1, bytes)

    # Read the next 100 bytes
    chunk2 = reader.read(100)
    assert len(chunk2) == 100
    assert isinstance(chunk2, bytes)

    # The two chunks should be different (different parts of the file)
    assert chunk1 != chunk2

    # Test tell
    position = reader.tell()
    assert position == 200

    # Test seek
    reader.seek(0)
    assert reader.tell() == 0

    # Re-reading from the beginning should give us the same data
    chunk1_again = reader.read(100)
    assert chunk1 == chunk1_again

    reader.close()


def test_parallel_reader_seek_whence() -> None:
    """Test seek with different whence values."""
    store = MemoryStore()
    data = b"0123456789" * 100  # 1000 bytes
    store.put("test.bin", data)

    reader = ObstoreParallelReader(store=store, path="test.bin", chunk_size=100)

    # whence=0 (SEEK_SET): from start
    pos = reader.seek(500)
    assert pos == 500
    assert reader.tell() == 500

    # whence=1 (SEEK_CUR): from current position
    pos = reader.seek(100, 1)
    assert pos == 600
    assert reader.tell() == 600

    # whence=2 (SEEK_END): from end
    pos = reader.seek(-100, 2)
    assert pos == 900
    assert reader.tell() == 900

    # Read from position 900
    chunk = reader.read(50)
    assert chunk == data[900:950]

    reader.close()


def test_parallel_reader_chunked_reads() -> None:
    """Test reading across chunk boundaries with parallel fetching."""
    store = MemoryStore()
    data = bytes(range(256)) * 40  # 10240 bytes
    store.put("test.bin", data)

    # Use small chunks to test boundary handling
    reader = ObstoreParallelReader(store=store, path="test.bin", chunk_size=100)

    # Read across chunk boundary (chunk 0 ends at 100)
    chunk = reader.read(150)
    assert chunk == data[:150]
    assert reader.tell() == 150

    # Read more, crossing another boundary
    chunk = reader.read(100)
    assert chunk == data[150:250]
    assert reader.tell() == 250

    reader.close()


def test_parallel_reader_large_read() -> None:
    """Test reading a large range that spans many chunks."""
    store = MemoryStore()
    data = bytes(range(256)) * 400  # 102400 bytes
    store.put("test.bin", data)

    # Use small chunks to ensure parallel fetching kicks in
    reader = ObstoreParallelReader(
        store=store, path="test.bin", chunk_size=1000, batch_size=8
    )

    # Read a large chunk that spans multiple batches
    chunk = reader.read(50000)
    assert chunk == data[:50000]
    assert reader.tell() == 50000

    reader.close()


def test_parallel_reader_context_manager() -> None:
    """Test context manager properly closes resources."""
    store = MemoryStore()
    store.put("test.bin", b"hello world")

    with ObstoreParallelReader(store=store, path="test.bin") as reader:
        data = reader.readall()
        assert data == b"hello world"

    # After closing, operations should raise
    try:
        reader.read(1)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_parallel_reader_read_beyond_eof() -> None:
    """Test reading beyond end of file."""
    store = MemoryStore()
    store.put("test.bin", b"short")

    reader = ObstoreParallelReader(store=store, path="test.bin", chunk_size=100)

    # Read more than available
    data = reader.read(1000)
    assert data == b"short"
    assert reader.tell() == 5

    # Read at EOF returns empty
    data = reader.read(100)
    assert data == b""

    reader.close()


def test_parallel_reader_caching() -> None:
    """Test that the LRU cache works correctly."""
    store = MemoryStore()
    data = bytes(range(256)) * 10  # 2560 bytes
    store.put("test.bin", data)

    # Small cache to test eviction
    reader = ObstoreParallelReader(
        store=store, path="test.bin", chunk_size=100, max_cached_chunks=3
    )

    # Read first 100 bytes (caches chunk 0)
    chunk0 = reader.read(100)
    assert chunk0 == data[:100]

    # Seek to chunk 5 and read (should trigger cache eviction)
    reader.seek(500)
    chunk5 = reader.read(100)
    assert chunk5 == data[500:600]

    # Seek back to start and read again (chunk 0 should be re-fetched)
    reader.seek(0)
    chunk0_again = reader.read(100)
    assert chunk0_again == data[:100]

    reader.close()


# =============================================================================
# ObstoreHybridReader Tests
# =============================================================================


def test_hybrid_reader(local_netcdf4_file) -> None:
    """Test that ObstoreHybridReader works with xarray."""
    ds_fsspec = xr.open_dataset(local_netcdf4_file, engine="h5netcdf")
    with ObstoreHybridReader(store=LocalStore(), path=local_netcdf4_file) as reader:
        ds_obstore = xr.open_dataset(reader, engine="h5netcdf")
        xr.testing.assert_allclose(ds_fsspec, ds_obstore)


def test_hybrid_reader_interface(local_netcdf4_file) -> None:
    """Test that ObstoreHybridReader implements the same interface as ObstoreReader."""
    store = LocalStore()
    regular_reader = ObstoreReader(store=store, path=local_netcdf4_file)
    hybrid_reader = ObstoreHybridReader(store=store, path=local_netcdf4_file)

    # Test readall
    data_regular = regular_reader.readall()
    data_hybrid = hybrid_reader.readall()
    assert data_regular == data_hybrid
    assert isinstance(data_hybrid, bytes)

    hybrid_reader.close()


def test_hybrid_reader_multiple_reads(local_netcdf4_file) -> None:
    """Test that ObstoreHybridReader can perform multiple reads."""
    store = LocalStore()
    reader = ObstoreHybridReader(store=store, path=local_netcdf4_file)

    # Read the first 100 bytes
    chunk1 = reader.read(100)
    assert len(chunk1) == 100
    assert isinstance(chunk1, bytes)

    # Read the next 100 bytes
    chunk2 = reader.read(100)
    assert len(chunk2) == 100
    assert isinstance(chunk2, bytes)

    # The two chunks should be different (different parts of the file)
    assert chunk1 != chunk2

    # Test tell
    position = reader.tell()
    assert position == 200

    # Test seek
    reader.seek(0)
    assert reader.tell() == 0

    # Re-reading from the beginning should give us the same data
    chunk1_again = reader.read(100)
    assert chunk1 == chunk1_again

    reader.close()


def test_hybrid_reader_exponential_readahead() -> None:
    """Test that the exponential readahead cache grows correctly."""
    store = MemoryStore()
    data = bytes(range(256)) * 100  # 25600 bytes
    store.put("test.bin", data)

    reader = ObstoreHybridReader(
        store=store,
        path="test.bin",
        initial_readahead=100,
        readahead_multiplier=2.0,
    )

    # First read triggers initial readahead (100 bytes)
    chunk = reader.read(50)
    assert chunk == data[:50]
    assert reader._seq_len >= 100  # Should have fetched at least initial size

    # Sequential read should use cached data or extend
    chunk = reader.read(100)
    assert chunk == data[50:150]

    # After multiple reads, readahead should have grown
    reader.read(500)
    # Readahead should have grown: 100 -> 200 -> 400 -> ...
    assert reader._last_readahead_size >= 100

    reader.close()


def test_hybrid_reader_random_access() -> None:
    """Test that random access uses chunk-based fetching."""
    store = MemoryStore()
    data = bytes(range(256)) * 100  # 25600 bytes
    store.put("test.bin", data)

    reader = ObstoreHybridReader(
        store=store,
        path="test.bin",
        initial_readahead=100,
        chunk_size=500,
    )

    # First read from start (uses sequential cache)
    reader.read(50)
    assert reader._seq_len > 0

    # Seek way past sequential cache
    reader.seek(10000)
    chunk = reader.read(100)
    assert chunk == data[10000:10100]

    # Should have used chunk cache, not extended sequential
    # Sequential cache should still be small
    assert reader._seq_len < 1000

    reader.close()


def test_hybrid_reader_seek_whence() -> None:
    """Test seek with different whence values."""
    store = MemoryStore()
    data = b"0123456789" * 100  # 1000 bytes
    store.put("test.bin", data)

    reader = ObstoreHybridReader(store=store, path="test.bin")

    # whence=0 (SEEK_SET): from start
    pos = reader.seek(500)
    assert pos == 500
    assert reader.tell() == 500

    # whence=1 (SEEK_CUR): from current position
    pos = reader.seek(100, 1)
    assert pos == 600
    assert reader.tell() == 600

    # whence=2 (SEEK_END): from end
    pos = reader.seek(-100, 2)
    assert pos == 900
    assert reader.tell() == 900

    reader.close()


def test_hybrid_reader_context_manager() -> None:
    """Test context manager properly closes resources."""
    store = MemoryStore()
    store.put("test.bin", b"hello world")

    with ObstoreHybridReader(store=store, path="test.bin") as reader:
        data = reader.readall()
        assert data == b"hello world"

    # After closing, operations should raise
    try:
        reader.read(1)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_hybrid_reader_read_beyond_eof() -> None:
    """Test reading beyond end of file."""
    store = MemoryStore()
    store.put("test.bin", b"short")

    reader = ObstoreHybridReader(store=store, path="test.bin")

    # Read more than available
    data = reader.read(1000)
    assert data == b"short"
    assert reader.tell() == 5

    # Read at EOF returns empty
    data = reader.read(100)
    assert data == b""

    reader.close()


def test_hybrid_reader_mixed_access_pattern() -> None:
    """Test mixed sequential and random access patterns."""
    store = MemoryStore()
    data = bytes(range(256)) * 200  # 51200 bytes
    store.put("test.bin", data)

    reader = ObstoreHybridReader(
        store=store,
        path="test.bin",
        initial_readahead=1000,
        chunk_size=2000,
    )

    # Start with sequential reads (uses readahead cache)
    chunk1 = reader.read(500)
    assert chunk1 == data[:500]

    chunk2 = reader.read(500)
    assert chunk2 == data[500:1000]

    # Jump to random location (uses chunk cache)
    reader.seek(40000)
    chunk3 = reader.read(1000)
    assert chunk3 == data[40000:41000]

    # Jump back near start (should still use sequential cache)
    reader.seek(100)
    chunk4 = reader.read(200)
    assert chunk4 == data[100:300]

    # Jump to another random location
    reader.seek(25000)
    chunk5 = reader.read(500)
    assert chunk5 == data[25000:25500]

    reader.close()
