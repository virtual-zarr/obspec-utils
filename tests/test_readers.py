"""
Common tests for all reader classes, parameterized across BufferedStoreReader,
EagerStoreReader, and ParallelStoreReader.
"""

import pickle
from io import BytesIO

import pytest
from obstore.store import MemoryStore

from obspec_utils.obspec import (
    BufferedStoreReader,
    EagerStoreReader,
    ParallelStoreReader,
)

from .mocks import PicklableStore


ALL_READERS = [BufferedStoreReader, EagerStoreReader, ParallelStoreReader]


# =============================================================================
# Basic operations
# =============================================================================


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_basic_operations(ReaderClass):
    """Test basic read, seek, tell operations for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world from store reader")

    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(5) == b"hello"
    assert reader.tell() == 5

    reader.seek(6)
    assert reader.read(5) == b"world"

    reader.seek(-5, 1)
    assert reader.read(5) == b"world"

    reader.seek(0)
    assert reader.readall() == b"hello world from store reader"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_end(ReaderClass):
    """Test SEEK_END functionality for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789")

    reader = ReaderClass(memstore, "test.txt")
    reader.seek(-2, 2)
    assert reader.read(2) == b"89"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_all_seek_modes(ReaderClass):
    """Test all seek modes for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = ReaderClass(memstore, "test.txt")

    reader.seek(5)
    assert reader.tell() == 5
    assert reader.read(3) == b"567"

    reader.seek(-3, 1)
    assert reader.tell() == 5
    assert reader.read(3) == b"567"

    reader.seek(-4, 2)
    assert reader.read(4) == b"CDEF"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_past_end(ReaderClass):
    """Test reading past end of file for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"short")

    reader = ReaderClass(memstore, "test.txt")
    assert reader.read(100) == b"short"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_minus_one(ReaderClass):
    """Test read(-1) reads entire file for all readers."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    assert reader.read(-1) == b"hello world"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_minus_one_from_middle(ReaderClass):
    """Test read(-1) reads from current position to end."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    reader.seek(6)
    assert reader.read(-1) == b"world"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_context_manager(ReaderClass):
    """Test that readers work as context managers and release resources."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    with ReaderClass(memstore, "test.txt") as reader:
        assert reader.read(5) == b"hello"
        assert reader.tell() == 5

    if hasattr(reader, "_buffer"):
        if isinstance(reader._buffer, bytes):
            assert reader._buffer == b""
        else:
            assert reader._buffer.getvalue() == b""
    if hasattr(reader, "_cache"):
        assert len(reader._cache) == 0


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_close(ReaderClass):
    """Test that readers can be explicitly closed."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"hello world")

    reader = ReaderClass(memstore, "test.txt")
    assert reader.read(5) == b"hello"
    reader.close()

    if hasattr(reader, "_buffer"):
        if isinstance(reader._buffer, bytes):
            assert reader._buffer == b""
        else:
            assert reader._buffer.getvalue() == b""
    if hasattr(reader, "_cache"):
        assert len(reader._cache) == 0


# =============================================================================
# BytesIO consistency tests
# =============================================================================


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_matches_bytesio(ReaderClass):
    """Reader read(n) matches BytesIO behavior."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(5) == ref.read(5)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_zero_matches_bytesio(ReaderClass):
    """Reader read(0) returns empty bytes like BytesIO."""
    data = b"hello world"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(0) == ref.read(0)
    assert reader.read(0) == b""
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_all_matches_bytesio(ReaderClass):
    """Reader read(-1) matches BytesIO.read(-1)."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(-1) == ref.read(-1)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_no_arg_matches_bytesio(ReaderClass):
    """Reader read() with no argument matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_sequential_reads_match_bytesio(ReaderClass):
    """Multiple consecutive reads match BytesIO behavior."""
    data = b"0123456789ABCDEF"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    for _ in range(4):
        assert reader.read(4) == ref.read(4)
        assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_set_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_SET) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(5) == ref.seek(5)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_cur_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_CUR) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    reader.read(5)
    ref.read(5)

    assert reader.seek(3, 1) == ref.seek(3, 1)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_end_matches_bytesio(ReaderClass):
    """Reader seek(n, SEEK_END) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(-5, 2) == ref.seek(-5, 2)
    assert reader.tell() == ref.tell()
    assert reader.read() == ref.read()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_returns_position_matches_bytesio(ReaderClass):
    """Reader seek() return value matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.seek(10) == ref.seek(10)
    assert reader.seek(5, 1) == ref.seek(5, 1)
    assert reader.seek(-3, 2) == ref.seek(-3, 2)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_tell_matches_bytesio(ReaderClass):
    """Reader tell() matches BytesIO after various operations."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.tell() == ref.tell()
    reader.read(5)
    ref.read(5)
    assert reader.tell() == ref.tell()
    reader.seek(10)
    ref.seek(10)
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_read_past_eof_matches_bytesio(ReaderClass):
    """Reading past EOF matches BytesIO behavior."""
    data = b"short"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(100) == ref.read(100)
    assert reader.tell() == ref.tell()
    assert reader.read(10) == ref.read(10)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_negative_cur_matches_bytesio(ReaderClass):
    """Reader seek(-n, SEEK_CUR) matches BytesIO."""
    data = b"hello world test data"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    reader.read(10)
    ref.read(10)

    assert reader.seek(-5, 1) == ref.seek(-5, 1)
    assert reader.tell() == ref.tell()
    assert reader.read(5) == ref.read(5)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_empty_file_matches_bytesio(ReaderClass):
    """Empty file behavior matches BytesIO."""
    data = b""

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()
    assert reader.read(10) == ref.read(10)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_read_sequence_matches_bytesio(ReaderClass):
    """Interleaved seek/read operations match BytesIO."""
    data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    assert reader.read(10) == ref.read(10)
    assert reader.seek(5) == ref.seek(5)
    assert reader.read(5) == ref.read(5)
    assert reader.seek(-3, 1) == ref.seek(-3, 1)
    assert reader.read(10) == ref.read(10)
    assert reader.seek(-5, 2) == ref.seek(-5, 2)
    assert reader.read() == ref.read()
    assert reader.tell() == ref.tell()


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_seek_invalid_whence_raises(ReaderClass):
    """Reader raises ValueError for invalid whence like BytesIO."""
    data = b"hello world"

    ref = BytesIO(data)
    memstore = MemoryStore()
    memstore.put("test.txt", data)
    reader = ReaderClass(memstore, "test.txt")

    with pytest.raises(ValueError):
        ref.seek(0, 3)

    with pytest.raises(ValueError):
        reader.seek(0, 3)


# =============================================================================
# Pickling tests
# =============================================================================


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_pickle_roundtrip(ReaderClass):
    """Reader can be pickled and unpickled."""
    store = PicklableStore()
    store.put("test.txt", b"hello world")

    reader = ReaderClass(store, "test.txt")

    pickled = pickle.dumps(reader)
    restored = pickle.loads(pickled)

    assert isinstance(restored, ReaderClass)


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_pickle_preserves_path(ReaderClass):
    """Unpickled reader preserves the file path."""
    store = PicklableStore()
    store.put("test.txt", b"hello world")

    reader = ReaderClass(store, "test.txt")
    restored = pickle.loads(pickle.dumps(reader))

    assert restored._path == "test.txt"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_pickle_restored_is_functional(ReaderClass):
    """Restored reader can read data."""
    store = PicklableStore()
    store.put("test.txt", b"hello world")

    reader = ReaderClass(store, "test.txt")
    restored = pickle.loads(pickle.dumps(reader))

    # Should be able to read data
    assert restored.read(5) == b"hello"
    assert restored.read(6) == b" world"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_pickle_preserves_position(ReaderClass):
    """Unpickled reader preserves the current position."""
    store = PicklableStore()
    store.put("test.txt", b"hello world")

    reader = ReaderClass(store, "test.txt")
    reader.read(5)  # Move position to 5
    assert reader.tell() == 5

    restored = pickle.loads(pickle.dumps(reader))

    assert restored.tell() == 5
    assert restored.read(6) == b" world"


@pytest.mark.parametrize("ReaderClass", ALL_READERS)
def test_reader_pickle_multiple_protocols(ReaderClass):
    """Pickling works with different pickle protocols.

    Note: EagerStoreReader uses BytesIO which requires protocol >= 2.
    """
    store = PicklableStore()
    store.put("test.txt", b"hello world")

    reader = ReaderClass(store, "test.txt")

    # BytesIO (used by EagerStoreReader) requires protocol >= 2
    min_protocol = 2 if ReaderClass == EagerStoreReader else 0

    for protocol in range(min_protocol, pickle.HIGHEST_PROTOCOL + 1):
        pickled = pickle.dumps(reader, protocol=protocol)
        restored = pickle.loads(pickled)

        restored.seek(0)
        assert restored.read() == b"hello world"
