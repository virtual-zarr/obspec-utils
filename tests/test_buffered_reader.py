"""Tests specific to BufferedStoreReader."""

from io import BytesIO

import pytest
from obstore.store import MemoryStore

from obspec_utils.obspec import BufferedStoreReader
from obspec_utils.tracing import TracingReadableStore, RequestTrace


def test_buffered_reader_buffering():
    """Test that BufferedStoreReader buffering works correctly."""
    memstore = MemoryStore()
    memstore.put("test.txt", b"0123456789ABCDEF")

    reader = BufferedStoreReader(memstore, "test.txt", buffer_size=8)

    assert reader.read(2) == b"01"
    assert reader.read(2) == b"23"


class TestBufferBoundaryConditions:
    """Test buffer boundary conditions for off-by-one errors."""

    def test_read_exactly_last_byte_of_buffer(self):
        """Read exactly the last byte of a buffered region."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=5)
        reader.read(1)
        reader.seek(4)
        assert reader.read(1) == b"4"

    def test_read_at_buffer_end_boundary(self):
        """Read starting exactly at buffer_end should trigger new fetch."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        trace = RequestTrace()
        traced_store = TracingReadableStore(memstore, trace)

        reader = BufferedStoreReader(traced_store, "test.txt", buffer_size=5)

        assert reader.read(5) == b"01234"
        initial_requests = trace.total_requests

        # Position is now 5, which equals buffer_end
        # Condition: 0 <= 5 < 5 is False, so should refetch
        assert reader.read(1) == b"5"
        assert trace.total_requests > initial_requests

    def test_read_spanning_buffer_boundary(self):
        """Read that starts inside buffer but extends beyond it."""
        data = b"0123456789ABCDEF"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=5)

        assert reader.read(3) == b"012"
        assert reader.read(5) == b"34567"

    def test_read_exactly_available_bytes_from_buffer(self):
        """Read exactly the number of available bytes in buffer."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        trace = RequestTrace()
        traced_store = TracingReadableStore(memstore, trace)

        reader = BufferedStoreReader(traced_store, "test.txt", buffer_size=5)

        assert reader.read(2) == b"01"
        assert trace.total_requests == 2  # get (size) + get_range (buffer)

        # available = 5 - 2 = 3 bytes, read exactly 3
        assert reader.read(3) == b"234"
        assert trace.total_requests == 2  # served from buffer

    def test_read_one_more_than_available(self):
        """Read one byte more than available in buffer triggers refetch."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        trace = RequestTrace()
        traced_store = TracingReadableStore(memstore, trace)

        reader = BufferedStoreReader(traced_store, "test.txt", buffer_size=5)

        assert reader.read(2) == b"01"
        assert trace.total_requests == 2  # get (size) + get_range (buffer)

        # available = 3, requesting 4
        assert reader.read(4) == b"2345"
        assert trace.total_requests == 3  # refetch needed

    def test_buffer_reuse_after_backward_seek(self):
        """Seek backward within buffer should reuse buffered data."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        trace = RequestTrace()
        traced_store = TracingReadableStore(memstore, trace)

        reader = BufferedStoreReader(traced_store, "test.txt", buffer_size=5)

        assert reader.read(5) == b"01234"
        assert trace.total_requests == 2  # get (size) + get_range (buffer)

        reader.seek(2)
        assert reader.read(2) == b"23"
        assert trace.total_requests == 2  # served from buffer

    def test_buffer_exactly_matches_file_size(self):
        """Buffer size equals file size - entire file in buffer."""
        data = b"12345"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        trace = RequestTrace()
        traced_store = TracingReadableStore(memstore, trace)

        reader = BufferedStoreReader(traced_store, "test.txt", buffer_size=5)

        assert reader.read(3) == b"123"
        assert trace.total_requests == 2  # get (size) + get_range (buffer)

        reader.seek(0)
        assert reader.read(5) == b"12345"
        assert trace.total_requests == 2  # served from buffer

    def test_sequential_reads_consuming_entire_buffer(self):
        """Sequential reads that exactly consume the buffer."""
        data = b"0123456789ABCDEF"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=4)
        ref = BytesIO(data)

        for _ in range(4):
            assert reader.read(4) == ref.read(4)
            assert reader.tell() == ref.tell()

    def test_buffer_offset_calculation_at_various_positions(self):
        """Test buffer_offset = position - buffer_start at various positions."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=5)
        ref = BytesIO(data)

        for num in range(3, 8):
            reader.seek(num)
            ref.seek(num)
            assert reader.read(1) == ref.read(1)

    def test_empty_buffer_initial_state(self):
        """Empty buffer at start should trigger fetch."""
        data = b"hello"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=10)

        # Buffer is empty initially (len = 0)
        # buffer_end = buffer_start + len(buffer) = 0 + 0 = 0
        # position (0) < buffer_end (0) is False
        # So should fetch from store
        assert reader.read(5) == b"hello"

    @pytest.mark.parametrize("buffer_size", [1, 2, 3, 5, 8, 10, 16, 32])
    def test_various_buffer_sizes(self, buffer_size):
        """Test buffer logic with various buffer sizes."""
        data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=buffer_size)
        ref = BytesIO(data)

        while True:
            reader_data = reader.read(3)
            ref_data = ref.read(3)
            assert reader_data == ref_data
            if not reader_data:
                break

    @pytest.mark.parametrize("read_size", [1, 2, 3, 4, 5])
    def test_various_read_sizes_within_buffer(self, read_size):
        """Test different read sizes that should be satisfied from buffer."""
        data = b"0123456789"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=10)
        ref = BytesIO(data)

        # Fill buffer
        reader.read(1)
        reader.seek(0)
        ref.read(1)
        ref.seek(0)

        assert reader.read(read_size) == ref.read(read_size)

    def test_buffer_offset_zero_case(self):
        """Test when buffer_offset = position - buffer_start = 0."""
        data = b"hello world"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=5)

        # After first read, buffer_start = 0
        # Seek to 0, so buffer_offset = 0 - 0 = 0
        reader.read(1)
        reader.seek(0)

        # available = len(buffer) - 0 = 5
        assert reader.read(5) == b"hello"

    def test_buffer_offset_max_case(self):
        """Test when buffer_offset = len(buffer) - 1 (last valid offset)."""
        data = b"01234"
        memstore = MemoryStore()
        memstore.put("test.txt", data)

        reader = BufferedStoreReader(memstore, "test.txt", buffer_size=5)

        # Fill buffer with all 5 bytes
        reader.read(1)

        # Seek to last byte position (4)
        # buffer_offset = 4 - 0 = 4
        # available = 5 - 4 = 1
        reader.seek(4)
        assert reader.read(1) == b"4"
