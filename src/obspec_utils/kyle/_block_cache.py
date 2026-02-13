from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from cachetools import LRUCache
from obspec import GetRange, GetRangeAsync, GetRanges, GetRangesAsync

if TYPE_CHECKING:
    from collections.abc import Buffer, Sequence


class GetRangeAndGetRanges(GetRange, GetRanges, Protocol):
    """Protocol for backends supporting both GetRange and GetRanges."""

    pass


class GetRangeAsyncAndGetRangesAsync(GetRangeAsync, GetRangesAsync, Protocol):
    """Protocol for backends supporting both GetRangeAsync and GetRangesAsync."""

    pass


@dataclass
class MemoryCache:
    """Block-aligned LRU memory cache for remote data."""

    block_size: int = 4 * 1024 * 1024  # 4 MiB
    max_blocks: int = 128  # 512 MiB default

    # (path, block_index) -> block_data (may be smaller than block_size at EOF)
    _blocks: LRUCache[tuple[str, int], bytes] = field(init=False)

    def __post_init__(self) -> None:
        self._blocks = LRUCache(maxsize=self.max_blocks)

    def _block_index(self, offset: int) -> int:
        """Which block contains this byte offset."""
        return offset // self.block_size

    def _block_start(self, block_idx: int) -> int:
        """Starting byte offset of a block."""
        return block_idx * self.block_size

    def get(self, path: str, start: int, end: int) -> bytes | list[tuple[int, int]]:
        """Get data from cache, or return missing ranges to fetch.

        Returns:
            bytes if fully cached, or list of (start, end) ranges that need fetching.
            Missing ranges are block-aligned and coalesced based on COALESCE_BLOCKS.
        """
        start_block = self._block_index(start)
        end_block = self._block_index(end - 1)  # -1 because end is exclusive

        # First pass: identify which blocks are missing
        missing_blocks: list[int] = []
        hit_eof = False

        for block_idx in range(start_block, end_block + 1):
            key = (path, block_idx)
            if key not in self._blocks:
                if not hit_eof:
                    missing_blocks.append(block_idx)
            else:
                # Check if this cached block is partial (EOF marker)
                if len(self._blocks[key]) < self.block_size:
                    hit_eof = True

        if missing_blocks:
            return self._coalesce_missing_blocks(missing_blocks)

        # All blocks cached - assemble result
        result = bytearray(end - start)
        result_offset = 0

        for block_idx in range(start_block, end_block + 1):
            block_data = self._blocks[(path, block_idx)]
            block_start = self._block_start(block_idx)

            # Calculate slice within this block
            slice_start = max(0, start - block_start)
            slice_end = min(len(block_data), end - block_start)
            chunk = block_data[slice_start:slice_end]

            result[result_offset : result_offset + len(chunk)] = chunk
            result_offset += len(chunk)

            # If this block is smaller than block_size, we hit EOF
            if len(block_data) < self.block_size:
                break

        # Truncate if we hit EOF before filling the buffer
        return bytes(result[:result_offset])

    def _coalesce_missing_blocks(
        self, missing_blocks: list[int]
    ) -> list[tuple[int, int]]:
        """Coalesce consecutive missing blocks into ranges.

        Adjacent missing blocks are always coalesced. Non-adjacent missing blocks
        (with cached blocks in between) are kept as separate ranges to avoid
        re-fetching cached data.
        """
        if not missing_blocks:
            return []

        ranges: list[tuple[int, int]] = []
        range_start = missing_blocks[0]
        range_end = missing_blocks[0]

        for block_idx in missing_blocks[1:]:
            # Only coalesce if blocks are adjacent (gap of 1 means consecutive)
            if block_idx - range_end == 1:
                range_end = block_idx
            else:
                # There's a gap (cached block in between), start new range
                ranges.append(
                    (
                        self._block_start(range_start),
                        self._block_start(range_end + 1),
                    )
                )
                range_start = block_idx
                range_end = block_idx

        # Don't forget the last range
        ranges.append(
            (
                self._block_start(range_start),
                self._block_start(range_end + 1),
            )
        )

        return ranges

    def store(self, path: str, fetch_start: int, data: Buffer) -> None:
        """Store fetched data as blocks. fetch_start must be block-aligned.

        The last block may be smaller than block_size if we hit EOF.
        """
        assert fetch_start % self.block_size == 0, "fetch_start must be block-aligned"

        data_bytes = bytes(data)
        offset = 0
        block_idx = fetch_start // self.block_size

        while offset < len(data_bytes):
            block_data = data_bytes[offset : offset + self.block_size]
            self._blocks[(path, block_idx)] = block_data
            offset += self.block_size
            block_idx += 1


@dataclass
class SyncBlockCache:
    """Synchronous block cache wrapping a GetRange backend."""

    backend: GetRangeAndGetRanges
    cache: MemoryCache = field(default_factory=MemoryCache)

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> bytes:
        if end is None:
            if length is None:
                raise ValueError("Either end or length must be provided")
            end = start + length

        result = self.cache.get(path, start, end)
        if isinstance(result, list):
            # result is list of missing ranges - fetch them
            self._fetch_missing(path, result)
            # Now should be cached
            result = self.cache.get(path, start, end)
            assert isinstance(result, bytes)

        return result

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[bytes]:
        """Return the bytes stored at the specified location in the given byte ranges."""
        if ends is None:
            if lengths is None:
                raise ValueError("Either ends or lengths must be provided")
            ends = [s + length for s, length in zip(starts, lengths)]

        # Collect all missing ranges across all requests
        all_missing: list[tuple[int, int]] = []
        for start, end in zip(starts, ends):
            result = self.cache.get(path, start, end)
            if isinstance(result, list):
                all_missing.extend(result)

        # Fetch all missing ranges in one batch
        if all_missing:
            self._fetch_missing(path, all_missing)

        # Now all should be cached - collect results
        results: list[bytes] = []
        for start, end in zip(starts, ends):
            result = self.cache.get(path, start, end)
            assert isinstance(result, bytes)
            results.append(result)

        return results

    def _fetch_missing(self, path: str, ranges: list[tuple[int, int]]) -> None:
        """Fetch missing ranges from backend and store in cache."""
        if len(ranges) == 1:
            start, end = ranges[0]
            data = self.backend.get_range(path, start=start, end=end)
            self.cache.store(path, start, data)
        else:
            starts = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            buffers: Sequence[Buffer] = self.backend.get_ranges(
                path, starts=starts, ends=ends
            )
            for (range_start, _), data in zip(ranges, buffers):
                self.cache.store(path, range_start, data)


@dataclass
class AsyncBlockCache(GetRangeAsync, GetRangesAsync):
    """Async block cache wrapping a GetRangeAsync backend."""

    backend: GetRangeAsyncAndGetRangesAsync
    cache: MemoryCache = field(default_factory=MemoryCache)

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> bytes:
        if end is None:
            if length is None:
                raise ValueError("Either end or length must be provided")
            end = start + length

        result = self.cache.get(path, start, end)
        if isinstance(result, list):
            # result is list of missing ranges - fetch them
            await self._fetch_missing(path, result)
            # Now should be cached
            result = self.cache.get(path, start, end)
            assert isinstance(result, bytes)

        return result

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[bytes]:
        """Return the bytes stored at the specified location in the given byte ranges."""
        if ends is None:
            if lengths is None:
                raise ValueError("Either ends or lengths must be provided")
            ends = [s + length for s, length in zip(starts, lengths)]

        # Collect all missing ranges across all requests
        all_missing: list[tuple[int, int]] = []
        for start, end in zip(starts, ends):
            result = self.cache.get(path, start, end)
            if isinstance(result, list):
                all_missing.extend(result)

        # Fetch all missing ranges in one batch
        if all_missing:
            await self._fetch_missing(path, all_missing)

        # Now all should be cached - collect results
        results: list[bytes] = []
        for start, end in zip(starts, ends):
            result = self.cache.get(path, start, end)
            assert isinstance(result, bytes)
            results.append(result)

        return results

    async def _fetch_missing(self, path: str, ranges: list[tuple[int, int]]) -> None:
        """Fetch missing ranges from backend and store in cache."""
        if len(ranges) == 1:
            start, end = ranges[0]
            data = await self.backend.get_range_async(path, start=start, end=end)
            self.cache.store(path, start, data)
        else:
            starts = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            buffers: Sequence[Buffer] = await self.backend.get_ranges_async(
                path, starts=starts, ends=ends
            )
            for (range_start, _), data in zip(ranges, buffers):
                self.cache.store(path, range_start, data)
