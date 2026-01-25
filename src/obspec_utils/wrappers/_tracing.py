"""Request tracing utilities for obspec-utils.

This module provides wrappers to trace HTTP/S3 range requests made by stores,
useful for debugging, profiling, and visualizing access patterns.
"""

from __future__ import annotations

import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, TypedDict

from obspec_utils.protocols import ReadableStore

if TYPE_CHECKING:
    from collections.abc import Buffer

    from obspec import GetOptions, GetResult, GetResultAsync, ObjectMeta


class _TraceInfo(TypedDict, total=False):
    """Info collected during a traced operation."""

    start: int
    length: int
    range_style: Literal["end", "length"] | None


@dataclass
class RequestRecord:
    """Record of a single range request.

    Note
    ----
    The ``duration`` field measures the time spent in the store method call.
    For ``get_range`` and ``get_ranges``, this includes the actual data transfer.
    For ``get`` and ``get_async``, the duration may not include the full transfer
    time if the underlying store returns a lazy ``GetResult`` whose data is only
    fetched when ``.buffer()`` is called.
    """

    path: str
    start: int
    length: int
    end: int  # start + length
    timestamp: float
    duration: float | None = None
    method: Literal["get", "get_range", "get_ranges", "head"] = "get_range"
    range_style: Literal["end", "length"] | None = None


@dataclass
class RequestTrace:
    """Collection of request records with analysis methods."""

    requests: list[RequestRecord] = field(default_factory=list)

    def add(
        self,
        path: str,
        start: int,
        length: int,
        timestamp: float,
        duration: float | None = None,
        method: Literal["get", "get_range", "get_ranges", "head"] = "get_range",
        range_style: Literal["end", "length"] | None = None,
    ) -> None:
        """Add a request record."""
        self.requests.append(
            RequestRecord(
                path=path,
                start=start,
                length=length,
                end=start + length,
                timestamp=timestamp,
                duration=duration,
                method=method,
                range_style=range_style,
            )
        )

    def clear(self) -> None:
        """Clear all recorded requests."""
        self.requests.clear()

    def to_dataframe(self):
        """Convert to pandas DataFrame."""
        import pandas as pd

        if not self.requests:
            return pd.DataFrame(
                columns=[
                    "path",
                    "start",
                    "length",
                    "end",
                    "timestamp",
                    "duration",
                    "method",
                    "range_style",
                ]
            )

        return pd.DataFrame(
            [
                {
                    "path": r.path,
                    "start": r.start,
                    "length": r.length,
                    "end": r.end,
                    "timestamp": r.timestamp,
                    "duration": r.duration,
                    "method": r.method,
                    "range_style": r.range_style,
                }
                for r in self.requests
            ]
        )

    @property
    def total_bytes(self) -> int:
        """Total bytes requested."""
        return sum(r.length for r in self.requests)

    @property
    def total_requests(self) -> int:
        """Total number of requests."""
        return len(self.requests)

    def summary(self) -> dict[str, Any]:
        """Get summary statistics."""
        if not self.requests:
            return {
                "total_requests": 0,
                "total_bytes": 0,
                "unique_files": 0,
            }

        paths = set(r.path for r in self.requests)
        lengths = [r.length for r in self.requests]

        return {
            "total_requests": len(self.requests),
            "total_bytes": sum(lengths),
            "unique_files": len(paths),
            "min_request_size": min(lengths),
            "max_request_size": max(lengths),
            "mean_request_size": sum(lengths) / len(lengths),
        }


class TracingReadableStore(ReadableStore):
    """
    A wrapper that traces all requests made to an underlying store.

    This wrapper records all get/get_range/get_ranges calls for later analysis.

    Examples
    --------
    ```python
    import obstore as obs
    from obspec_utils.wrappers import TracingReadableStore, RequestTrace

    # Create the underlying store
    store = obs.store.from_url("s3://bucket", region="us-east-1")

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingReadableStore(store, trace)

    # Use traced_store in place of store
    # ... do operations ...

    # Analyze the trace
    df = trace.to_dataframe()
    print(trace.summary())
    ```
    """

    def __init__(
        self,
        store: ReadableStore,
        trace: RequestTrace,
        *,
        on_request: Callable[[RequestRecord], None] | None = None,
    ) -> None:
        """
        Create a tracing wrapper around a store.

        Parameters
        ----------
        store
            Any object implementing the full read interface: [Get][obspec.Get],
            [GetAsync][obspec.GetAsync], [GetRange][obspec.GetRange],
            [GetRangeAsync][obspec.GetRangeAsync], [GetRanges][obspec.GetRanges],
            [GetRangesAsync][obspec.GetRangesAsync], [Head][obspec.Head],
            and [HeadAsync][obspec.HeadAsync].
        trace
            RequestTrace instance to record requests to.
        on_request
            Optional callback called for each request (e.g., for logging).
        """
        self._store = store
        self._trace = trace
        self._on_request = on_request

    def __getattr__(self, name: str) -> Any:
        """Forward unknown attributes to the underlying store.

        This ensures TracingReadableStore is transparent for any additional
        methods the underlying store may have (e.g., head() for the Head protocol).
        """
        return getattr(self._store, name)

    @contextmanager
    def _record(
        self,
        path: str,
        method: Literal["get", "get_range", "get_ranges", "head"],
    ) -> Generator[_TraceInfo, None, None]:
        """Context manager to record a request with automatic timing.

        Yields a dict that the caller populates with start, length, and range_style.
        Duration is measured automatically. Records are saved even if the operation
        raises an exception.
        """
        info: _TraceInfo = {}
        start_time = time.time()
        try:
            yield info
        finally:
            duration = time.time() - start_time
            self._trace.add(
                path=path,
                start=info.get("start", 0),
                length=info.get("length", 0),
                timestamp=start_time,
                duration=duration,
                method=method,
                range_style=info.get("range_style"),
            )
            if self._on_request:
                self._on_request(self._trace.requests[-1])

    def _record_ranges(
        self,
        path: str,
        starts: Sequence[int],
        lengths: Sequence[int],
        range_style: Literal["end", "length"],
        duration: float,
    ) -> None:
        """Record multiple range requests from a single get_ranges call."""
        per_request_duration = duration / len(starts) if starts else 0
        timestamp = time.time()
        for start, length in zip(starts, lengths):
            self._trace.add(
                path=path,
                start=start,
                length=length,
                timestamp=timestamp,
                duration=per_request_duration,
                method="get_ranges",
                range_style=range_style,
            )
            if self._on_request:
                self._on_request(self._trace.requests[-1])

    # Implement ReadableStore protocol by delegating to underlying store

    def get(self, path: str, *, options: GetOptions | None = None) -> GetResult:
        """Get entire file (delegates to underlying store)."""
        with self._record(path, "get") as info:
            result = self._store.get(path, options=options)
            size = result.meta.get("size", 0) if hasattr(result, "meta") else 0
            info["start"] = 0
            info["length"] = size
            return result

    async def get_async(
        self, path: str, *, options: GetOptions | None = None
    ) -> GetResultAsync:
        """Get entire file async (delegates to underlying store)."""
        with self._record(path, "get") as info:
            result = await self._store.get_async(path, options=options)
            size = result.meta.get("size", 0) if hasattr(result, "meta") else 0
            info["start"] = 0
            info["length"] = size
            return result

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Get a byte range (delegates to underlying store)."""
        with self._record(path, "get_range") as info:
            info["start"] = start
            if length is not None:
                info["length"] = length
                info["range_style"] = "length"
                return self._store.get_range(path, start=start, length=length)
            elif end is not None:
                info["length"] = end - start
                info["range_style"] = "end"
                return self._store.get_range(path, start=start, end=end)
            else:
                raise ValueError("Either 'end' or 'length' must be provided")

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        """Get a byte range async (delegates to underlying store)."""
        with self._record(path, "get_range") as info:
            info["start"] = start
            if length is not None:
                info["length"] = length
                info["range_style"] = "length"
                return await self._store.get_range_async(
                    path, start=start, length=length
                )
            elif end is not None:
                info["length"] = end - start
                info["range_style"] = "end"
                return await self._store.get_range_async(path, start=start, end=end)
            else:
                raise ValueError("Either 'end' or 'length' must be provided")

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[Buffer]:
        """Get multiple byte ranges (delegates to underlying store)."""
        start_time = time.time()
        if lengths is not None:
            results = self._store.get_ranges(path, starts=starts, lengths=lengths)
            duration = time.time() - start_time
            self._record_ranges(path, starts, list(lengths), "length", duration)
        elif ends is not None:
            results = self._store.get_ranges(path, starts=starts, ends=ends)
            duration = time.time() - start_time
            record_lengths = [end - start for start, end in zip(starts, ends)]
            self._record_ranges(path, starts, record_lengths, "end", duration)
        else:
            raise ValueError("Either 'ends' or 'lengths' must be provided")
        return results

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[Buffer]:
        """Get multiple byte ranges async (delegates to underlying store)."""
        start_time = time.time()
        if lengths is not None:
            results = await self._store.get_ranges_async(
                path, starts=starts, lengths=lengths
            )
            duration = time.time() - start_time
            self._record_ranges(path, starts, list(lengths), "length", duration)
        elif ends is not None:
            results = await self._store.get_ranges_async(path, starts=starts, ends=ends)
            duration = time.time() - start_time
            record_lengths = [end - start for start, end in zip(starts, ends)]
            self._record_ranges(path, starts, record_lengths, "end", duration)
        else:
            raise ValueError("Either 'ends' or 'lengths' must be provided")
        return results

    def head(self, path: str) -> ObjectMeta:
        """Get file metadata (delegates to underlying store)."""
        with self._record(path, "head") as info:
            result = self._store.head(path)
            info["start"] = 0
            info["length"] = 0  # HEAD requests don't transfer data
            return result

    async def head_async(self, path: str) -> ObjectMeta:
        """Get file metadata async (delegates to underlying store)."""
        with self._record(path, "head") as info:
            result = await self._store.head_async(path)
            info["start"] = 0
            info["length"] = 0  # HEAD requests don't transfer data
            return result


__all__ = [
    "RequestRecord",
    "RequestTrace",
    "TracingReadableStore",
]
