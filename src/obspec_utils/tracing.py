"""Request tracing utilities for obspec-utils.

This module provides wrappers to trace HTTP/S3 range requests made by stores,
useful for debugging, profiling, and visualizing access patterns.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Callable

from obspec_utils.obspec import ReadableStore


@dataclass
class RequestRecord:
    """Record of a single range request."""

    path: str
    start: int
    length: int
    end: int  # start + length
    timestamp: float
    duration: float | None = None
    method: str = "get_range"  # "get_range", "get_ranges", "get"


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
        method: str = "get_range",
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


class TracingStore:
    """
    A wrapper that traces all requests made to an underlying store.

    This wrapper implements the ReadableStore protocol and records all
    get_range/get_ranges calls for later analysis.

    Examples
    --------
    ```python
    import obstore as obs
    from obspec_utils.tracing import TracingStore, RequestTrace

    # Create the underlying store
    store = obs.store.from_url("s3://bucket", region="us-east-1")

    # Wrap with tracing
    trace = RequestTrace()
    traced_store = TracingStore(store, trace)

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
            The underlying store to wrap.
        trace
            RequestTrace instance to record requests to.
        on_request
            Optional callback called for each request (e.g., for logging).
        """
        self._store = store
        self._trace = trace
        self._on_request = on_request

    def _record(
        self,
        path: str,
        start: int,
        length: int,
        duration: float | None = None,
        method: str = "get_range",
    ) -> None:
        """Record a request and call the callback if set."""
        self._trace.add(
            path=path,
            start=start,
            length=length,
            timestamp=time.time(),
            duration=duration,
            method=method,
        )
        if self._on_request:
            self._on_request(self._trace.requests[-1])

    # Implement ReadableStore protocol by delegating to underlying store

    def get(self, path: str, *, options: dict | None = None):
        """Get entire file (delegates to underlying store)."""
        start_time = time.time()
        result = self._store.get(path, options=options)
        duration = time.time() - start_time
        # Record as a full-file get
        size = result.meta.get("size", 0) if hasattr(result, "meta") else 0
        self._record(path, 0, size, duration=duration, method="get")
        return result

    async def get_async(self, path: str, *, options: dict | None = None):
        """Get entire file async (delegates to underlying store)."""
        start_time = time.time()
        result = await self._store.get_async(path, options=options)
        duration = time.time() - start_time
        size = result.meta.get("size", 0) if hasattr(result, "meta") else 0
        self._record(path, 0, size, duration=duration, method="get")
        return result

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ):
        """Get a byte range (delegates to underlying store)."""
        # Calculate length from end if not provided
        if length is None:
            if end is None:
                raise ValueError("Either 'end' or 'length' must be provided")
            length = end - start

        start_time = time.time()
        result = self._store.get_range(path, start=start, end=end, length=length)
        duration = time.time() - start_time
        self._record(path, start, length, duration=duration, method="get_range")
        return result

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ):
        """Get a byte range async (delegates to underlying store)."""
        # Calculate length from end if not provided
        if length is None:
            if end is None:
                raise ValueError("Either 'end' or 'length' must be provided")
            length = end - start

        start_time = time.time()
        result = await self._store.get_range_async(
            path, start=start, end=end, length=length
        )
        duration = time.time() - start_time
        self._record(path, start, length, duration=duration, method="get_range")
        return result

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ):
        """Get multiple byte ranges (delegates to underlying store)."""
        # Calculate lengths from ends if not provided
        if lengths is None:
            if ends is None:
                raise ValueError("Either 'ends' or 'lengths' must be provided")
            lengths = [end - start for start, end in zip(starts, ends)]

        start_time = time.time()
        results = self._store.get_ranges(
            path, starts=starts, ends=ends, lengths=lengths
        )
        duration = time.time() - start_time

        # Record each range request
        per_request_duration = duration / len(starts) if starts else 0
        for start, length in zip(starts, lengths):
            self._record(
                path, start, length, duration=per_request_duration, method="get_ranges"
            )

        return results

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ):
        """Get multiple byte ranges async (delegates to underlying store)."""
        # Calculate lengths from ends if not provided
        if lengths is None:
            if ends is None:
                raise ValueError("Either 'ends' or 'lengths' must be provided")
            lengths = [end - start for start, end in zip(starts, ends)]

        start_time = time.time()
        results = await self._store.get_ranges_async(
            path, starts=starts, ends=ends, lengths=lengths
        )
        duration = time.time() - start_time

        per_request_duration = duration / len(starts) if starts else 0
        for start, length in zip(starts, lengths):
            self._record(
                path, start, length, duration=per_request_duration, method="get_ranges"
            )

        return results


__all__ = [
    "RequestRecord",
    "RequestTrace",
    "TracingStore",
]
