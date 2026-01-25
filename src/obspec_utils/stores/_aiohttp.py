"""
Aiohttp-based implementation of the ReadableStore protocol.

This module provides an alternative HTTP backend using aiohttp instead of obstore's HTTPStore.
It's useful for generic HTTPS access (e.g., THREDDS, NASA data from outside AWS region)
where obstore's HTTPStore (designed for WebDAV/S3-like semantics) may not be ideal.

Example
-------

```python
from obspec_utils import ObjectStoreRegistry
from obspec_utils.stores import AiohttpStore

# Use the store as an async context manager for efficient session reuse
async with AiohttpStore("https://example.com/data") as store:
    # Register it with the registry
    registry = ObjectStoreRegistry({"https://example.com/data": store})

    # Now VirtualiZarr can use this store for HTTP access
    resolved_store, path = registry.resolve("https://example.com/data/file.nc")
    data = await resolved_store.get_range_async(path, start=0, end=1000)
```
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from obspec import GetResult, GetResultAsync

from obspec_utils.protocols import ReadableStore

if TYPE_CHECKING:
    from obspec import Attributes, GetOptions, ObjectMeta

try:
    import aiohttp
except ImportError as e:
    raise ImportError(
        "aiohttp is required for AiohttpStore. Install it with: pip install aiohttp"
    ) from e


@dataclass
class AiohttpGetResult(GetResult):
    """
    Result from a get request using aiohttp.

    Implements the obspec GetResult protocol for synchronous iteration.
    """

    _data: bytes
    _meta: ObjectMeta
    _attributes: Attributes = field(default_factory=dict)
    _range: tuple[int, int] = (0, 0)

    def __post_init__(self):
        if self._range == (0, 0):
            self._range = (0, len(self._data))

    @property
    def attributes(self) -> Attributes:
        """Additional object attributes."""
        return self._attributes

    def buffer(self) -> bytes:
        """Return the data as a buffer."""
        return self._data

    @property
    def meta(self) -> ObjectMeta:
        """The ObjectMeta for this object."""
        return self._meta

    @property
    def range(self) -> tuple[int, int]:
        """The range of bytes returned by this request."""
        return self._range

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over chunks of the data."""
        yield self._data


@dataclass
class AiohttpGetResultAsync(GetResultAsync):
    """
    Result from an async get request using aiohttp.

    Implements the obspec GetResultAsync protocol for asynchronous iteration.
    """

    _data: bytes
    _meta: ObjectMeta
    _attributes: Attributes = field(default_factory=dict)
    _range: tuple[int, int] = (0, 0)

    def __post_init__(self):
        if self._range == (0, 0):
            self._range = (0, len(self._data))

    @property
    def attributes(self) -> Attributes:
        """Additional object attributes."""
        return self._attributes

    async def buffer_async(self) -> bytes:
        """Return the data as a buffer."""
        return self._data

    @property
    def meta(self) -> ObjectMeta:
        """The ObjectMeta for this object."""
        return self._meta

    @property
    def range(self) -> tuple[int, int]:
        """The range of bytes returned by this request."""
        return self._range

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Async iterate over chunks of the data."""
        yield self._data


class AiohttpStore(ReadableStore):
    """
    An [aiohttp](https://docs.aiohttp.org/en/stable/)-based object store implementation.

    This provides a lightweight alternative to obstore's [HTTPStore][obstore.store.HTTPStore] for generic
    HTTP/HTTPS access. It's particularly useful for:

    - THREDDS data servers
    - NASA data access from outside AWS regions
    - Any generic HTTP endpoint that doesn't need S3-like semantics

    The store should be used as an async context manager to efficiently reuse
    a single HTTP session across multiple requests.

    Parameters
    ----------
    base_url
        The base URL for this store. All paths are resolved relative to this URL.
    headers
        Optional HTTP headers to include in all requests (e.g., authentication).
    timeout
        Request timeout in seconds. Default is 30.

    Examples
    --------

    Recommended usage with async context manager:

    ```python
    async with AiohttpStore("https://example.com/data") as store:
        # All requests share the same session
        result = await store.get_async("file.nc")
        data = await result.buffer_async()

        # Byte range requests
        chunk = await store.get_range_async("file.nc", start=0, end=1000)
    ```

    Synchronous usage (creates a session per request):

    ```python
    store = AiohttpStore("https://example.com/data")
    result = store.get("file.nc")
    data = result.buffer()
    ```

    With authentication:

    ```python
    async with AiohttpStore(
        "https://api.example.com/data",
        headers={"Authorization": "Bearer <token>"}
    ) as store:
        result = await store.get_async("protected/file.nc")
    ```
    """

    def __init__(
        self,
        base_url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "AiohttpStore":
        """Enter the async context manager, creating a reusable session."""
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers=self.headers,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the async context manager, closing the session."""
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _build_url(self, path: str) -> str:
        """Build the full URL from base URL and path."""
        path = path.removeprefix("/")
        return f"{self.base_url}/{path}" if path else self.base_url

    def _get_header_case_insensitive(
        self, headers: dict, name: str, default: str | None = None
    ) -> str | None:
        """Get a header value with case-insensitive name lookup."""
        # Try exact match first
        if name in headers:
            return headers[name]
        # Try case-insensitive lookup
        name_lower = name.lower()
        for key, value in headers.items():
            if key.lower() == name_lower:
                return value
        return default

    def _parse_meta_from_headers(
        self, path: str, headers: dict, content_length: int | None = None
    ) -> ObjectMeta:
        """Extract ObjectMeta from HTTP response headers."""
        # Parse last-modified header
        last_modified_str = self._get_header_case_insensitive(headers, "Last-Modified")
        if last_modified_str:
            # Parse HTTP date format
            try:
                from email.utils import parsedate_to_datetime

                last_modified = parsedate_to_datetime(last_modified_str)
            except (ValueError, TypeError):
                last_modified = datetime.now(timezone.utc)
        else:
            last_modified = datetime.now(timezone.utc)

        # Get size from Content-Length or Content-Range
        size = content_length or 0
        content_range = self._get_header_case_insensitive(headers, "Content-Range")
        if content_range and "/" in content_range:
            # Format: bytes 0-999/1234
            total_str = content_range.split("/")[-1]
            if total_str != "*":
                size = int(total_str)
        else:
            content_length_str = self._get_header_case_insensitive(
                headers, "Content-Length"
            )
            if content_length_str:
                size = int(content_length_str)

        return {
            "path": path,
            "last_modified": last_modified,
            "size": size,
            "e_tag": self._get_header_case_insensitive(headers, "ETag"),
            "version": None,
        }

    def _parse_attributes_from_headers(self, headers: dict) -> Attributes:
        """Extract Attributes from HTTP response headers."""
        attrs: Attributes = {}
        header_names = [
            "Content-Disposition",
            "Content-Encoding",
            "Content-Language",
            "Content-Type",
            "Cache-Control",
        ]
        for header_name in header_names:
            value = self._get_header_case_insensitive(headers, header_name)
            if value is not None:
                attrs[header_name] = value
        return attrs

    # --- Async methods (primary implementation) ---

    async def _do_get_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
        *,
        options: GetOptions | None = None,
    ) -> AiohttpGetResultAsync:
        """Internal method that performs the actual GET request."""
        url = self._build_url(path)
        request_headers = {} if self._session else dict(self.headers)

        # Handle range option if specified
        byte_range = (0, 0)
        if options and "range" in options:
            range_opt = options["range"]
            if isinstance(range_opt, tuple):
                start, end = range_opt[0], range_opt[1]
                request_headers["Range"] = f"bytes={start}-{end - 1}"
                byte_range = (start, end)
            elif isinstance(range_opt, dict):
                if (offset := range_opt.get("offset")) is not None:
                    request_headers["Range"] = f"bytes={offset}-"
                elif (suffix := range_opt.get("suffix")) is not None:
                    request_headers["Range"] = f"bytes=-{suffix}"

        async with session.get(url, headers=request_headers) as response:
            response.raise_for_status()
            data = await response.read()
            meta = self._parse_meta_from_headers(
                path, dict(response.headers), len(data)
            )
            attrs = self._parse_attributes_from_headers(dict(response.headers))

            if byte_range == (0, 0):
                byte_range = (0, len(data))

            return AiohttpGetResultAsync(
                _data=data,
                _meta=meta,
                _attributes=attrs,
                _range=byte_range,
            )

    async def get_async(
        self,
        path: str,
        *,
        options: GetOptions | None = None,
    ) -> AiohttpGetResultAsync:
        """
        Download a file asynchronously.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        options
            Optional get options (range, conditionals, etc.).

        Returns
        -------
        AiohttpGetResultAsync
            Result object with buffer_async() method and metadata.
        """
        if self._session is not None:
            return await self._do_get_async(self._session, path, options=options)

        # Fallback: create a temporary session for this request
        async with aiohttp.ClientSession(
            timeout=self.timeout, headers=self.headers
        ) as session:
            return await self._do_get_async(session, path, options=options)

    async def _do_get_range_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
        *,
        start: int,
        end: int,
    ) -> bytes:
        """Internal method that performs the actual range GET request."""
        url = self._build_url(path)
        request_headers = {} if self._session else dict(self.headers)
        # HTTP Range is inclusive on both ends, obspec end is exclusive
        request_headers["Range"] = f"bytes={start}-{end - 1}"

        async with session.get(url, headers=request_headers) as response:
            response.raise_for_status()
            return await response.read()

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> bytes:
        """
        Download a byte range asynchronously.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        start
            Start byte offset.
        end
            End byte offset (exclusive). Either end or length must be provided.
        length
            Number of bytes to read. Either end or length must be provided.

        Returns
        -------
        bytes
            The requested byte range.
        """
        if end is None and length is None:
            raise ValueError("Either 'end' or 'length' must be provided")
        if end is None:
            end = start + length  # type: ignore[operator]

        if self._session is not None:
            return await self._do_get_range_async(
                self._session, path, start=start, end=end
            )

        # Fallback: create a temporary session for this request
        async with aiohttp.ClientSession(
            timeout=self.timeout, headers=self.headers
        ) as session:
            return await self._do_get_range_async(session, path, start=start, end=end)

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[bytes]:
        """
        Download multiple byte ranges asynchronously.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        starts
            Sequence of start byte offsets.
        ends
            Sequence of end byte offsets (exclusive).
        lengths
            Sequence of lengths. Either ends or lengths must be provided.

        Returns
        -------
        Sequence[bytes]
            The requested byte ranges.
        """
        if ends is None and lengths is None:
            raise ValueError("Either 'ends' or 'lengths' must be provided")
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]  # type: ignore[arg-type]

        if self._session is not None:
            # Use managed session for all concurrent requests
            tasks = [
                self._do_get_range_async(self._session, path, start=s, end=e)
                for s, e in zip(starts, ends)
            ]
            return await asyncio.gather(*tasks)

        # Fallback: create a single temporary session for all requests
        async with aiohttp.ClientSession(
            timeout=self.timeout, headers=self.headers
        ) as session:
            tasks = [
                self._do_get_range_async(session, path, start=s, end=e)
                for s, e in zip(starts, ends)
            ]
            return await asyncio.gather(*tasks)

    # --- Sync methods (wrap async) ---

    def get(
        self,
        path: str,
        *,
        options: GetOptions | None = None,
    ) -> AiohttpGetResult:
        """
        Download a file synchronously.

        This wraps the async implementation for convenience.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        options
            Optional get options.

        Returns
        -------
        AiohttpGetResult
            Result object with buffer() method and metadata.
        """
        result = asyncio.run(self.get_async(path, options=options))
        return AiohttpGetResult(
            _data=result._data,
            _meta=result._meta,
            _attributes=result._attributes,
            _range=result._range,
        )

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> bytes:
        """
        Download a byte range synchronously.

        This wraps the async implementation for convenience.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        start
            Start byte offset.
        end
            End byte offset (exclusive).
        length
            Number of bytes to read.

        Returns
        -------
        bytes
            The requested byte range.
        """
        return asyncio.run(
            self.get_range_async(path, start=start, end=end, length=length)
        )

    def get_ranges(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[bytes]:
        """
        Download multiple byte ranges synchronously.

        This wraps the async implementation for convenience.

        Parameters
        ----------
        path
            Path to the file relative to base_url.
        starts
            Sequence of start byte offsets.
        ends
            Sequence of end byte offsets (exclusive).
        lengths
            Sequence of lengths.

        Returns
        -------
        Sequence[bytes]
            The requested byte ranges.
        """
        return asyncio.run(
            self.get_ranges_async(path, starts=starts, ends=ends, lengths=lengths)
        )

    # --- Head methods ---

    async def _do_head_async(
        self,
        session: aiohttp.ClientSession,
        path: str,
    ) -> ObjectMeta:
        """Internal method that performs the actual HEAD request."""
        url = self._build_url(path)
        request_headers = {} if self._session else dict(self.headers)

        async with session.head(url, headers=request_headers) as response:
            response.raise_for_status()
            return self._parse_meta_from_headers(path, dict(response.headers))

    async def head_async(self, path: str) -> ObjectMeta:
        """
        Get file metadata asynchronously via HEAD request.

        Parameters
        ----------
        path
            Path to the file relative to base_url.

        Returns
        -------
        ObjectMeta
            File metadata including size, last_modified, e_tag, etc.
        """
        if self._session is not None:
            return await self._do_head_async(self._session, path)

        # Fallback: create a temporary session for this request
        async with aiohttp.ClientSession(
            timeout=self.timeout, headers=self.headers
        ) as session:
            return await self._do_head_async(session, path)

    def head(self, path: str) -> ObjectMeta:
        """
        Get file metadata synchronously via HEAD request.

        This wraps the async implementation for convenience.

        Parameters
        ----------
        path
            Path to the file relative to base_url.

        Returns
        -------
        ObjectMeta
            File metadata including size, last_modified, e_tag, etc.
        """
        return asyncio.run(self.head_async(path))


__all__ = ["AiohttpStore", "AiohttpGetResult", "AiohttpGetResultAsync"]
