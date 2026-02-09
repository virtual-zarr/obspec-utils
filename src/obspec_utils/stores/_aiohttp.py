"""Aiohttp-based implementation of the ReadableStore protocol.

This module provides an alternative HTTP backend using aiohttp instead of obstore's
HTTPStore.
It's useful for generic HTTPS access (e.g., THREDDS, NASA data from outside AWS region)
where obstore's HTTPStore (designed for WebDAV/S3-like semantics) may not be ideal.

Example:
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
from typing import TYPE_CHECKING, Self, overload

from obspec import GetRangeAsync, GetRangesAsync

try:
    import aiohttp
except ImportError as e:
    msg = "aiohttp is required for AiohttpStore. Install it with: pip install aiohttp"
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from collections.abc import Sequence

    from aiohttp import ClientSession


__all__ = ["AiohttpStore"]


class AiohttpStore(GetRangeAsync, GetRangesAsync):
    """An [aiohttp]-based object store implementation.

    [aiohttp]: https://docs.aiohttp.org/en/stable/

    This provides a lightweight alternative to obstore's
    [HTTPStore][obstore.store.HTTPStore] for generic HTTP/HTTPS access. It's
    particularly useful for:

    - THREDDS data servers
    - NASA data access from outside AWS regions
    - Any generic HTTP endpoint that doesn't provide S3-like semantics

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

    With authentication:

    ```python
    async with AiohttpStore(
        "https://api.example.com/data",
        headers={"Authorization": "Bearer <token>"}
    ) as store:
        result = await store.get_async("protected/file.nc")
    ```

    """

    _session: ClientSession | None

    @overload
    def __init__(
        self,
        base_url_or_client: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None: ...
    @overload
    def __init__(
        self,
        base_url_or_client: ClientSession,
        *,
        headers: None = None,
        timeout: float = 30.0,
    ) -> None: ...
    def __init__(
        self,
        base_url_or_client: str | ClientSession,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        if isinstance(base_url_or_client, aiohttp.ClientSession):
            self._session = base_url_or_client
            self.base_url = ""
            self._user_provided_session = True
        else:
            self._session = None
            self.base_url = base_url_or_client.rstrip("/")
            self.headers = headers or {}
            self.timeout = aiohttp.ClientTimeout(total=timeout)
            self._user_provided_session = False

    async def __aenter__(self) -> Self:
        """Enter the async context manager, creating a reusable session."""
        if not self._user_provided_session:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers=self.headers,
            )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Exit the async context manager, closing the session."""
        if self._user_provided_session:
            # Don't close a user-provided session
            return

        if self._session is not None:
            await self._session.close()

    def _build_url(self, path: str) -> str:
        """Build the full URL from base URL and path."""
        path = path.removeprefix("/")
        return f"{self.base_url}/{path}" if path else self.base_url

    def _get_valid_session(self) -> ClientSession:
        """Assert that the session is valid for making requests."""
        if self._session is None:
            msg = (
                "Aiohttp session not initialized.\n"
                "Either provide a session or use as an async context manager."
            )
            raise RuntimeError(msg)

        return self._session

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> bytes:
        """Download a byte range asynchronously.

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

        session = self._get_valid_session()

        url = self._build_url(path)
        request_headers = {} if self._session else dict(self.headers)
        # HTTP Range is inclusive on both ends, obspec end is exclusive
        request_headers["Range"] = f"bytes={start}-{end - 1}"

        async with session.get(url, headers=request_headers) as response:
            response.raise_for_status()
            return await response.read()

    async def get_ranges_async(
        self,
        path: str,
        *,
        starts: Sequence[int],
        ends: Sequence[int] | None = None,
        lengths: Sequence[int] | None = None,
    ) -> Sequence[bytes]:
        """Download multiple byte ranges asynchronously.

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
            ends = [s + ln for s, ln in zip(starts, lengths, strict=False)]  # type: ignore[arg-type]

        # TODO: coalesce ranges
        tasks = [
            self.get_range_async(path, start=s, end=e)
            for s, e in zip(starts, ends, strict=False)
        ]
        return await asyncio.gather(*tasks)
