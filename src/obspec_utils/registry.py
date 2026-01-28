"""
Based on https://docs.rs/object_store/0.12.2/src/object_store/registry.rs.html#176-218
"""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterator
from typing import Generic, TypeVar
from urllib.parse import urlparse

from obspec import Get

from obspec_utils.typing import Path, Url

T = TypeVar("T", bound=Get)
"""Type variable for store types, bounded by [Get][obspec.Get]."""

UrlKey = namedtuple("UrlKey", ["scheme", "netloc"])
"""
A named tuple containing a URL's scheme and authority/netloc.

Used as the primary key in ObjectStoreRegistry.map.

Attributes
----------
scheme
    The URL scheme (e.g., 's3', 'https', 'file').
netloc
    The network location/authority (e.g., 'bucket-name', 'example.com').
"""


def get_url_key(url: Url) -> UrlKey:
    """
    Generate the UrlKey containing a url's scheme and authority/netloc that is used a the
    primary key's in a [ObjectStoreRegistry.map][obspec_utils.registry.ObjectStoreRegistry.map]

    Parameters
    ----------
    url
        Url to generate a UrlKey from

    Returns
    -------
        NamedTuple containing the Url's scheme and authority/netloc

    Raises
    ------
    ValueError
        If provided Url does not contain a scheme based on [urllib.parse.urlparse][]
    """
    parsed = urlparse(url)
    if not parsed.scheme:
        raise ValueError(
            f"Urls are expected to contain a scheme (e.g., `file://` or `s3://`), received {url} which parsed to {parsed}"
        )
    return UrlKey(parsed.scheme, parsed.netloc)


class PathEntry(Generic[T]):
    """
    Construct a tree of path segments starting from the root

    For example the following paths:
    * `/` => store1
    * `/foo/bar` => store2

    Would be represented by:
    store: Some(store1)
    children:
      foo:
        store: None
        children:
          bar:
            store: Some(store2)
    """

    def __init__(self) -> None:
        self.store: T | None = None
        self.children: dict[str, PathEntry[T]] = {}

    def iter_stores(self) -> Iterator[T]:
        """Iterate over all stores in this entry and its children."""
        if self.store is not None:
            yield self.store
        for child in self.children.values():
            yield from child.iter_stores()

    def lookup(self, to_resolve: str) -> tuple[T, int] | None:
        """
        Lookup a store based on URL path

        Returns the store and its path segment depth
        """
        current = self
        ret = (self.store, 0) if self.store is not None else None
        depth = 0

        # Traverse the PathEntry tree to find the longest match
        for segment in path_segments(to_resolve):
            if segment in current.children:
                current = current.children[segment]
                depth += 1
                if current.store is not None:
                    ret = (current.store, depth)
            else:
                break

        return ret


class ObjectStoreRegistry(Generic[T]):
    """
    A generic registry that maps URLs to object stores.

    The registry is parameterized by the store type `T`, which must implement
    at least [Get][obspec.Get]. Downstream libraries can specify stricter
    protocol requirements by using a more specific type parameter.

    The registry can be used as an async context manager to automatically manage
    the lifecycle of stores that support it (like
    [AiohttpStore][obspec_utils.stores.AiohttpStore]). Stores that don't implement
    the async context manager protocol (like obstore's [S3Store][obstore.store.S3Store])
    are unaffected.

    Examples
    --------

    Basic usage with obstore:

    ```python
    from obstore.store import S3Store
    from obspec_utils.registry import ObjectStoreRegistry

    registry = ObjectStoreRegistry({
        "s3://my-bucket": S3Store(bucket="my-bucket"),
    })
    store, path = registry.resolve("s3://my-bucket/file.nc")
    ```

    Using with a specific protocol for type safety:

    ```python
    from typing import Protocol
    from obspec import List, ListAsync, Head, HeadAsync
    from obspec_utils.registry import ObjectStoreRegistry

    class ZarrProtocol(List, ListAsync, Head, HeadAsync, Protocol):
        '''Protocol for Zarr chunk discovery.'''

    registry: ObjectStoreRegistry[ZarrProtocol] = ObjectStoreRegistry({
        "s3://my-bucket": s3_store,
    })
    store, path = registry.resolve("s3://my-bucket/data.zarr")
    store.list(path)  # Type checker knows this is valid
    ```

    Using as an async context manager:

    ```python
    from obspec_utils.aiohttp import AiohttpStore

    registry = ObjectStoreRegistry({
        "https://example.com": AiohttpStore("https://example.com"),
    })

    async with registry:
        store, path = registry.resolve("https://example.com/file.nc")
        data = await store.get_range_async(path, start=0, end=1000)
    # AiohttpStore session is closed automatically
    ```
    """

    def __init__(self, stores: dict[Url, T] | None = None) -> None:
        """
        Create a new store registry.

        The registry accepts any object that implements at least [Get][obspec.Get].
        For stricter type checking, parameterize the registry with a more specific
        protocol type.

        Parameters
        ----------
        stores
            Mapping of URLs to stores to register.

        Examples
        --------

        ```python  exec="on" source="above" session="registry-examples"
        from obstore.store import S3Store
        from obspec_utils.registry import ObjectStoreRegistry

        s3store = S3Store(bucket="my-bucket-1", prefix="orig-path")
        reg = ObjectStoreRegistry({"s3://my-bucket-1": s3store})

        ret, path = reg.resolve("s3://my-bucket-1/orig-path/group/my-file.nc")
        assert path == "group/my-file.nc"
        assert ret is s3store
        ```
        """
        # Mapping from UrlKey (containing scheme and netlocs) to PathEntry
        self.map: dict[UrlKey, PathEntry[T]] = {}
        stores = stores or {}
        for url, store in stores.items():
            self.register(url, store)

    def register(self, url: Url, store: T) -> None:
        """
        Register a new store for the provided URL.

        If a store with the same URL existed before, it is replaced.

        Parameters
        ----------
        url
            URL to register the store under.
        store
            Any object implementing at least [Get][obspec.Get].

        Examples
        --------

        ```python exec="on" source="above" session="registry-examples"
        from obstore.store import S3Store
        from obspec_utils.registry import ObjectStoreRegistry

        reg = ObjectStoreRegistry()
        orig_store = S3Store(bucket="my-bucket-1", prefix="orig-path")
        reg.register("s3://my-bucket-1", orig_store)

        new_store = S3Store(bucket="my-bucket-1", prefix="updated-path")
        reg.register("s3://my-bucket-1", new_store)
        ```
        """
        parsed = urlparse(url)

        key = get_url_key(url)

        if key not in self.map:
            self.map[key] = PathEntry()

        entry = self.map[key]

        # Navigate to the correct path in the tree
        for segment in path_segments(parsed.path):
            if segment not in entry.children:
                entry.children[segment] = PathEntry()
            entry = entry.children[segment]
        # Update the store
        entry.store = store

    def resolve(self, url: Url) -> tuple[T, Path]:
        """
        Resolve a URL within the [ObjectStoreRegistry][obspec_utils.registry.ObjectStoreRegistry].

        If [ObjectStoreRegistry.register][obspec_utils.registry.ObjectStoreRegistry.register] has been called
        with a URL with the same scheme and authority/netloc as the object URL, and a path that is a prefix
        of the provided url's, it is returned along with the trailing path. Paths are matched on a
        path segment basis, and in the event of multiple possibilities the longest path match is used.

        Parameters
        ----------
        url
            Url to resolve in the [ObjectStoreRegistry][obspec_utils.registry.ObjectStoreRegistry]

        Returns
        -------
        T
            The store registered at the resolved url.
        Path
            The trailing portion of the url after the prefix of the matching store in the
            [ObjectStoreRegistry][obspec_utils.registry.ObjectStoreRegistry].

        Raises
        ------
        ValueError
            If the URL cannot be resolved, meaning that [ObjectStoreRegistry.register][obspec_utils.registry.ObjectStoreRegistry.register]
            has not been called with a URL with the same scheme and authority/netloc as the object URL, and a path that is a prefix
            of the provided url's.

        Examples
        --------

        ```python exec="on" source="above" session="registry-resolve-examples"
        from obstore.store import MemoryStore, S3Store
        from obspec_utils.registry import ObjectStoreRegistry

        registry = ObjectStoreRegistry()
        memstore1 = MemoryStore()
        registry.register("s3://bucket1", memstore1)
        url = "s3://bucket1/path/to/object"
        ret, path = registry.resolve(url)
        assert path == "path/to/object"
        assert ret is memstore1
        print(f"Resolved url: `{url}` to store: `{ret}` and path: `{path}`")
        ```

        ```python exec="on" source="above" session="registry-resolve-examples"
        memstore2 = MemoryStore()
        base = "https://s3.region.amazonaws.com/bucket"
        registry.register(base, memstore2)

        url = "https://s3.region.amazonaws.com/bucket/path/to/object"
        ret, path = registry.resolve(url)
        assert path == "bucket/path/to/object"
        assert ret is memstore2
        print(f"Resolved url: `{url}` to store: `{ret}` and path: `{path}`")
        ```

        ```python exec="on" source="above" session="registry-resolve-examples"
        s3store = S3Store(bucket = "my-bucket", prefix="my-data/prefix/")
        registry.register("s3://my-bucket", s3store)
        ret, path = registry.resolve("s3://my-bucket/my-data/prefix/my-file.nc")
        assert path == "my-file.nc"
        assert ret is s3store
        ```
        """
        parsed = urlparse(url)
        path = parsed.path

        key = UrlKey(parsed.scheme, parsed.netloc)

        if key in self.map:
            result = self.map[key].lookup(path)
            if result:
                store, _ = result
                if hasattr(store, "prefix") and store.prefix:
                    prefix = str(store.prefix).lstrip("/")
                    path_after_prefix = (
                        path.lstrip("/").removeprefix(prefix).lstrip("/")
                    )
                elif hasattr(store, "url"):
                    prefix = urlparse(store.url).path.lstrip("/")
                    path_after_prefix = (
                        path.lstrip("/").removeprefix(prefix).lstrip("/")
                    )
                else:
                    path_after_prefix = path.lstrip("/")
                return store, path_after_prefix
        raise ValueError(f"Could not find an ObjectStore matching the url `{url}`")

    def _iter_stores(self) -> Iterator[T]:
        """Iterate over all registered stores."""
        for entry in self.map.values():
            yield from entry.iter_stores()

    async def __aenter__(self) -> "ObjectStoreRegistry[T]":
        """
        Enter the async context manager, opening all stores that support it.

        Stores that implement the async context manager protocol (like
        [AiohttpStore][obspec_utils.stores.AiohttpStore]) will have their
        sessions initialized. Stores that don't support it (like
        obstore's [S3Store][obstore.store.S3Store]) are unaffected.

        Examples
        --------

        ```python
        from obstore.store import S3Store
        from obspec_utils.registry import ObjectStoreRegistry
        from obspec_utils.aiohttp import AiohttpStore

        registry = ObjectStoreRegistry({
            "s3://my-bucket": S3Store(bucket="my-bucket"),
            "https://example.com": AiohttpStore("https://example.com"),
        })

        async with registry:
            # S3Store works as-is, AiohttpStore session is opened
            store, path = registry.resolve("https://example.com/file.nc")
            data = await store.get_range_async(path, start=0, end=1000)
        # AiohttpStore session is closed
        ```
        """
        for store in self._iter_stores():
            if hasattr(store, "__aenter__"):
                await store.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the async context manager, closing all stores that support it.

        Stores that implement the async context manager protocol will have their
        resources cleaned up. Stores that don't support it are unaffected.
        """
        for store in self._iter_stores():
            if hasattr(store, "__aexit__"):
                await store.__aexit__(exc_type, exc_val, exc_tb)


def path_segments(path: str) -> Iterator[str]:
    """
    Returns the non-empty segments of a path

    Note: We filter out empty segments unlike urllib.parse
    """
    return filter(lambda x: x, path.split("/"))


__all__ = ["ObjectStoreRegistry"]
