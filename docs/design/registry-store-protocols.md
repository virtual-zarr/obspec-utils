# Parser Protocol Requirements via ObjectStoreRegistry

**Question:** How should VirtualiZarr parsers designate which obspec protocols they require?

## obspec's Recommended Approach

[obspec uses independent protocols](https://developmentseed.org/obspec/latest/blog/2025/06/25/introducing-obspec-a-python-protocol-for-interfacing-with-object-storage/) rather than a monolithic interface. The philosophy:

- **Compose flat, independent protocols** for each use case
- **Don't force unnecessary capabilities** — requiring fewer operations means more backend compatibility
- **Avoid hierarchical tiers** — they create artificial coupling between unrelated capabilities

Each parser should define exactly the protocols it needs:

```python
from typing import Protocol
from obspec import Get, GetAsync, GetRange, GetRangeAsync, Head, HeadAsync, List, ListAsync

# Kerchunk - truly minimal
class KerchunkProtocol(Get, GetAsync, Protocol):
    """Fetch whole objects only."""

# HDF5 - range requests + file size
class HDF5Protocol(GetRange, GetRangeAsync, Head, HeadAsync, Protocol):
    """Random access with metadata."""

# Zarr - enumeration + file size
class ZarrProtocol(List, ListAsync, Head, HeadAsync, Protocol):
    """Chunk discovery and size detection."""

# COG - parallel ranges + file size
class COGProtocol(GetRange, GetRangeAsync, GetRanges, GetRangesAsync, Head, HeadAsync, Protocol):
    """Parallel tile fetching."""
```

## obspec Protocol Reference

| Protocol | Methods | Use Case |
|----------|---------|----------|
| `Get` | `get()`, `get_async()` | Fetch entire objects |
| `GetRange` | `get_range()`, `get_range_async()` | Fetch byte ranges |
| `GetRanges` | `get_ranges()`, `get_ranges_async()` | Parallel byte ranges |
| `Head` | `head()`, `head_async()` | Get size/metadata |
| `List` | `list()`, `list_async()` | Enumerate objects |


## Why Not Protocol Tiers?

A tiered approach (`MinimalStore` → `ReadableStore` → `ListableStore`) creates artificial coupling:

| Tier approach | Problem |
|---------------|---------|
| `ReadableStore` bundles `GetRange` + `GetRanges` + `Head` | Some range readers don't need `Head` (size passed explicitly) |
| `ReadableStore` requires `GetRanges` | Some backends only support single `GetRange` |
| `ListableStore` requires all of `ReadableStore` | ZarrParser needs `List` + `Head`, not `GetRanges` |

Flat composition avoids these issues — each protocol includes only what's actually needed.

## Parser Requirements

| Parser | Protocol Composition | Why |
|--------|---------------------|-----|
| Kerchunk-based | `Get`, `GetAsync` | All offsets pre-computed |
| HDF5Parser | `GetRange`, `GetRangeAsync`, `Head`, `HeadAsync` | Random access, file size |
| ZarrParser | `List`, `ListAsync`, `Head`, `HeadAsync` | Chunk discovery, size detection |

Without validation, users get confusing errors like `AttributeError: 'HttpStore' object has no attribute 'list'`.

## obspec-utils Internal Design

obspec-utils uses two patterns for protocol requirements:

### Readers: Nested `Store` Protocols

Each reader defines its own nested `Store` protocol with exactly what it needs:

```python
class BufferedStoreReader:
    class Store(Get, GetRange, Protocol):
        """Requires Get + GetRange."""
        pass

class EagerStoreReader:
    class Store(Get, GetRanges, Protocol):
        """Requires Get + GetRanges (+ optional Head)."""
        pass
```

### Wrappers: Internal `ReadableStore`

Transparent proxy wrappers (`CachingReadableStore`, `TracingReadableStore`, `SplittingReadableStore`) share an internal `ReadableStore` protocol since they all need the same full read interface:

```python
# Internal to obspec-utils (not exported)
class ReadableStore(Get, GetAsync, GetRange, GetRangeAsync, GetRanges, GetRangesAsync, Protocol):
    """Full read interface for transparent store wrappers."""
```

This is not exported — external consumers should compose their own protocols from obspec.

## Generic Registry Design

The registry is generic with [Get][obspec.Get] as the bound, allowing callers to specify their exact protocol requirements:

```python
from typing import TypeVar, Generic
from obspec import Get

T = TypeVar("T", bound=Get)

class ObjectStoreRegistry(Generic[T]):
    def __init__(self, stores: dict[Url, T] | None = None) -> None: ...
    def register(self, url: Url, store: T) -> None: ...
    def resolve(self, url: Url) -> tuple[T, Path]: ...
```

Usage with parser-specific protocols:

```python
# Zarr workflow
registry: ObjectStoreRegistry[ZarrProtocol] = ObjectStoreRegistry({
    "s3://bucket": s3_store,
})
store, path = registry.resolve(url)  # store: ZarrProtocol
store.list(path)  # OK
store.head(path)  # OK

# Kerchunk workflow - less restrictive
registry: ObjectStoreRegistry[Get] = ObjectStoreRegistry({
    "https://cdn.example.com": http_store,  # Only needs Get
})
```

## Runtime Validation

Since Protocol `isinstance()` checks are unreliable, parsers should validate at call time:

```python
class ZarrParser:
    def __call__(self, url: str, registry: ObjectStoreRegistry) -> ManifestStore:
        store, _ = registry.resolve(url)
        if not (hasattr(store, "list") and hasattr(store, "head")):
            raise TypeError(
                f"ZarrParser requires List + Head protocols. "
                f"{type(store).__name__} is missing required methods."
            )
        # ... proceed
```

## Escape Hatches

Provide parameters to reduce requirements where desired:

```python
class ZarrParser:
    def __init__(self, consolidated_metadata: dict | None = None):
        self.consolidated_metadata = consolidated_metadata  # Skip List requirement

class HDF5Parser:
    def __init__(self, file_size: int | None = None):
        self.file_size = file_size  # Skip Head requirement
```

## Backwards Compatibility

**Can VirtualiZarr depend on obspec-utils without parser changes?**

At runtime, `resolve()` returns the actual store object (e.g., `S3Store`), which has all methods. Type hints only affect static analysis.

| Layer | Behavior | Parser changes needed? |
|-------|----------|------------------------|
| Runtime | Stores have all methods | No |
| Static typing | Type checkers see declared protocol | Depends on approach |

### Migration Path

1. **Immediate:** Duck typing — no changes, works at runtime, type checkers complain
2. **Incremental:** Type-ignore pragmas — `store.list(path)  # type: ignore[attr-defined]`
3. **Full type safety:** Generic registry with parser-specific protocols

## VirtualiZarr Implementation Guide

VirtualiZarr parsers should define their protocol requirements in VirtualiZarr, not in obspec-utils. This keeps obspec-utils minimal and lets VirtualiZarr evolve its requirements independently.

### Defining Parser Protocols

In `virtualizarr/parsers/protocols.py`:

```python
from typing import Protocol
from obspec import Get, GetAsync, GetRange, GetRangeAsync, Head, HeadAsync, List, ListAsync

class KerchunkStore(Get, GetAsync, Protocol):
    """Store protocol for Kerchunk-based parsers (pre-indexed offsets)."""
    pass

class HDF5Store(GetRange, GetRangeAsync, Head, HeadAsync, Protocol):
    """Store protocol for HDF5 parsing (random access + file size)."""
    pass

class ZarrStore(List, ListAsync, Head, HeadAsync, Protocol):
    """Store protocol for Zarr parsing (chunk discovery + sizes)."""
    pass
```

### Using Protocols in Parsers

Each parser uses its protocol for type hints and validates at runtime:

```python
# virtualizarr/parsers/zarr.py
from typing import Protocol
from obspec import List, ListAsync, Head, HeadAsync
from obspec_utils import ObjectStoreRegistry

class ZarrStore(List, ListAsync, Head, HeadAsync, Protocol):
    """Store protocol for Zarr parsing."""
    pass

class ZarrParser:
    def __call__(
        self,
        url: str,
        registry: ObjectStoreRegistry[ZarrStore],
    ) -> ManifestStore:
        store, path = registry.resolve(url)

        # Runtime validation with clear error message
        missing = []
        if not hasattr(store, "list"):
            missing.append("List")
        if not hasattr(store, "head"):
            missing.append("Head")
        if missing:
            raise TypeError(
                f"ZarrParser requires {', '.join(missing)} protocols. "
                f"{type(store).__name__} does not support these operations. "
                "Use S3Store, LocalStore, or another store with listing support."
            )

        # Type checker knows store has list() and head()
        chunks = store.list(path)
        # ...
```

### Creating Typed Registries

Users create registries with the appropriate protocol for their workflow:

```python
# For Zarr workflows
from virtualizarr.parsers.protocols import ZarrStore

registry: ObjectStoreRegistry[ZarrStore] = ObjectStoreRegistry({
    "s3://my-bucket": S3Store(bucket="my-bucket"),
})

# Type checker enforces that only ZarrStore-compatible stores are registered
# and that resolved stores have list() and head() methods
```

### Nested Store Protocol Pattern

Following obspec-utils' reader pattern, parsers can define their protocol as a nested class:

```python
class ZarrParser:
    class Store(List, ListAsync, Head, HeadAsync, Protocol):
        """Store protocol required by ZarrParser."""
        pass

    def __call__(
        self,
        url: str,
        registry: ObjectStoreRegistry["ZarrParser.Store"],
    ) -> ManifestStore:
        # ...
```

This is self-documenting — the protocol is defined alongside the parser that requires it.

## Summary

1. **Flat composition over tiers** — each consumer defines exactly the protocols it needs
2. **Generic registry** with [Get][obspec.Get] bound
3. **obspec-utils internal patterns:**
   - Readers use nested `Store` protocols (each with specific requirements)
   - Wrappers share internal `ReadableStore` (not exported)
4. **External consumers** (like VirtualiZarr) should compose protocols from obspec directly
5. **Runtime validation** in parsers with clear error messages
6. **Escape hatches** where feasible (`file_size`, `consolidated_metadata`)
7. **Backwards compatible** — duck typing works immediately; generics for full type safety
