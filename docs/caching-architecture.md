# Caching Architecture

This document describes the caching architecture in obspec-utils, the design decisions behind it, and guidance on when to use different caching strategies.

## Overview

obspec-utils provides caching at two levels:

1. **Reader-level caching**: Temporary, scoped to a single file reader's lifetime
2. **Store-level caching**: Shared across all consumers of a store

These serve different phases of a typical VirtualiZarr workflow and have different trade-offs.

## Two-Phase Workflow

When working with virtual datasets (e.g., via VirtualiZarr), data access happens in two distinct phases:

### Phase 1: Parsing (Reader-Level)

```
┌─────────────────────────────────────────────────────────────┐
│                    PARSING PHASE                            │
│  (per-file, short-lived)                                    │
│                                                             │
│  open_virtual_dataset()                                     │
│        │                                                    │
│        ▼                                                    │
│  Reader ──────► Store ──────► Network                       │
│    │                                                        │
│    └── Reader-level caching                                 │
│        - Caches full file for HDF5/NetCDF parsing           │
│        - Released when reader closes                        │
│        - Isolated per reader instance                       │
└─────────────────────────────────────────────────────────────┘
```

During parsing:

- A reader opens the file and parses headers/metadata
- The file may be read multiple times during parsing (HDF5 structure traversal)
- Once parsing completes, the reader closes and cache is released
- Result: Virtual dataset with chunk *references* (not actual data)

**Reader-level caching is appropriate here because:**

- File metadata is read once per file, then discarded
- Cache lifecycle matches reader lifecycle (automatic cleanup)
- No cross-contamination between different files being parsed
- Memory is freed immediately when parsing completes

### Phase 2: Data Access (Store-Level)

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA ACCESS PHASE                        │
│  (shared, long-lived)                                       │
│                                                             │
│  xarray computation / .load() / .compute()                  │
│        │                                                    │
│        ▼                                                    │
│  ManifestStore ──────► Store ──────► Network                │
│                          │                                  │
│                          └── Store-level caching            │
│                              - Shared across calls          │
│                              - Persists across              │
│                                different consumers          │
└─────────────────────────────────────────────────────────────┘
```

During data access:

- Computations trigger reads of actual chunk data
- Same chunks may be accessed multiple times (overlapping operations, retries)
- Cache should persist and be shared across different access patterns

**Store-level caching is appropriate here because:**

- Chunks may be re-read by different computations
- Cache should be shared across all consumers of the store
- Lifecycle is independent of any single reader

## Caching Implementations

### Reader-Level: EagerStoreReader

`EagerStoreReader` loads the entire file into memory on construction:

```python
from obspec_utils.obspec import EagerStoreReader

# File is fully loaded into memory
reader = EagerStoreReader(store, "file.nc")

# All reads served from memory
data = reader.read(1000)
reader.seek(0)
more_data = reader.read(500)

# Cache released when reader closes
reader.close()
```

**Characteristics:**

- Fetches file using parallel `get_ranges()` for speed
- Caches in `BytesIO` buffer
- Cache is isolated to this reader instance
- Memory freed on `close()` or context manager exit

**When to use:**

- Parsing HDF5/NetCDF files (need random access during parsing)
- Small-to-medium files that fit in memory
- When you'll read most of the file anyway

### Reader-Level: ParallelStoreReader

`ParallelStoreReader` uses chunk-based LRU caching:

```python
from obspec_utils.obspec import ParallelStoreReader

reader = ParallelStoreReader(
    store, "file.nc",
    chunk_size=256 * 1024,      # 256 KB chunks
    max_cached_chunks=64,       # Up to 64 chunks cached
)

# Chunks fetched on demand via get_ranges()
data = reader.read(1000)
```

**Characteristics:**

- Bounded memory usage: `chunk_size * max_cached_chunks`
- LRU eviction when cache is full
- Good for sparse/random access patterns

**When to use:**

- Large files with sparse access
- Memory-constrained environments
- Unknown access patterns

### Reader-Level: BufferedStoreReader

`BufferedStoreReader` provides read-ahead buffering for sequential access:

```python
from obspec_utils.obspec import BufferedStoreReader

reader = BufferedStoreReader(store, "file.nc", buffer_size=1024 * 1024)

# Sequential reads benefit from buffering
while chunk := reader.read(4096):
    process(chunk)
```

**Characteristics:**

- Position-aware buffering (read-ahead)
- Best for sequential/streaming access
- Minimal memory overhead

**When to use:**

- Sequential file processing
- Streaming workloads
- When you won't revisit earlier data

### Store-Level: CachingReadableStore

`CachingReadableStore` wraps any store to cache full objects:

```python
from obspec_utils.cache import CachingReadableStore

# Wrap the store with caching
cached_store = CachingReadableStore(
    store,
    max_size=256 * 1024 * 1024,  # 256 MB cache
)

# All consumers share the same cache
data1 = cached_store.get("file1.nc")  # Fetched from network, cached
data2 = cached_store.get("file1.nc")  # Served from cache
```

**Characteristics:**

- Caches full objects (entire files)
- LRU eviction when `max_size` exceeded
- Thread-safe (works with `ThreadPoolExecutor`)
- Shared across all consumers of the wrapped store

**When to use:**

- Multiple consumers reading the same files
- Repeated access to small-to-medium files
- When store-level sharing is beneficial

**Limitations:**

- Not shared across processes (Dask workers, ProcessPoolExecutor)
- Each process maintains its own cache
- Full-object granularity (not ideal for partial reads of large files)

## Distributed Considerations

### Threading (Shared Memory)

With `ThreadPoolExecutor` or similar:

- Store-level caching IS shared across threads
- All threads benefit from the same cache
- Thread-safe implementations required (provided)

### Multi-Process (Separate Memory)

With `ProcessPoolExecutor`, Dask distributed, or Lithops:

- Each worker process has its own memory space
- Store wrappers are serialized and copied to each worker
- **Caches are NOT shared** across workers
- Each worker maintains an independent cache

This is typically acceptable when:

- Workloads are partitioned by file (each worker processes different files)
- The alternative (no caching) would be worse

For workloads requiring cross-worker cache sharing, consider:

- External caching (Redis, memcached)
- Shared filesystem caching
- Restructuring workloads to minimize cross-worker file access

## Decision Guide

### Which reader should I use?

| Access Pattern | Recommended Reader |
|---------------|-------------------|
| Parse HDF5/NetCDF file | `EagerStoreReader` |
| Sequential streaming | `BufferedStoreReader` |
| Sparse random access | `ParallelStoreReader` |
| Unknown pattern, large file | `ParallelStoreReader` |
| Small file, repeated access | `EagerStoreReader` |

### Should I use store-level caching?

| Scenario | Recommendation |
|----------|---------------|
| Single-threaded, repeated file access | Yes, `CachingReadableStore` |
| Multi-threaded, shared files | Yes, `CachingReadableStore` |
| Distributed workers, partitioned by file | Optional (per-worker cache) |
| Distributed workers, shared files | Consider external caching |
| One-time file processing | No (use reader-level only) |

## Store Wrappers

### SplittingReadableStore

`SplittingReadableStore` accelerates `get()` by splitting large requests into parallel `get_ranges()`:

```python
from obspec_utils.splitting import SplittingReadableStore

fast_store = SplittingReadableStore(
    store,
    request_size=12 * 1024 * 1024,  # 12 MB per request
    max_concurrent_requests=18,
)
```

This extracts the parallel fetching logic from `EagerStoreReader` into a composable wrapper. It composes naturally with `CachingReadableStore`:

```python
from obspec_utils.splitting import SplittingReadableStore
from obspec_utils.cache import CachingReadableStore

# Compose: fast parallel fetches + caching
store = S3Store(bucket="my-bucket")
store = SplittingReadableStore(store)  # Split large fetches
store = CachingReadableStore(store)    # Cache results

# First get(): parallel fetch -> cache
# Second get(): served from cache
```

**Characteristics:**

- Only affects `get()` and `get_async()` - range requests pass through unchanged
- Requires `head()` support to determine file size (falls back to single request otherwise)
- Tuned for cloud storage (12 MB chunks, 18 concurrent requests by default)

## Summary

| Layer | Scope | Lifetime | Use Case |
|-------|-------|----------|----------|
| Reader-level | Per-file, per-instance | Reader lifetime | Parsing phase |
| Store-level | Shared across consumers | Application lifetime | Data access phase |

The two-level architecture reflects the reality that:

1. **Parsing** benefits from isolated, temporary caching (reader-level)
2. **Data access** benefits from shared, persistent caching (store-level)
3. **Distributed settings** require understanding that in-memory caches are per-process
