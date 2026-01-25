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
from obspec_utils.readers import EagerStoreReader

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
from obspec_utils.readers import ParallelStoreReader

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
from obspec_utils.readers import BufferedStoreReader

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
from obspec_utils.wrappers import CachingReadableStore

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

### Pickling and Serialization

`CachingReadableStore` supports Python's pickle protocol for use with multiprocessing and distributed frameworks. When a `CachingReadableStore` is pickled and unpickled (e.g., sent to a worker process), it is **recreated with an empty cache**.

```python
import pickle
from obspec_utils.wrappers import CachingReadableStore

# Main process: create and populate cache
cached_store = CachingReadableStore(store, max_size=256 * 1024 * 1024)
cached_store.get("file1.nc")  # Cached
cached_store.get("file2.nc")  # Cached
print(cached_store.cache_size)  # Non-zero

# Simulate sending to worker (pickle roundtrip)
restored = pickle.loads(pickle.dumps(cached_store))

# Worker receives store with empty cache
print(restored.cache_size)  # 0
print(restored._max_size)   # 256 * 1024 * 1024 (preserved)
```

**Design rationale:**

1. **Cache contents are not serialized**: Serializing the full cache would defeat the purpose of distributed processing—workers would receive potentially huge payloads, and the data may not even be relevant to their partition.

2. **Fresh cache per worker**: Each worker builds its own cache based on its workload. For file-partitioned workloads (common in data processing), this is optimal—each worker caches only the files it processes.

3. **Configuration is preserved**: The `max_size` and underlying store are preserved, so workers use the same caching policy as the main process.

**Requirements for pickling:**

The underlying store (`_store`) must also be picklable. For cloud stores, this typically means using stores that can be reconstructed from configuration:

```python
# Works: store can be pickled (configuration-based)
from obstore.store import S3Store
s3_store = S3Store(bucket="my-bucket", region="us-east-1")
cached = CachingReadableStore(s3_store)
pickle.dumps(cached)  # OK

# May not work: some Rust-backed stores aren't picklable
from obstore.store import MemoryStore
mem_store = MemoryStore()
cached = CachingReadableStore(mem_store)
pickle.dumps(cached)  # TypeError: cannot pickle 'MemoryStore' object
```

### Distributed Usage Patterns

#### Pattern 1: File-Partitioned Workloads (Recommended)

When each worker processes a distinct set of files, per-worker caching works well:

```python
from concurrent.futures import ProcessPoolExecutor
from obspec_utils.wrappers import CachingReadableStore

def process_files(cached_store, file_paths):
    """Each worker gets its own cache, processes its own files."""
    results = []
    for path in file_paths:
        # First access: fetch from network, cache locally
        data = cached_store.get(path)
        # Subsequent accesses to same file: served from cache
        result = analyze(data)
        results.append(result)
    return results

# Create cached store in main process
store = S3Store(bucket="my-bucket")
cached_store = CachingReadableStore(store, max_size=512 * 1024 * 1024)

# Partition files across workers
all_files = ["file1.nc", "file2.nc", "file3.nc", "file4.nc"]
partitions = [all_files[:2], all_files[2:]]

with ProcessPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(process_files, cached_store, partition)
        for partition in partitions
    ]
    results = [f.result() for f in futures]
```

#### Pattern 2: Dask Distributed

With Dask, the cached store is serialized to each worker:

```python
import dask
from dask.distributed import Client
from obspec_utils.wrappers import CachingReadableStore

client = Client()

store = S3Store(bucket="my-bucket")
cached_store = CachingReadableStore(store)

@dask.delayed
def process_file(cached_store, path):
    # Worker receives cached_store with empty cache
    # Cache builds up as this worker processes files
    data = cached_store.get(path)
    return analyze(data)

tasks = [process_file(cached_store, f) for f in file_list]
results = dask.compute(*tasks)
```


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
from obspec_utils.wrappers import SplittingReadableStore

fast_store = SplittingReadableStore(
    store,
    request_size=12 * 1024 * 1024,  # 12 MB per request
    max_concurrent_requests=18,
)
```

This extracts the parallel fetching logic from `EagerStoreReader` into a composable wrapper. It composes naturally with `CachingReadableStore`:

```python
from obspec_utils.wrappers import CachingReadableStore, SplittingReadableStore

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
