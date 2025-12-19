# Benchmarking

`obspec-utils` includes a benchmark script for comparing the performance of different approaches to reading cloud-hosted data.

## Benchmark Script

The benchmark script compares fsspec, obstore readers, and VirtualiZarr + Icechunk approaches for reading NetCDF files from S3.

??? note "View full script"

    ```python
    --8<-- "scripts/benchmark_readers.py"
    ```

## Running the Benchmark

```bash
# Full benchmark with default settings
uv run scripts/benchmark_readers.py

# Quick test with fewer files
uv run scripts/benchmark_readers.py --n-files 2

# Skip specific benchmarks
uv run scripts/benchmark_readers.py --skip fsspec_default obstore_eager

# Label results for a specific environment
uv run scripts/benchmark_readers.py --environment cloud --description "AWS us-west-2"
```

## Benchmark Results

```python exec="on"
import json
from pathlib import Path

results_file = Path("scripts/benchmark_timings.json")

if results_file.exists():
    with open(results_file) as f:
        all_results = json.load(f)

    for env_name, env_data in all_results.items():
        print(f"### {env_data.get('description', env_name)}")
        print()
        print(f"- **Environment**: {env_data.get('environment', 'unknown')}")
        print(f"- **Files tested**: {env_data.get('n_files', 'N/A')}")
        print(f"- **Timestamp**: {env_data.get('timestamp', 'N/A')}")
        print()

        timings = env_data.get("timings", {})
        if timings:
            # Sort by total time
            sorted_methods = sorted(timings.items(), key=lambda x: x[1].get("total", float("inf")))
            fastest_total = sorted_methods[0][1].get("total", 1) if sorted_methods else 1

            print("| Method | Open | Spatial | Time Slice | Timeseries | **Total** |")
            print("|--------|-----:|--------:|-----------:|-----------:|----------:|")

            for method, times in sorted_methods:
                total = times.get("total", 0)
                speedup = total / fastest_total if fastest_total > 0 else 1
                speedup_str = " ⚡" if speedup <= 1.01 else f" ({speedup:.1f}x)"

                print(
                    f"| {method} | "
                    f"{times.get('open', 0):.2f}s | "
                    f"{times.get('spatial_subset_load', 0):.2f}s | "
                    f"{times.get('time_slice_load', 0):.2f}s | "
                    f"{times.get('timeseries_load', 0):.2f}s | "
                    f"**{total:.2f}s**{speedup_str} |"
                )

            print()
            print("*All times in seconds. Lower is better.*")
            print()
else:
    print("*No benchmark results available. Run the benchmark script to generate results.*")
```


## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--environment` | `local` | Label for this run (`local` or `cloud`) |
| `--description` | auto | Description for this run |
| `--n-files` | 5 | Number of files to test with |
| `--output` | `benchmark_timings.json` | Output JSON file |
| `--skip` | `fsspec_default` | Benchmarks to skip |

## Benchmarked Methods

| Method | Description |
|--------|-------------|
| `fsspec_default_cache` | fsspec with default caching strategy |
| `fsspec_block_cache` | fsspec with 8MB block cache |
| `obstore_reader` | Basic `ObstoreReader` with buffered reads |
| `obstore_eager` | `ObstoreEagerReader` - loads entire file into memory |
| `obstore_prefetch` | `ObstorePrefetchReader` - background prefetching |
| `obstore_parallel` | `ObstoreParallelReader` - parallel range fetching |
| `obstore_hybrid` | `ObstoreHybridReader` - exponential readahead + parallel fetching |
| `virtualzarr_icechunk` | VirtualiZarr + Icechunk for virtual Zarr stores |


## File Handlers Comparison

### ObstoreReader

The basic reader with configurable buffer size. Best for simple sequential reads.

```python
from obspec_utils import ObstoreReader

reader = ObstoreReader(store, path, buffer_size=1024*1024)
```

### ObstoreEagerReader

Loads the entire file into memory before reading. Best when files will be read multiple times and are small enough to fit in memory.

```python
from obspec_utils import ObstoreEagerReader

reader = ObstoreEagerReader(store, path)
```

### ObstorePrefetchReader

Prefetches upcoming byte ranges in background threads. Best for sequential read patterns.

```python
from obspec_utils import ObstorePrefetchReader

reader = ObstorePrefetchReader(
    store, path,
    prefetch_size=4*1024*1024,  # 4 MB ahead
    chunk_size=1024*1024,        # 1 MB chunks
    max_workers=2,
)
```

### ObstoreParallelReader

Fetches multiple byte ranges in parallel using `get_ranges`. Best for random access patterns.

```python
from obspec_utils import ObstoreParallelReader

reader = ObstoreParallelReader(
    store, path,
    chunk_size=1024*1024,   # 1 MB chunks
    batch_size=16,          # Up to 16 parallel fetches
)
```

### ObstoreHybridReader

Combines exponential readahead (for metadata) with parallel chunk fetching (for data). Best for HDF5/NetCDF files.

```python
from obspec_utils import ObstoreHybridReader

reader = ObstoreHybridReader(
    store, path,
    initial_readahead=32*1024,   # Start with 32 KB
    readahead_multiplier=2.0,    # Double each time
    chunk_size=1024*1024,        # 1 MB chunks for data
)
```

## Choosing the Right Reader

| Use Case | Recommended Reader |
|----------|-------------------|
| Small files, repeated access | `ObstoreEagerReader` |
| Sequential reads, streaming | `ObstorePrefetchReader` |
| Random access, array chunks | `ObstoreParallelReader` |
| HDF5/NetCDF files | `ObstoreHybridReader` |
| Simple, one-time reads | `ObstoreReader` |
