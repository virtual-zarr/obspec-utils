# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "obspec-utils",
#     "obstore",
#     "fsspec",
#     "s3fs",
#     "xarray",
#     "h5netcdf",
#     "icechunk",
#     "zarr>=3",
#     "numpy",
#     "dask",
# ]
#
# [tool.uv.sources]
# obspec-utils = { path = ".." }
# ///
"""
Benchmark script comparing different approaches for reading cloud-hosted data.
"""

from __future__ import annotations

import argparse
import json
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any

import fsspec
import xarray as xr
import zarr

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)

# Configure zarr for better async performance
zarr.config.set({"threading.max_workers": 32, "async.concurrency": 128})


# =============================================================================
# Configuration
# =============================================================================

# S3 bucket and path for NLDAS3 data
BUCKET = "nasa-waterinsight"
PREFIX = "NLDAS3/forcing/daily"
REGION = "us-west-2"

# Test parameters for consistent benchmarking
# Note: time selections use method="nearest" for robustness with varying file counts
SPATIAL_SUBSET_KWARGS = {
    "lat": slice(10, 15),
    "lon": slice(-60, -55),
}
TIME_SLICE_KWARGS = {"time": "2001-01-05", "method": "nearest"}
SPATIAL_POINT_KWARGS = {"lat": 45, "lon": -150, "method": "nearest"}


@dataclass
class TimingResult:
    """Container for timing results."""

    method: str
    open_time: float = 0.0
    spatial_subset_time: float = 0.0
    time_slice_time: float = 0.0
    timeseries_time: float = 0.0

    @property
    def total_time(self) -> float:
        return (
            self.open_time
            + self.spatial_subset_time
            + self.time_slice_time
            + self.timeseries_time
        )

    def to_dict(self) -> dict[str, float]:
        return {
            "open": self.open_time,
            "spatial_subset_load": self.spatial_subset_time,
            "time_slice_load": self.time_slice_time,
            "timeseries_load": self.timeseries_time,
            "total": self.total_time,
        }


@dataclass
class BenchmarkResults:
    """Container for all benchmark results."""

    environment: str
    description: str
    n_files: int
    timestamp: str = field(default_factory=lambda: time.strftime("%Y-%m-%d %H:%M:%S"))
    results: dict[str, TimingResult] = field(default_factory=dict)

    def add_result(self, result: TimingResult) -> None:
        self.results[result.method] = result

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "description": self.description,
            "n_files": self.n_files,
            "timestamp": self.timestamp,
            "timings": {
                method: result.to_dict() for method, result in self.results.items()
            },
        }

    def save(self, filepath: Path) -> None:
        """Save results to JSON file."""
        all_results = {}
        if filepath.exists():
            with open(filepath) as f:
                all_results = json.load(f)

        all_results[self.environment] = self.to_dict()

        with open(filepath, "w") as f:
            json.dump(all_results, f, indent=2)

        print(f"\n{'='*60}")
        print(f"Results saved to {filepath}")
        print(f"{'='*60}")


def print_timing(category: str, method: str, elapsed: float) -> None:
    """Print a timing result."""
    print(f"    {category}: {elapsed:.2f}s")


def run_benchmarks(ds: xr.Dataset, n_files: int) -> tuple[float, float, float]:
    """Run standard benchmark operations on a dataset."""
    # Spatial subset (single time step)
    start = perf_counter()
    _ = ds["Tair"].isel(time=0).sel(**SPATIAL_SUBSET_KWARGS).load()
    spatial_time = perf_counter() - start

    # Time slice (all spatial data for one time)
    start = perf_counter()
    _ = ds["Tair"].sel(**TIME_SLICE_KWARGS).load()
    time_slice_time = perf_counter() - start

    # Timeseries at a point
    start = perf_counter()
    _ = ds["Tair"].sel(**SPATIAL_POINT_KWARGS).isel(time=slice(0, n_files)).load()
    timeseries_time = perf_counter() - start

    return spatial_time, time_slice_time, timeseries_time


def print_result_summary(result: TimingResult) -> None:
    """Print a summary box for a timing result."""
    print(f"\n  {'─'*50}")
    print(f"  {result.method}")
    print(f"  {'─'*50}")
    print(f"    Open:           {result.open_time:>8.2f}s")
    print(f"    Spatial subset: {result.spatial_subset_time:>8.2f}s")
    print(f"    Time slice:     {result.time_slice_time:>8.2f}s")
    print(f"    Timeseries:     {result.timeseries_time:>8.2f}s")
    print(f"  {'─'*50}")
    print(f"    TOTAL:          {result.total_time:>8.2f}s")
    print(f"  {'─'*50}")


# =============================================================================
# Benchmark Methods
# =============================================================================


def benchmark_fsspec_default(files: list[str], n_files: int) -> TimingResult:
    """Benchmark fsspec with default cache settings."""
    print("\n[fsspec_default_cache] Testing fsspec + h5netcdf (default cache)...")

    result = TimingResult(method="fsspec_default_cache")

    start = perf_counter()
    fs = fsspec.filesystem("s3", anon=True)
    file_objs = [fs.open(f) for f in files[:n_files]]
    ds = xr.open_mfdataset(
        file_objs, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    # Cleanup
    for f in file_objs:
        f.close()
    del fs, file_objs, ds

    print_result_summary(result)
    return result


def benchmark_fsspec_block_cache(files: list[str], n_files: int) -> TimingResult:
    """Benchmark fsspec with block cache settings."""
    print("\n[fsspec_block_cache] Testing fsspec + h5netcdf (block cache)...")

    result = TimingResult(method="fsspec_block_cache")

    fsspec_caching = {
        "cache_type": "blockcache",
        "block_size": 1024 * 1024 * 8,  # 8 MB blocks
    }

    start = perf_counter()
    fs = fsspec.filesystem("s3", anon=True)
    file_objs = [fs.open(f, **fsspec_caching) for f in files[:n_files]]
    ds = xr.open_mfdataset(
        file_objs, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    # Cleanup
    for f in file_objs:
        f.close()
    del fs, file_objs, ds

    print_result_summary(result)
    return result


def benchmark_obstore_reader(files: list[str], n_files: int) -> TimingResult:
    """Benchmark obstore ObstoreReader."""
    from obstore.store import S3Store

    from obspec_utils import ObstoreReader

    print("\n[obstore_reader] Testing ObstoreReader...")

    result = TimingResult(method="obstore_reader")

    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [
        ObstoreReader(store=store, path=f.replace(f"s3://{BUCKET}/", ""))
        for f in files[:n_files]
    ]
    ds = xr.open_mfdataset(
        readers, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    del store, readers, ds

    print_result_summary(result)
    return result


def benchmark_obstore_eager(files: list[str], n_files: int) -> TimingResult:
    """Benchmark obstore ObstoreEagerReader."""
    from obstore.store import S3Store

    from obspec_utils import ObstoreEagerReader

    print("\n[obstore_eager] Testing ObstoreEagerReader...")

    result = TimingResult(method="obstore_eager")

    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [
        ObstoreEagerReader(store=store, path=f.replace(f"s3://{BUCKET}/", ""))
        for f in files[:n_files]
    ]
    ds = xr.open_mfdataset(
        readers, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    del store, readers, ds

    print_result_summary(result)
    return result


def benchmark_obstore_prefetch(files: list[str], n_files: int) -> TimingResult:
    """Benchmark obstore ObstorePrefetchReader."""
    from obstore.store import S3Store

    from obspec_utils import ObstorePrefetchReader

    print("\n[obstore_prefetch] Testing ObstorePrefetchReader...")

    result = TimingResult(method="obstore_prefetch")

    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [
        ObstorePrefetchReader(
            store=store,
            path=f.replace(f"s3://{BUCKET}/", ""),
            prefetch_size=4 * 1024 * 1024,  # 4 MB prefetch
            chunk_size=1024 * 1024,  # 1 MB chunks
        )
        for f in files[:n_files]
    ]
    ds = xr.open_mfdataset(
        readers, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    # Cleanup prefetch readers
    for r in readers:
        r.close()
    del store, readers, ds

    print_result_summary(result)
    return result


def benchmark_obstore_parallel(files: list[str], n_files: int) -> TimingResult:
    """Benchmark obstore ObstoreParallelReader."""
    from obstore.store import S3Store

    from obspec_utils import ObstoreParallelReader

    print("\n[obstore_parallel] Testing ObstoreParallelReader...")

    result = TimingResult(method="obstore_parallel")

    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [
        ObstoreParallelReader(
            store=store,
            path=f.replace(f"s3://{BUCKET}/", ""),
            chunk_size=1024 * 1024,  # 1 MB chunks
            batch_size=16,  # Fetch up to 16 ranges in parallel
        )
        for f in files[:n_files]
    ]
    ds = xr.open_mfdataset(
        readers, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    # Cleanup parallel readers
    for r in readers:
        r.close()
    del store, readers, ds

    print_result_summary(result)
    return result


def benchmark_obstore_hybrid(files: list[str], n_files: int) -> TimingResult:
    """Benchmark obstore ObstoreHybridReader."""
    from obstore.store import S3Store

    from obspec_utils import ObstoreHybridReader

    print("\n[obstore_hybrid] Testing ObstoreHybridReader...")

    result = TimingResult(method="obstore_hybrid")

    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [
        ObstoreHybridReader(
            store=store,
            path=f.replace(f"s3://{BUCKET}/", ""),
            initial_readahead=32 * 1024,  # 32 KB initial
            readahead_multiplier=2.0,  # Double each time
            chunk_size=1024 * 1024,  # 1 MB chunks for random access
            batch_size=16,
        )
        for f in files[:n_files]
    ]
    ds = xr.open_mfdataset(
        readers, engine="h5netcdf", combine="nested", concat_dim="time", parallel=True
    )
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    # Cleanup hybrid readers
    for r in readers:
        r.close()
    del store, readers, ds

    print_result_summary(result)
    return result


def benchmark_virtualzarr_icechunk(n_files: int) -> TimingResult:
    """Benchmark VirtualiZarr + Icechunk approach."""
    import icechunk

    print("\n[virtualzarr_icechunk] Testing VirtualiZarr + Icechunk...")

    result = TimingResult(method="virtualzarr_icechunk")

    start = perf_counter()
    storage = icechunk.s3_storage(
        bucket=BUCKET,
        prefix="virtual-zarr-store/NLDAS-3-icechunk",
        region="us-west-2",
        anonymous=True,
    )

    chunk_url = f"s3://{BUCKET}/{PREFIX}/"
    virtual_credentials = icechunk.containers_credentials(
        {chunk_url: icechunk.s3_anonymous_credentials()}
    )

    repo = icechunk.Repository.open(
        storage=storage,
        authorize_virtual_chunk_access=virtual_credentials,
    )

    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store, consolidated=False, zarr_format=3, chunks={})
    result.open_time = perf_counter() - start

    (
        result.spatial_subset_time,
        result.time_slice_time,
        result.timeseries_time,
    ) = run_benchmarks(ds, n_files)

    del session, repo, storage, ds

    print_result_summary(result)
    return result


# =============================================================================
# Results Display
# =============================================================================


def print_comparison_table(results: BenchmarkResults) -> None:
    """Print a comparison table of all results."""
    print("\n")
    print("=" * 80)
    print(" BENCHMARK COMPARISON")
    print("=" * 80)
    print(f" Environment: {results.environment}")
    print(f" Description: {results.description}")
    print(f" Files tested: {results.n_files}")
    print(f" Timestamp: {results.timestamp}")
    print("=" * 80)

    # Header
    print(
        f"\n{'Method':<25} {'Open':>10} {'Spatial':>10} {'Time':>10} {'Series':>10} {'TOTAL':>10}"
    )
    print("-" * 80)

    # Sort by total time
    sorted_results = sorted(results.results.values(), key=lambda r: r.total_time)

    fastest = sorted_results[0].total_time if sorted_results else 1

    for result in sorted_results:
        speedup = result.total_time / fastest if fastest > 0 else 1
        speedup_str = f"({speedup:.1f}x)" if speedup > 1.01 else "(fastest)"

        print(
            f"{result.method:<25} "
            f"{result.open_time:>10.2f} "
            f"{result.spatial_subset_time:>10.2f} "
            f"{result.time_slice_time:>10.2f} "
            f"{result.timeseries_time:>10.2f} "
            f"{result.total_time:>10.2f} {speedup_str}"
        )

    print("-" * 80)
    print("\nAll times in seconds. Lower is better.")


# =============================================================================
# Main
# =============================================================================


def get_nldas_files() -> list[str]:
    """Get list of NLDAS3 files from S3."""
    print("Discovering NLDAS3 files on S3...")
    fs = fsspec.filesystem("s3", anon=True)
    files = fs.glob(f"s3://{BUCKET}/{PREFIX}/**/*.nc")
    files = sorted(["s3://" + f for f in files])
    print(f"Found {len(files)} files")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark cloud data reading approaches",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--environment",
        choices=["local", "cloud"],
        default="local",
        help="Environment label for this run (default: local)",
    )
    parser.add_argument(
        "--description",
        default=None,
        help="Description for this run (e.g., 'MacBook Pro - Durham')",
    )
    parser.add_argument(
        "--n-files",
        type=int,
        default=5,
        help="Number of files to test with (default: 5)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("scripts/benchmark_timings.json"),
        help="Output file for timing results (default: scripts/benchmark_timings.json)",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        default=[],
        choices=[
            "fsspec_default",
            "fsspec_block",
            "obstore_reader",
            "obstore_eager",
            "obstore_prefetch",
            "obstore_parallel",
            "obstore_hybrid",
            "virtualzarr",
        ],
        help="Skip specific benchmarks",
    )

    args = parser.parse_args()

    description = args.description or (
        "Cloud compute (us-west-2)" if args.environment == "cloud" else "Local machine"
    )

    print("=" * 60)
    print(" Cloud Data Reading Benchmark")
    print("=" * 60)
    print(f" Environment: {args.environment}")
    print(f" Description: {description}")
    print(f" N files: {args.n_files}")
    print("=" * 60)

    # Get file list
    files = get_nldas_files()

    if len(files) < args.n_files:
        print(f"Warning: Only {len(files)} files available, using all of them")
        args.n_files = len(files)

    # Initialize results
    results = BenchmarkResults(
        environment=args.environment,
        description=description,
        n_files=args.n_files,
    )

    # Run benchmarks
    benchmarks: list[tuple[str, Callable[[], TimingResult]]] = []

    if "fsspec_default" not in args.skip:
        benchmarks.append(
            ("fsspec_default", lambda: benchmark_fsspec_default(files, args.n_files))
        )

    if "fsspec_block" not in args.skip:
        benchmarks.append(
            (
                "fsspec_block",
                lambda: benchmark_fsspec_block_cache(files, args.n_files),
            )
        )

    if "obstore_reader" not in args.skip:
        benchmarks.append(
            ("obstore_reader", lambda: benchmark_obstore_reader(files, args.n_files))
        )

    if "obstore_eager" not in args.skip:
        benchmarks.append(
            (
                "obstore_eager",
                lambda: benchmark_obstore_eager(files, args.n_files),
            )
        )

    if "obstore_prefetch" not in args.skip:
        benchmarks.append(
            (
                "obstore_prefetch",
                lambda: benchmark_obstore_prefetch(files, args.n_files),
            )
        )

    if "obstore_parallel" not in args.skip:
        benchmarks.append(
            (
                "obstore_parallel",
                lambda: benchmark_obstore_parallel(files, args.n_files),
            )
        )

    if "obstore_hybrid" not in args.skip:
        benchmarks.append(
            (
                "obstore_hybrid",
                lambda: benchmark_obstore_hybrid(files, args.n_files),
            )
        )

    if "virtualzarr" not in args.skip:
        benchmarks.append(
            ("virtualzarr", lambda: benchmark_virtualzarr_icechunk(args.n_files))
        )

    for name, benchmark_fn in benchmarks:
        try:
            result = benchmark_fn()
            results.add_result(result)
        except Exception as e:
            print(f"\n[{name}] FAILED: {e}")

    # Print comparison and save results
    print_comparison_table(results)
    results.save(args.output)


if __name__ == "__main__":
    main()
