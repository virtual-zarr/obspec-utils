# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "obspec-utils",
#     "obstore",
#     "fsspec",
#     "s3fs",
#     "xarray",
#     "h5netcdf",
#     "h5py",
#     "numpy",
#     "dask",
#     "matplotlib",
# ]
#
# [tool.uv.sources]
# obspec-utils = { path = ".." }
# ///
"""
Benchmark comparing ObstoreReader (native obstore API) vs StoreReader (obspec protocol).

This benchmark tests whether using the ReadableStore protocol abstraction adds
measurable overhead compared to using obstore's native open_reader() API directly.

Both readers are backed by the same obstore S3Store, so any difference in
performance would be due to the abstraction layer.
"""

from __future__ import annotations

import argparse
import gc
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Protocol

import fsspec
import xarray as xr


# S3 bucket and path for NLDAS3 data
BUCKET = "nasa-waterinsight"
PREFIX = "NLDAS3/forcing/daily"
REGION = "us-west-2"


class FileLikeReader(Protocol):
    """Protocol for file-like readers."""

    def read(self, size: int, /) -> bytes: ...
    def seek(self, offset: int, whence: int = 0, /) -> int: ...
    def tell(self) -> int: ...


# Type alias for reader factory functions
ReaderFactory = Callable[[Any, str], FileLikeReader]


@dataclass
class TimingRecord:
    """Single timing measurement with execution order."""

    execution_order: int
    time: float


@dataclass
class BenchmarkResult:
    """Container for benchmark timing results."""

    name: str
    open_times: list[TimingRecord]
    spatial_subset_times: list[TimingRecord]
    time_slice_times: list[TimingRecord]

    def _times(self, records: list[TimingRecord]) -> list[float]:
        return [r.time for r in records]

    @property
    def open_mean(self) -> float:
        return statistics.mean(self._times(self.open_times))

    @property
    def open_std(self) -> float:
        times = self._times(self.open_times)
        return statistics.stdev(times) if len(times) > 1 else 0

    @property
    def spatial_mean(self) -> float:
        return statistics.mean(self._times(self.spatial_subset_times))

    @property
    def spatial_std(self) -> float:
        times = self._times(self.spatial_subset_times)
        return statistics.stdev(times) if len(times) > 1 else 0

    @property
    def time_slice_mean(self) -> float:
        return statistics.mean(self._times(self.time_slice_times))

    @property
    def time_slice_std(self) -> float:
        times = self._times(self.time_slice_times)
        return statistics.stdev(times) if len(times) > 1 else 0

    @property
    def total_mean(self) -> float:
        return self.open_mean + self.spatial_mean + self.time_slice_mean


def get_nldas_files(n_files: int) -> list[str]:
    """Get list of NLDAS3 files from S3."""
    print("Discovering NLDAS3 files on S3...")
    fs = fsspec.filesystem("s3", anon=True)
    files = fs.glob(f"s3://{BUCKET}/{PREFIX}/**/*.nc")
    files = sorted(["s3://" + f for f in files])[:n_files]
    print(f"Using {len(files)} files")
    return files


def run_single_iteration(
    name: str,
    reader_factory: ReaderFactory,
    files: list[str],
) -> tuple[float, float, float]:
    """Run a single benchmark iteration for a reader implementation."""
    from obstore.store import S3Store

    gc.collect()

    # Open dataset
    start = perf_counter()
    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    readers = [reader_factory(store, f.replace(f"s3://{BUCKET}/", "")) for f in files]
    ds = xr.open_mfdataset(
        readers,
        engine="h5netcdf",
        combine="nested",
        concat_dim="time",
        parallel=True,
    )
    open_time = perf_counter() - start

    # Spatial subset
    start = perf_counter()
    _ = ds["Tair"].isel(time=0).sel(lat=slice(10, 15), lon=slice(-60, -55)).load()
    spatial_time = perf_counter() - start

    # Time slice
    start = perf_counter()
    _ = ds["Tair"].sel(time="2001-01-05", method="nearest").load()
    time_slice_time = perf_counter() - start

    ds.close()
    del ds, readers, store

    return open_time, spatial_time, time_slice_time


def run_benchmarks_interleaved(
    benchmarks: list[tuple[str, ReaderFactory]],
    files: list[str],
    n_iterations: int,
) -> list[BenchmarkResult]:
    """Run benchmarks with iterations interleaved and randomized."""
    import random

    # Create list of all (name, factory, iteration_index) tuples
    tasks = [
        (name, factory, i) for name, factory in benchmarks for i in range(n_iterations)
    ]

    # Randomize order
    random.shuffle(tasks)

    # Storage for results with execution order
    results_dict: dict[str, dict[str, list[TimingRecord]]] = {
        name: {"open": [], "spatial": [], "time_slice": []} for name, _ in benchmarks
    }

    total_tasks = len(tasks)
    for task_num, (name, factory, iteration) in enumerate(tasks, 1):
        print(
            f"  [{task_num}/{total_tasks}] {name} (iteration {iteration + 1}/{n_iterations})"
        )
        open_time, spatial_time, time_slice_time = run_single_iteration(
            name, factory, files
        )
        results_dict[name]["open"].append(TimingRecord(task_num, open_time))
        results_dict[name]["spatial"].append(TimingRecord(task_num, spatial_time))
        results_dict[name]["time_slice"].append(TimingRecord(task_num, time_slice_time))

    # Convert to BenchmarkResult objects
    return [
        BenchmarkResult(
            name=name,
            open_times=results_dict[name]["open"],
            spatial_subset_times=results_dict[name]["spatial"],
            time_slice_times=results_dict[name]["time_slice"],
        )
        for name, _ in benchmarks
    ]


def plot_results(results: list[BenchmarkResult], output_path: str) -> None:
    """Plot timing results over execution order to check for trends."""
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    metrics = [
        ("open_times", "Open Time (s)"),
        ("spatial_subset_times", "Spatial Subset Time (s)"),
        ("time_slice_times", "Time Slice Time (s)"),
    ]

    for ax, (attr, ylabel) in zip(axes, metrics):
        for result in results:
            records = getattr(result, attr)
            orders = [r.execution_order for r in records]
            times = [r.time for r in records]
            ax.scatter(orders, times, label=result.name, alpha=0.7)
        ax.set_ylabel(ylabel)
        ax.legend()
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Execution Order")
    axes[0].set_title("Benchmark Timing Over Execution Order (check for trends)")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"\nPlot saved to {output_path}")


def validate_readers(
    reader_factories: list[tuple[str, ReaderFactory]],
    files: list[str],
) -> None:
    """
    Validate that all reader implementations produce identical results.

    Runs outside of timing to verify correctness without affecting benchmarks.
    Raises AssertionError if results don't match.
    """
    from obstore.store import S3Store
    from xarray.testing import assert_allclose

    print("\nValidating reader implementations produce identical results...")

    store = S3Store(bucket=BUCKET, region=REGION, config={"skip_signature": "true"})
    paths = [f.replace(f"s3://{BUCKET}/", "") for f in files]

    # Load data with each reader
    datasets: list[tuple[str, xr.DataArray]] = []
    for name, factory in reader_factories:
        readers = [factory(store, path) for path in paths]
        ds = xr.open_mfdataset(
            readers,
            engine="h5netcdf",
            combine="nested",
            concat_dim="time",
            parallel=True,
        )
        data = (
            ds["Tair"].isel(time=0).sel(lat=slice(10, 15), lon=slice(-60, -55)).load()
        )
        ds.close()
        datasets.append((name, data))

    # Compare all datasets against the first
    reference_name, reference_data = datasets[0]
    for name, data in datasets[1:]:
        print(f"  Comparing {name} to {reference_name}...")
        assert_allclose(data, reference_data)
        print(f"  OK: {name} matches {reference_name}")

    print("  All reader implementations produce identical results.")


def print_results(results: list[BenchmarkResult]) -> None:
    """Print comparison table."""
    print("\n" + "=" * 80)
    print(" BENCHMARK RESULTS: ObstoreReader vs StoreReader")
    print("=" * 80)

    print(
        f"\n{'Reader':<25} {'Open (s)':<18} {'Spatial (s)':<18} {'Time Slice (s)':<18}"
    )
    print("-" * 80)

    for r in results:
        print(
            f"{r.name:<25} "
            f"{r.open_mean:>6.3f} ± {r.open_std:>5.3f}   "
            f"{r.spatial_mean:>6.3f} ± {r.spatial_std:>5.3f}   "
            f"{r.time_slice_mean:>6.3f} ± {r.time_slice_std:>5.3f}"
        )

    print("-" * 80)

    # Calculate relative performance
    if len(results) == 2:
        r1, r2 = results
        print("\nTotal time comparison:")
        print(f"  {r1.name}: {r1.total_mean:.3f}s")
        print(f"  {r2.name}: {r2.total_mean:.3f}s")

        if r1.total_mean < r2.total_mean:
            faster, slower = r1, r2
        else:
            faster, slower = r2, r1

        speedup = slower.total_mean / faster.total_mean
        diff_pct = (slower.total_mean - faster.total_mean) / slower.total_mean * 100
        print(f"\n  {faster.name} is {speedup:.2f}x faster ({diff_pct:.1f}% less time)")

    print("\n" + "=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark ObstoreReader vs StoreReader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--n-files",
        type=int,
        default=3,
        help="Number of files to test with (default: 3)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of iterations for each benchmark (default: 3)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate that all readers produce identical results (not timed)",
    )
    parser.add_argument(
        "--plot",
        type=str,
        metavar="FILE",
        help="Save timing plot to FILE (e.g., benchmark_plot.png)",
    )

    args = parser.parse_args()

    print("=" * 80)
    print(" ObstoreReader vs StoreReader Benchmark")
    print("=" * 80)
    print(f" Files: {args.n_files}")
    print(f" Iterations: {args.iterations}")
    print("=" * 80)

    files = get_nldas_files(args.n_files)

    # Define reader factories
    from obspec_utils.obspec import StoreReader
    from obspec_utils.obstore import ObstoreReader

    benchmarks = [
        (
            "ObstoreReader (native)",
            lambda store, path: ObstoreReader(store=store, path=path),
        ),
        (
            "StoreReader (protocol)",
            lambda store, path: StoreReader(store=store, path=path),
        ),
    ]

    if args.validate:
        validate_readers(benchmarks, files)

    print("\nRunning benchmarks (randomized and interleaved)...")
    results = run_benchmarks_interleaved(benchmarks, files, args.iterations)

    print_results(results)

    if args.plot:
        plot_results(results, args.plot)


if __name__ == "__main__":
    main()
