#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "xarray",
#     "h5netcdf",
#     "h5py",
#     "obspec-utils @ git+https://github.com/virtual-zarr/obspec-utils@main",
#     "obstore",
#     "aiohttp",
#     "fsspec",
# ]
# ///
"""
Compare obspec-utils readers against fsspec for NASA Earthdata access.

This script benchmarks different approaches for reading NISAR data:
- fsspec/earthaccess (baseline)
- AiohttpStore + BlockStoreReader
- HTTPStore + BlockStoreReader

Usage:
    uv run docs/examples/nisar/benchmark.py
    uv run docs/examples/nisar/benchmark.py --block-size 32
"""

import argparse
import time
from urllib.parse import urlparse

import earthaccess
import xarray as xr
from obstore.store import HTTPStore

from obspec_utils.readers import BlockStoreReader
from obspec_utils.stores import AiohttpStore

MB = 1024 * 1024


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare obspec-utils readers against fsspec for NASA Earthdata"
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=16,
        help="Block size in MB for BlockStoreReader (default: 16)",
    )
    return parser.parse_args()


def read_middle_pixel(file_like) -> float:
    """Open dataset and read a single pixel from the middle."""
    ds = xr.open_datatree(
        file_like,
        engine="h5netcdf",
        decode_timedelta=False,
        phony_dims="access",
    )
    ny, nx = ds.science.LSAR.GCOV.grids.frequencyA.HHHH.shape
    value = ds.science.LSAR.GCOV.grids.frequencyA.HHHH[ny // 2, nx // 2].values
    return float(value)


def main():
    args = parse_args()
    block_size_mb = args.block_size
    block_size = block_size_mb * MB

    print("=" * 60)
    print("Compare obspec-utils Readers vs fsspec")
    print("=" * 60)
    print(f"Block size: {block_size_mb} MB")

    # Authenticate and Query Data
    print("\nAuthenticating with NASA Earthdata...")
    earthaccess.login()

    query = earthaccess.DataGranules()
    query.short_name("NISAR_L2_GCOV_BETA_V1")
    query.params["attribute[]"] = "int,FRAME_NUMBER,77"
    query.params["attribute[]"] = "int,TRACK_NUMBER,5"
    results = query.get_all()

    print(f"Found {len(results)} granules")

    # Get the HTTPS URL for the first granule
    https_links = earthaccess.results.DataGranule.data_links(
        results[0], access="external"
    )
    https_url = https_links[0]
    print(f"HTTPS URL: {https_url}")

    # Parse the HTTPS URL to get base URL and path
    parsed = urlparse(https_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.lstrip("/")

    print(f"Base URL: {base_url}")
    print(f"Path: {path}")

    # Get the EDL token for authentication
    token = earthaccess.get_edl_token()["access_token"]

    # Create AiohttpStore with EDL token authentication
    aiohttp_store = AiohttpStore(
        base_url,
        headers={"Authorization": f"Bearer {token}"},
    )

    # Create HTTPStore with EDL token authentication
    http_store = HTTPStore.from_url(
        https_url,
        client_options={"default_headers": {"Authorization": f"Bearer {token}"}},
    )

    # Benchmark: fsspec (baseline)
    print("\n[1/3] Testing fsspec/earthaccess.open()...")
    start = time.perf_counter()

    fs_file = earthaccess.open(results[:1])[0]
    value_fsspec = read_middle_pixel(fs_file)

    elapsed_fsspec = time.perf_counter() - start
    print(f"      Value: {value_fsspec}")
    print(f"      Time: {elapsed_fsspec:.2f}s")

    # Benchmark: AiohttpStore + BlockStoreReader
    print(f"\n[2/3] Testing AiohttpStore + BlockStoreReader ({block_size_mb} MB)...")
    start = time.perf_counter()

    with BlockStoreReader(
        aiohttp_store,
        path,
        block_size=block_size,
        max_cached_blocks=1024,
    ) as reader:
        value_aiohttp = read_middle_pixel(reader)

    elapsed_aiohttp = time.perf_counter() - start
    print(f"      Value: {value_aiohttp}")
    print(f"      Time: {elapsed_aiohttp:.2f}s")

    # Benchmark: HTTPStore + BlockStoreReader
    print(f"\n[3/3] Testing HTTPStore + BlockStoreReader ({block_size_mb} MB)...")
    start = time.perf_counter()

    with BlockStoreReader(
        http_store,
        "",  # HTTPStore already includes the full path
        block_size=block_size,
        max_cached_blocks=1024,
    ) as reader:
        value_http = read_middle_pixel(reader)

    elapsed_http = time.perf_counter() - start
    print(f"      Value: {value_http}")
    print(f"      Time: {elapsed_http:.2f}s")

    # Results Summary
    print("\n" + "=" * 50)
    print("RESULTS SUMMARY")
    print("=" * 50)

    results_data = [
        ("fsspec (baseline)", elapsed_fsspec, 1.0),
        (
            "AiohttpStore + BlockStoreReader",
            elapsed_aiohttp,
            elapsed_fsspec / elapsed_aiohttp,
        ),
        ("HTTPStore + BlockStoreReader", elapsed_http, elapsed_fsspec / elapsed_http),
    ]

    print(f"{'Reader':<35} {'Time (s)':<12} {'Speedup':<10}")
    print("-" * 60)
    for name, elapsed, speedup in results_data:
        print(f"{name:<35} {elapsed:<12.2f} {speedup:<10.2f}x")

    # Verify all values match
    print("\nValue verification:")
    all_equal = value_fsspec == value_aiohttp == value_http
    print(f"  All values equal: {all_equal}")


if __name__ == "__main__":
    main()
