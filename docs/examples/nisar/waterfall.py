#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "xarray",
#     "h5netcdf",
#     "h5py",
#     "obspec-utils @ git+https://github.com/virtual-zarr/obspec-utils@main",
#     "aiohttp",
#     "pandas",
#     "holoviews",
#     "bokeh",
# ]
# ///
"""
Waterfall visualization of HTTP range requests when reading NISAR data.

Creates an interactive HTML visualization showing:
- Request byte ranges over time (waterfall chart)
- Request concurrency over time
- Statistics (throughput, request sizes, timing)

Usage:
    uv run docs/examples/nisar/waterfall.py
    uv run docs/examples/nisar/waterfall.py --block-size 8
    uv run docs/examples/nisar/waterfall.py --output my_waterfall.html
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from urllib.parse import urlparse

import earthaccess
import holoviews as hv
import numpy as np
import pandas as pd
import xarray as xr

from obspec_utils.readers import BlockStoreReader
from obspec_utils.stores import AiohttpStore
from obspec_utils.wrappers import RequestTrace, TracingReadableStore

hv.extension("bokeh")

MB = 1024 * 1024


def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize HTTP range requests when reading NISAR data"
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=13,
        help="Block size in MB for BlockStoreReader (default: 13)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output HTML file path (default: waterfall.html in script dir)",
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


def visualize_waterfall(
    requests_df: pd.DataFrame,
    title: str = "Request Waterfall",
    file_size: int | None = None,
    read_time: float | None = None,
):
    """
    Create a waterfall visualization of requests over time.

    X-axis: byte position in file (MB)
    Y-axis: time (inverted, so time flows downward)
    """
    if requests_df.empty or "timestamp" not in requests_df.columns:
        return hv.Div("<p>No request timing data available</p>")

    requests_sorted = requests_df.sort_values("timestamp").reset_index(drop=True)

    # Calculate time relative to first request
    min_ts = requests_sorted["timestamp"].min()

    # Determine file boundaries (in MB)
    file_end_bytes = file_size if file_size else requests_df["end"].max()
    file_end_mb = file_end_bytes / MB

    # Create rectangles for each request (x-axis in MB)
    waterfall_rects = []
    for _, row in requests_sorted.iterrows():
        time_sent_s = float(row["timestamp"] - min_ts)
        duration_s = float(row.get("duration", 0))
        time_received_s = time_sent_s + duration_s

        waterfall_rects.append(
            {
                "x0": row["start"] / MB,
                "x1": row["end"] / MB,
                "y0": time_sent_s,
                "y1": time_received_s,
                "length_mb": row["length"] / MB,
                "duration_s": duration_s,
                "time_sent_s": time_sent_s,
                "time_received_s": time_received_s,
                "method": row.get("method", "unknown"),
            }
        )

    waterfall_df = pd.DataFrame(waterfall_rects)

    # Ensure proper numeric dtypes
    waterfall_df = waterfall_df.astype(
        {
            "x0": "float64",
            "x1": "float64",
            "y0": "float64",
            "y1": "float64",
            "length_mb": "float64",
            "duration_s": "float64",
            "time_sent_s": "float64",
            "time_received_s": "float64",
        }
    )

    # Debug info
    print(
        f"      Request byte ranges: {waterfall_df['x0'].min():.2f} - {waterfall_df['x1'].max():.2f} MB"
    )
    print(
        f"      Request sizes: min={waterfall_df['length_mb'].min():.3f} MB, max={waterfall_df['length_mb'].max():.3f} MB, mean={waterfall_df['length_mb'].mean():.3f} MB"
    )

    # Calculate max time for y-axis
    max_time_s = waterfall_df["time_received_s"].max()

    # Create the main waterfall plot colored by time sent
    waterfall_plot = hv.Rectangles(
        waterfall_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=["length_mb", "duration_s", "time_sent_s", "time_received_s", "method"],
    ).opts(
        color="time_sent_s",
        cmap="viridis",
        colorbar=True,
        clabel="Time Sent (s)",
        alpha=0.7,
        line_color="black",
        line_width=0.5,
        tools=["hover"],
        width=1000,
        height=600,
        xlabel="Offset (MB)",
        ylabel="Time (s)",
        title=title,
        invert_yaxis=True,
        xlim=(0, file_end_mb),
        show_grid=True,
        gridstyle={"grid_line_alpha": 0.3, "grid_line_dash": [4, 4]},
        ylim=(max_time_s * 1.05, -max_time_s * 0.02),
    )

    # Add file boundary lines
    start_line = hv.VLine(0).opts(color="red", line_width=2, line_dash="dashed")
    end_line = hv.VLine(file_end_mb).opts(color="red", line_width=2, line_dash="dashed")

    waterfall_plot = waterfall_plot * start_line * end_line

    # Create concurrency plot
    time_resolution_s = max(0.001, max_time_s / 200)
    time_bins = np.arange(0, max_time_s + time_resolution_s, time_resolution_s)
    concurrent_counts = np.zeros(len(time_bins) - 1)

    for _, row in waterfall_df.iterrows():
        start_bin = int(row["time_sent_s"] / time_resolution_s)
        end_bin = int(row["time_received_s"] / time_resolution_s) + 1
        start_bin = max(0, min(start_bin, len(concurrent_counts) - 1))
        end_bin = max(0, min(end_bin, len(concurrent_counts)))
        concurrent_counts[start_bin:end_bin] += 1

    concurrency_df = pd.DataFrame(
        {
            "time_s": time_bins[:-1] + time_resolution_s / 2,
            "concurrent": concurrent_counts,
        }
    )

    concurrency_plot = hv.Area(
        concurrency_df,
        kdims=["time_s"],
        vdims=["concurrent"],
    ).opts(
        width=1000,
        height=150,
        xlabel="Time (s)",
        ylabel="Concurrent Requests",
        title="Request Concurrency Over Time",
        color="steelblue",
        alpha=0.7,
        tools=["hover"],
        show_grid=True,
        gridstyle={"grid_line_alpha": 0.3, "grid_line_dash": [4, 4]},
    )

    # Calculate statistics
    total_request_bytes = requests_df["length"].sum()
    total_bytes_mb = total_request_bytes / MB
    avg_duration_s = waterfall_df["duration_s"].mean()
    max_duration_s = waterfall_df["duration_s"].max()
    min_duration_s = waterfall_df["duration_s"].min()
    max_concurrent = int(concurrent_counts.max())

    # Throughput
    throughput_mbps = (
        (total_request_bytes * 8 / 1_000_000) / max_time_s if max_time_s > 0 else 0
    )

    read_time_str = f"{read_time:.2f} s" if read_time is not None else "N/A"

    stats_div = hv.Div(f"""
        <div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 4px;
                    padding: 10px; font-family: monospace; font-size: 11px; line-height: 1.5;">
            <b>Waterfall Statistics</b><br>
            File Size: {file_end_mb:.2f} MB<br>
            Total Requests: {len(requests_df):,}<br>
            Total Bytes: {total_bytes_mb:.2f} MB<br>
            <br>
            <b>Timing:</b><br>
            Total Duration: {max_time_s:.2f} s<br>
            Avg Request Duration: {avg_duration_s:.3f} s<br>
            Min Request Duration: {min_duration_s:.3f} s<br>
            Max Request Duration: {max_duration_s:.3f} s<br>
            <br>
            <b>Concurrency:</b><br>
            Max Concurrent Requests: {max_concurrent}<br>
            Effective Throughput: {throughput_mbps:.1f} Mbps<br>
            <br>
            <b>Read time: {read_time_str}</b>
        </div>
    """)

    layout = (
        (waterfall_plot + concurrency_plot + stats_div).opts(shared_axes=False).cols(1)
    )

    return layout


def main():
    args = parse_args()
    block_size_mb = args.block_size
    block_size = block_size_mb * MB

    print("=" * 60)
    print("BlockStoreReader Request Waterfall")
    print("=" * 60)
    print(f"Block size: {block_size_mb} MB")

    # Authenticate and Query Data
    print("\n[1/6] Authenticating with NASA Earthdata...")
    earthaccess.login()

    query = earthaccess.DataGranules()
    query.short_name("NISAR_L2_GCOV_BETA_V1")
    query.params["attribute[]"] = "int,FRAME_NUMBER,77"
    query.params["attribute[]"] = "int,TRACK_NUMBER,5"
    results = query.get_all()

    print(f"      Found {len(results)} granules")

    # Get the HTTPS URL
    https_links = earthaccess.results.DataGranule.data_links(
        results[0], access="external"
    )
    https_url = https_links[0]
    print(f"      HTTPS URL: {https_url}")

    # Parse URL
    parsed = urlparse(https_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.lstrip("/")

    # Get EDL token
    print("\n[2/6] Getting EDL token...")
    token = earthaccess.get_edl_token()["access_token"]

    # Create store chain
    print("\n[3/6] Setting up store chain...")
    base_store = AiohttpStore(
        base_url,
        headers={"Authorization": f"Bearer {token}"},
    )
    trace = RequestTrace()
    traced_store = TracingReadableStore(base_store, trace)
    print("      AiohttpStore -> TracingReadableStore")

    # Get file size
    print("\n[4/6] Getting file size...")
    meta = traced_store.head(path)
    file_size = meta["size"]
    print(f"      File size: {file_size / MB:.2f} MB")

    # Clear HEAD request from trace
    trace.clear()

    # Read with BlockStoreReader
    print(
        f"\n[5/6] Reading middle pixel with BlockStoreReader ({block_size_mb} MB blocks)..."
    )
    start_time = time.perf_counter()

    with BlockStoreReader(
        traced_store,
        path,
        block_size=block_size,
        max_cached_blocks=1024,
    ) as reader:
        value = read_middle_pixel(reader)

    read_time = time.perf_counter() - start_time

    print(f"      Value: {value}")
    print(f"      Read time: {read_time:.2f}s")
    print(f"      Total requests: {trace.total_requests}")
    print(f"      Total bytes: {trace.total_bytes / MB:.2f} MB")

    # Get request dataframe
    requests_df = trace.to_dataframe()

    # Create visualization
    print("\n[6/6] Creating waterfall visualization...")
    plot = visualize_waterfall(
        requests_df,
        title=f"BlockStoreReader ({block_size_mb} MB) + AiohttpStore: NISAR Request Waterfall",
        file_size=file_size,
        read_time=read_time,
    )

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).parent / "waterfall.html"
    hv.save(plot, str(output_path))
    print(f"\nVisualization saved to: {output_path}")


if __name__ == "__main__":
    main()
