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
Analyze HTTP range request patterns when reading NISAR data.

Creates an interactive HTML visualization showing:
- Request timeline colored by byte offset
- Request size distribution histogram
- Byte coverage analysis (unique vs re-read bytes)
- Statistics panel

Usage:
    uv run docs/examples/nisar/analyze.py
    uv run docs/examples/nisar/analyze.py --block-size 16
    uv run docs/examples/nisar/analyze.py --output my_analysis.html
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
        description="Analyze HTTP range request patterns when reading NISAR data"
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=32,
        help="Block size in MB for BlockStoreReader (default: 32)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output HTML file path (default: analyze.html in script dir)",
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


def visualize_requests(
    requests_df: pd.DataFrame,
    title: str = "BlockStoreReader Request Analysis",
    file_size: int | None = None,
    read_time: float | None = None,
):
    """Create a visualization of request patterns."""
    if requests_df.empty:
        return hv.Div("<p>No requests recorded</p>")

    requests_sorted = requests_df.sort_values("start").reset_index(drop=True)

    # Determine file boundaries (in MB)
    file_end_bytes = file_size if file_size else requests_df["end"].max()
    file_end_mb = file_end_bytes / MB

    # Calculate relative time from first request
    min_ts = requests_df["timestamp"].min()

    # Requests colored by time sent (x-axis in MB)
    request_segments_sent = [
        {
            "x0": row["start"] / MB,
            "x1": row["end"] / MB,
            "y0": 1,
            "y1": 1,
            "source": "Requests (time sent)",
            "length_mb": row["length"] / MB,
            "method": row.get("method", "unknown"),
            "duration_s": row.get("duration", 0),
            "time_s": row.get("timestamp", 0) - min_ts,
        }
        for _, row in requests_sorted.iterrows()
    ]

    # Requests colored by time received (x-axis in MB)
    request_segments_recv = [
        {
            "x0": row["start"] / MB,
            "x1": row["end"] / MB,
            "y0": 0,
            "y1": 0,
            "source": "Requests (time received)",
            "length_mb": row["length"] / MB,
            "method": row.get("method", "unknown"),
            "duration_s": row.get("duration", 0),
            "time_s": row.get("timestamp", 0) - min_ts + row.get("duration", 0),
        }
        for _, row in requests_sorted.iterrows()
    ]

    request_sent_df = pd.DataFrame(request_segments_sent)
    request_recv_df = pd.DataFrame(request_segments_recv)

    # Color requests by time using plasma colormap
    request_sent_plot = hv.Segments(
        request_sent_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=["source", "length_mb", "method", "duration_s", "time_s"],
    ).opts(
        color="time_s",
        cmap="plasma",
        colorbar=True,
        clabel="Time (s)",
        alpha=0.8,
        line_width=15,
        tools=["hover"],
    )

    request_recv_plot = hv.Segments(
        request_recv_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=["source", "length_mb", "method", "duration_s", "time_s"],
    ).opts(
        color="time_s",
        cmap="plasma",
        alpha=0.8,
        line_width=15,
        tools=["hover"],
    )

    # File boundary lines (in MB)
    start_line = hv.VLine(0).opts(color="red", line_width=2, line_dash="dashed")
    end_line = hv.VLine(file_end_mb).opts(color="red", line_width=2, line_dash="dashed")

    boundary_labels = hv.Labels(
        {
            "x": [0, file_end_mb],
            "y": [1.4, 1.4],
            "text": ["File Start (0)", f"File End ({file_end_mb:.1f} MB)"],
        },
        kdims=["x", "y"],
        vdims=["text"],
    ).opts(text_font_size="8pt", text_color="red")

    # Create timeline plot (colorbar in MB)
    timeline_segments = []
    for idx, row in requests_sorted.iterrows():
        time_sent = row["timestamp"] - min_ts
        duration = row.get("duration", 0)
        time_received = time_sent + duration
        timeline_segments.append(
            {
                "x0": time_sent,
                "x1": time_received,
                "y0": idx % 20,
                "y1": idx % 20,
                "length_mb": row["length"] / MB,
                "start_mb": row["start"] / MB,
                "duration_s": duration,
            }
        )

    timeline_df = pd.DataFrame(timeline_segments)
    timeline_plot = hv.Segments(
        timeline_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=["length_mb", "start_mb", "duration_s"],
    ).opts(
        color="start_mb",
        cmap="viridis",
        colorbar=True,
        clabel="Offset (MB)",
        alpha=0.8,
        line_width=8,
        tools=["hover"],
        width=900,
        height=200,
        xlabel="Time (s)",
        ylabel="Request (stacked)",
        title="Request Timeline (colored by offset)",
    )

    # Compute statistics

    total_request_bytes = requests_df["length"].sum()

    # Analyze byte coverage
    byte_read_count = np.zeros(file_end_bytes, dtype=np.uint8)
    for _, row in requests_df.iterrows():
        start = int(row["start"])
        end = min(int(row["end"]), file_end_bytes)
        if start < file_end_bytes:
            byte_read_count[start:end] += 1

    unique_bytes_read = np.sum(byte_read_count > 0)
    bytes_read_multiple = np.sum(byte_read_count > 1)
    total_reread_bytes = total_request_bytes - unique_bytes_read
    max_reads = int(np.max(byte_read_count)) if len(byte_read_count) > 0 else 0

    # Last request received time
    last_request_received_s = max(seg["time_s"] for seg in request_segments_recv)

    # Format stats
    read_time_str = f"{read_time:.2f} s" if read_time is not None else "N/A"
    total_bytes_mb = total_request_bytes / MB
    unique_mb = unique_bytes_read / MB
    reread_mb = total_reread_bytes / MB

    # Throughput
    if last_request_received_s > 0:
        throughput_mbps = (
            total_request_bytes * 8 / 1_000_000
        ) / last_request_received_s
        throughput_str = f"{throughput_mbps:.1f} Mbps"
    else:
        throughput_str = "N/A"

    stats_div = hv.Div(
        f"""
        <div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 4px;
                    padding: 10px; font-family: monospace; font-size: 11px; line-height: 1.5;">
            <b>BlockStoreReader Statistics</b><br>
            File Size: {file_end_mb:.2f} MB<br>
            Requests: {len(requests_df):,}<br>
            <b>Total bytes received: {total_bytes_mb:.2f} MB</b><br>
            <b>Unique bytes read: {unique_mb:.2f} MB</b><br>
            <span style="color: {'red' if total_reread_bytes > 0 else 'green'};">
                <b>Bytes re-read: {reread_mb:.2f} MB</b>
            </span><br>
            Bytes read multiple times: {bytes_read_multiple:,} (max {max_reads}x)<br>
            <b>Read time: {read_time_str}</b><br>
            <b>Last request received: {last_request_received_s:.2f} s</b><br>
            <b>Effective throughput: {throughput_str}</b>
        </div>
    """
    )

    main_plot = (
        request_sent_plot * request_recv_plot * start_line * end_line * boundary_labels
    ).opts(
        width=900,
        height=300,
        title=title,
        xlabel="Offset (MB)",
        ylabel="",
        yticks=[(0, "Requests (received)"), (1, "Requests (sent)")],
        show_legend=True,
        ylim=(-0.5, 1.8),
    )

    # Request size histogram with log-scaled bins
    request_sizes_mb = requests_df["length"] / (1024 * 1024)
    min_size = max(1e-6, request_sizes_mb.min())
    max_size = request_sizes_mb.max()
    log_bins = np.geomspace(min_size, max_size, num=31)
    frequencies, edges = np.histogram(request_sizes_mb, bins=log_bins)
    request_histogram = hv.Histogram((edges, frequencies)).opts(
        width=900,
        height=200,
        xlabel="Request Size (MB)",
        ylabel="Count",
        title="Request Size Distribution (log-scaled bins)",
        tools=["hover"],
        color="steelblue",
        logx=True,
    )

    return (
        (main_plot + timeline_plot + request_histogram + stats_div)
        .opts(shared_axes=False)
        .cols(1)
    )


def print_summary(requests_df: pd.DataFrame, file_size: int) -> None:
    """Print a summary of the request analysis."""
    total_bytes = requests_df["length"].sum()
    min_size = requests_df["length"].min()
    max_size = requests_df["length"].max()
    mean_size = requests_df["length"].mean()

    print(
        f"""
Request Summary
---------------
Total Requests:     {len(requests_df):,}
Total Bytes:        {total_bytes / MB:.2f} MB
File Size:          {file_size / MB:.2f} MB
Coverage:           {(total_bytes / file_size) * 100:.1f}%

Request Sizes:
  Min:              {min_size / MB:.4f} MB ({min_size:,} bytes)
  Max:              {max_size / MB:.4f} MB ({max_size:,} bytes)
  Mean:             {mean_size / MB:.4f} MB

Byte Range:
  Start:            {requests_df['start'].min():,}
  End:              {requests_df['end'].max():,}
"""
    )


def main():
    args = parse_args()
    block_size_mb = args.block_size
    block_size = block_size_mb * MB

    print("=" * 60)
    print("BlockStoreReader Request Analysis")
    print("=" * 60)
    print(f"Block size: {block_size_mb} MB")

    # Authenticate and Query Data
    print("\n[1/5] Authenticating with NASA Earthdata...")
    earthaccess.login()

    query = earthaccess.DataGranules()
    query.short_name("NISAR_L2_GCOV_BETA_V1")
    query.params["attribute[]"] = "int,FRAME_NUMBER,77"
    query.params["attribute[]"] = "int,TRACK_NUMBER,5"
    results = query.get_all()

    print(f"      Found {len(results)} granules")

    # Get the HTTPS URL for the first granule
    https_links = earthaccess.results.DataGranule.data_links(
        results[0], access="external"
    )
    https_url = https_links[0]
    print(f"      HTTPS URL: {https_url}")

    # Parse URL
    parsed = urlparse(https_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.lstrip("/")

    print(f"      Base URL: {base_url}")
    print(f"      Path: {path}")

    # Get EDL token
    print("\n[2/5] Getting EDL token...")
    token = earthaccess.get_edl_token()["access_token"]

    # Create store chain: AiohttpStore -> TracingReadableStore
    print("\n[3/5] Setting up store chain...")
    base_store = AiohttpStore(
        base_url,
        headers={"Authorization": f"Bearer {token}"},
    )
    trace = RequestTrace()
    traced_store = TracingReadableStore(base_store, trace)
    print("      AiohttpStore -> TracingReadableStore")

    # Get file size via HEAD request
    print("\n[4/5] Getting file size...")
    meta = traced_store.head(path)
    file_size = meta["size"]
    print(f"      File size: {file_size / MB:.2f} MB")

    # Clear the HEAD request from the trace
    trace.clear()

    # Read with BlockStoreReader
    print(
        f"\n[5/5] Reading middle pixel with BlockStoreReader ({block_size_mb} MB blocks)..."
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

    # Print summary
    print_summary(requests_df, file_size)

    # Create visualization
    print("\nCreating visualization...")
    plot = visualize_requests(
        requests_df,
        title=f"BlockStoreReader ({block_size_mb} MB): NISAR Request Analysis",
        file_size=file_size,
        read_time=read_time,
    )

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).parent / "analyze.html"
    hv.save(plot, str(output_path))
    print(f"\nVisualization saved to: {output_path}")


if __name__ == "__main__":
    main()
