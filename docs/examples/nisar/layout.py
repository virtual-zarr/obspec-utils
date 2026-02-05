#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "virtualizarr[hdf] @ git+https://github.com/maxrjones/VirtualiZarr@c-dtype",
#     "obspec-utils @ git+https://github.com/virtual-zarr/obspec-utils@main",
#     "aiohttp",
#     "pandas",
#     "holoviews",
#     "bokeh",
# ]
# ///
"""
Visualize file layout: where data chunks vs metadata are in a NISAR file.

Creates an interactive HTML visualization showing:
- Data chunks (from VirtualiZarr ManifestStore)
- Metadata regions (gaps between chunks)
- HTTP requests made during parsing

This helps understand what bytes need to be fetched for virtualization
vs what bytes contain the actual array data.

Usage:
    uv run docs/examples/nisar/layout.py
    uv run docs/examples/nisar/layout.py --output my_layout.html
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from urllib.parse import urlparse

import earthaccess
import holoviews as hv
import pandas as pd
import virtualizarr as vz

from obspec_utils.stores import AiohttpStore
from obspec_utils.registry import ObjectStoreRegistry
from obspec_utils.wrappers import RequestTrace, TracingReadableStore

hv.extension("bokeh")

MB = 1024 * 1024


def parse_args():
    parser = argparse.ArgumentParser(
        description="Visualize data vs metadata layout in NISAR files"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output HTML file path (default: layout.html in script dir)",
    )
    return parser.parse_args()


def extract_chunks_dataframe(manifest_store) -> pd.DataFrame:
    """Extract all chunks from a ManifestStore into a DataFrame."""
    from virtualizarr.manifests import ManifestGroup

    records = []

    def process_group(group: ManifestGroup, group_path: str = ""):
        """Recursively process all arrays in groups."""
        # Process arrays at this level
        for array_name, array in group.arrays.items():
            var_path = f"{group_path}/{array_name}" if group_path else array_name
            manifest = array.manifest
            for chunk_key, entry in manifest.dict().items():
                records.append(
                    {
                        "variable": var_path,
                        "chunk_key": chunk_key,
                        "path": entry["path"],
                        "start": entry["offset"],
                        "length": entry["length"],
                        "end": entry["offset"] + entry["length"],
                    }
                )

        # Recursively process subgroups
        for group_name, subgroup in group.groups.items():
            sub_path = f"{group_path}/{group_name}" if group_path else group_name
            process_group(subgroup, group_path=sub_path)

    # Start from root group
    process_group(manifest_store._group)

    return pd.DataFrame(records)


def compute_gaps(
    chunks_df: pd.DataFrame,
    file_size: int,
    requests_df: pd.DataFrame | None = None,
) -> list[dict]:
    """Compute gaps between chunks and classify by request coverage.

    Args:
        chunks_df: DataFrame with chunk info (start, end columns)
        file_size: Total file size in bytes
        requests_df: Optional DataFrame with request info (start, end columns)

    Returns:
        List of gap dicts with coverage info:
        - x0, x1: byte range
        - length: gap size in bytes
        - bytes_read: how many bytes in this gap were read during parsing
        - coverage: fraction of gap that was read (0.0 to 1.0)
        - classification: "confirmed metadata", "unread gap", or "partially read"
        - description: human-readable description
    """

    def compute_coverage(gap_start: int, gap_end: int) -> int:
        """Compute how many bytes in a gap were read by requests."""
        if requests_df is None or requests_df.empty:
            return 0
        bytes_read = 0
        for _, req in requests_df.iterrows():
            overlap_start = max(gap_start, int(req["start"]))
            overlap_end = min(gap_end, int(req["end"]))
            if overlap_start < overlap_end:
                bytes_read += overlap_end - overlap_start
        return bytes_read

    def classify_gap(coverage: float) -> str:
        """Classify gap based on how much was read."""
        if coverage >= 0.9:
            return "confirmed metadata"
        elif coverage <= 0.1:
            return "unread gap"
        else:
            return "partially read"

    if chunks_df.empty:
        bytes_read = compute_coverage(0, file_size)
        coverage = bytes_read / file_size if file_size > 0 else 0.0
        return [
            {
                "x0": 0,
                "x1": file_size,
                "length": file_size,
                "bytes_read": bytes_read,
                "coverage": coverage,
                "classification": classify_gap(coverage),
                "description": f"Entire file: 0 - {file_size:,} ({coverage:.0%} read)",
            }
        ]

    sorted_chunks = chunks_df.sort_values("start").reset_index(drop=True)
    gaps = []
    current_pos = 0

    for _, row in sorted_chunks.iterrows():
        chunk_start = int(row["start"])
        chunk_end = int(row["end"])

        if chunk_start > current_pos:
            gap_length = chunk_start - current_pos
            bytes_read = compute_coverage(current_pos, chunk_start)
            coverage = bytes_read / gap_length if gap_length > 0 else 0.0
            classification = classify_gap(coverage)
            gaps.append(
                {
                    "x0": current_pos,
                    "x1": chunk_start,
                    "length": gap_length,
                    "bytes_read": bytes_read,
                    "coverage": coverage,
                    "classification": classification,
                    "description": f"Gap: {current_pos:,} - {chunk_start:,} ({coverage:.0%} read, {classification})",
                }
            )

        current_pos = max(current_pos, chunk_end)

    if current_pos < file_size:
        gap_length = file_size - current_pos
        bytes_read = compute_coverage(current_pos, file_size)
        coverage = bytes_read / gap_length if gap_length > 0 else 0.0
        classification = classify_gap(coverage)
        gaps.append(
            {
                "x0": current_pos,
                "x1": file_size,
                "length": gap_length,
                "bytes_read": bytes_read,
                "coverage": coverage,
                "classification": classification,
                "description": f"Gap: {current_pos:,} - {file_size:,} ({coverage:.0%} read, {classification})",
            }
        )

    return gaps


def visualize_layout(
    chunks_df: pd.DataFrame,
    requests_df: pd.DataFrame,
    file_size: int,
    title: str = "File Layout: Data vs Metadata",
    parse_time: float | None = None,
):
    """Create a visualization of file layout."""
    file_end_mb = file_size / MB

    # Data chunks
    chunk_segments = [
        {
            "x0": row["start"] / MB,
            "x1": row["end"] / MB,
            "y0": 2,
            "y1": 2,
            "source": "Data Chunks",
            "variable": row["variable"],
            "length_mb": row["length"] / MB,
        }
        for _, row in chunks_df.iterrows()
    ]

    # Metadata gaps (colored by classification)
    gaps = compute_gaps(chunks_df, file_size, requests_df)

    # Color mapping for gap classification
    classification_colors = {
        "confirmed metadata": "orange",
        "partially read": "yellow",
        "unread gap": "gray",
    }

    gap_rects = [
        {
            "x0": gap["x0"] / MB,
            "x1": gap["x1"] / MB,
            "y0": 0.85,
            "y1": 1.15,
            "source": gap["classification"],
            "length_mb": gap["length"] / MB,
            "bytes_read_mb": gap["bytes_read"] / MB,
            "coverage_pct": gap["coverage"] * 100,
            "description": gap["description"],
            "color": classification_colors[gap["classification"]],
        }
        for gap in gaps
    ]

    # Requests (colored by time)
    if not requests_df.empty and "timestamp" in requests_df.columns:
        min_ts = requests_df["timestamp"].min()
        request_segments = [
            {
                "x0": row["start"] / MB,
                "x1": row["end"] / MB,
                "y0": 0,
                "y1": 0,
                "source": "Requests",
                "length_mb": row["length"] / MB,
                "time_s": row["timestamp"] - min_ts,
                "duration_s": row.get("duration", 0),
            }
            for _, row in requests_df.iterrows()
        ]
    else:
        request_segments = []

    # Create plots
    chunk_df = pd.DataFrame(chunk_segments)
    chunk_plot = hv.Segments(
        chunk_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=["source", "variable", "length_mb"],
    ).opts(color="green", alpha=0.7, line_width=15, tools=["hover"])

    gap_df = pd.DataFrame(gap_rects)
    gap_plot = hv.Rectangles(
        gap_df,
        kdims=["x0", "y0", "x1", "y1"],
        vdims=[
            "source",
            "length_mb",
            "bytes_read_mb",
            "coverage_pct",
            "description",
            "color",
        ],
    ).opts(
        color="color",
        alpha=0.8,
        line_color="black",
        line_width=1,
        tools=["hover"],
    )

    if request_segments:
        request_df = pd.DataFrame(request_segments)
        request_plot = hv.Segments(
            request_df,
            kdims=["x0", "y0", "x1", "y1"],
            vdims=["source", "length_mb", "time_s", "duration_s"],
        ).opts(
            color="time_s",
            cmap="plasma",
            colorbar=True,
            clabel="Time (s)",
            alpha=0.8,
            line_width=15,
            tools=["hover"],
        )
    else:
        request_plot = hv.Overlay()

    # File boundaries
    start_line = hv.VLine(0).opts(color="red", line_width=2, line_dash="dashed")
    end_line = hv.VLine(file_end_mb).opts(color="red", line_width=2, line_dash="dashed")

    boundary_labels = hv.Labels(
        {
            "x": [0, file_end_mb],
            "y": [2.4, 2.4],
            "text": ["File Start (0)", f"File End ({file_end_mb:.1f} MB)"],
        },
        kdims=["x", "y"],
        vdims=["text"],
    ).opts(text_font_size="8pt", text_color="red")

    # Statistics
    total_chunk_bytes = chunks_df["length"].sum()
    total_gap_bytes = sum(g["length"] for g in gaps)
    total_gap_bytes_read = sum(g["bytes_read"] for g in gaps)
    total_request_bytes = requests_df["length"].sum() if not requests_df.empty else 0

    chunk_pct = (total_chunk_bytes / file_size) * 100
    gap_pct = (total_gap_bytes / file_size) * 100
    gap_coverage_pct = (
        (total_gap_bytes_read / total_gap_bytes * 100) if total_gap_bytes > 0 else 0
    )

    # Count gaps by classification
    confirmed_gaps = [g for g in gaps if g["classification"] == "confirmed metadata"]
    partial_gaps = [g for g in gaps if g["classification"] == "partially read"]
    unread_gaps = [g for g in gaps if g["classification"] == "unread gap"]

    parse_time_str = f"{parse_time:.2f} s" if parse_time else "N/A"

    stats_div = hv.Div(f"""
        <div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 4px;
                    padding: 10px; font-family: monospace; font-size: 11px; line-height: 1.5;">
            <b>File Layout Statistics</b><br>
            <br>
            <b>File Size:</b> {file_size / MB:.2f} MB<br>
            <br>
            <span style="color: green;"><b>Data Chunks:</b></span><br>
            &nbsp;&nbsp;Count: {len(chunks_df):,}<br>
            &nbsp;&nbsp;Size: {total_chunk_bytes / MB:.2f} MB ({chunk_pct:.1f}%)<br>
            <br>
            <b>Gap Regions:</b> {len(gaps):,} totaling {total_gap_bytes / MB:.2f} MB ({gap_pct:.1f}%)<br>
            &nbsp;&nbsp;<span style="color: orange;">Confirmed metadata:</span> {len(confirmed_gaps)} ({sum(g["length"] for g in confirmed_gaps) / MB:.2f} MB)<br>
            &nbsp;&nbsp;<span style="color: #cccc00;">Partially read:</span> {len(partial_gaps)} ({sum(g["length"] for g in partial_gaps) / MB:.2f} MB)<br>
            &nbsp;&nbsp;<span style="color: gray;">Unread gaps:</span> {len(unread_gaps)} ({sum(g["length"] for g in unread_gaps) / MB:.2f} MB)<br>
            &nbsp;&nbsp;<b>Gap coverage:</b> {gap_coverage_pct:.1f}% of gap bytes were read<br>
            <br>
            <b>Requests Made:</b> {len(requests_df):,}<br>
            <b>Bytes Requested:</b> {total_request_bytes / MB:.2f} MB<br>
            <b>Parse Time:</b> {parse_time_str}
        </div>
    """)

    # Combine plots
    main_plot = (
        chunk_plot * gap_plot * request_plot * start_line * end_line * boundary_labels
    ).opts(
        width=1000,
        height=400,
        title=title,
        xlabel="Offset (MB)",
        ylabel="",
        yticks=[(0, "Requests"), (1, "Metadata"), (2, "Data Chunks")],
        ylim=(-0.5, 2.8),
        show_grid=True,
        gridstyle={"grid_line_alpha": 0.3, "grid_line_dash": [4, 4]},
    )

    # Variable breakdown
    if not chunks_df.empty:
        var_summary = (
            chunks_df.groupby("variable")
            .agg({"length": ["count", "sum"]})
            .reset_index()
        )
        var_summary.columns = ["variable", "chunk_count", "total_bytes"]
        var_summary["total_mb"] = var_summary["total_bytes"] / MB
        var_summary = var_summary.sort_values("total_bytes", ascending=False).head(15)

        bars = hv.Bars(
            var_summary,
            kdims=["variable"],
            vdims=["total_mb"],
        ).opts(
            width=1000,
            height=250,
            xrotation=45,
            xlabel="Variable",
            ylabel="Size (MB)",
            title="Top 15 Variables by Data Size",
            tools=["hover"],
            color="steelblue",
        )
    else:
        bars = hv.Div("<p>No chunk data available</p>")

    return (main_plot + bars + stats_div).opts(shared_axes=False).cols(1)


def main():
    args = parse_args()

    print("=" * 60)
    print("NISAR File Layout: Data vs Metadata")
    print("=" * 60)

    # Authenticate
    print("\n[1/5] Authenticating with NASA Earthdata...")
    earthaccess.login()

    query = earthaccess.DataGranules()
    query.short_name("NISAR_L2_GCOV_BETA_V1")
    query.params["attribute[]"] = "int,FRAME_NUMBER,77"
    query.params["attribute[]"] = "int,TRACK_NUMBER,5"
    results = query.get_all()

    print(f"      Found {len(results)} granules")

    https_links = earthaccess.results.DataGranule.data_links(
        results[0], access="external"
    )
    https_url = https_links[0]
    print(f"      HTTPS URL: {https_url}")

    parsed = urlparse(https_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.lstrip("/")

    # Get EDL token
    print("\n[2/5] Getting EDL token...")
    token = earthaccess.get_edl_token()["access_token"]

    # Create traced store
    print("\n[3/5] Setting up tracing...")
    base_store = AiohttpStore(
        base_url,
        headers={"Authorization": f"Bearer {token}"},
    )
    trace = RequestTrace()
    traced_store = TracingReadableStore(base_store, trace)
    registry = ObjectStoreRegistry({base_url: traced_store})

    # Debug: verify registry setup
    print(f"      Registry keys: {list(registry.map.keys())}")
    print(f"      URL to resolve: {https_url}")
    try:
        resolved_store, resolved_path = registry.resolve(https_url)
        print(f"      Resolved store type: {type(resolved_store).__name__}")
        print(f"      Resolved path: {resolved_path}")
        print(
            f"      Store is traced: {isinstance(resolved_store, TracingReadableStore)}"
        )
    except ValueError as e:
        print(f"      ERROR: Resolution failed: {e}")

    # Get file size
    meta = traced_store.head(path)
    file_size = meta["size"]
    print(f"      File size: {file_size / MB:.2f} MB")
    trace.clear()

    # Parse file to ManifestStore
    print("\n[4/5] Parsing file to ManifestStore...")
    start_time = time.perf_counter()

    parser = vz.parsers.HDFParser()
    manifest_store = parser(https_url, registry=registry)

    parse_time = time.perf_counter() - start_time

    print(f"      Requests: {trace.total_requests}")
    print(f"      Bytes requested: {trace.total_bytes / MB:.2f} MB")
    print(f"      Time: {parse_time:.2f}s")

    # Extract chunk info
    chunks_df = extract_chunks_dataframe(manifest_store)
    requests_df = trace.to_dataframe()
    print(f"      Chunks in manifest: {len(chunks_df)}")

    # Debug: inspect requests DataFrame
    print(f"\n      [DEBUG] requests_df shape: {requests_df.shape}")
    print(f"      [DEBUG] requests_df columns: {requests_df.columns.tolist()}")
    print(f"      [DEBUG] requests_df empty: {requests_df.empty}")
    if not requests_df.empty:
        print("      [DEBUG] First 3 requests:")
        for i, row in requests_df.head(3).iterrows():
            print(
                f"        {i}: start={row['start']:,}, length={row['length']:,}, method={row['method']}"
            )
        print(
            f"      [DEBUG] Request methods: {requests_df['method'].value_counts().to_dict()}"
        )
    else:
        print("      [DEBUG] No requests captured - tracing may not be working")

    # Create visualization
    print("\n[5/5] Creating visualization...")
    plot = visualize_layout(
        chunks_df,
        requests_df,
        file_size,
        title=f"NISAR File Layout: {path.split('/')[-1]}",
        parse_time=parse_time,
    )

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).parent / "layout.html"
    hv.save(plot, str(output_path))
    print(f"\nVisualization saved to: {output_path}")

    # Print summary
    total_chunk_bytes = chunks_df["length"].sum()
    gaps = compute_gaps(chunks_df, file_size, requests_df)
    total_gap_bytes = sum(g["length"] for g in gaps)
    total_gap_bytes_read = sum(g["bytes_read"] for g in gaps)
    gap_coverage_pct = (
        (total_gap_bytes_read / total_gap_bytes * 100) if total_gap_bytes > 0 else 0
    )

    confirmed_gaps = [g for g in gaps if g["classification"] == "confirmed metadata"]
    partial_gaps = [g for g in gaps if g["classification"] == "partially read"]
    unread_gaps = [g for g in gaps if g["classification"] == "unread gap"]

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(
        f"Data chunks:     {total_chunk_bytes / MB:>10.2f} MB ({100 * total_chunk_bytes / file_size:.1f}%)"
    )
    print(
        f"Gap regions:     {total_gap_bytes / MB:>10.2f} MB ({100 * total_gap_bytes / file_size:.1f}%)"
    )
    print(
        f"  Confirmed metadata: {len(confirmed_gaps):>3} gaps, {sum(g['length'] for g in confirmed_gaps) / MB:.2f} MB"
    )
    print(
        f"  Partially read:     {len(partial_gaps):>3} gaps, {sum(g['length'] for g in partial_gaps) / MB:.2f} MB"
    )
    print(
        f"  Unread gaps:        {len(unread_gaps):>3} gaps, {sum(g['length'] for g in unread_gaps) / MB:.2f} MB"
    )
    print(f"  Gap coverage:       {gap_coverage_pct:.1f}% of gap bytes were read")
    print(
        f"Requests made:   {trace.total_bytes / MB:>10.2f} MB ({100 * trace.total_bytes / file_size:.1f}%)"
    )


if __name__ == "__main__":
    main()
