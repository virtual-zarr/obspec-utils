# NISAR Data Access Examples

These scripts demonstrate obspec-utils for accessing NASA NISAR data via HTTPS with Earthdata authentication.

## Prerequisites

- NASA Earthdata account ([register here](https://urs.earthdata.nasa.gov/users/new))
- `earthaccess` configured with your credentials

## Scripts

### `virtualize.py` (not functional)

Minimal example showing how to create a virtual datatree from a remote NISAR file.

> **Note:** This example currently fails due to upstream limitations:
> 1. The "crosstalk" variable has a complex dtype not supported by Zarr
> 2. `drop_variables` doesn't yet work for variables in nested HDF5 groups
>
> Fixes are needed in [VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr).

```bash
uv run --script docs/examples/nisar/virtualize.py
```

### `benchmark.py`

Compare obspec-utils readers against fsspec for performance.

```bash
uv run --script docs/examples/nisar/benchmark.py
uv run --script docs/examples/nisar/benchmark.py --block-size 32
```

### `waterfall.py`

Visualize HTTP range requests as a waterfall chart (byte position vs time).

```bash
uv run --script docs/examples/nisar/waterfall.py
uv run --script docs/examples/nisar/waterfall.py --block-size 8
uv run --script docs/examples/nisar/waterfall.py --output my_waterfall.html
```

### `analyze.py`

Analyze request patterns with timeline, size histogram, and byte coverage stats.

```bash
uv run --script docs/examples/nisar/analyze.py
uv run --script docs/examples/nisar/analyze.py --block-size 16
uv run --script docs/examples/nisar/analyze.py --output my_analysis.html
```

### `layout.py` (not functional)

Visualize file layout showing where data chunks vs metadata are located. Uses VirtualiZarr to extract chunk locations from the manifest.

```bash
uv run --script docs/examples/nisar/layout.py
uv run --script docs/examples/nisar/layout.py --output my_layout.html
```

## Output

- `benchmark.py` prints results to stdout
- `waterfall.py` generates `waterfall.html`
- `analyze.py` generates `analyze.html`
- `layout.py` generates `layout.html`
