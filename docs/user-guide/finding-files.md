# Finding files matching patterns in cloud storage

This guide shows how to use `obspec-utils` glob functions to find files matching patterns in cloud storage.

## Overview

The `glob` function works similarly to Python's `pathlib.glob()` or `glob.glob()`, but operates on object stores. It efficiently lists objects matching a pattern by extracting the longest literal prefix for server-side filtering.

## Quick Start

This example finds NetCDF files in the [NASA Earth Exchange (NEX) Data Collection](https://registry.opendata.aws/nasanex/) on AWS Open Data:

```python exec="on" source="above" session="glob" result="code"
from obstore.store import S3Store
from obspec_utils import glob

# Access public AWS Open Data (no credentials needed)
store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,  # Anonymous access
)

# Find all NetCDF files for a specific model/year
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc"))
print(f"Found {len(paths)} files:")
for path in paths[:5]:  # Show first 5
    print(f"  {path}")
```

## Pattern Syntax

The glob functions support standard glob patterns:

| Pattern | Matches |
|---------|---------|
| `*` | Any characters within a single path segment |
| `**` | Any number of path segments (recursive) |
| `?` | Exactly one character |
| `[abc]` | Any character in the set |
| `[a-z]` | Any character in the range |
| `[!abc]` | Any character NOT in the set |

### Examples

```python exec="on" source="above" session="glob" result="code"
# Single wildcard: match files for different climate models in 2100
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_2100.nc"))
print(f"Single wildcard (*): {len(paths)} files")
for p in paths[:3]:
    print(f"  {p.split('/')[-1]}")

# Question mark: match files for years 2096-2099
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209?.nc"))
print(f"\nSingle character (?): {len(paths)} files")
for p in paths:
    print(f"  {p.split('/')[-1]}")

# Character range: match specific years using [0-5] for 2090-2095
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209[0-5].nc"))
print(f"\nCharacter range ([0-5]): {len(paths)} files")
for p in paths:
    print(f"  {p.split('/')[-1]}")
```

## Getting Object Metadata

Use `glob_objects` to get full metadata (size, last modified, etc.) instead of just paths:

```python exec="on" source="above" session="glob" result="code"
from obspec_utils import glob_objects

# Get metadata for matching files
objects = list(glob_objects(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc"))

# Calculate total size
total_bytes = sum(obj["size"] for obj in objects)
print(f"Total size: {total_bytes / 1e9:.2f} GB across {len(objects)} files")

# Show details for first file
if objects:
    obj = objects[0]
    print(f"\nFirst file:")
    print(f"  Path: {obj['path']}")
    print(f"  Size: {obj['size'] / 1e6:.1f} MB")
    print(f"  Last modified: {obj['last_modified']}")
```

## Async Usage

For async contexts, use `glob_async` and `glob_objects_async`:

```python exec="on" source="above" session="glob" result="code"
import asyncio
from obspec_utils import glob_async

async def find_files():
    paths = []
    async for path in glob_async(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209?.nc"):
        paths.append(path)
    return paths

# Run the async function
paths = asyncio.run(find_files())
print(f"Found {len(paths)} files asynchronously:")
for p in paths:
    print(f"  {p.split('/')[-1]}")
```

## Performance Tips

The glob functions automatically extract the longest literal prefix from patterns to minimize the number of objects listed from the store:

- `data/2024/**/*.nc` lists from prefix `data/2024/`
- `data/*.nc` lists from prefix `data/`
- `**/*.nc` lists from the root (no prefix filtering)

For best performance, make your patterns as specific as possible at the start.
