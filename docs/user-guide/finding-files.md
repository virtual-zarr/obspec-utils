# Finding Files on Cloud Storage

This guide shows how to discover and list files stored in cloud object storage.

## Listing Files in a Directory

To see what files exist in a specific location, use the store's `list()` method with a prefix:

```python exec="on" source="above" session="find" result="code"
from obstore.store import S3Store

# Access public AWS Open Data
store = S3Store(
    bucket="nasanex",
    aws_region="us-west-2",
    skip_signature=True,
)

# List files in a specific directory
prefix = "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/"
files = []
for chunk in store.list(prefix=prefix):
    files.extend(chunk)

print(f"Found {len(files)} files in {prefix}")
print(f"\nFirst 5 files:")
for f in files[:5]:
    print(f"  {f['path'].split('/')[-1]}")
```

!!! warning "Use the class methods rather than `obstore` top-level functions"
    When using `obspec_utils` wrappers like [`CachingReadableStore`][obspec_utils.wrappers.CachingReadableStore], call methods
    directly on the store (e.g., `store.list()`) rather than using `obstore` functions
    (e.g., `obstore.list(store)`). The wrappers implement the `obspec` protocol, which decouples them from specific store instances. `Obstore` top-level functions are tied to the specific stores implemented by `obstore`, so they will not work with the `obspec`-based wrappers provided by `obspec-utils`.

## Finding Files Matching a Pattern

When you need files matching specific criteria (e.g., all files from year 2100), use [`glob`][obspec_utils.glob.glob]:

```python exec="on" source="above" session="find" result="code"
from obspec_utils import glob

# Find all NetCDF files for year 2100
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc"))
print(f"Found {len(paths)} files for 2100:")
for path in paths[:5]:
    print(f"  {path.split('/')[-1]}")
```

### Pattern Syntax

| Pattern | Matches | Example |
|---------|---------|---------|
| `*` | Any characters in one segment | `*_2100.nc` matches any model for 2100 |
| `**` | Any number of segments | `data/**/*.nc` matches all .nc files recursively |
| `?` | Exactly one character | `*_209?.nc` matches 2090-2099 |
| `[abc]` | Any character in set | `*_209[012].nc` matches 2090, 2091, 2092 |
| `[a-z]` | Any character in range | `*_209[0-5].nc` matches 2090-2095 |
| `[!abc]` | Any character NOT in set | `*_209[!9].nc` excludes 2099 |

### More Pattern Examples

```python exec="on" source="above" session="find" result="code"
# Match a range of years (2096-2099) using ?
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209?.nc"))
print(f"Years 2090-2099: {len(paths)} files")
for p in paths[-4:]:  # Show last 4 (2096-2099)
    print(f"  {p.split('/')[-1]}")

# Match specific years using character range
paths = list(glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209[5-9].nc"))
print(f"\nYears 2095-2099: {len(paths)} files")
for p in paths:
    print(f"  {p.split('/')[-1]}")
```

## Getting File Sizes and Dates

To get metadata (size, last modified time) along with paths, use [`glob_objects`][obspec_utils.glob.glob_objects]:

```python exec="on" source="above" session="find" result="code"
from obspec_utils import glob_objects

# Get metadata for matching files
objects = list(glob_objects(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc"))

# Calculate total size
total_bytes = sum(obj["size"] for obj in objects)
print(f"Total: {total_bytes / 1e9:.2f} GB across {len(objects)} files")

# Show details for a few files
print(f"\nSample files:")
for obj in objects[:3]:
    print(f"  {obj['path'].split('/')[-1]}")
    print(f"    Size: {obj['size'] / 1e6:.1f} MB")
    print(f"    Modified: {obj['last_modified'].date()}")
```

## Improving Performance

Listing files in cloud storage requires network requests. The more files the server needs to enumerate, the slower the operation. Here's how to keep searches fast.

### Use Specific Prefixes

The [`glob`][obspec_utils.glob.glob] function automatically extracts the longest literal prefix from your pattern to minimize the files the server must enumerate:

| Pattern | Server lists from | Files enumerated |
|---------|-------------------|------------------|
| `data/2024/january/*.nc` | `data/2024/january/` | Only January files |
| `data/2024/*/*.nc` | `data/2024/` | All of 2024 |
| `data/**/*.nc` | `data/` | Everything under data/ |
| `**/*.nc` | (root) | Entire bucket |

Move literal path segments before wildcards when possible:

```python
# Slower: wildcard early means listing more files
glob(store, "NEX-GDDP/**/tasmax/**/v1.0/*_2100.nc")

# Faster: specific prefix narrows the listing
glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc")
```

### Process Results Lazily

Both [`glob`][obspec_utils.glob.glob] and [`glob_objects`][obspec_utils.glob.glob_objects] return iterators, so you can process results as they arrive without loading all paths into memory:

```python exec="on" source="above" session="find" result="code"
# Stop after finding 3 files (doesn't load all results)
count = 0
for path in glob(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_2100.nc"):
    print(f"Found: {path.split('/')[-1]}")
    count += 1
    if count >= 3:
        break
```

## Async Usage

For async contexts, use [`glob_async`][obspec_utils.glob.glob_async] and [`glob_objects_async`][obspec_utils.glob.glob_objects_async]:

```python exec="on" source="above" session="find" result="code"
import asyncio
from obspec_utils import glob_async

async def find_recent_years():
    paths = []
    async for path in glob_async(store, "NEX-GDDP/BCSD/rcp85/day/atmos/tasmax/r1i1p1/v1.0/*_inmcm4_209?.nc"):
        paths.append(path)
    return paths

paths = asyncio.run(find_recent_years())
print(f"Found {len(paths)} files asynchronously")
```
