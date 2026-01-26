# Glob Implementation Design

This document describes the design of `obspec_utils.glob`, which provides glob pattern matching for object stores using the obspec `List` primitive.

## Overview

The glob module provides functions to match paths against glob patterns, similar to `fsspec.glob`, `pathlib.glob`, and `glob.glob`. It enables users to find objects in stores using familiar wildcard patterns like `data/**/*.nc`.

## API Design

### Two-Function Approach

We provide two separate functions rather than a single function with a `detail` kwarg:

```python
from obspec_utils import glob, glob_objects

# Get paths only
paths = list(glob(store, "data/**/*.nc"))
# ['data/2024/file1.nc', 'data/2024/01/file2.nc', ...]

# Get full metadata
for obj in glob_objects(store, "data/**/*.nc"):
    print(f"{obj['path']}: {obj['size']} bytes")
```

**Rationale:**

| Approach | Typing | API Clarity |
|----------|--------|-------------|
| Two functions | Clean return types | Explicit intent |
| Single function with kwarg | Requires `@overload` decorators | Runtime-dependent return type |

Following Python's "explicit is better than implicit" philosophy, two functions provide:

- **Clean typing** — each function has a single return type
- **Discoverability** — both options visible in autocomplete
- **No ambiguity** — return type known at call site

### Function Matrix

| Function | Protocol | Returns |
|----------|----------|---------|
| `glob` | `obspec.List` | `Iterator[str]` |
| `glob_objects` | `obspec.List` | `Iterator[ObjectMeta]` |
| `glob_async` | `obspec.ListAsync` | `AsyncIterator[str]` |
| `glob_objects_async` | `obspec.ListAsync` | `AsyncIterator[ObjectMeta]` |

### Protocol Requirements

Following obspec's philosophy, we use `obspec.List` and `obspec.ListAsync` directly rather than defining wrapper protocols:

```python
from obspec import List

def glob(store: List, pattern: str) -> Iterator[str]:
    ...
```

This keeps the API minimal and avoids unnecessary abstraction layers.

## Pattern Support

The glob functions support standard Unix-style glob patterns:

| Pattern | Meaning | Example |
|---------|---------|---------|
| `*` | Matches any characters within a single path segment | `data/*.nc` matches `data/file.nc` but not `data/sub/file.nc` |
| `**` | Matches any number of path segments (recursive) | `data/**/*.nc` matches `data/a/b/c/file.nc` |
| `?` | Matches exactly one character | `file?.nc` matches `file1.nc` but not `file10.nc` |
| `[abc]` | Matches characters in set | `file[123].nc` matches `file1.nc`, `file2.nc`, `file3.nc` |
| `[a-z]` | Matches characters in range | `file[a-c].nc` matches `filea.nc`, `fileb.nc`, `filec.nc` |
| `[!abc]` | Matches characters NOT in set | `file[!0-9].nc` matches `filea.nc` but not `file1.nc` |

## Implementation Algorithm

### 1. Prefix Extraction

Extract the literal prefix from the pattern to optimize the `list()` call:

```python
GLOB_CHARS = frozenset('*?[')

def _parse_pattern(pattern: str) -> tuple[str, str]:
    """Find the longest prefix without glob characters."""
    for i, char in enumerate(pattern):
        if char in GLOB_CHARS:
            prefix_end = pattern.rfind('/', 0, i) + 1
            return pattern[:prefix_end], pattern[prefix_end:]
    return pattern, ""
```

Examples:
- `data/2024/**/*.nc` → prefix `data/2024/`, remaining `**/*.nc`
- `data/*.nc` → prefix `data/`, remaining `*.nc`
- `**/*.nc` → prefix `""`, remaining `**/*.nc`

### 2. Pattern Compilation

Convert the glob pattern to a compiled regex using a segment-by-segment approach
inspired by CPython's `glob.translate()`:

```python
import re

def _compile_pattern(pattern: str) -> re.Pattern[str]:
    """
    Convert glob pattern to regex, processing segment by segment.

    Inspired by CPython 3.13+ glob.translate() but simplified for
    object stores (/ separator only, no hidden file handling).
    """
    segments = pattern.split('/')
    regex_parts = []

    i = 0
    while i < len(segments):
        segment = segments[i]
        is_last = (i == len(segments) - 1)

        if segment == '**':
            # Skip consecutive ** segments
            while i + 1 < len(segments) and segments[i + 1] == '**':
                i += 1
            is_last = (i == len(segments) - 1)

            if is_last:
                # ** at end: match everything remaining
                regex_parts.append('.*')
            else:
                # ** in middle: match zero or more segments
                regex_parts.append('(?:.+/)?')
        else:
            # Convert segment with wildcards
            segment_regex = _translate_segment(segment)
            if is_last:
                regex_parts.append(segment_regex)
            else:
                regex_parts.append(segment_regex + '/')

        i += 1

    return re.compile(''.join(regex_parts) + r'\Z')

def _translate_segment(segment: str) -> str:
    """Translate a single path segment (no /) to regex."""
    # Handle *, ?, [abc], [!abc], [a-z] and literal characters
    # * -> [^/]* (any chars except /)
    # ? -> [^/] (single char except /)
    # [...] -> [...] (character class, passed through)
    ...
```

**Key design choices** (inspired by CPython `glob.translate()`):

| Pattern | Regex | Rationale |
|---------|-------|-----------|
| `*` | `[^/]*` | Match any chars within segment (not across `/`) |
| `**` (middle) | `(?:.+/)?` | Match zero or more complete segments |
| `**` (end) | `.*` | Match everything remaining |
| `?` | `[^/]` | Match single char within segment |
| `[abc]` | `[abc]` | Character class (passed through) |
| `[!abc]` | `[^abc]` | Negated character class |

**Differences from CPython:**
- Object stores use `/` only (no `os.sep` handling)
- No hidden file handling (object stores don't have this concept)
- Simpler implementation focused on object store paths

### 3. List and Filter

```python
def _glob_impl(store: List, pattern: str) -> Iterator[ObjectMeta]:
    list_prefix, _ = _parse_pattern(pattern)
    compiled = _compile_pattern(pattern)

    for chunk in store.list(prefix=list_prefix if list_prefix else None):
        for obj in chunk:
            if compiled.match(obj["path"]):
                yield obj
```

Note: The compiled pattern includes `\Z` anchor at the end, so `match()` (which anchors at the start)
effectively performs a full match. This is more efficient than `fullmatch()` in some regex engines.

## Behavior Comparison

| Feature | `obspec_utils.glob` | `fsspec.glob` | `pathlib.glob` | `glob.glob` |
|---------|---------------------|---------------|----------------|-------------|
| Returns | `Iterator[str]` or `Iterator[ObjectMeta]` | `list[str]` or `dict` | `Iterator[Path]` | `list[str]` |
| `*` matches `/` | No | No | No | No |
| `**` recursive | Yes (always) | Yes | Yes (always) | Yes (if `recursive=True`) |
| Hidden files | Matched | Matched | Matched | Only if pattern starts with `.` |
| Case sensitive | Yes (always) | Platform-dependent | Platform-dependent | Platform-dependent |
| Directories | Not included | Yes (`withdirs`) | Yes | Yes |
| `maxdepth` | Not supported | Yes | No | No |
| Metadata | `glob_objects()` | `detail=True` | No | No |
| Streaming | Yes (iterator) | No (returns list) | Yes (iterator) | No (returns list) |

### Key Differences

1. **Two functions for different return types**: `glob()` returns paths, `glob_objects()` returns `ObjectMeta`. This provides clean typing without runtime-dependent return types.
2. **No `maxdepth` parameter**: The obspec `List` primitive is always recursive. Adding `maxdepth` would require post-processing and counting path segments.
3. **Always case-sensitive**: Object stores treat paths as case-sensitive. Unlike filesystem-based implementations, we don't vary by platform.
4. **No directory results**: Object stores don't have real directories as separate entities. Only actual objects are returned, not directory prefixes.
5. **Streaming results**: Returns an iterator that yields results as they're received from the store, enabling efficient processing of large result sets.

## Usage Examples

### Basic Patterns

```python
from obspec_utils import glob, glob_objects

# Find all NetCDF files in a directory
paths = list(glob(store, "data/2024/*.nc"))

# Find all NetCDF files recursively
paths = list(glob(store, "data/**/*.nc"))

# Find files with single-character suffix
paths = list(glob(store, "data/file?.nc"))

# Find files matching character set
paths = list(glob(store, "data/[abc]*.nc"))
```

### With Metadata

```python
# Get file sizes for matching objects
total_size = sum(obj["size"] for obj in glob_objects(store, "data/**/*.nc"))

# Find recently modified files
from datetime import datetime, timedelta, timezone
cutoff = datetime.now(timezone.utc) - timedelta(days=7)
recent = [
    obj for obj in glob_objects(store, "data/**/*.nc")
    if obj["last_modified"] > cutoff
]
```

### Async Usage

```python
async def process_files(store):
    async for path in glob_async(store, "data/**/*.nc"):
        await process(path)
```

## Dependencies

- `obspec` — for `List`, `ListAsync`, and `ObjectMeta` types
- `re` — standard library regex

No new external dependencies required. We implement our own `translate()` function
rather than using `fnmatch.translate()` to properly handle path separators and `**` patterns.
