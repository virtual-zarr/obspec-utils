"""
Glob pattern matching for object stores using obspec primitives.

This module provides functions to match paths against glob patterns,
similar to `fsspec.glob`, `pathlib.glob`, and `glob.glob`.

The pattern compilation is inspired by CPython's `glob.translate()` but
simplified for object stores (/ separator only, no hidden file handling).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from obspec import List, ListAsync, ObjectMeta

# Characters that indicate a glob pattern
_GLOB_CHARS = frozenset("*?[")


def _parse_pattern(pattern: str) -> tuple[str, str]:
    """
    Extract the literal prefix from a pattern for use with store.list().

    Returns (prefix, remaining_pattern) where prefix contains no wildcards
    and ends at a path separator boundary (suitable for obspec's segment-based
    prefix matching).

    Examples
    --------
    >>> _parse_pattern("data/2024/**/*.nc")
    ('data/2024/', '**/*.nc')
    >>> _parse_pattern("data/*.nc")
    ('data/', '*.nc')
    >>> _parse_pattern("**/*.nc")
    ('', '**/*.nc')
    >>> _parse_pattern("data/file.nc")
    ('data/', 'file.nc')
    >>> _parse_pattern("file.nc")
    ('', 'file.nc')
    """
    for i, char in enumerate(pattern):
        if char in _GLOB_CHARS:
            # Find the last '/' before the first glob char
            prefix_end = pattern.rfind("/", 0, i) + 1
            return pattern[:prefix_end], pattern[prefix_end:]

    # No glob chars - pattern is literal
    # Use parent directory as prefix (obspec uses segment-based prefix matching)
    last_slash = pattern.rfind("/")
    if last_slash >= 0:
        return pattern[: last_slash + 1], pattern[last_slash + 1 :]
    return "", pattern


def _translate_segment(segment: str) -> str:
    """
    Translate a single path segment (containing no /) to a regex pattern.

    Handles:
    - `*` -> `[^/]*` (any chars except /)
    - `?` -> `[^/]` (single char except /)
    - `[abc]` -> `[abc]` (character class)
    - `[!abc]` -> `[^abc]` (negated character class)
    - `[a-z]` -> `[a-z]` (character range)
    - Literal characters are escaped

    Parameters
    ----------
    segment
        A single path segment (no / characters).

    Returns
    -------
    str
        Regex pattern for the segment.
    """
    result = []
    i = 0
    n = len(segment)

    while i < n:
        char = segment[i]

        if char == "*":
            # * matches any characters except /
            result.append("[^/]*")
            i += 1

        elif char == "?":
            # ? matches single character except /
            result.append("[^/]")
            i += 1

        elif char == "[":
            # Character class - find the closing ]
            j = i + 1
            # Handle [! or [^ for negation
            if j < n and segment[j] in "!^":
                j += 1
            # Handle ] as first char in class (literal)
            if j < n and segment[j] == "]":
                j += 1
            # Find closing ]
            while j < n and segment[j] != "]":
                j += 1

            if j >= n:
                # No closing ] found, treat [ as literal
                result.append(re.escape(char))
                i += 1
            else:
                # Extract the character class
                char_class = segment[i : j + 1]
                # Convert [!...] to [^...]
                if len(char_class) > 2 and char_class[1] == "!":
                    char_class = "[^" + char_class[2:]
                result.append(char_class)
                i = j + 1

        else:
            # Literal character - escape regex special chars
            result.append(re.escape(char))
            i += 1

    return "".join(result)


def _compile_pattern(pattern: str) -> re.Pattern[str]:
    """
    Convert a glob pattern to a compiled regex.

    Processes the pattern segment by segment, handling:
    - `*` matches within a single path segment
    - `**` matches across path separators (recursive)
    - `?`, `[abc]`, `[!abc]`, `[a-z]` standard glob patterns

    This approach is inspired by CPython's `glob.translate()` but simplified
    for object stores (/ separator only, no hidden file handling).

    Parameters
    ----------
    pattern
        Glob pattern to compile.

    Returns
    -------
    re.Pattern[str]
        Compiled regex pattern for matching paths.
    """
    segments = pattern.split("/")
    regex_parts: list[str] = []

    i = 0
    while i < len(segments):
        segment = segments[i]
        is_last = i == len(segments) - 1

        if segment == "**":
            # Skip consecutive ** segments
            while i + 1 < len(segments) and segments[i + 1] == "**":
                i += 1
            is_last = i == len(segments) - 1

            if is_last:
                # ** at end: match everything remaining (including empty)
                regex_parts.append(".*")
            else:
                # ** in middle: match zero or more complete segments
                # (?:.+/)? matches optional "something/" or nothing
                regex_parts.append("(?:.+/)?")

        elif segment == "":
            # Empty segment (e.g., leading / or trailing /)
            if not is_last:
                regex_parts.append("/")

        else:
            # Regular segment with potential wildcards
            segment_regex = _translate_segment(segment)
            if is_last:
                regex_parts.append(segment_regex)
            else:
                regex_parts.append(segment_regex + "/")

        i += 1

    # Anchor at end
    return re.compile("".join(regex_parts) + r"\Z")


def _glob_impl(store: List, pattern: str) -> Iterator[ObjectMeta]:
    """
    Internal implementation shared by glob() and glob_objects().

    Parameters
    ----------
    store
        Any store implementing the obspec.List protocol.
    pattern
        Glob pattern to match.

    Yields
    ------
    ObjectMeta
        Metadata for each matching object.
    """
    # Extract literal prefix for efficient listing
    list_prefix, _ = _parse_pattern(pattern)

    # Compile the full pattern for matching
    compiled = _compile_pattern(pattern)

    # List objects and filter by pattern
    for chunk in store.list(prefix=list_prefix if list_prefix else None):
        for obj in chunk:
            if compiled.match(obj["path"]):
                yield obj


async def _glob_impl_async(store: ListAsync, pattern: str) -> AsyncIterator[ObjectMeta]:
    """
    Async internal implementation shared by glob_async() and glob_objects_async().

    Parameters
    ----------
    store
        Any store implementing the obspec.ListAsync protocol.
    pattern
        Glob pattern to match.

    Yields
    ------
    ObjectMeta
        Metadata for each matching object.
    """
    # Extract literal prefix for efficient listing
    list_prefix, _ = _parse_pattern(pattern)

    # Compile the full pattern for matching
    compiled = _compile_pattern(pattern)

    # List objects and filter by pattern
    async for chunk in store.list_async(prefix=list_prefix if list_prefix else None):
        for obj in chunk:
            if compiled.match(obj["path"]):
                yield obj


def glob(store: List, pattern: str) -> Iterator[str]:
    """
    Match paths against a glob pattern using the obspec List primitive.

    Parameters
    ----------
    store
        Any store implementing the [obspec.List][obspec.List] protocol.
    pattern
        Glob pattern to match. Supports:

        - `*` : matches any characters within a single path segment
        - `**` : matches any number of path segments (recursive)
        - `?` : matches exactly one character
        - `[abc]` : matches characters in set
        - `[a-z]` : matches characters in range
        - `[!abc]` : matches characters NOT in set

    Yields
    ------
    str
        Paths of matching objects.

    Examples
    --------
    Find all NetCDF files in a directory:

    ```python
    paths = list(glob(store, "data/2024/*.nc"))
    ```

    Find all NetCDF files recursively:

    ```python
    paths = list(glob(store, "data/**/*.nc"))
    ```

    Find files with single-character suffix:

    ```python
    paths = list(glob(store, "data/file?.nc"))
    ```

    See Also
    --------
    glob_objects : Returns full ObjectMeta instead of just paths.
    glob_async : Async version of this function.
    """
    for obj in _glob_impl(store, pattern):
        yield obj["path"]


def glob_objects(store: List, pattern: str) -> Iterator[ObjectMeta]:
    """
    Match paths against a glob pattern, returning full object metadata.

    Same as [glob][obspec_utils.glob.glob], but yields
    [ObjectMeta][obspec.ObjectMeta] dicts containing:

    - `path`: str - The full path to the object
    - `last_modified`: datetime - The last modified time
    - `size`: int - The size in bytes
    - `e_tag`: str | None - The unique identifier (ETag)
    - `version`: str | None - A version indicator

    Parameters
    ----------
    store
        Any store implementing the [obspec.List][obspec.List] protocol.
    pattern
        Glob pattern to match. See [glob][obspec_utils.glob.glob] for
        supported patterns.

    Yields
    ------
    ObjectMeta
        Metadata for each matching object.

    Examples
    --------
    Get file sizes for matching objects:

    ```python
    total_size = sum(obj["size"] for obj in glob_objects(store, "data/**/*.nc"))
    ```

    Find recently modified files:

    ```python
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    recent = [
        obj for obj in glob_objects(store, "data/**/*.nc")
        if obj["last_modified"] > cutoff
    ]
    ```

    See Also
    --------
    glob : Returns just paths instead of full metadata.
    glob_objects_async : Async version of this function.
    """
    yield from _glob_impl(store, pattern)


async def glob_async(store: ListAsync, pattern: str) -> AsyncIterator[str]:
    """
    Async version of [glob][obspec_utils.glob.glob].

    Match paths against a glob pattern using the obspec ListAsync primitive.

    Parameters
    ----------
    store
        Any store implementing the [obspec.ListAsync][obspec.ListAsync] protocol.
    pattern
        Glob pattern to match. See [glob][obspec_utils.glob.glob] for
        supported patterns.

    Yields
    ------
    str
        Paths of matching objects.

    Examples
    --------
    ```python
    async def process_files(store):
        async for path in glob_async(store, "data/**/*.nc"):
            await process(path)
    ```

    See Also
    --------
    glob : Sync version of this function.
    glob_objects_async : Returns full ObjectMeta instead of just paths.
    """
    async for obj in _glob_impl_async(store, pattern):
        yield obj["path"]


async def glob_objects_async(
    store: ListAsync, pattern: str
) -> AsyncIterator[ObjectMeta]:
    """
    Async version of [glob_objects][obspec_utils.glob.glob_objects].

    Match paths against a glob pattern, returning full object metadata.

    Parameters
    ----------
    store
        Any store implementing the [obspec.ListAsync][obspec.ListAsync] protocol.
    pattern
        Glob pattern to match. See [glob][obspec_utils.glob.glob] for
        supported patterns.

    Yields
    ------
    ObjectMeta
        Metadata for each matching object.

    Examples
    --------
    ```python
    async def get_total_size(store):
        total = 0
        async for obj in glob_objects_async(store, "data/**/*.nc"):
            total += obj["size"]
        return total
    ```

    See Also
    --------
    glob_objects : Sync version of this function.
    glob_async : Returns just paths instead of full metadata.
    """
    async for obj in _glob_impl_async(store, pattern):
        yield obj


__all__ = ["glob", "glob_objects", "glob_async", "glob_objects_async"]
