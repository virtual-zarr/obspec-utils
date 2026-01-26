"""
Compare obspec_utils.glob behavior against glob.glob and pathlib.glob.

Uses pairwise comparisons where obspec_utils.glob is compared against each
reference implementation to verify consistent behavior.
"""

from __future__ import annotations

import glob as stdlib_glob
from pathlib import Path
from typing import Generator
import pytest

from obspec_utils import glob
from obstore.store import LocalStore


@pytest.fixture
def temp_store(tmp_path: Path) -> tuple[Path, LocalStore]:
    """Create a temporary directory structure for testing."""
    # Create directory structure
    (tmp_path / "data" / "2024" / "01").mkdir(parents=True)
    (tmp_path / "data" / "2024" / "02").mkdir(parents=True)
    (tmp_path / "data" / "2023").mkdir(parents=True)
    (tmp_path / "other").mkdir(parents=True)

    # Create files
    files = [
        "data/file1.nc",
        "data/file2.nc",
        "data/2024/01/temp_data.nc",
        "data/2024/01/perm_data.nc",
        "data/2024/02/temp_data.nc",
        "data/2023/old_data.nc",
        "data/readme.txt",
        "other/file.nc",
        "root.nc",
    ]
    for f in files:
        (tmp_path / f).write_text(f"content of {f}")

    store = LocalStore(str(tmp_path))
    return tmp_path, store


def stdlib_glob_paths(base: Path, pattern: str, recursive: bool = False) -> set[str]:
    """Run stdlib glob and return normalized relative paths."""
    # stdlib glob needs absolute pattern
    full_pattern = str(base / pattern)
    result = stdlib_glob.glob(full_pattern, recursive=recursive)
    # Normalize to relative paths
    return {str(Path(p).relative_to(base)) for p in result}


def pathlib_glob_paths(base: Path, pattern: str) -> set[str]:
    """Run pathlib glob and return normalized relative paths."""
    result: Generator[Path, None, None] = base.glob(pattern)
    return {str(p.relative_to(base)) for p in result}


def obspec_glob_paths(store: LocalStore, pattern: str) -> set[str]:
    """Run obspec_utils glob and return paths as set."""
    return set(glob(store, pattern))


class TestVsStdlibGlob:
    """Compare obspec_utils.glob against stdlib glob.glob."""

    def test_single_star(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_single_star_nested(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/2024/01/*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_double_star(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/**/*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(
            base, pattern, recursive=True
        )

    def test_double_star_with_prefix(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/**/temp_*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(
            base, pattern, recursive=True
        )

    def test_question_mark(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file?.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_character_class(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file[12].nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_negated_character_class(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file[!1].nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_no_matches(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "nonexistent/*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_literal_path(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file1.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(base, pattern)

    def test_multiple_wildcards(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/202?/**/*.nc"
        assert obspec_glob_paths(store, pattern) == stdlib_glob_paths(
            base, pattern, recursive=True
        )


class TestVsPathlibGlob:
    """Compare obspec_utils.glob against pathlib.Path.glob."""

    def test_single_star(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_single_star_nested(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/2024/01/*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_double_star(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/**/*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_double_star_with_prefix(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/**/temp_*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_question_mark(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file?.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_character_class(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file[12].nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_negated_character_class(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file[!1].nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_no_matches(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "nonexistent/*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_literal_path(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/file1.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)

    def test_multiple_wildcards(self, temp_store: tuple[Path, LocalStore]):
        base, store = temp_store
        pattern = "data/202?/**/*.nc"
        assert obspec_glob_paths(store, pattern) == pathlib_glob_paths(base, pattern)
