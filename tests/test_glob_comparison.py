"""
Compare obspec_utils.glob behavior against glob.glob and pathlib.glob.

Uses parameterized tests where obspec_utils.glob is compared against each
reference implementation to verify consistent behavior.
"""

from __future__ import annotations

import glob as stdlib_glob
from collections.abc import Callable
from pathlib import Path

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
        # Files for testing ] as first char in character class
        "data/file].nc",
        "data/file[.nc",
        "data/filea.nc",
    ]
    for f in files:
        (tmp_path / f).write_text(f"content of {f}")

    store = LocalStore(str(tmp_path))
    return tmp_path, store


def _stdlib_glob(base: Path, pattern: str) -> set[str]:
    """Run stdlib glob and return normalized relative paths."""
    full_pattern = str(base / pattern)
    # stdlib glob requires recursive=True for ** patterns
    recursive = "**" in pattern
    result = stdlib_glob.glob(full_pattern, recursive=recursive)
    return {str(Path(p).relative_to(base)) for p in result}


def _pathlib_glob(base: Path, pattern: str) -> set[str]:
    """Run pathlib glob and return normalized relative paths."""
    return {str(p.relative_to(base)) for p in base.glob(pattern)}


def _obspec_glob(store: LocalStore, pattern: str) -> set[str]:
    """Run obspec_utils glob and return paths as set."""
    return set(glob(store, pattern))


# Reference implementations to compare against
ReferenceGlob = Callable[[Path, str], set[str]]
REFERENCE_IMPLS: list[tuple[str, ReferenceGlob]] = [
    ("stdlib", _stdlib_glob),
    ("pathlib", _pathlib_glob),
]


class TestGlobComparison:
    """Compare obspec_utils.glob against reference implementations."""

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_single_star(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_single_star_nested(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/2024/01/*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_double_star(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/**/*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_double_star_with_prefix(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/**/temp_*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_question_mark(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/file?.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_character_class(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/file[12].nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_negated_character_class(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/file[!1].nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_no_matches(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "nonexistent/*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_literal_path(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/file1.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_multiple_wildcards(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        base, store = temp_store
        pattern = "data/202?/**/*.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_bracket_literal_in_class(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        """[]] should match literal ] as first char in class."""
        base, store = temp_store
        pattern = "data/file[]].*"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_negated_bracket_literal_in_class(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        """[!]] should match any char except ] as first char after !."""
        base, store = temp_store
        pattern = "data/file[!]].nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)

    @pytest.mark.parametrize("impl_name,ref_glob", REFERENCE_IMPLS)
    def test_unclosed_bracket_as_literal(
        self,
        temp_store: tuple[Path, LocalStore],
        impl_name: str,
        ref_glob: ReferenceGlob,
    ):
        """[ without closing ] should be treated as literal character."""
        base, store = temp_store
        pattern = "data/file[.nc"
        assert _obspec_glob(store, pattern) == ref_glob(base, pattern)
