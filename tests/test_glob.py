"""Tests for glob pattern matching functionality."""

from __future__ import annotations

import pytest

from obspec_utils.glob import (
    _compile_pattern,
    _parse_pattern,
    glob,
    glob_async,
    glob_objects,
    glob_objects_async,
)
from tests.mocks import MockListStore


class TestParsePattern:
    """Tests for _parse_pattern() prefix extraction."""

    def test_pattern_with_double_star(self):
        """Prefix should end before ** segment."""
        prefix, remaining = _parse_pattern("data/2024/**/*.nc")
        assert prefix == "data/2024/"
        assert remaining == "**/*.nc"

    def test_pattern_with_single_star(self):
        """Prefix should end before * wildcard."""
        prefix, remaining = _parse_pattern("data/*.nc")
        assert prefix == "data/"
        assert remaining == "*.nc"

    def test_pattern_with_question_mark(self):
        """Prefix should end before ? wildcard."""
        prefix, remaining = _parse_pattern("data/file?.nc")
        assert prefix == "data/"
        assert remaining == "file?.nc"

    def test_pattern_with_bracket(self):
        """Prefix should end before [ character class."""
        prefix, remaining = _parse_pattern("data/file[123].nc")
        assert prefix == "data/"
        assert remaining == "file[123].nc"

    def test_pattern_without_wildcards(self):
        """Literal pattern should use parent directory as prefix."""
        prefix, remaining = _parse_pattern("data/file.nc")
        assert prefix == "data/"
        assert remaining == "file.nc"

    def test_pattern_without_wildcards_no_directory(self):
        """Literal pattern with no directory should have empty prefix."""
        prefix, remaining = _parse_pattern("file.nc")
        assert prefix == ""
        assert remaining == "file.nc"

    def test_pattern_starting_with_wildcard(self):
        """Pattern starting with wildcard should have empty prefix."""
        prefix, remaining = _parse_pattern("**/*.nc")
        assert prefix == ""
        assert remaining == "**/*.nc"

    def test_pattern_with_star_in_filename(self):
        """Prefix should include full directory path before wildcard."""
        prefix, remaining = _parse_pattern("data/2024/01/file*.nc")
        assert prefix == "data/2024/01/"
        assert remaining == "file*.nc"


class TestCompilePattern:
    """Tests for _compile_pattern() regex compilation."""

    def test_single_star_matches_segment(self):
        """* should match characters within a single segment."""
        pattern = _compile_pattern("data/*.nc")
        assert pattern.match("data/file.nc")
        assert pattern.match("data/another.nc")
        assert not pattern.match("data/sub/file.nc")  # * doesn't cross /

    def test_double_star_matches_recursive(self):
        """** should match across multiple segments."""
        pattern = _compile_pattern("data/**/*.nc")
        assert pattern.match("data/file.nc")
        assert pattern.match("data/sub/file.nc")
        assert pattern.match("data/a/b/c/file.nc")

    def test_double_star_at_end(self):
        """** at end should match everything remaining under the prefix."""
        pattern = _compile_pattern("data/**")
        assert pattern.match("data/file.nc")
        assert pattern.match("data/sub/file.nc")
        assert pattern.match("data/")
        # Note: "data" without trailing / is NOT matched by "data/**"
        # This matches pathlib behavior where ** expands to directory contents

    def test_question_mark_matches_single_char(self):
        """? should match exactly one character."""
        pattern = _compile_pattern("file?.nc")
        assert pattern.match("file1.nc")
        assert pattern.match("fileA.nc")
        assert not pattern.match("file10.nc")  # ? is single char
        assert not pattern.match("file.nc")  # ? requires one char

    def test_character_class(self):
        """[abc] should match characters in set."""
        pattern = _compile_pattern("file[123].nc")
        assert pattern.match("file1.nc")
        assert pattern.match("file2.nc")
        assert pattern.match("file3.nc")
        assert not pattern.match("file4.nc")

    def test_character_range(self):
        """[a-z] should match characters in range."""
        pattern = _compile_pattern("file[a-c].nc")
        assert pattern.match("filea.nc")
        assert pattern.match("fileb.nc")
        assert pattern.match("filec.nc")
        assert not pattern.match("filed.nc")

    def test_negated_character_class(self):
        """[!abc] should match characters NOT in set."""
        pattern = _compile_pattern("file[!0-9].nc")
        assert pattern.match("filea.nc")
        assert pattern.match("filez.nc")
        assert not pattern.match("file1.nc")
        assert not pattern.match("file9.nc")

    def test_bracket_as_first_char_in_class(self):
        """[]] should match literal ] when ] is first char in class."""
        pattern = _compile_pattern("file[]].*")
        assert pattern.match("file].nc")
        assert pattern.match("file].txt")
        assert not pattern.match("filea.nc")
        assert not pattern.match("file[.nc")

    def test_bracket_as_first_char_in_negated_class(self):
        """[!]] should match any char except ] when ] is first after !."""
        pattern = _compile_pattern("file[!]].nc")
        assert pattern.match("filea.nc")
        assert pattern.match("file1.nc")
        assert pattern.match("file[.nc")
        assert not pattern.match("file].nc")

    def test_bracket_in_class_with_other_chars(self):
        """[]abc] should match ], a, b, or c."""
        pattern = _compile_pattern("file[]ab].nc")
        assert pattern.match("file].nc")
        assert pattern.match("filea.nc")
        assert pattern.match("fileb.nc")
        assert not pattern.match("filec.nc")
        assert not pattern.match("file[.nc")

    def test_unclosed_bracket_as_literal(self):
        """[ without closing ] should be treated as literal character."""
        pattern = _compile_pattern("file[.nc")
        assert pattern.match("file[.nc")
        assert not pattern.match("filea.nc")
        assert not pattern.match("file].nc")

    def test_unclosed_bracket_with_content(self):
        """[abc without closing ] should treat [ as literal."""
        pattern = _compile_pattern("file[abc.nc")
        assert pattern.match("file[abc.nc")
        assert not pattern.match("filea.nc")
        assert not pattern.match("fileb.nc")

    def test_trailing_slash_ignored(self):
        """Trailing slash should not affect matching."""
        pattern = _compile_pattern("data/")
        assert pattern.match("data/")
        assert not pattern.match("data")
        assert not pattern.match("data/file.nc")

    def test_double_slash_in_pattern(self):
        """Double slash should match paths with double slash."""
        pattern = _compile_pattern("data//file.nc")
        assert pattern.match("data//file.nc")
        assert not pattern.match("data/file.nc")

    def test_leading_slash_in_pattern(self):
        """Leading slash should be preserved in match."""
        pattern = _compile_pattern("/data/file.nc")
        assert pattern.match("/data/file.nc")
        assert not pattern.match("data/file.nc")

    def test_consecutive_double_stars(self):
        """Consecutive ** segments should be collapsed."""
        pattern = _compile_pattern("data/**/**/*.nc")
        assert pattern.match("data/file.nc")
        assert pattern.match("data/sub/file.nc")

    def test_literal_pattern(self):
        """Literal pattern should match exactly."""
        pattern = _compile_pattern("data/file.nc")
        assert pattern.match("data/file.nc")
        assert not pattern.match("data/file2.nc")
        assert not pattern.match("data/sub/file.nc")

    def test_special_regex_chars_escaped(self):
        """Special regex characters should be escaped in literals."""
        pattern = _compile_pattern("data/file.name.nc")
        assert pattern.match("data/file.name.nc")
        assert not pattern.match("data/fileXname.nc")  # . shouldn't match any char


class TestGlob:
    """Tests for glob() function."""

    def test_single_star_pattern(self):
        """glob should filter by * pattern within segment."""
        store = MockListStore(
            [
                "data/file1.nc",
                "data/file2.nc",
                "data/subdir/file3.nc",
            ]
        )
        result = list(glob(store, "data/*.nc"))
        assert result == ["data/file1.nc", "data/file2.nc"]

    def test_double_star_pattern(self):
        """glob should match recursively with ** pattern."""
        store = MockListStore(
            [
                "data/file1.nc",
                "data/2024/file2.nc",
                "data/2024/01/file3.nc",
            ]
        )
        result = list(glob(store, "data/**/*.nc"))
        assert "data/file1.nc" in result
        assert "data/2024/file2.nc" in result
        assert "data/2024/01/file3.nc" in result

    def test_question_mark_pattern(self):
        """glob should match ? as single character."""
        store = MockListStore(
            [
                "file1.nc",
                "file2.nc",
                "file10.nc",
            ]
        )
        result = list(glob(store, "file?.nc"))
        assert result == ["file1.nc", "file2.nc"]

    def test_character_class_pattern(self):
        """glob should match [abc] character classes."""
        store = MockListStore(
            [
                "file1.nc",
                "file2.nc",
                "file3.nc",
                "file4.nc",
            ]
        )
        result = list(glob(store, "file[123].nc"))
        assert result == ["file1.nc", "file2.nc", "file3.nc"]

    def test_prefix_optimization(self):
        """glob should pass prefix to list() for efficiency."""
        store = MockListStore(
            [
                "data/2024/file1.nc",
                "data/2024/file2.nc",
                "other/file.nc",
            ]
        )
        list(glob(store, "data/2024/*.nc"))
        # Verify list was called with the correct prefix
        assert store.list_calls == ["data/2024/"]

    def test_no_matches(self):
        """glob should return empty iterator when nothing matches."""
        store = MockListStore(["data/file.txt"])
        result = list(glob(store, "data/*.nc"))
        assert result == []

    def test_returns_iterator(self):
        """glob should return an iterator, not a list."""
        store = MockListStore(["data/file.nc"])
        result = glob(store, "data/*.nc")
        # Should be an iterator/generator, not a list
        assert hasattr(result, "__iter__")
        assert hasattr(result, "__next__")


class TestGlobObjects:
    """Tests for glob_objects() function."""

    def test_returns_object_meta(self):
        """glob_objects should return full ObjectMeta dicts."""
        store = MockListStore(["data/file.nc"])
        result = list(glob_objects(store, "data/*.nc"))
        assert len(result) == 1
        obj = result[0]
        assert obj["path"] == "data/file.nc"
        assert "size" in obj
        assert "last_modified" in obj
        assert "e_tag" in obj
        assert "version" in obj

    def test_matches_same_as_glob(self):
        """glob_objects should match the same paths as glob."""
        store = MockListStore(
            [
                "data/file1.nc",
                "data/file2.nc",
                "data/subdir/file3.nc",
            ]
        )
        paths_from_glob = list(glob(store, "data/*.nc"))
        paths_from_glob_objects = [
            obj["path"] for obj in glob_objects(store, "data/*.nc")
        ]
        assert paths_from_glob == paths_from_glob_objects


class TestGlobAsync:
    """Tests for async glob functions."""

    @pytest.mark.asyncio
    async def test_glob_async(self):
        """glob_async should return paths asynchronously."""
        store = MockListStore(
            [
                "data/file1.nc",
                "data/file2.nc",
            ]
        )
        result = []
        async for path in glob_async(store, "data/*.nc"):
            result.append(path)
        assert result == ["data/file1.nc", "data/file2.nc"]

    @pytest.mark.asyncio
    async def test_glob_objects_async(self):
        """glob_objects_async should return ObjectMeta asynchronously."""
        store = MockListStore(["data/file.nc"])
        result = []
        async for obj in glob_objects_async(store, "data/*.nc"):
            result.append(obj)
        assert len(result) == 1
        assert result[0]["path"] == "data/file.nc"
        assert "size" in result[0]


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_empty_store(self):
        """glob should handle empty stores."""
        store = MockListStore([])
        result = list(glob(store, "**/*.nc"))
        assert result == []

    def test_pattern_with_trailing_slash(self):
        """Pattern with trailing slash should work."""
        store = MockListStore(["data/subdir/file.nc"])
        # This tests that trailing slash in prefix is handled
        result = list(glob(store, "data/subdir/*.nc"))
        assert result == ["data/subdir/file.nc"]

    def test_complex_pattern(self):
        """Complex pattern with multiple wildcards should work."""
        store = MockListStore(
            [
                "data/2024/01/temp_file1.nc",
                "data/2024/02/temp_file2.nc",
                "data/2024/01/perm_file1.nc",
                "data/2023/01/temp_file1.nc",
            ]
        )
        result = list(glob(store, "data/2024/**/temp_*.nc"))
        assert "data/2024/01/temp_file1.nc" in result
        assert "data/2024/02/temp_file2.nc" in result
        assert "data/2024/01/perm_file1.nc" not in result
        assert "data/2023/01/temp_file1.nc" not in result

    def test_double_star_matches_zero_segments(self):
        """** should match zero segments (direct child)."""
        store = MockListStore(
            [
                "data/file.nc",
                "data/sub/file.nc",
            ]
        )
        result = list(glob(store, "data/**/*.nc"))
        # Both should match - ** can match zero segments
        assert "data/file.nc" in result
        assert "data/sub/file.nc" in result

    def test_multiple_extensions(self):
        """Should correctly handle files with multiple dots."""
        store = MockListStore(
            [
                "data/file.tar.gz",
                "data/file.nc",
            ]
        )
        result = list(glob(store, "data/*.tar.gz"))
        assert result == ["data/file.tar.gz"]
