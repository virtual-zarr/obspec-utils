"""Shared mock classes for tests."""


class MockGetResult:
    """Mock GetResult for testing."""

    def __init__(self, data):
        self._data = data

    @property
    def attributes(self):
        return {}

    def buffer(self):
        return self._data

    @property
    def meta(self):
        return {
            "path": "",
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    @property
    def range(self):
        return (0, len(self._data))

    def __iter__(self):
        yield self._data


class MockGetResultAsync:
    """Mock async GetResult for testing."""

    def __init__(self, data):
        self._data = data

    @property
    def attributes(self):
        return {}

    async def buffer_async(self):
        return self._data

    @property
    def meta(self):
        return {
            "path": "",
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    @property
    def range(self):
        return (0, len(self._data))

    async def __aiter__(self):
        yield self._data


class MockReadableStore:
    """A mock store implementing the ReadableStore protocol."""

    def __init__(self, data: bytes = b"test data"):
        self._data = data

    def head(self, path):
        return {
            "path": path,
            "last_modified": None,
            "size": len(self._data),
            "e_tag": None,
            "version": None,
        }

    def get(self, path, *, options=None):
        return MockGetResult(self._data)

    async def get_async(self, path, *, options=None):
        return MockGetResultAsync(self._data)

    def get_range(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    async def get_range_async(self, path, *, start, end=None, length=None):
        if end is None:
            end = start + length
        return self._data[start:end]

    def get_ranges(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]

    async def get_ranges_async(self, path, *, starts, ends=None, lengths=None):
        if ends is None:
            ends = [s + ln for s, ln in zip(starts, lengths)]
        return [self._data[s:e] for s, e in zip(starts, ends)]


class PicklableStore:
    """A picklable store supporting multiple paths, for testing pickle support.

    Unlike obstore's MemoryStore (Rust-backed, not picklable), this pure-Python
    store can be pickled and unpickled, allowing tests to verify pickle support
    for wrappers like CachingReadableStore and reader classes.
    """

    def __init__(self, data: dict[str, bytes] | None = None):
        self._data = data if data is not None else {}

    def put(self, path: str, data: bytes) -> None:
        self._data[path] = data

    def head(self, path: str) -> dict:
        return {"size": len(self._data[path])}

    def get(self, path: str, *, options=None):
        return MockGetResult(self._data[path])

    async def get_async(self, path: str, *, options=None):
        return MockGetResultAsync(self._data[path])

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ):
        data = self._data[path]
        if length is not None:
            return data[start : start + length]
        elif end is not None:
            return data[start:end]
        return data[start:]

    async def get_range_async(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ):
        return self.get_range(path, start=start, end=end, length=length)

    def get_ranges(self, path: str, *, starts, ends=None, lengths=None):
        if lengths is not None:
            return [
                self._data[path][start : start + length]
                for start, length in zip(starts, lengths)
            ]
        elif ends is not None:
            return [self._data[path][s:e] for s, e in zip(starts, ends)]
        raise ValueError("Must provide ends or lengths")

    async def get_ranges_async(self, path: str, *, starts, ends=None, lengths=None):
        return self.get_ranges(path, starts=starts, ends=ends, lengths=lengths)
