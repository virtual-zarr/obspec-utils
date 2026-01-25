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


class MockReadableStoreWithHead:
    """A mock store that supports the Head protocol."""

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


class MockReadableStoreWithoutHead:
    """A mock store without the Head protocol."""

    def __init__(self, data: bytes = b"test data"):
        self._data = data

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
