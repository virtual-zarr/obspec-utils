# Debugging Slow Data Access

This guide shows how to understand what network requests your code is making when data access is slower than expected.

## Tracing Xarray Operations

Wrap your store with [`TracingReadableStore`][obspec_utils.wrappers.TracingReadableStore] to see what requests are made when opening a dataset:

```python exec="on" source="above" session="trace" result="code"
import xarray as xr
from obstore.store import HTTPStore
from obspec_utils.wrappers import TracingReadableStore, RequestTrace
from obspec_utils.readers import EagerStoreReader

# Access sample NetCDF files over HTTP
store = HTTPStore.from_url("https://raw.githubusercontent.com/pydata/xarray-data/refs/heads/master/")

trace = RequestTrace()
traced_store = TracingReadableStore(store, trace)

path = "air_temperature.nc"

with EagerStoreReader(traced_store, path) as reader:
    ds = xr.open_dataset(reader, engine="scipy")
    var_names = list(ds.data_vars)

summary = trace.summary()
print(f"Opening dataset required:")
print(f"  {summary['total_requests']} request(s)")
print(f"  {summary['total_bytes'] / 1e6:.2f} MB transferred")
print(f"Variables found: {var_names}")
```

The [`RequestTrace`][obspec_utils.wrappers.RequestTrace] collects information about each request, including byte ranges, timing, and request method. Use [`summary()`][obspec_utils.wrappers.RequestTrace.summary] for quick statistics or access individual [`RequestRecord`][obspec_utils.wrappers.RequestRecord] objects via `trace.requests`.

## Common Patterns to Look For

When analyzing traces, watch for:

| Pattern | Symptom | Solution |
|---------|---------|----------|
| Many small requests | High request count, low bytes per request | Use [`EagerStoreReader`][obspec_utils.readers.EagerStoreReader] to fetch full file or [`BlockStoreReader`][obspec_utils.readers.BlockStoreReader] to fetch and cache larger blocks |
| Duplicate requests | Same file/range requested multiple times | Add [`CachingReadableStore`][obspec_utils.wrappers.CachingReadableStore] |
| Sequential tiny reads | Many requests with incrementing offsets | Increase buffer size or use eager loading |
