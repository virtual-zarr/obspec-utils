# Changelog

## v0.9.0 (2025-01-28)

This release includes the addition of globbing functionality, a rename of ParallelStoreReader to BlockStoreReader, improvements to file-like properties, and expanded user guide documentation covering xarray integration, globbing, caching, and debugging.

### Breaking Changes

- Rename ParallelStoreReader to BlockStoreReader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/44

### Features

- Implement globbing in obspec_utils by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/42
- Start user guide with xarray section by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/46
- Add user guide section on globbing by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/51
- Add user guide section on debugging slow access by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/53
- Add user guide section on caching by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/54
- Add user guide section on rasterio by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/56

### Bug Fixes

- Add closed, readable, seekable, writable properties by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/52
- fix: allow Head redirects by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/49

### Chores

- Add changelog by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/45
- Minor typing improvements by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/47
- Add functions used in docs to exported API by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/48

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.8.0...v0.9.0

## v0.8.0 (2025-01-25)

This release includes a redesign of sub-module structure, a significant bug fix in ParallelStoreReader, pickling support for CachingReadableStore, and the addition of the Head protocol to ReadableStore for more efficient file size determination.

### Breaking Changes

- Always use head for file size determination by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/39

### Features

- Support pickling CachingReadableStore by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/36
- Make ObjectStoreRegistry typing generic by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/38
- Increase default size of ParallelStoreReader cache by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/35
- Improve sub-module organization by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/40

### Bug Fixes

- Fix cache eviction bug in ParallelStoreReader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/34
- Fix recursion when pickling by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/37

### Chores

- Split out reader tests by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/33
- Fix link by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/41

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.7.0...v0.8.0

## v0.7.0 (2025-01-24)

### What's Changed

* Feat: Add caching and request splitting readable stores by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/27

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.6.0...v0.7.0

## v0.6.0 (2025-01-24)

### What's Changed

* Feat: Adaptive request splitting in EagerStoreReader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/26

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.5.0...v0.6.0

## v0.5.0 (2025-01-24)

Major redesign around using protocols from obspec to support generic usage libraries other than obstore

### What's Changed

* Use prek for code standards by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/11
* Use obspec protocols by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/10
* Remove ReadableFile wrappers in favor of obspec protocol by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/12
* Feat: Add reader with multi-chunk fetching and an LRU cache by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/13
* Fix note in contributing guide by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/14
* Add a ReadableStore that provides request tracing by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/15
* Add ReadableFile protocol by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/16
* Optionally split eager reading across requests by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/17
* Add close methods to buffered readers by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/18
* Change verbosity by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/19
* Test tracing module by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/20
* Test AiohttpStore by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/21
* Improve registry tests by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/22
* Add guidance on choosing a reader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/23
* Test behavior against BytesIO reference by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/24
* More aiohttp tests by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/25

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.4.1...v0.5.0

## v0.4.1 (2024-12-19)

### What's Changed

* Add docs site by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/7

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.4.0...v0.4.1

## v0.4.0 (2024-12-18)

### What's Changed

* feature(typing): add py.typed file to package root by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/5
* Support py3.13/py3.14 by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/6

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.3.0...v0.4.0

## v0.3.0 (2024-12-18)

### What's Changed

* Add ObjectStoreRegistry by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/4

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.2.0...v0.3.0

## v0.2.0 (2024-12-10)

### What's Changed

* Expose buffer_size kwarg in open_reader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/3

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.1.0...v0.2.0

## v0.1.0 (2024-11-21)

### What's Changed

* Define reader usable by Xarray by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/1
* Add ObstoreMemCacheReader by @maxrjones in https://github.com/virtual-zarr/obspec-utils/pull/2

## New Contributors

* @maxrjones made their first contribution in https://github.com/virtual-zarr/obspec-utils/pull/1

**Full Changelog**: https://github.com/virtual-zarr/obspec-utils/compare/v0.1.0b1...v0.1.0

## v0.1.0b1 (2024-06-20)

Initial beta release.
