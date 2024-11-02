# Changelog

## [0.4.16] - 2024-10-29

The base version of this changelog.

## [0.5.0] - 2024-11-02

### Added

- Websocket support in `frid.web.asgid`.
- New utility functions `iter_stack_info()` and `warn()` in `frid.lib.oslib`.

### Changed

- Improved MIME-type handling in `frid.web.mixin`.
- Refactored `frid.FridLoader` to put the parsing offset and the data path
  into loader object as attributes.
- Description about dependencies in `README.md`.

### Removed

- The modules `frid.auitls`, `frid.osutil`, and `frid.strops`, deprecated
  as of 0.4.0.
