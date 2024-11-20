# Changelog

## [0.5.7]-2024-11-19

### Fixed

- `http.web` fixes for command-line handling.
- `http.web`: the FileRouter now accepts and drops non-empty query string.

### Added

- The webserver command-lines in `http.web.????d` now sets up the logging
  and checks the envvar `FRID_LOG_LEVEL`.

## [0.5.6]-2024-11-18

### Added

- `int_to_str()` and `str_to_int()`: support number grouping with separators.

### Changed

- `HttpError.to_str()` will now add "venue" field if it is provided.
- `frid.web`: How the functor router is called is changed: a functor router
  that is an class instance object can only be called for HTTP GET; a functor
  router that is an class type object can be called with all HTTP methods.

## [0.5.5]-2024-11-15

### Added

- `parse_http_body()` and `build_http_body()` are now available in `frid.web`.
- `mime_type=form` is supported for `build_http_body()` as well as
  `HttpMixin.set_response()`.
- Support encoding argument (other than the default `utf-8`) by
  `build_http_body()` and `HttpMixin.set_response()`.

### Changed

- Additional stacktrace printing in `frid.web.asgid`.

## [0.5.4]-2024-11-14

### Added

- `int_to_str()` and `str_to_int()` in `frid.lib` for base up to 36.
- `datetime_to_murky36()` and `murky36_to_datetime` to encode a datetime
  in a compact format of variable base up to 36.

## [0.5.3]-2024-11-13

### Changed

- Changed pseudo method for websocket from `:websocket:` to a shorter
  name `:ws:`.
- Added quotes to the string representation of `FridError` and `HttpError`.

### Fixed

- `frid.web.asgid`: limit the reason string to 100 characters because websocket
  control message cannot exceed 125 bytes.

## [0.5.2]-2024-11-13

### Fixed

- `frid.web.asgid` to use `HttpError.to_str()` as the correct reason string f
  or websocket close.

## [0.5.1]-2024-11-12

### Added

- Websocket support for ASGi webserver in `frid.web.asgid`.
- FridValue is now available in the top module `frid`.

### Changed

- Better FridError and HttpError error checking and conversion to string format.
- In the route table, we now accept class objects as routers, in which case
  meta arguments will be passed to the construtor of those classes.
- The command-line tools for web-servers will not automatically add `()` for
  loaded classes since class objects can be passed as routers.
- Functions `base64url_encode()` in `frid.lib` output bytes instead of strings;
  `base64url_decode()` accept both string and bytes.

### Fixed

- Fixing a parsing issue in `FridLoader.scan_naked_args()` in the case
  with only positional arguments.

## [0.5.0]-2024-11-02

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

## 0.4.16-2024-10-29

The base version of this changelog.

[0.5.6]: ../../compare/v0.5.5...v0.5.6
[0.5.5]: ../../compare/v0.5.4...v0.5.5
[0.5.4]: ../../compare/v0.5.3...v0.5.4
[0.5.3]: ../../compare/v0.5.2...v0.5.3
[0.5.2]: ../../compare/v0.5.1...v0.5.2
[0.5.1]: ../../compare/v0.5.0...v0.5.1
[0.5.0]: ../../compare/v0.4.16...v0.5.0
