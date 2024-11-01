FRID: Flexibly Represented Interactive Data
===========================================

This Python package is a tool for data manipulation.

Supported data types include:

- All JSON types: string, integer, floating point, boolean, null,
  array (as Python lists), and object (as Python dictionaries).
- Additional data types: binary types (bytes/bytearray/memoryview) and
  date types (date/time/datetime).
- Base classes are provided for user-extensible data structures,
  allowing users to convert between any customized data structures
  and string representations.

Current key features include:

- A data serialization/deserialization tool:
    + Data can be dumped into and loaded from a string representation that is
      more concise than the JSON format.
    + The library is capable of encoding data in fully JSON- or JSON5-compatible
      formats, including escape sequences in strings to support additional data
      types.
- A number of utilities functions and classes, including some asyncio utilities.
- A key/value store supporting memory, file system, redis, and sqlalchemy
  as backends.
- Web application support tools, such as:
    + Converting data from HTTP request bodies based on content type.
    + Converting data types to HTTP requests and setting the correct headers.
    + Sending streaming responses if the data of the body comes from an
      asynchronous iterator.
    + A mimimal web server routing framework, supporting both WSGi and ASGi.
    + Websocket support for ASGi.

Dependencies:

The package do not have required dependencies, but it needs a few optional
dependencies to run some features:

- `redis`: for Redis support; also a system command `redis-server` to run
  unit tests.
- `sqlalchemy`: for SQL support, with some additional packages depending on
  backend; to run async key/value store with Sqlite, one need `aiosqlite`
  and `greenlet` to run the unit tests. For Postgres, the following are
  needed:
    + `psycopg[binary]` for sync Postgres support,
    + `asyncpg` for async Postgres support.
- For web servers:
    + `gunicorn` to run WSGi unit tests.
    + `uvicorn`  to run ASGi unit tests.
- For websocket support (ASGi only):
    + `websockets` with `uvicorn` (or install `uvicorn[standard]`);
      it also allows us to run websockets' threading-based sync client
      for unit tests.
    + `websocket-client` to run websocket-client-based unit tests.

Other possibilities that haven't been tested:

- For ASGi: `daphne` and `hypercorn`;
- For ASGi with `uvicorn`: `wsproto` is an alternative to `websockets`.
