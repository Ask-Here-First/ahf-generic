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

- Data can be dumped into and loaded from a string representation that is
  more concise than the JSON format.
- The library is capable of encoding data in fully JSON- or JSON5-compatible
  formats, including escape sequences in strings to support additional data
  types.
- Comparison of two data trees using a highly flexible comparator.
- Web application support tools, such as:
    + Converting data from HTTP request bodies based on content type.
    + Converting data types to HTTP requests and setting the correct headers.
    + Sending streaming responses if the data of the body comes from an
      asynchronous iterator.
