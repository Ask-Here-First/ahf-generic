import math, base64
from collections.abc import Callable, Iterable, Mapping
from typing import Any, TextIO

from .typing import BlobTypes, FridMixin, FridPrime, FridValue, StrKeyMap, JsonLevel
from .chrono import DateTypes, strfr_datetime
from .guards import is_frid_identifier, is_frid_quote_free, is_identifier_head
from .pretty import PPToTextIOMixin, PrettyPrint, PPTokenType, PPToStringMixin
from .strops import StringEscapeEncode

JSON_QUOTED_KEYSET = (
    'true', 'false', 'null',
)
JSON1_ESCAPE_PAIRS = "\nn\tt\rr\ff\vv\bb"
JSON5_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\vv\x000"
EXTRA_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\aa\x1be"

class FridDumper(PrettyPrint):
    """Dump data structure into Frid or JSON format (or Frid-escaped JSON format).

    Constructor arguments:
    - `json_level`, indicates the json compatibility level; possible values:
        + None (or False or 0): frid format
        + True: JSON format
        + 5: JSON5 format
        + (a string): JSON format, but all unsupported data is in a quoted
          Frid format after a prefix given by this string.
    - `ascii_only`: encode all unicode characters into ascii in quoted string.
    - `print_real`: a user callback to convert an int or flat value to string.
    - `print_date`: a user callback to convert date/time/datetime value to string.
    - `print_blob`: a user callback to convert blob type to string.
    - `print_user`: a user callback to convert any unrecognized data types to string.
    - Other constructor parameter as supported by `PrettyPrint` class
    """
    def __init__(self, *args, json_level: JsonLevel=None, ascii_only: bool=False,
                 print_real: Callable[[int|float,str],str|None]|None=None,
                 print_date: Callable[[DateTypes,str],str|None]|None=None,
                 print_blob: Callable[[BlobTypes,str],str|None]|None=None,
                 print_user: Callable[[Any,str],str|None]|None=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.json_level = json_level
        self.using_frid = not (json_level or isinstance(json_level, str))
        self.ascii_only = ascii_only
        self.print_real = print_real
        self.print_date = print_date
        self.print_blob = print_blob
        self.print_user = print_user
        if self.using_frid:
            pairs = EXTRA_ESCAPE_PAIRS
            hex_prefix = ('x', 'u', 'U')
        elif json_level == 5:
            pairs = JSON5_ESCAPE_PAIRS
            hex_prefix = ('x', 'u', None)
        else:
            pairs = JSON1_ESCAPE_PAIRS
            hex_prefix = (None, 'u', None)
        if ascii_only:
            self.se_encoder = StringEscapeEncode(pairs, '\\')
        else:
            self.se_encoder = StringEscapeEncode(pairs, '\\', hex_prefix)

    def print(self, token: str, ttype: PPTokenType, /):
        """Default token print behavior:
        - Do not show optional separator.
        - Add a space after the required separator.
        """
        if ttype not in (PPTokenType.OPT_0, PPTokenType.OPT_1):
            self._print(token)
        if ttype in (PPTokenType.SEP_0, PPTokenType.SEP_1):
            self._print(' ')

    def real_to_str(self, data: int|float, path: str, /) -> str:
        """Convert an integer or real number to string."""
        if isinstance(data, int):
            return str(data)
        if isinstance(self.json_level, str) or self.using_frid:
            if math.isnan(data):
                out = "+." if data >= 0 else "-."
            elif math.isinf(data):
                out = "++" if data >= 0 else "--"
            else:
                return str(data)
            if self.using_frid:
                return out
            return '"' + self.json_level + out + '"'  # type: ignore -- analyzer is stupid here
        if self.json_level == 5:
            if math.isnan(data):
                return "NaN"
            if math.isinf(data):
                return "+Infinity" if data >= 0 else "-Infinity"
            return str(data)
        if self.json_level:
            if math.isnan(data):
                raise ValueError(f"NaN is not supported by JSON at {path}")
            if math.isinf(data):
                raise ValueError(f"Infinity is not supported by JSON at {path}")
            return str(data)
        raise ValueError(f"Invalid {self.json_level=} at {path=}")

    def date_to_str(self, data: DateTypes, path: str, /) -> str:
        """Convert Python date, time, or datetime into string representation."""
        out = strfr_datetime(data)
        if out is None:
            raise ValueError(f"Unsupported datetime type {type(data)} at {path=}")
        if self.using_frid:
            return out
        if isinstance(self.json_level, str):
            return '"' + self.json_level + out + '"'
        raise ValueError(f"Unsupported data for json={self.json_level} at {path=}: {out}")

    def blob_to_str(self, data: BlobTypes, path: str) -> str:
        """Convert a blob into string representation, quoted if needed."""
        # TODO: support line splitting and indentation
        out = base64.urlsafe_b64decode(data).decode()
        if not out.endswith("="):
            out = ".." + out
        elif out.endswith("=="):
            out = ".." + out[:-2] + ".."
        else:
            out = ".." + out[:-1] + "."
        if self.using_frid:
            return out
        if isinstance(self.json_level, str):
            return '"' + self.json_level + out + '"'
        raise ValueError(f"Blobs are unsupported by json={self.json_level} at {path=}")

    def _maybe_quoted(self, s: str, path: str) -> str:
        if self.using_frid:
            return s
        escaped = self.se_encoder(s, '"')
        if isinstance(self.json_level, str):
            return '"' + self.json_level + escaped + '"'
        raise ValueError(f"Unsupported customized data with json={self.json_level} at {path=}")

    def prime_data_to_str(self, data: FridValue, path: str, /) -> str|None:
        """Converts prime data to string representation.
        - Prime data types include int, float, bool, null, quote-free text, blob.
        - Return None if the data is not prime data.
        """
        if self.using_frid:
            if data is None:
                return '.'
            if isinstance(data, bool):
                return '+' if data else '-'
            if is_frid_identifier(data):
                return data
        else:
            # Do not need to use quoted and escaped json string for these constants
            if data is None:
                return 'null'
            if isinstance(data, bool):
                return 'true' if data else 'false'
            if isinstance(data, str):
                return None
        if isinstance(data, int|float):
            if self.print_real is not None and (out := self.print_real(data, path)) is not None:
                return self._maybe_quoted(out, path)
            return self.real_to_str(data, path)
        if isinstance(data, DateTypes):
            if self.print_date is not None and (out := self.print_date(data, path)) is not None:
                return self._maybe_quoted(out, path)
            return self.date_to_str(data, path)
        if isinstance(data, BlobTypes):
            if self.print_blob is not None and (out := self.print_blob(data, path)) is not None:
                return self._maybe_quoted(out, path)
            return self.blob_to_str(data, path)
        if self.json_level or self.json_level == '':
            return None
        # If if a string has non-ascii with ascii_only configfation, quotes are needed
        if not isinstance(data, str) or (self.ascii_only and not data.isascii()):
            return None
        if is_frid_quote_free(data):
            return data
        return None

    def print_quoted_str(self, data: str, path: str, /, as_key: bool=False, quote: str='\"'):
        """Prints a quoted string to stream with quotes."""
        self.print(quote + self.se_encoder(data, quote) + quote,
                   PPTokenType.LABEL if as_key else PPTokenType.ENTRY)

    def print_prime_data(self, data: FridPrime, path: str, /):
        """Prints some prime data to the stream and raise and error if quotes are needed."""
        s = self.prime_data_to_str(data, path)
        if s is None:
            raise ValueError(f"Invalid data type {type(data)}")
        self.print(s, PPTokenType.ENTRY)

    def print_naked_list(self, data: Iterable[FridValue], path: str="", /, sep: str=','):
        """Prints a list/array to the stream without opening and closing delimiters."""
        non_empty = False  # Use this flag in case bool(data) data not work
        for i, x in enumerate(data):
            if i > 0:
                self.print(sep[0], PPTokenType.SEP_0)
            self.print_frid_value(x, path + '[' + str(i) + ']')
            non_empty = True
        if non_empty and (self.using_frid or self.json_level == 5):
            self.print(sep[0], PPTokenType.OPT_0)

    def _is_unquoted_key(self, key: str):
        """Checks if the key does not need to be quoted"""
        if self.ascii_only and not key.isascii():
            return False
        if self.using_frid:
            return is_frid_identifier(key)
        if self.json_level != 5:
            return False
        # JSON 5 identifiers, first not ECMAScript keywords but not in Python
        if key in JSON_QUOTED_KEYSET:
            return False
        key = key.replace('$', '_')  # Handle $ the same way as _
        # Use python identifiers as it is generally more restrictive than JSON5
        return key.isidentifier()

    def print_naked_dict(self, data: StrKeyMap, path: str="", /, sep: str=',:'):
        """Prints a map to the stream without opening and closing delimiters."""
        for i, (k, v) in enumerate(data.items()):
            if i > 0:
                self.print(sep[0], PPTokenType.SEP_0)
            if not isinstance(k, str):
                raise ValueError(f"Key is not a string: {k}")
            if self._is_unquoted_key(k):
                self.print(k, PPTokenType.LABEL)
            else:
                self.print_quoted_str(k, path, as_key=True)
            self.print(sep[1], PPTokenType.SEP_1)
            self.print_frid_value(v, path)
        if data and (self.using_frid or self.json_level == 5):
            self.print(sep[0], PPTokenType.OPT_0)

    def print_frid_mixin(self, data: FridMixin, path: str, /):
        """Print any Frid mixin types."""
        (name, args, kwas) = data.frid_repr()
        path = path + '(' + name + ')'
        if self.using_frid:
            assert is_frid_identifier(name)
            self.print(name, PPTokenType.ENTRY)
            self.print('(', PPTokenType.START)
            self.print_naked_list(args, path, ',')
            self.print(',', PPTokenType.SEP_0)
            self.print_naked_dict(kwas, path, ',=')
            self.print(')', PPTokenType.CLOSE)
            return
        if not isinstance(self.json_level, str):
            raise ValueError(f"FridMixin is not supported by json={self.json_level} at {path=}")
        if kwas:
            self.print('{', PPTokenType.START)
            self.print_quoted_str('', path, as_key=True)
        # Print as an array
        if args:
            self.print('[', PPTokenType.START)
            self.print_quoted_str(self.json_level + name, path)
            self.print(',', PPTokenType.SEP_0)
            self.print_naked_list(args)
            self.print(']', PPTokenType.CLOSE)
        else:
            self.print_quoted_str(self.json_level + name, path)
        if kwas:
            self.print_naked_dict(kwas)
            self.print('}', PPTokenType.CLOSE)

    def print_frid_value(self, data: FridValue, path: str='', /):
        """Print the any value that Frid supports to the stream."""
        s = self.prime_data_to_str(data, path)
        if s is not None:
            self.print(s, PPTokenType.ENTRY)
        elif isinstance(data, str):
            self.print_quoted_str(data, path)
        elif isinstance(data, Mapping):
            self.print('{', PPTokenType.START)
            self.print_naked_dict(data, path)
            self.print('}', PPTokenType.CLOSE)
        elif isinstance(data, Iterable):
            self.print('[', PPTokenType.START)
            self.print_naked_list(data, path)
            self.print(']', PPTokenType.CLOSE)
        elif isinstance(data, FridMixin):
            self.print_frid_mixin(data, path)
        elif self.print_user is not None and (out := self.print_user(data, path)) is not None:
            return self._maybe_quoted(out, path)
        else:
            raise ValueError(f"Invalid type {type(data)} for json={self.json_level} at {path=}")

class FridStringDumper(PPToStringMixin, FridDumper):
    pass

class FridTextIODumper(PPToTextIOMixin, FridDumper):
    pass

def dump_into_str(data: FridValue, *args, **kwargs) -> str:
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_frid_value(data)
    return str(dumper)

def dump_info_tio(data: FridValue, io: TextIO, *args, **kwargs):
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_frid_value(data)
