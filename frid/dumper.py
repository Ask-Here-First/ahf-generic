import math, base64
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import Any, Literal, TextIO, overload

from .typing import MISSING, PRESENT, FridBasic, FridBeing, BlobTypes, FridNameArgs, ValueArgs
from .typing import FridArray, FridMixin, FridPrime, FridValue, StrKeyMap
from .chrono import DateTypes, strfr_datetime, timeonly, datetime, dateonly
from .guards import is_frid_identifier, is_frid_quote_free, is_list_like
from .pretty import MultilineFormatMixin, PPToTextIOMixin, PrettyPrint, PPTokenType, PPToStringMixin
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
    - `json_level`, an integer indicating the json compatibility level; possible values:
        + 0 (default): frid format
        + 1: JSON format
        + 5: JSON5 format
    - `escape_seq`: a string starting at the beginning of quoted string to mean special
      data as supported by frid. Set to None if not supporting escaping.
    - `ascii_only`: encode all unicode characters into ascii in quoted string.
    - `print_real`: a user callback to convert an int or flat value to string.
    - `print_date`: a user callback to convert date/time/datetime value to string.
    - `print_blob`: a user callback to convert blob type to string.
    - `print_user`: a user callback to convert any unrecognized data types to string.
    - Other constructor parameter as supported by `PrettyPrint` class
    """
    def __init__(self, *args, json_level: Literal[0,1,5]=0, escape_seq: str|None=None,
                 ascii_only: bool=False,
                 mixin_args: Iterable[ValueArgs[type[FridMixin]]]|None=None,
                 print_real: Callable[[int|float],str|None]|None=None,
                 print_date: Callable[[DateTypes],str|None]|None=None,
                 print_blob: Callable[[BlobTypes],str|None]|None=None,
                 print_user: Callable[[Any,str],str|None]|None=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.json_level = json_level
        self.escape_seq = escape_seq
        self.ascii_only = ascii_only
        self.print_real = print_real
        self.print_date = print_date
        self.print_blob = print_blob
        self.print_user = print_user
        self.mixin_args: dict[type[FridMixin],ValueArgs[type[FridMixin]]] = {}
        if mixin_args:
            for item in mixin_args:
                self.mixin_args[item.data] = item
        if not self.json_level:
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

    def real_to_str(self, data: int|float, path: str, /) -> str:
        """Convert an integer or real number to string."""
        if isinstance(data, int):
            return str(data)
        if self.json_level == 5:
            if math.isnan(data):
                return "NaN"
            if math.isinf(data):
                return "+Infinity" if data >= 0 else "-Infinity"
            return str(data)
        if self.json_level and self.escape_seq is None:
            if math.isnan(data):
                raise ValueError(f"NaN is not supported by JSON at {path=}")
            if math.isinf(data):
                raise ValueError(f"Infinity is not supported by JSON at {path=}")
            return str(data)
        if math.isnan(data):
            out = "+-" if math.copysign(1.0, data) >= 0 else "-+"
        elif math.isinf(data):
            out = "++" if data >= 0 else "--"
        else:
            return str(data)
        if not self.json_level:
             return out
        assert self.escape_seq is not None
        return '"' + self.escape_seq + out + '"'

    def date_to_str(self, data: DateTypes, path: str, /) -> str:
        """Convert Python date, time, or datetime into string representation."""
        out = strfr_datetime(data)
        if out is None:
            raise ValueError(f"Unsupported datetime type {type(data)} at {path=}")
        if not self.json_level:
            return out
        if self.escape_seq is not None:
            return '"' + self.escape_seq + out + '"'
        raise ValueError(f"Unsupported data for json={self.json_level} at {path=}: {out}")

    def blob_to_str(self, data: BlobTypes, path: str) -> str:
        """Convert a blob into string representation, quoted if needed."""
        # TODO: support line splitting and indentation
        out = base64.urlsafe_b64encode(data).decode()
        if not out.endswith("="):
            out = ".." + out
        elif out.endswith("=="):
            out = ".." + out[:-2] + ".."
        else:
            out = ".." + out[:-1] + "."
        if not self.json_level:
            return out
        if self.escape_seq is not None:
            return '"' + self.escape_seq + out + '"'
        raise ValueError(f"Blobs are unsupported by json={self.json_level} at {path=}")

    def _maybe_quoted(self, s: str, path: str) -> str:
        if not self.json_level:
            return s
        escaped = self.se_encoder(s, '"')
        if self.escape_seq is not None:
            return '"' + self.escape_seq + escaped + '"'
        raise ValueError(f"Unsupported data {s} with json={self.json_level} at {path=}")

    def prime_data_to_str(self, data: FridValue, path: str, /) -> str|None:
        """Converts prime data to string representation.
        - Prime data types include int, float, bool, null, quote-free text, blob.
        - Return None if the data is not prime data.
        """
        if self.json_level:
            # Do not need to use quoted and escaped json string for these constants
            if data is None:
                return 'null'
            if isinstance(data, bool):
                return 'true' if data else 'false'
            if isinstance(data, str):
                return None
        else:
            if data is None:
                return '.'
            if isinstance(data, bool):
                return '+' if data else '-'
            if is_frid_identifier(data):
                return data
        if isinstance(data, int|float):
            if self.print_real is not None and (out := self.print_real(data)) is not None:
                return out  # integer or real is never quoted
            return self.real_to_str(data, path)
        if isinstance(data, DateTypes):
            if self.print_date is not None and (out := self.print_date(data)) is not None:
                return self._maybe_quoted(out, path)
            return self.date_to_str(data, path)
        if isinstance(data, BlobTypes):
            if self.print_blob is not None and (out := self.print_blob(data)) is not None:
                return self._maybe_quoted(".." + out, path)
            return self.blob_to_str(data, path)
        if isinstance(data, FridBasic):
            return data.frid_repr()
        if self.json_level or self.json_level == '':
            return None
        # If if a string has non-ascii with ascii_only configfation, quotes are needed
        if not isinstance(data, str) or (self.ascii_only and not data.isascii()):
            return None
        if is_frid_quote_free(data):
            return data
        return None

    def print_quoted_str(self, data: str, path: str,
                         /, as_key: bool=False, quote: str='\"', escape: bool=False):
        """Prints a quoted string to stream with quotes."""
        if self.escape_seq and (escape or data.startswith(self.escape_seq)):
            data = self.escape_seq + data
        self.print(quote + self.se_encoder(data, quote) + quote,
                   PPTokenType.LABEL if as_key else PPTokenType.ENTRY)

    def print_naked_list(self, data: Iterable[FridValue], path: str="",
                         /, sep: str=',', end_sep=True):
        """Prints a list/array to the stream without opening and closing delimiters."""
        non_empty = False  # Use this flag in case bool(data) data not work
        for i, x in enumerate(data):
            if i > 0:
                self.print(sep[0], PPTokenType.SEP_0)
            if x == '':
                self.print('""', PPTokenType.ENTRY)  # Force quoted string in list
            else:
                self.print_frid_value(x, path + '[' + str(i) + ']')
            non_empty = True
        if end_sep and non_empty and self.json_level in (0, 5):
            self.print(sep[0], PPTokenType.OPT_0)

    def _is_unquoted_key(self, key: str):
        """Checks if the key does not need to be quoted"""
        if self.ascii_only and not key.isascii():
            return False
        if not self.json_level:
            return is_frid_identifier(key)
        if self.json_level != 5:
            return False
        # JSON 5 identifiers, first not ECMAScript keywords but not in Python
        if key in JSON_QUOTED_KEYSET:
            return False
        key = key.replace('$', '_')  # Handle $ the same way as _
        # Use python identifiers as it is generally more restrictive than JSON5
        return key.isidentifier()

    def print_naked_dict(self, data: StrKeyMap, path: str="",
                         /, sep: str=',:', end_sep=True):
        """Prints a map to the stream without opening and closing delimiters."""
        i = 0
        for k, v in data.items():
            if v is MISSING:
                continue
            if i > 0:
                self.print(sep[0], PPTokenType.SEP_0)
            i += 1
            if not isinstance(k, str):
                raise ValueError(f"Key is not a string: {k}")
            # Empty key with non-present value we can omit the key (i.e., unquoted)
            if k != '' or v is PRESENT:
                if self._is_unquoted_key(k):
                    self.print(k, PPTokenType.LABEL)
                else:
                    self.print_quoted_str(k, path, as_key=True)
                if v is PRESENT:  # If the value is PRESENT, print only key without colon
                    continue
            self.print(sep[1], PPTokenType.SEP_1)
            assert not isinstance(v, FridBeing)
            self.print_frid_value(v, path)
        if end_sep and data and self.json_level in (0, 5):
            self.print(sep[0], PPTokenType.OPT_0)

    def print_named_args(self, name_args: FridNameArgs, path: str, /, sep: str=',:'):
        path = path + '(' + name_args.name + ')'
        if not self.json_level:
            # assert not name or is_frid_identifier(name) # Do not check name
            self.print(name_args.name, PPTokenType.ENTRY)
            self.print('(', PPTokenType.START)
            if name_args.args:
                self.print_naked_list(name_args.args, path, ',', end_sep=False)
            if name_args.args and name_args.kwds:
                self.print(',', PPTokenType.SEP_0)
            if name_args.kwds:
                self.print_naked_dict(name_args.kwds, path, ',=', end_sep=False)
            if name_args.args or name_args.kwds:
                self.print(sep[0], PPTokenType.OPT_0)
            self.print(')', PPTokenType.CLOSE)
            return
        if self.escape_seq is None:
            raise ValueError(f"FridMixin is not supported by json={self.json_level} at {path=}")
        if name_args.kwds:
            assert isinstance(name_args.kwds, Mapping), str(name_args.kwds)
            self.print('{', PPTokenType.START)
            self.print_quoted_str('', path, as_key=True)
            self.print(sep[1], PPTokenType.SEP_0)
        # Print as an array
        if name_args.args:
            assert isinstance(name_args.args, Sequence), str(name_args.args)
            self.print('[', PPTokenType.START)
            self.print_quoted_str(name_args.name, path, escape=True)
            self.print(sep[0], PPTokenType.SEP_0)
            self.print_naked_list(name_args.args)
            self.print(']', PPTokenType.CLOSE)
        else:
            self.print_quoted_str((name_args.name if name_args.kwds else name_args.name + "()"),
                                  path, escape=True)
        if name_args.kwds:
            self.print(sep[0], PPTokenType.SEP_0)
            self.print_naked_dict(name_args.kwds)
            self.print('}', PPTokenType.CLOSE)

    def print_frid_mixin(self, data: FridMixin, path: str, /):
        """Print any Frid mixin types."""
        entry = self.mixin_args.get(data.__class__)
        if entry is not None:
            name_args = data.frid_repr(*entry.args, **entry.kwds)
        else:
            name_args = data.frid_repr()
        self.print_named_args(name_args, path)

    def print_frid_value(self, data: FridValue, path: str='', /, top_delim: bool=True):
        """Print the any value that Frid supports to the stream."""
        s = self.prime_data_to_str(data, path)
        if s is not None:
            self.print(s, PPTokenType.ENTRY)
        elif isinstance(data, str):
            self.print_quoted_str(data, path)
        elif isinstance(data, Mapping):
            if top_delim:
                self.print('{', PPTokenType.START)
            self.print_naked_dict(data, path)
            if top_delim:
                self.print('}', PPTokenType.CLOSE)
        elif isinstance(data, Set):
            if self.json_level:
                raise ValueError(f"Set is not supported with json={self.json_level}")
            # We won't be able to load empty set back as set
            if data:
                if top_delim:
                    self.print('{', PPTokenType.START)
                self.print_naked_list(data, path)
                if top_delim:
                    self.print('}', PPTokenType.CLOSE)
            else:
                # An empty set is represented by "{,}"
                self.print('{,}', PPTokenType.ENTRY)
        elif isinstance(data, Iterable):
            if top_delim:
                self.print('[', PPTokenType.START)
            self.print_naked_list(data, path)
            if top_delim:
                self.print(']', PPTokenType.CLOSE)
        elif isinstance(data, FridMixin):
            self.print_frid_mixin(data, path)
        elif self.print_user is not None and (out := self.print_user(data, path)) is not None:
            return self._maybe_quoted(out, path)
        else:
            raise ValueError(f"Invalid type {type(data)} for json={self.json_level} at {path=}")

class FridStringDumper(PPToStringMixin, MultilineFormatMixin, FridDumper):
    pass

class FridTextIODumper(PPToTextIOMixin, MultilineFormatMixin, FridDumper):
    pass

def dump_frid_str(data: FridValue, /, *args, init_path: str='',
                  top_delim: bool=True, **kwargs) -> str:
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_frid_value(data, init_path, top_delim=top_delim)
    return str(dumper)

def dump_frid_tio(data: FridValue, /, file: TextIO, *args, init_path: str='',
                  top_delim: bool=True, **kwargs) -> TextIO:
    dumper = FridTextIODumper(file, *args, **kwargs)
    dumper.print_frid_value(data, init_path, top_delim=top_delim)
    return file

def dump_args_str(named_args: FridNameArgs, *args, **kwargs) -> str:
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_named_args(named_args, '')
    return str(dumper)

def dump_args_tio(named_args: FridNameArgs, /, file: TextIO, *args, **kwargs) -> TextIO:
    dumper = FridTextIODumper(file, *args, **kwargs)
    dumper.print_named_args(named_args, '')
    return file

@overload
def frid_redact(data: FridPrime, depth: int=16) -> FridPrime: ...
@overload
def frid_redact(data: FridArray, depth: int=16) -> FridArray: ...
@overload
def frid_redact(data: FridMixin, depth: int=16) -> str: ...
@overload
def frid_redact(data: StrKeyMap, depth: int=16) -> StrKeyMap: ...
@overload
def frid_redact(data: FridValue, depth: int=16) -> FridValue: ...
@overload
def frid_redact(data: FridBeing, depth: int=16) -> FridBeing: ...
def frid_redact(data, depth: int=16) -> FridValue|FridBeing:
    """Redacts the `data` of any type to a certain depth.
    - Keeps null and boolean as is.
    - Converts string to 's' + length.
    - Converts bytes to 'b' + length.
    - Converts integer to string 'i', float to string 'f', date/datetime to 'd', time to 't'.
    - Converts mixins to its type name string.
    - Recursively process the sequence and the mapping with decremented depth.
    - Converts non-empty sequence to a single element of integer length if the depth is zero.
    - Converts non-empty mapping to keys with no value if the depth reaches zero.
    - Returns the redacted value.
    This function is usually used before dump.
    """
    if data is None:
        return None
    if isinstance(data, bool):
        return data
    if isinstance(data, str):
        return 's' + str(len(data))
    if isinstance(data, BlobTypes):
        return 'b' + str(len(data))
    if isinstance(data, int):
        return 'i'
    if isinstance(data, float):
        return 'f'
    if isinstance(data, timeonly):
        return 't'
    if isinstance(data, datetime|dateonly):
        return 'd'
    if isinstance(data, FridBasic):
        return data.__class__.__name__
    if isinstance(data, FridMixin):
        return data.frid_keys()[0]
    if isinstance(data, FridBeing):
        return data
    if not data:
        return data   # As is for empty mapping or sequence
    if isinstance(data, Mapping):
        if depth <= 0:
            return {k: frid_redact(v, depth) if is_list_like(v) else PRESENT
                    for k, v in data.items() if v is not MISSING}
        # Do not decrement the depth if value is a sequence; keep elipsis as is
        return {k: frid_redact(v, depth if is_list_like(v) else depth - 1)
                for k, v in data.items() if v is not MISSING}
    if isinstance(data, Sequence):
        if depth <= 0:
            return [len(data)]
        return [frid_redact(x, depth-1) for x in data]
    return "??"

