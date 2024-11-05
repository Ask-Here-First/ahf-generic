import math
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import Any, Literal, TextIO, TypedDict
from enum import Enum, auto

from .typing import Unpack
from .typing import MISSING, PRESENT, FridBasic, FridBeing, BlobTypes, FridNameArgs, ValueArgs
from .typing import FridMixin, FridValue, StrKeyMap
from .chrono import DateTypes, strfr_datetime
from .guards import is_frid_identifier, is_frid_quote_free
from .lib import base64url_encode
from .lib.texts import StringEscapeEncode

JSON_QUOTED_KEYSET = (
    'true', 'false', 'null',
)
JSON1_ESCAPE_PAIRS = "\nn\tt\rr\ff\vv\bb"
JSON5_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\vv\x000"
EXTRA_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\aa\x1be"


class PPTokenType(Enum):
    START = auto()      # The block starting token, such as [{(
    CLOSE = auto()      # The block ending token, such as )}]
    LABEL = auto()      # The label (the key of dict)
    ENTRY = auto()      # The entry: prime values, including list items and dict values
    PIECE = auto()      # The partial data of an entry
    SEP_0 = auto()      # The primary separator (e.g., comma)
    SEP_1 = auto()      # The secondary separator (e.g., colon)
    OPT_0 = auto()      # The optional primary separator (e.g., comma at the end)
    OPT_1 = auto()      # The optional secondary secondary (e.g, colon with no value)

class PrettyPrint(ABC):
    """This abstract base class supports two kinds of mixins:
    - Data backend mixins. The data can be printed into a string or written
      to a stream, depending on the mixin.
    - Pretty format mixins. They constrol how whitespaces are inserted (e.g,
      spaces, new lines, and identitations). The default is not to insert any
      whitespaces.
    """
    @abstractmethod
    def _print(self, token: str, /):
        """This method is for the backend to override."""
        raise NotImplementedError

    def print(self, token: str, ttype: PPTokenType, /):
        """Default token print behavior:
        - Do not show optional separator.
        - Add a space after the required seqarator ',:'.
        """
        if ttype not in (PPTokenType.OPT_0, PPTokenType.OPT_1):
            self._print(token)
        if ttype in (PPTokenType.SEP_0, PPTokenType.SEP_1) and token in ':,':
            self._print(' ')

class PPToStringMixin(PrettyPrint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = []
    def _print(self, token: str, /):
        self.buffer.append(token)
    def __str__(self):
        return ''.join(self.buffer)

class PPToTextIOMixin(PrettyPrint):
    def __init__(self, stream: TextIO, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream = stream
    def _print(self, token: str, /):
        self.stream.write(token)

class MultilineFormatMixin(PrettyPrint):
    class PPParams(TypedDict, total=False):
        indent: int|str
        extra_comma: bool
        break_quoted: bool
        newline: str
        newline_after: str
    def __init__(self, *args, **kwargs: Unpack[PPParams]):
        self.indent = ' ' * indent if isinstance((
            indent := kwargs.pop('indent', None)
        ), int) else indent
        self.newline = kwargs.pop('newline', '\n')
        self.extra_comma = kwargs.pop('extra_comma', False)
        self.break_quoted = kwargs.pop('break_quoted', False)
        self.newline_after = kwargs.pop('newline_after', "{[")
        super().__init__(*args, **kwargs)  # type: ignore  -- No deconstructor/unpacker for map
        self._level = 0
        self._delta: list[bool] = []
        self._indented_back = False
        self._start_newline = False
    def print(self, token: str, ttype: PPTokenType, /):
        if self.indent is None:
            return super().print(token, ttype)
        if self._start_newline or self._indented_back:
            prefix = self.newline + self.indent * self._level
        else:
            prefix = ''
        self._indented_back = False
        self._start_newline = False
        match ttype:
            case PPTokenType.START:
                if token in self.newline_after:
                    self._level += 1
                    self._start_newline = True
                self._delta.append(self._start_newline)
            case PPTokenType.CLOSE:
                self._indented_back = self._delta.pop()
                # Need to recompute the prefix
                if self._indented_back:
                    self._level -= 1
                    prefix = self.newline + self.indent * self._level
            case PPTokenType.SEP_0:
                prefix = ''
                self._start_newline = self._delta and self._delta[-1]
                if not self._start_newline:
                    token += ' '
            case PPTokenType.SEP_1:
                if token == ':':
                    token += ' '
            case PPTokenType.OPT_0:
                prefix = ''
                self._start_newline = self._delta and self._delta[-1]
                if not self._start_newline or not self.extra_comma:
                    token = ''
            case PPTokenType.OPT_1:
                token = ''
            case PPTokenType.ENTRY:
                if self.break_quoted:
                    lines = self._split_quoted_lines(token)
                    if lines is not None:
                        if not prefix:
                            prefix = self.newline + self.indent * (self._level + 1)
                        token = ('"' + prefix + '"').join(lines)
        if prefix:
            self._print(prefix)
        if token:
            self._print(token)
        if self._level <= 0:
            self._print(self.newline)
    def _split_quoted_lines(self, token: str) -> list[str]|None:
        """Split quoted multi-line string into multiple strings."""
        if not token.startswith('"') and not token.endswith('"'):
            return None
        lines = []
        index = 0
        prev_index = 0
        while (index := token.find("\\n", index)) >= 0:
            index += 2
            # Check if the quoted string is at the end
            if index >= len(token) or token[index] == '"':
                continue
            # Count the number of preceding backslash
            n = 0
            while index > n + 2 and token[index-n-3] == '\\':
                n += 1
            if n & 1:
                continue
            lines.append(token[prev_index:index])
            prev_index = index
        if not lines:
            return None
        if prev_index < len(token):
            lines.append(token[prev_index:])
        return lines

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
    class Params(TypedDict, total=False):
        json_level: Literal[0,1,5]
        escape_seq: str
        ascii_only: bool
        mixin_args: Iterable[ValueArgs[type[FridMixin]]]
        print_real: Callable[[int|float],str|None]
        print_date: Callable[[DateTypes],str|None]
        print_blob: Callable[[BlobTypes],str|None]
        print_user: Callable[[Any,str],str|None]
    def __init__(self, *args, **kwargs: Unpack[Params]):
        self.json_level = kwargs.pop('json_level', 0)
        self.escape_seq = kwargs.pop('escape_seq', None)
        self.ascii_only = kwargs.pop('ascii_only', False)
        self.print_real = kwargs.pop('print_real', None)
        self.print_date = kwargs.pop('print_date', None)
        self.print_blob = kwargs.pop('print_blob', None)
        self.print_user = kwargs.pop('print_user', None)
        self.mixin_args: dict[type[FridMixin],ValueArgs[type[FridMixin]]] = {}
        if (mixin_args := kwargs.pop('mixin_args', None)):
            for item in mixin_args:
                self.mixin_args[item.data] = item
        super().__init__(*args, **kwargs)  # type: ignore -- wait until dict unpack in Python
        if not self.json_level:
            pairs = EXTRA_ESCAPE_PAIRS
            hex_prefix = ('x', 'u', 'U')
        elif self.json_level == 5:
            pairs = JSON5_ESCAPE_PAIRS
            hex_prefix = ('x', 'u', None)
        else:
            pairs = JSON1_ESCAPE_PAIRS
            hex_prefix = (None, 'u', None)
        if self.ascii_only:
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
        out = ".." + base64url_encode(data).decode()   # Do not do padding
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

class FridMultilineDumperParams(FridDumper.Params, MultilineFormatMixin.PPParams, total=False):
    pass

class FridStringDumper(PPToStringMixin, MultilineFormatMixin, FridDumper):
    pass

class FridTextIODumper(PPToTextIOMixin, MultilineFormatMixin, FridDumper):
    pass

def dump_frid_str(data: FridValue, /, *args, init_path: str='',
                  top_delim: bool=True, **kwargs: Unpack[FridMultilineDumperParams]) -> str:
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_frid_value(data, init_path, top_delim=top_delim)
    return str(dumper)

def dump_frid_tio(data: FridValue, /, file: TextIO, *args, init_path: str='',
                  top_delim: bool=True, **kwargs: Unpack[FridMultilineDumperParams]) -> TextIO:
    dumper = FridTextIODumper(file, *args, **kwargs)
    dumper.print_frid_value(data, init_path, top_delim=top_delim)
    return file

def dump_args_str(named_args: FridNameArgs, *args,
                  **kwargs: Unpack[FridMultilineDumperParams]) -> str:
    dumper = FridStringDumper(*args, **kwargs)
    dumper.print_named_args(named_args, '')
    return str(dumper)

def dump_args_tio(named_args: FridNameArgs, /, file: TextIO, *args,
                  **kwargs: Unpack[FridMultilineDumperParams]) -> TextIO:
    dumper = FridTextIODumper(file, *args, **kwargs)
    dumper.print_named_args(named_args, '')
    return file
