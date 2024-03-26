import math, base64
from collections.abc import Callable, Iterator, Mapping
from typing import  Any, Literal, NoReturn, TypeVar

from .typing import BlobTypes, DateTypes, FridArray, FridMixin, FridPrime, FridValue, JsonLevel, StrKeyMap
from .guards import is_frid_identifier, is_frid_quote_free, is_quote_free_char
from .errors import FridError
from .strops import str_find_any, StringEscapeDecode
from .chrono import parse_datetime
from .dumper import EXTRA_ESCAPE_PAIRS

NO_QUOTE_CHARS = "~!?@#$%^&*/"
ALLOWED_QUOTES = "'`\""

T = TypeVar('T')

class ParseError(FridError):
    def __init__(self, s: str, index: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.notes.append(s[:index] + '\u274e' + s[index:])
        self.input_string = s
        self.error_offset = index

class FridLoader:
    """This class loads data in buffer into Frid-allowed data structures.

    Constructor arguments (all optional):
    - `buffer`: the optional buffer for the (initial parts) of the data stream.
    - `length`: the total length of the buffer; the default is the buffer length
      if buffer is given or a huge number of buffer is not given.
    - `json_level`, indicates the json compatibility level; possible values:
        + None (or False or 0): frid format
        + True: JSON format
        + 5: JSON5 format
        + (a string): JSON format, but all unsupported data is in a quoted
          Frid format after a prefix given by this string.
    - `frid_mixin`: a map of a list of key/value pairs to find to FridMixin
      constructors by name. The constructors are called with the positional
      and keyword arguments enclosed in parantheses after the function name.
    - `parse_real`, `parse_date`, `parse_blob`: parse int/float, date/time/datetime,
      and binary types respectively, accepting a single string as input and return
      value of parsed type, or None if data is not the type.
    - `parse_expr`: callback to parse data in parentheses; must return a FridMixin
      type of data. The function accepts an additional path parameter for path
      in the tree.
    - `parse_misc`: Callback to parse any unparsable data; must return a Frid
      compatible type. The function accepts an additional path parameter for path
      in the tree.
    """
    def __init__(
            self, buffer: str|None=None, length: int|None=None, offset: int=0, /,
            *, json_level: JsonLevel=None,
            frid_mixin: Mapping[str,type[FridMixin]]|Iterator[type[FridMixin]]|None=None,
            parse_real: Callable[[str],int|float|None]|None=None,
            parse_date: Callable[[str],DateTypes|None]|None=None,
            parse_blob: Callable[[str],BlobTypes|None]|None=None,
            parse_expr: Callable[[str,str],FridMixin]|None=None,
            parse_misc: Callable[[str,str],FridValue]|None=None,
    ):
        self.buffer = buffer or ""
        self.offset = offset
        self.length = length if length is not None else 1<<62 if buffer is None else len(buffer)
        self.anchor: int|None = None   # A place where the location is marked
        self.json_level = json_level
        self.using_frid = not (json_level or isinstance(json_level, str))
        self.parse_real = parse_real
        self.parse_date = parse_date
        self.parse_blob = parse_blob
        self.parse_expr = parse_expr
        self.parse_misc = parse_misc
        self.frid_mixin: dict[str,type[FridMixin]] = {}
        if isinstance(frid_mixin, Mapping):
            self.frid_mixin.update(frid_mixin)
        elif frid_mixin is not None:
            for mixin in frid_mixin:
                for key in mixin.frid_keys():
                    self.frid_mixin[key] = mixin
        self.se_decoder = StringEscapeDecode(
            EXTRA_ESCAPE_PAIRS + ''.join(x + x for x in ALLOWED_QUOTES),
            '\\', ('x', 'u', 'U')
        )

    def error(self, index: int, error: str) -> NoReturn:
        """Raise an ParseError at the current `index` with the given `error`."""
        raise ParseError(self.buffer, index, error)

    def fetch(self, index: int, path: str, /) -> int:
        """Fetchs more data into the buffer from the back stream.
        - `index`: the current parsing index in the current buffer.
        - `path`: the frid path for the current object to be parsed.
        - Returns the updated parsing index.
        The data before the initial parsing index may be remove to save memory,
        so the updated index may be smaller than the input.
        """
        tot_len = self.length + self.offset
        buf_end = self.offset + len(self.buffer)
        self.error(index, f"Stream ends at {index} when parsing {path=}; "
                   f"Total length: {tot_len}, Buffer {self.offset}-{buf_end}")

    def parse_prime_str(self, s: str, default: T, /) -> FridPrime|T:
        """Parses unquoted string or non-string prime types.
        - `s`: The input string, already stripped.
        - Returns the `default` if the string is not a simple unquoted value
          (including empty string)
        """
        if not s:
            return default
        if not self.using_frid:
            match s:
                case 'true':
                    return True
                case 'false':
                    return False
                case 'null':
                    return None
                case 'Infinity' | '+Infinity':
                    return +math.inf
                case '-Infinity':
                    return -math.inf
                case 'NaN':
                    return math.nan
        if s[0] not in "+-.0123456789":
            if is_frid_quote_free(s):
                return s
            return default
        if len(s) == 1:
            match s:
                case '.':
                    return None
                case '+':
                    return True
                case '-':
                    return False
                case _:
                    return int(s)  # Single digit so must be integer
        if len(s) == 2:
            match s:
                case "++":
                    return +math.inf
                case "--":
                    return -math.inf
                case "+.":
                    return +math.nan
                case "-.":
                    return -math.nan
        if s.startswith('..'):
            # Base64 URL safe encoding with padding with dot. Space in between is allowed.
            s = s[2:]
            if self.parse_blob is not None:
                return self.parse_blob(s)
            if not s.endswith('.'):
                return base64.urlsafe_b64decode(s)
            data = s[:-2] + "==" if s.endswith('..') else s[:-1] + "="
            return base64.urlsafe_b64decode(data)
        if self.parse_date:
            t = self.parse_date(s)
            if t is not None:
                return t
        else:
            t = parse_datetime(s)
            if t is not None:
                return t
        if self.parse_real:
            r = self.parse_real(s)
            if r is not None:
                return r
        else:
            try:
                return int(s, 0)  # for arbitrary bases
            except Exception:
                pass
            try:
                return float(s)
            except Exception:
                pass
        return default

    def skip_fixed_size(self, index: int, path: str, nchars: int) -> int:
        """Skips a number of characters without checking the content."""
        index += nchars
        if index > self.length:
            self.error(self.length, f"Trying to pass beyound the end of stream at {index}")
        return index

    def skip_whitespace(self, index: int, path: str) -> int:
        """Skips the all following whitespaces."""
        while True:
            try:
                while index < self.length and self.buffer[index].isspace():
                    index += 1
                break
            except IndexError:
                index = self.fetch(index, path)
        return index

    def skip_prefix_str(self, index: int, path: str, prefix: str) -> int:
        """Skips the `prefix` if it matches, or raise an ParseError."""
        while len(self.buffer) < index + len(prefix) <= self.length:
            index = self.fetch(index, path)
        if not self.buffer.startswith(prefix, index):
            raise ValueError(f"Stream endsin while expecting '{prefix}'")
        return index + len(prefix)

    def peek_fixed_size(self, index: int, path: str, nchars: int) -> str:
        """Peeks a string with a fixed size given by `nchars`.
        - Returns the string with these number of chars, or shorter if end of
          stream is reached.
        """
        while True:
            try:
                if index >= self.length:
                    return ''
                if index + nchars > self.length:
                    return self.buffer[index:self.length]
                return self.buffer[index:(index + nchars)]
            except IndexError:
                index = self.fetch(index, path)

    def scan_fixed_size(self, index: int, path: str, nchars: int) -> tuple[int,str]:
        """Scans a string with a fixed size given by `nchars`."""
        while True:
            try:
                return (index + nchars, self.buffer[index:(index + nchars)])
            except IndexError:
                index = self.fetch(index, path)

    def scan_prime_data(self, index: int, path: str, /, empty: Any='',
                        accept=NO_QUOTE_CHARS) -> tuple[int,FridValue]:
        """Scans the unquoted data that are identifier chars plus the est given by `accept`."""
        start = index
        while True:
            try:
                while index < self.length:
                    c = self.buffer[index]
                    if not is_quote_free_char(c) and c not in accept:
                        break
                    index += 1
                break
            except IndexError:
                index = self.fetch(start, path)
        data = self.buffer[start:index].strip()
        if not data:
            return (index, empty)
        value = self.parse_prime_str(data, ...)
        if value is ...:
            raise ParseError(self.buffer, start, f"Fail to parse unquoted value {data}")
        return (index, value)

    def scan_data_until(
            self, index: int, path: str, /, char_set: str,
            *, paired="{}[]()", quotes=ALLOWED_QUOTES, escape='\\'
    ) -> tuple[int,str]:
        while True:
            try:
                ending = str_find_any(self.buffer, char_set, index, self.length,
                                      paired=paired, quotes=quotes, escape=escape)
                return (ending, self.buffer[index:ending])
            except IndexError:
                index = self.fetch(index, path)

    def scan_escape_str(self, index: int, path: str, /, stop: str) -> tuple[int,str]:
        """Scans a text string with escape sequences."""
        while True:
            try:
                (count, value) = self.se_decoder(self.buffer, stop, index, self.length)
                break
            except IndexError:
                index = self.fetch(index, path)
        return (index + count, value)

    def scan_quoted_seq(self, index: int, path: str, /, quotes: str) -> tuple[int,FridPrime]:
        """Scan a continuationm of quoted string after the first quoted str."""
        out = []
        while True:
            index = self.skip_whitespace(index, path)
            c = self.peek_fixed_size(index, path, 1)
            if not c or c not in quotes:
                break
            index = self.skip_fixed_size(index, path, len(c))
            (index, value) = self.scan_escape_str(index, path, c)
            out.append(value)
            index = self.skip_prefix_str(index, path, c)
        data = ''.join(out)
        if isinstance(self.json_level, str) and data.startswith(self.json_level):
            data = data[len(self.json_level):]
            if (out := self.parse_prime_str(data, ...)) is not ...:
                return (index, out)
        return (index, data)

    def scan_naked_list(self, index: int, path: str,
                        /, stop: str='', sep: str=',') -> tuple[int,FridArray]:
        out = []
        while True:
            (index, value) = self.scan_frid_value(index, path, empty=...)
            index = self.skip_whitespace(index, path)
            if (c := self.peek_fixed_size(index, path, 1)) in stop:  # Empty is also a sub-seq
                break
            if c != sep[0]:
                self.error(index, f"Unexpected '{c}' after {len(out)}th list entry at {path=}")
            index = self.skip_fixed_size(index, path, 1)
            out.append(value if value is not ... else '')
        # The last entry that is not an empty string will be added to the data.
        if value is not ...:
            out.append(value)
        return (index, out)

    def scan_naked_dict(self, index: int, path: str,
                        /, stop: str='', sep: str=",:") -> tuple[int,StrKeyMap]:
        out = {}
        while True:
            (index, key) = self.scan_frid_value(index, path, empty=...)
            if key is ...:
                break
            if not isinstance(key, str):
                self.error(index, f"Invalid key type {type(key).__name__} of a map at {path=}")
            if key in out:
                self.error(index, f"Existing key '{key}' of a map at {path=}")
            index = self.skip_whitespace(index, path)
            if self.peek_fixed_size(index, path, 1) != sep[1]:
                self.error(index, f"Expect '{sep[1]}' after key '{key}' of a map at {path=}")
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_frid_value(index, path + '/' + key)
            out[key] = value
            index = self.skip_whitespace(index, path)
            if (c := self.peek_fixed_size(index, path, 1)) in stop:  # Empty is also a sub-seq
                break
            if c != sep[0]:
                self.error(index, f"Expect '{sep[0]}' after the value for '{key}' at {path=}")
            index = self.skip_fixed_size(index, path, len(c))
        return (index, out)

    def scan_naked_args(
            self, index: int, path: str, /, stop: str='', sep: str=",="
    ) -> tuple[int,list[FridValue],dict[str,FridValue]]:
        args = []
        kwas = {}
        while True:
            (index, name) = self.scan_frid_value(index, path)
            index = self.skip_whitespace(index, path)
            if index >= self.length:
                self.error(index, f"Unexpected ending after '{name}' of a map at {path=}")
            c = self.peek_fixed_size(index, path, 1)
            if c == sep[0]:
                index = self.skip_fixed_size(index, path, 1)
                args.append(name)
                continue
            if c != sep[1]:
                self.error(index, f"Expect '{sep[1]}' after key '{name}' of a map at {path=}")
            if not isinstance(name, str):
                self.error(index, f"Invalid name type {type(name).__name__} of a map at {path=}")
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_frid_value(index, path + '/' + name)
            if name in kwas:
                self.error(index, f"Existing key '{name}' of a map at {path=}")
            kwas[name] = value
            index = self.skip_whitespace(index, path)
            if (c := self.peek_fixed_size(index, path, 1)) in stop:
                break
            if c != sep[0]:
                self.error(index, f"Expect '{sep[0]}' after the value for '{name}' at {path=}")
            index = self.skip_fixed_size(index, path, 1)
        return (index, args, kwas)

    def construct_mixin(
            self, index: int, path: str, start: int,
            /, name: str,  args: list[FridValue], kwas: dict[str,FridValue]
    ) -> tuple[int,FridValue]:
        mixin = self.frid_mixin.get(name)
        if mixin is None:
            self.error(start, f"Cannot find constructor called {name}")
        return (index, mixin.frid_from(name, *args, **kwas))

    def scan_frid_value(self, index: int, path: str, /, empty: Any='') -> tuple[int,FridValue]:
        """Load the text representation."""
        index = self.skip_whitespace(index, path)
        if index >= self.length:
            return (index, empty)
        c = self.peek_fixed_size(index, path, 1)
        if c == '[':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_list(index, path, ']')
            return (self.skip_prefix_str(index, path, ']'), value)
        if c == '{':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_dict(index, path, '}')
            return (self.skip_prefix_str(index, path, '}'), value)
        if c in ALLOWED_QUOTES:
            return self.scan_quoted_seq(index, path, quotes=ALLOWED_QUOTES)
        if c == '(' and self.parse_expr is not None:
            (index, value) = self.scan_data_until(index, path, ')')
            index = self.skip_prefix_str(index, path, ')')
            return (index, self.parse_expr(value, path))
        # Now scan regular non quoted data
        self.anchor = index
        try:
            (index, value) = self.scan_prime_data(index, path, empty=empty)
            if index < self.length or not isinstance(value, str):
                return (index, value)
            index = self.skip_whitespace(index, path)
            c = self.peek_fixed_size(index, path, 1)
            if self.frid_mixin and c == '(' and is_frid_identifier(value):
                name = value
                (index, args, kwas) = self.scan_naked_args(index, path, ')')
                index = self.skip_prefix_str(index, path, ')')
                return self.construct_mixin(index, path, self.anchor, name, args, kwas)
            return (index, value)
        except ParseError:
            index = self.anchor
            if self.parse_misc:
                (index, value) = self.scan_data_until(index, path, ",)]}")
                return (index, self.parse_misc(value, path))
            raise
        finally:
            self.anchor = None

    def load(self, start: int=0, path: str='',
             type: Literal['list','dict']|None=None) -> FridValue:
        match type:
            case None:
                (index, value) = self.scan_frid_value(start, path)
            case 'list':
                (index, value) = self.scan_naked_list(start, path)
            case 'dict':
                (index, value) = self.scan_naked_dict(start, path)
            case _:
                raise ValueError(f"Invalid input {type}")
        if index < 0:
            self.error(0, f"Failed to parse data at {path=}")
        if index < self.length:
            index = self.skip_whitespace(index, path)
            if index < self.length:
                self.error(index, f"Trailing data at {index} at {path=}: {self.buffer[index:]}")
        return value


def load_from_str(s: str, *args, **kwargs) -> FridValue:
    return FridLoader(s, *args, **kwargs).load()
