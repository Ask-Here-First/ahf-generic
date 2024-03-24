from collections.abc import Callable, Iterator, Mapping
import re, math, base64
from typing import  Any, Literal, NoReturn, TypeVar

from frid.dumper import EXTRA_ESCAPE_SOURCE, EXTRA_ESCAPE_TARGET

from .typing import DateTypes, FridArray, FridMixin, FridPrime, FridValue, StrKeyMap
from .guards import is_frid_identifier, is_identifier_char
from .errors import FridError
from .strops import str_find_any, str_unescape
from .chrono import parse_datetime

LOAD_UNQUOTED_CHARS = "!?@#$%^&*/"
LOAD_ALLOWED_QUOTES = "'`\""
LOAD_ESCAPED_SOURCE = EXTRA_ESCAPE_SOURCE + LOAD_ALLOWED_QUOTES
LOAD_ESCAPED_TARGET = EXTRA_ESCAPE_TARGET + LOAD_ALLOWED_QUOTES

quote_free_re = re.compile(r"[A-Za-z_](?:[\w\s@.+-]*\w)?")  # Quote free strings
plain_list_re = re.compile(r"\[[\w\s@,.+-]*\]")   # Plain list with quote free entries
whitespace_re = re.compile(r"\s")


T = TypeVar('T')

class ParseError(FridError):
    def __init__(self, s: str, index: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_string = s
        self.error_offset = index
    def frid_repr(self) -> dict[str,str|int|list[str]]:
        out = super().frid_repr()
        out['input_string'] = self.input_string
        out['error_offset'] = self.error_offset
        return out

class FridLoader:
    def __init__(
            self, buffer: str="", length: int|None=None, offset: int=0, /,
            *, json_const: bool=False,
            frid_mixin: Mapping[str,type[FridMixin]]|Iterator[type[FridMixin]],
            parse_real: Callable[[str],int|float|None],
            parse_date: Callable[[str],DateTypes|None],
            parse_expr: Callable[[str,str],FridMixin],
            parse_misc: Callable[[str,str],FridValue],
    ):
        self.buffer = buffer
        self.offset = offset
        self.length = len(buffer) if length is None else length
        self.anchor: int|None = None   # A place where the location is marked
        self.allow_json = json_const
        self.parse_real = parse_real
        self.parse_date = parse_date
        self.parse_expr = parse_expr
        self.parse_misc = parse_misc
        if isinstance(frid_mixin, Mapping):
            self.frid_mixin = dict(frid_mixin)
        else:
            self.frid_mixin: dict[str,type[FridMixin]] = {}
            for mixin in frid_mixin:
                for key in mixin.frid_keys():
                    self.frid_mixin[key] = mixin

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
        self.error(index, f"Stream ends when parsing {path=}")

    def parse_prime_str(self, s: str, default: T, /) -> FridPrime|T:
        """Parses unquoted string or non-string prime types.
        - `s`: The input string, already stripped.
        - Returns the `default` if the string is not a simple unquoted value.
        """
        if not s:
            return ""
        if s[0] not in "+-.0123456789":
            if quote_free_re.fullmatch(s):
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
        if self.allow_json:
            match s:
                case 'true':
                    return True
                case 'false':
                    return False
                case 'null':
                    return None
        if s.startswith('..'):
            # Base64 URL safe encoding with padding with dot. Space in between is allowed.
            s = s[2:]
            if not s.endswith('.'):
                return base64.urlsafe_b64decode(s)
            return base64.urlsafe_b64decode(s[:-2] + "==" if s.endswith('==') else s[:-1] + "=")
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
            if s.isnumeric() or (s[0] in "+-" and s[1:].strip().isnumeric()):
                try:
                    return int(s)
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
        while True:
            try:
                result = self.buffer.startswith(prefix, index)
                break
            except IndexError:
                index = self.fetch(index, path)
        if result:
            return index + len(prefix)
        self.error(index, f"Expecting '{prefix}' at {path=}")

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

    def scan_prime_data(self, index: int, path: str, /,
                        accept=LOAD_UNQUOTED_CHARS) -> tuple[int,FridValue]:
        """Scans the unquoted data that are identifier chars plus the est given by `accept`."""
        while True:
            try:
                c = self.buffer[index]
                while is_identifier_char(c) or c in accept:
                    index += 1
                    c = self.buffer[index]
                break
            except IndexError:
                index = self.fetch(index, path)
        value = self.parse_prime_str(self.buffer[index:index].strip(), ...)
        if value is ...:
            raise ParseError(self.buffer, index, "Fail to parse unquoted value")
        return (index, value)

    def scan_data_until(self, index: int, path: str, /, char_set: str,
                        *, paired="{}[]()", quotes="'`\"", escape='\\') -> tuple[int,str]:
        while True:
            try:
                ending = str_find_any(self.buffer, char_set, index, self.length,
                                      paired=paired, quotes=quotes, escape=escape)
                return (ending, self.buffer[index:ending])
            except IndexError:
                index = self.fetch(index, path)

    @staticmethod
    def _find_escape_seq(s: str, start: int, bound: int, prefix: str='\\') -> tuple[int,str]:
        """Finds the escape sequence, to be used by `find_transforms()`."""
        index = len(prefix)
        c = s[start + index]
        j = LOAD_ESCAPED_TARGET.find(c)
        if j >= 0:
            return (index + 1, LOAD_ESCAPED_SOURCE[j])
        if c == 'x':
            n = 2
        elif c == 'u':
            n = 4
        elif c == 'U':
            n = 8
        else:
            raise ParseError(s, start, f"Invalid escape sequence \\{c}")
        # The 2 or 4 or 8 hex chars to load
        index += 1
        n = 8 if c == 'U' else 4
        if start + index + n > bound:
            raise ParseError(s, start, f"Less than {n} letters for \\{c}")
        try:
            codepoint = int(s[(start + index):(start + index + 4)], 16)
            return (index + n, chr(codepoint))
        except ValueError as exc:
            raise ParseError(s, start, "Invalid unicode spec for \\{c}") from exc

    def scan_quoted_str(self, index: int, path: str, /, stop: str) -> tuple[int,str]:
        """Scans a text string with escape sequences."""
        while True:
            try:
                (count, value) = str_unescape(self.buffer, '\\', self._find_escape_seq,
                                              index, self.length, stop_at=stop)
                break
            except IndexError:
                index = self.fetch(index, path)
        return (index + count, value)

    def scan_quoted_seq(self, index: int, path: str, /, quotes: str) -> tuple[int,str]:
        """Scan a continuationm of quoted string after the first quoted str."""
        out = []
        while True:
            index = self.skip_whitespace(index, path)
            c = self.peek_fixed_size(index, path, 1)
            if c not in quotes:
                break
            (index, value) = self.scan_quoted_str(self.skip_fixed_size(index, path, 1), path, c)
            out.append(value)
            index = self.skip_prefix_str(index, path, c)
        return (index, ''.join(out))

    def scan_naked_list(self, index: int, path: str,
                        /, stop: str='', sep: str=',') -> tuple[int,FridArray]:
        out = []
        while True:
            (index, value) = self.scan_frid_value(index, path)
            out.append(value)
            index = self.skip_whitespace(index, path)
            if (c := self.peek_fixed_size(index, path, 1)) in stop:  # Empty is also a sub-seq
                break
            if c != sep[0]:
                self.error(index, f"Unexpected '{c}' after {len(out)}th list entry at {path=}")
            index = self.skip_fixed_size(index, path, 1)
        return (index, out)

    def scan_naked_dict(self, index: int, path: str,
                        /, stop: str='', sep: str=",:") -> tuple[int,StrKeyMap]:
        out = {}
        while True:
            (index, key) = self.scan_frid_value(index, path)
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

    def scan_frid_value(self, index: int, path: str, /, prev: Any=...) -> tuple[int,FridValue]:
        """Load the text representation."""
        index = self.skip_whitespace(index, path)
        if index >= self.length:
            return (index, '')
        c = self.peek_fixed_size(index, path, 1)
        if c == '[':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_list(index, path, ']')
            return (self.skip_prefix_str(index, path, ']'), value)
        if c == '{':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_dict(index, path, '}')
            return (self.skip_prefix_str(index, path, '}'), value)
        if c in "'`\"":
            return self.scan_quoted_seq(index, path, quotes="'`\"")
        if c == '(' and self.parse_expr is not None:
            (index, value) = self.scan_data_until(index, path, ')')
            index = self.skip_prefix_str(index, path, ')')
            return (index, self.parse_expr(value, path))
        # Now scan regular non quoted data
        self.anchor = index
        try:
            (index, value) = self.scan_prime_data(index, path, c)
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
                (count, value) = self.scan_frid_value(start, path)
            case 'list':
                (count, value) = self.scan_naked_list(start, path)
            case 'dict':
                (count, value) = self.scan_naked_dict(start, path)
            case _:
                raise ValueError(f"Invalid input {type}")
        if count < 0:
            self.error(0, f"Failed to parse data at {path=}")
        if count < self.length:
            index = self.skip_whitespace(count, path)
            if index < self.length:
                self.error(index, f"Trailing data at the end at {path=}")
        return value
