import re, math, base64
from typing import  Any, Literal, NoReturn, TypeVar

from frid.dumper import EXTRA_ESCAPE_SOURCE, EXTRA_ESCAPE_TARGET

from .typing import FridArray, FridPrime, FridValue, StrKeyMap
from .guards import is_frid_identifier, is_identifier_char
from .errors import FridError
from .strops import str_unescape
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
    def __init__(self, s: str, *, json: bool=False):
        self.json = json
        self.buffer = s
        self.offset = 0
        self.bound = len(s)

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
        if self.json:
            match s:
                case 'true':
                    return True
                case 'false':
                    return False
                case 'null':
                    return None
        # TODO: user defined parser or parsers should be call here, the list of fixed literals
        if s.startswith('..'):
            # Base64 URL safe encoding with padding with dot. Space in between is allowed.
            s = s[2:]
            if not s.endswith('.'):
                return base64.urlsafe_b64decode(s)
            return base64.urlsafe_b64decode(s[:-2] + "==" if s.endswith('==') else s[:-1] + "=")
        if (t := parse_datetime(s)) is not None:
            return t
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

    def skip_whitespace(self, index: int, path: str) -> int:
        """Skips the all following whitespaces."""
        while True:
            try:
                while self.buffer[index].isspace():
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
                                              index, self.bound)
                break
            except IndexError:
                index = self.fetch(index, path)
        return (index + count, value)

    def scan_naked_list(self, index: int, path: str,
                        /, stop: str='', sep: str=',') -> tuple[int,FridArray]:
        out = []
        while True:
            (index, value) = self.scan_multi_data(index, path)
            index = self.skip_whitespace(index, path)
            (index, c) = self.scan_fixed_size(index, path, 1)
            if index >= self.bound or c in stop:
                break
            out.append(value)
            if c != sep:
                self.error(index, f"Unexpected '{c}' after {len(out)}th list entry at {path=}")
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
            if index >= self.bound:
                self.error(index, f"Unexpected ending after '{key}' of a map at {path=}")
            if self.buffer[index] != sep[1]:
                self.error(index, f"Expect '{sep[1]}' after key '{key}' of a map at {path=}")
            index += 1
            (index, value) = self.scan_multi_data(index, path + '/' + key)
            out[key] = value
            index = self.skip_whitespace(index, path)
            if index >= self.bound or self.buffer[index] in stop:
                break
            (index, c) = self.scan_fixed_size(index, path, 1)
            if c != sep[0]:
                self.error(index, f"Expect '{sep[0]}' after the value for '{key}' at {path=}")
        return (index, out)

    def scan_expression(self, index: int, path: str,
                        /, stop: str='', name: str|None=None) -> tuple[int,FridValue]:
        raise NotImplementedError

    def scan_frid_value(self, index: int, path: str, /, prev: Any=...) -> tuple[int,FridValue]:
        """Load the text representation."""
        index = self.skip_whitespace(index, path)
        if index >= self.bound:
            return (index, '')
        c = self.scan_fixed_size(index, path, 1)
        index += 1
        match c:
            case '[':
                if prev is not ...:
                    self.error(index, f"List after a value of {type(prev)} at {path=}")
                ending = ']'
                (index, value) = self.scan_naked_list(index, path, ending)
            case '{':
                if prev is not ...:
                    self.error(index, f"Map after a value of {type(prev)} at {path=}")
                (index, value) = self.scan_naked_dict(index, path, '}')
                index = self.skip_prefix_str(index, path, '}')
            case '"' | '\'' | '`':
                if prev is ... or not isinstance(prev, str):
                    self.error(index, f"Quoted string after a value of {type(prev)} at {path=}")
                (index, value) = self.scan_quoted_str(index, path, c)
                if isinstance(prev, str):
                    value = prev + value
                index = self.skip_prefix_str(index, path, c)
            case '(':
                if prev is ... or not is_frid_identifier(prev):
                    self.error(index, f"expression after a value of {type(prev)} at {path=}")
                (index, value) = self.scan_expression(index, path, ')',
                                                      None if prev is ... else prev)
                index = self.skip_prefix_str(index, path, ')')
            case _:
                if prev is ... or not isinstance(prev, str):
                    self.error(index, f"data after a value of {type(prev)} at {path=}")
                (count, value) = self.scan_prime_data(index, path)
                if isinstance(prev, str):
                    if not isinstance(value, str):
                        self.error(index, f"Wrong {type(value)} after a string at {path=}")
                    value = prev + value
        return (index, value)

    def scan_multi_data(self, index: int, path: str, /, stop: str=''):
        value = ...
        while True:
            (index, value) = self.scan_frid_value(index, path, value)
            index = self.skip_whitespace(index, path)
            (index, c) = self.scan_fixed_size(index, path, 1)
            if c in stop:
                break
        return (index, value)

    def load(self, start: int=0, path: str='',
             type: Literal['list','dict']|None=None) -> FridValue:
        match type:
            case None:
                (count, value) = self.scan_multi_data(start, path)
            case 'list':
                (count, value) = self.scan_naked_list(start, path)
            case 'dict':
                (count, value) = self.scan_naked_dict(start, path)
            case _:
                raise ValueError(f"Invalid input {type}")
        if count < 0:
            self.error(0, f"Failed to parse data at {path=}")
        if count < self.bound:
            index = self.skip_whitespace(count, path)
            if index < self.bound:
                self.error(index, f"Trailing data at the end at {path=}")
        return value
