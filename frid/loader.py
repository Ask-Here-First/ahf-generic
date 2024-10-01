import math, base64
from logging import warning
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from typing import  Any, Literal, NoReturn, TextIO, TypeVar, cast, overload

from .typing import (
    PRESENT, MISSING, BlobTypes, DateTypes, FridArray, FridBasic, FridBeing, FridMapVT,
    FridMixin, FridPrime, FridSeqVT, FridValue, FridNameArgs, StrKeyMap, ValueArgs
)
from .guards import (
    is_frid_identifier, is_frid_prime, is_frid_quote_free, is_frid_skmap,  is_quote_free_char
)
from .errors import FridError
from .strops import escape_control_chars, str_find_any, StringEscapeDecode
from .chrono import parse_datetime
from .dumper import EXTRA_ESCAPE_PAIRS

NO_QUOTE_CHARS = "~!?@$%^&"   # Extra no quote chars; not including/ * # for potential comments
ALLOWED_QUOTES = "'`\""

T = TypeVar('T')

class FridParseError(FridError):
    def __init__(self, s: str, index: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        note = s[max(index-32, 0):index] + '\u274e' + s[index:(index+32)]
        self.notes.append(note)
        self.input_string = s
        self.error_offset = index
    def __str__(self):
        s = super().__str__()
        if not self.notes:
            return s
        return s + " => " + " | ".join(self.notes)

class FridTruncError(FridParseError):
    pass

class DummyMixin(FridMixin):
    def __init__(self, name: str, args: list[FridSeqVT]|None=None,
                 kwds: dict[str,FridMapVT]|None=None):
        self.name = name
        self.args = args
        self.kwds = kwds
    def frid_repr(self) -> FridNameArgs:
        return FridNameArgs(self.name, self.args or (), self.kwds or {})

BasicTypeSpec = type[FridBasic]|ValueArgs[type[FridBasic]]
MixinTypeSpec = type[FridMixin]|ValueArgs[type[FridMixin]]

class FridLoader:
    """This class loads data in buffer into Frid-allowed data structures.

    Constructor arguments (all optional):
    - `buffer`: the optional buffer for the (initial parts) of the data stream.
    - `length`: the upper limit of total length of the buffer if all text are
      loaded; in other words, the length from the begining of the buffer to the
      end of stream. The default is the buffer length if buffer is given or a
      huge number of buffer is not given.
    - `offset`: the offset of the beginning of the buffer. Hence, `offset+length`
      is an upper bound of total length (and equals once the total length is known).
    - `json_level`: an integer indicating the json compatibility level; possible values:
        + 0: frid format (default)
        + 1: JSON format
        + 5: JSON5 format
    - `escape_seq`: the escape sequence for json formats (valid only if
      json_level is non-zero) used to identify data in quoted strings.
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
            *, comments: Sequence[tuple[str,str]]=(), newlines: str="\n\v\f",
            json_level: Literal[0,1,5]=0,
            escape_seq: str|None=None, loose_mode: bool=False,
            frid_basic: Iterator[BasicTypeSpec]|None=None,
            frid_mixin: Mapping[str,MixinTypeSpec]|Iterator[MixinTypeSpec]|None=None,
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
        if not all(item if isinstance(item, str) else (
            isinstance(item, tuple) and len(item) == 2 and all(isinstance(x, str) for x in item)
        ) for item in comments):
            raise ValueError(f"Invalid comments configuration: {comments}")
        self.comments: Sequence[tuple[str,str]|str] = comments
        self.newlines = newlines
        # The following are all constants
        self.json_level = json_level
        self.escape_seq = escape_seq
        self.loose_mode = loose_mode
        self.parse_real = parse_real
        self.parse_date = parse_date
        self.parse_blob = parse_blob
        self.parse_expr = parse_expr
        self.parse_misc = parse_misc
        self.frid_basic = list(frid_basic) if frid_basic else None
        self.frid_mixin: dict[str,MixinTypeSpec] = {}
        if isinstance(frid_mixin, Mapping):
            self.frid_mixin.update(frid_mixin)
        elif frid_mixin is not None:
            for entry in frid_mixin:
                mixin = entry.data if isinstance(entry, ValueArgs) else entry
                for key in mixin.frid_keys():
                    self.frid_mixin[key] = entry
        self.se_decoder = StringEscapeDecode(
            EXTRA_ESCAPE_PAIRS + ''.join(x + x for x in ALLOWED_QUOTES),
            '\\', ('x', 'u', 'U')
        )

    def alert(self, index: int, error: str):
        warning(error + ": " + self.buffer[max(index-32, 0):index]
                + '\u274e' + self.buffer[index:(index+32)])
    def error(self, index: int, error: str|BaseException) -> NoReturn:
        """Raise an FridParseError at the current `index` with the given `error`."""
        pos = self.offset + index
        end = "*" if self.length > len(self.buffer) + (1<<60) else self.offset + self.length
        msg = f"@{pos} ({index}/{len(self.buffer)}) in [{self.offset}:{end}] : {error}"
        if index >= self.length:
            if isinstance(error, BaseException):
                raise FridTruncError(self.buffer, index, msg) from error
            raise FridTruncError(self.buffer, index, msg)
        if isinstance(error, BaseException):
            raise FridParseError(self.buffer, index, msg) from error
        raise FridParseError(self.buffer, index, msg)

    def fetch(self, index: int, path: str, /) -> int:
        """Fetchs more data into the buffer from the back stream.
        - `index`: the current parsing index in the current buffer.
        - `path`: the frid path for the current object to be parsed.
        - Returns the updated parsing index.
        The data before the initial parsing index may be remove to save memory,
        so the updated index may be smaller than the input.
        Also self.anchor may also be changed if not None. Bytes after anchor
        or index, whichever is smaller, are preserved.
        By default this function raise an FridParseError.
        """
        tot_len = self.length + self.offset
        buf_end = self.offset + len(self.buffer)
        self.error(
            self.length, f"Stream ends at ${self.length} when parsing {path=} at {index}; "
            f"Total length: {tot_len}, Buffer {self.offset}-{buf_end}"
        )

    def parse_prime_str(self, s: str, default: T, /) -> FridPrime|T:
        """Parses unquoted string or non-string prime types.
        - `s`: The input string, already stripped.
        - Returns the `default` if the string is not a simple unquoted value
          (including empty string)
        """
        if not s:
            return default
        if self.json_level:
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
                case "+-":
                    return +math.nan
                case "-+":
                    return -math.nan
        if s[0] == '.' and len(s) >= 2:
            if s[1] not in "+-.0123456789":
                if is_frid_quote_free(s):
                    return s
                return default
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
        if self.frid_basic:
            for t in self.frid_basic:
                try:
                    if isinstance(t, ValueArgs):
                        result = t.data.frid_from(s, *t.args, **t.kwds)
                    else:
                        result = t.frid_from(s)
                except Exception:
                    continue
                if result is not None:
                    return result
        return default

    def peek_fixed_size(self, index: int, path: str, nchars: int) -> tuple[int,str]:
        """Peeks a string with a fixed size given by `nchars`.
        - Returns the string with these number of chars, or shorter if end of
          stream is reached.
        """
        while len(self.buffer) < min(index + nchars, self.length):
            index = self.fetch(index, path)
        while True:
            try:
                if index >= self.length:
                    return (index, '')
                if index + nchars > self.length:
                    return (index, self.buffer[index:self.length])
                return (index, self.buffer[index:(index + nchars)])
            except IndexError:
                index = self.fetch(index, path)

    def skip_fixed_size(self, index: int, path: str, nchars: int) -> int:
        """Skips a number of characters without checking the content."""
        index += nchars
        if index > self.length:
            self.error(self.length, f"Trying to pass beyound the EOS at {index}: {path=}")
        return index

    def skip_comments(self, index: int, path: str) -> tuple[int,str|None]:
        """Skip the comments in pairs."""
        content = []
        for item in self.comments:
            if isinstance(item, tuple):
                (opening, closing) = item
            else:
                assert isinstance(item, str)
                opening = item
                closing = None
            (index, token) = self.peek_fixed_size(index, path, len(opening))
            if token != opening:
                continue
            index = self.skip_fixed_size(index, path, len(opening))
            while True:
                if closing is None:
                    end_idx = str_find_any(self.buffer, self.newlines, index)
                else:
                    end_idx = self.buffer.find(closing, index)
                if end_idx >= 0:
                    assert end_idx >= index
                    content.append(self.buffer[index:end_idx])
                    if closing is not None:
                        end_idx += len(closing)
                    return (end_idx, ''.join(content))
                if len(self.buffer) >= self.length:
                    if closing is None:
                        # If the closing is a newline, it is optional at end
                        return (self.length,''.join(content))
                    self.error(index, ("Expecting '" + escape_control_chars(closing)
                                       + " after '" + escape_control_chars(opening) + "'"))
                index = self.fetch(index, path)
        return (index, None)

    def skip_characters(self, index: int, path: str, /, char_set: str) -> int:
        while True:
            try:
                while index < self.length and self.buffer[index] in char_set:
                    index += 1
                break
            except IndexError:
                index = self.fetch(index, path)
        return index

    def skip_whitespace(self, index: int, path: str, /) -> int:
        """Skips the all following whitespaces including comments."""
        while True:
            try:
                while index < self.length and self.buffer[index].isspace():
                    index += 1
                old_pos = self.offset + index
                (index, _) = self.skip_comments(index, path)
                if index >= self.length:
                    return index
                new_pos = self.offset + index
                if new_pos <= old_pos: # No progress
                    break
            except IndexError:
                index = self.fetch(index, path)
        return index

    def skip_prefix_str(self, index: int, path: str, prefix: str) -> int:
        """Skips the `prefix` if it matches, or raise an ParseError."""
        while len(self.buffer) < min(index + len(prefix), self.length):
            index = self.fetch(index, path)
        if not self.buffer.startswith(prefix, index):
            self.error(self.length, f"Stream ends while expecting '{prefix}' at {index}")
        return index + len(prefix)

    def scan_prime_data(self, index: int, path: str, /, empty: Any='',
                        accept=NO_QUOTE_CHARS) -> tuple[int,FridValue]:
        """Scans the unquoted data that are identifier chars plus the est given by `accept`."""
        # For loose mode, scan to the first , or : or any close delimiters.
        if self.loose_mode:
            start = index
            (index, data) = self.scan_data_until(index, path, ")]},:", True)
        else:
            while True:
                start = index
                try:
                    while index < self.length:
                        c = self.buffer[index]
                        if not is_quote_free_char(c) and c not in accept:
                            break
                        index += 1
                    break
                except IndexError:
                    index = self.fetch(start, path)
            data = self.buffer[start:index]
        data = data.strip()
        if not data:
            return (index, empty)
        value = self.parse_prime_str(data, ...)
        if value is ...:
            if self.loose_mode:
                return (index, data)
            self.error(start, f"Fail to parse unquoted value {data}")
        return (index, value)

    def scan_data_until(
            self, index: int, path: str, /, char_set: str, allow_missing: bool=False,
            *, paired="{}[]()", quotes=ALLOWED_QUOTES, escape='\\',
    ) -> tuple[int,str]:
        while True:
            try:
                ending = str_find_any(self.buffer, char_set, index, self.length,
                                      paired=paired, quotes=quotes, escape=escape)
                if ending < 0:
                    if len(self.buffer) < self.length:
                        index = self.fetch(index, path)
                        continue
                    if allow_missing:
                        return (len(self.buffer), self.buffer[index:])
                    self.error(index, f"Fail to find '{char_set}': {path=}")
                return (ending, self.buffer[index:ending])
            except IndexError:
                index = self.fetch(index, path)
            except ValueError as exc:
                self.error(index, exc)

    def scan_escape_str(self, index: int, path: str, /, stop: str) -> tuple[int,str]:
        """Scans a text string with escape sequences."""
        while True:
            try:
                (count, value) = self.se_decoder(self.buffer, stop, index, self.length)
                if count < 0:
                    index = self.fetch(index, path)
                    continue
                break
            except IndexError:
                index = self.fetch(index, path)
            except ValueError as exc:
                self.error(index, exc)
        return (index + count, value)

    def scan_quoted_seq(
            self, index: int, path: str, /, quotes: str, check_mixin: bool=False,
    ) -> tuple[int,FridPrime|FridBeing|FridMixin]:
        """Scan a sequence of quoted strings."""
        out = []
        while True:
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if not token or token not in quotes:
                break
            index = self.skip_fixed_size(index, path, len(token))
            (index, value) = self.scan_escape_str(index, path, token)
            out.append(value)
            index = self.skip_prefix_str(index, path, token)
        data = ''.join(out)
        if self.escape_seq and data.startswith(self.escape_seq):
            data = data[len(self.escape_seq):]
            if not data:
                return (index, PRESENT)
            if data.endswith("()"):
                name = data[:-2]
                if is_frid_identifier(name):
                    return (index, self.construct_mixin(index, path, name, (), {}))
            elif check_mixin and is_frid_identifier(data):
                return (index, DummyMixin(data))
            out = self.parse_prime_str(data, ...)
            if out is not ...:
                return (index, out)
        return (index, data)

    def construct_mixin(
            self, index: int, path: str,
            /, name: str, args: FridArray, kwds: StrKeyMap,
    ) -> FridMixin:
        entry = self.frid_mixin.get(name)
        if entry is None:
            self.error(index, f"Cannot find constructor called '{name}'")
        if not isinstance(entry, ValueArgs):
            return entry.frid_from(FridNameArgs(name, args, kwds))
        return entry.data.frid_from(FridNameArgs(name, args, kwds), *entry.args, **entry.kwds)
    def try_mixin_in_seq(
            self, data: list[FridSeqVT], index: int, path: str, *, parent_checking: bool=False
    ) -> FridMixin|list[FridSeqVT]:
        if not data:
            return data
        first = data[0]
        if not isinstance(first, DummyMixin):
            return data
        # If the first entry is already a dummy with arguments, construct it to the real one
        if first.args is not None:
            data[0] = self.construct_mixin(index, path, first.name, first.args, {})
            return data
        # If the first entry is just a mixin name, then construct a dummy include the rest
        if parent_checking:
            return DummyMixin(first.name, data[1:])
        # Otherwise construct a real mixin with the rest of the list as positional argument
        return self.construct_mixin(index, path, first.name, data[1:], {})
    def try_mixin_in_map(
            self, data: dict[str,FridMapVT], index: int, path: str
    ) -> FridMixin|dict[str,FridMapVT]:
        if not self.escape_seq:
            return data
        first = data.get('')
        if not isinstance(first, DummyMixin):
            return data
        data.pop('')
        return self.construct_mixin(index, path, first.name, first.args or (), data)

    def scan_naked_list(
            self, index: int, path: str,
            /, stop: str='', sep: str=',', check_mixin: bool=False,
    ) -> tuple[int,list[FridSeqVT]|FridMixin]:
        out: list[FridSeqVT] = []
        while True:
            (index, value) = self.scan_frid_value(
                index, path, empty=...,
                # Only check for mixin for the first item (`not out``) and with escape
                check_mixin=(not out and bool(self.escape_seq))
            )
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if token in stop:  # Empty is also a sub-seq
                break
            if token == sep[0]:
                index = self.skip_fixed_size(index, path, 1)
            elif self.loose_mode and not is_frid_prime(value):
                self.alert(index, "Loose mode: adding missing ','")
            else:
                self.error(index, f"Unexpected '{token}' after list entry #{len(out)}: {path=}")
            assert not isinstance(value, FridBeing)
            out.append(value if value is not ... else '')
        # The last entry that is not an empty string will be added to the data.
        if value is not ...:
            assert not isinstance(value, FridBeing)
            out.append(value)
        # Check if this is a mixin (only if caller does not ask for a mixin)
        return (index, self.try_mixin_in_seq(out, index, path, parent_checking=check_mixin))

    def scan_naked_dict(self, index: int, path: str,
                        /, stop: str='', sep: str=",:") -> tuple[int,StrKeyMap|Set|FridMixin]:
        out: dict[FridPrime,FridMapVT] = {}
        empty_entry = False
        while True:
            # Empty string is represented using MISSING
            (index, key) = self.scan_frid_value(index, path, empty=MISSING)
            if key is not MISSING and not is_frid_prime(key):
                self.error(index, f"Invalid key type {type(key).__name__} of a map: {path=}")
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if token == sep[0]:
                # Seeing item separator without key/value separator
                if key is MISSING:
                    if out or empty_entry:
                        self.error(index, "An empty key follows other entries")
                    empty_entry = True
                elif key in out:
                    self.error(index, f"Existing key '{key}' of a map: {path=}")
                # Using value PRESENT if key is non-empty
                index = self.skip_fixed_size(index, path, len(token))
                if key is not MISSING:
                    out[key] = PRESENT
                continue
            if token in stop:
                # If stops without key/value separator, add key=PRESENT only for non-empty key
                if key is not MISSING:
                    out[key] = PRESENT
                break
            # No key or key/value pairs can follow an empty entry
            if empty_entry:
                self.error(index, f"A key '{key}' follows an empty entry")
            if key is MISSING:
                key = ''
            if key in out:
                self.error(index, f"Existing key '{key}' of a map: {path=}")
            if token != sep[1]:
                self.error(index, f"Expect '{sep[1]}' after key '{key}' of a map: {path=}")
            # With value, key must be string
            if not isinstance(key, str):
                self.error(index, f"Invalid key type {type(key).__name__} of a map: {path=}")
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_frid_value(
                index, path + '/' + key, check_mixin=(not key and bool(self.escape_seq))
            )
            out[key] = value
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if token in stop:  # Empty is also a sub-seq
                break
            if token == sep[0]:
                index = self.skip_fixed_size(index, path, 1)
            elif self.loose_mode and not (is_frid_prime(value) or isinstance(value, FridBeing)):
                self.alert(index, "Loose mode: adding missing ','")
            else:
                self.error(index, f"Expect '{sep[0]}' after the value for '{key}': {path=}")
        # Convert into a set if non-empty and all values are PRESENT
        if not out and empty_entry:
            return (index, set())
        assert not empty_entry  # Cannot have empty entry following other entries
        if out and all(v is PRESENT for v in out.values()):
            return (index, set(out.keys()))
        if not is_frid_skmap(out):
            self.error(index, f"Not a set but keys are not all string: {path=}")
        # Now we check if this is a mixin
        if self.escape_seq:
            x = self.try_mixin_in_map(cast(dict[str,FridMapVT], out), index, path)
            if x is not out:
                return (index, x)
        return (index, out)

    def scan_naked_args(
            self, index: int, path: str, /, stop: str='', sep: str=",="
    ) -> tuple[int,list[FridValue],dict[str,FridValue]]:
        args = []
        kwds = {}
        while True:
            (index, name) = self.scan_frid_value(index, path)
            if not name:
                break
            index = self.skip_whitespace(index, path)
            if index >= self.length:
                self.error(index, f"Unexpected ending after '{name}' of a map: {path=}")
            (index, token) = self.peek_fixed_size(index, path, 1)
            if token == sep[0]:
                index = self.skip_fixed_size(index, path, 1)
                if kwds:
                    self.error(index, "Unnamed argument following keyword argument")
                args.append(name)
                continue
            if token != sep[1]:
                self.error(index, f"Expect '{sep[1]}' after key '{name}' of a map: {path=}")
            if not isinstance(name, str):
                self.error(index, f"Invalid name type {type(name).__name__} of a map: {path=}")
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_frid_value(index, path + '/' + name)
            if name in kwds:
                self.error(index, f"Existing key '{name}' of a map: {path=}")
            kwds[name] = value
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if token in stop:
                break
            if token == sep[0]:
                index = self.skip_fixed_size(index, path, 1)
            elif self.loose_mode and not is_frid_prime(value):
                self.alert(index, "Loose mode: adding missing ','")
            else:
                self.error(index, f"Expect '{sep[0]}' after the value for '{name}': {path=}")
        return (index, args, kwds)

    def scan_frid_value(
            self, index: int, path: str, /, empty: Any='', check_mixin: bool=False,
    ) -> tuple[int,FridValue|FridBeing]:
        """Load the text representation."""
        index = self.skip_whitespace(index, path)
        if index >= self.length:
            return (index, empty)
        (index, token) = self.peek_fixed_size(index, path, 1)
        if token == '[':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_list(index, path, ']', check_mixin=check_mixin)
            return (self.skip_prefix_str(index, path, ']'), value)
        if token == '{':
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_naked_dict(index, path, '}')
            return (self.skip_prefix_str(index, path, '}'), value)
        if token in ALLOWED_QUOTES:
            return self.scan_quoted_seq(
                index, path, quotes=ALLOWED_QUOTES, check_mixin=bool(check_mixin)
            )
        if token == '(' and self.parse_expr is not None:
            index = self.skip_fixed_size(index, path, 1)
            (index, value) = self.scan_data_until(index, path, ')')
            index = self.skip_prefix_str(index, path, ')')
            return (index, self.parse_expr(value, path))
        # Now scan regular non quoted data
        self.anchor = index
        try:
            (index, value) = self.scan_prime_data(index, path, empty=empty)
            if index >= self.length or not isinstance(value, str):
                return (index, value)
            offset = index - self.anchor
            index = self.skip_whitespace(index, path)
            (index, token) = self.peek_fixed_size(index, path, 1)
            if self.frid_mixin and token == '(' and is_frid_identifier(value):
                index = self.skip_fixed_size(index, path, 1)
                name = value
                (index, args, kwds) = self.scan_naked_args(index, path, ')')
                index = self.skip_prefix_str(index, path, ')')
                return (index, self.construct_mixin(self.anchor, path, name, args, kwds))
            return (self.anchor + offset, value)
        except FridParseError:
            index = self.anchor
            if self.parse_misc:
                (index, value) = self.scan_data_until(index, path, ",)]}")
                return (index, self.parse_misc(value, path))
            raise
        finally:
            self.anchor = None

    def scan(
            self, start: int=0, /, path: str='', stop: str='',
            *, top_dtype: Literal['list','dict','args']|None=None, until_eol: str|bool=False,
    ) -> tuple[int,FridValue|ValueArgs[str]]:
        match top_dtype:
            case None:
                (index, value) = self.scan_frid_value(start, path)
                if isinstance(value, FridBeing):
                    self.error(index, "PRESENT or MISSING is only supported for map values")
            case 'list':
                (index, value) = self.scan_naked_list(start, path, stop=stop)
            case 'dict':
                (index, value) = self.scan_naked_dict(start, path, stop=stop)
            case 'args':
                (index, args, kwds) = self.scan_naked_args(start, path, stop=stop)
                value = ValueArgs(path, *args, **kwds)
            case _:
                self.error(start, f"Invalid input {top_dtype}")
        # Skip to the end of the line (newlines in the comments are ignored)
        newlines = until_eol if isinstance(until_eol, str) else '\n\v\f'
        while True:
            index = self.skip_characters(index, path, char_set=' \r\t')
            (index, comments) = self.skip_comments(index, path)
            if comments is None:
                break
        # Check if the following character is a newline
        if index < self.length:
            (index, c) = self.peek_fixed_size(index, path, 1)
            if c in newlines:
                index = self.skip_fixed_size(index, path, 1)
            elif until_eol:
                self.error(index, "Trailing data at the end of line")
        return (index, value)
    def load(self, path: str='',
             top_dtype: Literal['list','dict','args']|None=None) -> FridValue|ValueArgs[str]:
        (index, value) = self.scan(0, path, top_dtype=top_dtype)
        if index < 0:
            self.error(0, f"Failed to parse data: {path=}")
        if index < self.length:
            index = self.skip_whitespace(index, path)
            if index < self.length:
                self.error(index, f"Trailing data at {index} ({path=})")
        if isinstance(value, FridBeing):
            self.error(index, "PRESENT or MISSING is only supported for map values")
        return value

@overload
def load_frid_str(s: str, *args, init_path: str='',
                  top_dtype: None=None, **kwargs) -> FridValue: ...
@overload
def load_frid_str(s: str, *args, init_path: str='',
                  top_dtype: Literal['list'], **kwargs) -> FridArray: ...
@overload
def load_frid_str(s: str, *args, init_path: str='',
                  top_dtype: Literal['dict'], **kwargs) -> StrKeyMap: ...
@overload
def load_frid_str(s: str, *args, init_path: str='',
                  top_dtype: Literal['args'], **kwargs) -> ValueArgs[str]: ...
def load_frid_str(
        s: str, *args, init_path: str='',
        top_dtype: Literal['list','dict','args']|None=None, **kwargs
) -> FridValue|ValueArgs[str]:
    return FridLoader(s, *args, **kwargs).load(init_path, top_dtype=top_dtype)

@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: None=None, **kwargs
) -> tuple[FridValue,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['list'], **kwargs
) -> tuple[FridArray,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['dict'], **kwargs
) -> tuple[StrKeyMap,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['args'], **kwargs
) -> tuple[ValueArgs[str],int]: ...
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['list','dict','args']|None=None, **kwargs
) -> tuple[FridValue|ValueArgs[str],int]:
    """Note: this function will raise TruncError if the string ends prematurely.
    For other parsing issues, a regular ParseError is returned.
    """
    (index, value) = FridLoader(s, *args, **kwargs).scan(
        start, init_path, end_chars, top_dtype=top_dtype, until_eol=until_eol
    )
    return (value, index)


class FridTextIOLoader(FridLoader):
    def __init__(self, t: TextIO, page: int = 16384, **kwargs):
        super().__init__("", 1<<62, 0, **kwargs)  # Do not pass any positional parameters; using default
        self.file: TextIO|None = t
        self.page: int = page
        self.offset: int = 0
    def __bool__(self):
        index = self.skip_whitespace(self.offset, '')
        return index >= self.length
    def __call__(self, *, init_path: str='', end_chars: str='', until_eol: str|bool=False,
                top_dtype: Literal['list','dict']|None=None, **kwargs):
        (index, value) = self.scan(self.offset, init_path, end_chars,
                                   top_dtype=top_dtype, until_eol=until_eol)
        self.offset = index
        return value
    def fetch(self, start: int, path: str) -> int:
        if self.file is None:
            return super().fetch(start, path)  # Just raise reaching end exception
        half_page = self.page >> 1
        index = start
        old_offset = self.offset
        new_start = index - half_page # Keep the past page
        if new_start > half_page:
            if self.anchor is not None and new_start > self.anchor:
                new_start = self.anchor
            if new_start > half_page:
                # Remove some of the past text
                self.buffer = self.buffer[new_start:]
                self.offset = old_offset + new_start
                index -= new_start
                if self.anchor is not None:
                    self.anchor -= new_start
        data = self.file.read(self.page)
        self.buffer += data
        if len(data) < self.page:
            self.length = len(self.buffer)
            self.file = None
        # print(f"Loaded {len(data)}B, anchor={self.anchor} offset={old_offset}->{self.offset} "
        #       f"index={start}->{index} buffer=[{len(self.buffer)}]: "
        #       f"{self.buffer[:index]}\u2728{self.buffer[index:]}")
        return index

@overload
def load_frid_tio(t: TextIO, *args, init_path: str='',
                  top_dtype: None=None, **kwargs) -> FridValue: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='',
                  top_dtype: Literal['list'], **kwargs) -> FridArray: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='',
                  top_dtype: Literal['dict']|None=None, **kwargs) -> StrKeyMap: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='',
                  top_dtype: Literal['args']|None=None, **kwargs) -> ValueArgs[str]: ...
def load_frid_tio(
        t: TextIO, *args, init_path: str='',
        top_dtype: Literal['list','dict','args']|None=None, **kwargs
) -> FridValue|ValueArgs[str]:
    """Loads the frid data from the text I/O stream `t`.
    - `*args` and `**kwargs` are passed to the constructor of `FridTextIOLoader`.
    - `init_path` is passed to `FridTextIOLoader.load()` as `path`.
    - `top_dtype` is passed to `FridTextIOLoader.load()`.
    """
    return FridTextIOLoader(t, *args, **kwargs).load(init_path, top_dtype=top_dtype)

@overload
def open_frid_tio(t: TextIO, *args, top_dtype: None=None,
                  **kwargs) -> Callable[...,FridValue]:...
@overload
def open_frid_tio(t: TextIO, *args, top_dtype: Literal['list'],
                  **kwargs) -> Callable[...,FridArray]:...
@overload
def open_frid_tio(t: TextIO, *args, top_dtype: Literal['dict'],
                  **kwargs) -> Callable[...,StrKeyMap]:...
@overload
def open_frid_tio(t: TextIO, *args, top_dtype: Literal['args'],
                  **kwargs) -> Callable[...,ValueArgs[str]]:...
def open_frid_tio(t: TextIO, *args, **kwargs) -> Callable[...,FridValue|ValueArgs[str]]:
    """Scans possibly multiple data from the text I/O stream `t`.
    - `*args` and `**kwargs` are passed to the constructor of `FridTextIOLoader`.
    - `init_path` is passed to `FridTextIOLoader.load()` as `path`.
    - `top_dtype` is passed to `FridTextIOLoader.load()`.
    Returns an instance of FridTextIOLoader as a functor.
    """
    return FridTextIOLoader(t, *args, **kwargs)
