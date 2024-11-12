import math, itertools
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import  Any, Literal, NoReturn, TextIO, TypeVar, TypedDict, cast, overload


from .typing import Unpack
from .typing import (
    PRESENT, MISSING, BlobTypes, DateTypes,
    FridArray, FridBasic, FridBeing, FridMapVT,
    FridMixin, FridPrime, FridSeqVT, FridValue, FridNameArgs, StrKeyMap, ValueArgs,
)
from .guards import (
    is_frid_identifier, is_frid_prime, is_frid_quote_free, is_frid_skmap,  is_quote_free_char
)
from .typing import FridError
from .lib import str_encode_nonprints, str_find_any, base64url_decode, warn
from .lib.texts import StringEscapeDecode
from .chrono import parse_datetime
from ._dumps import EXTRA_ESCAPE_PAIRS

NO_QUOTE_CHARS = "~!?@$%^&"   # Extra no quote chars; not including/ * # for potential comments
ALLOWED_QUOTES = "'`\""

T = TypeVar('T')

class FridParseError(FridError):
    def __init__(self, s: str, index: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if index < 0:
            index = 0
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

# Unforntately Unpack does not support dataclasses so we have to repeat

class FridLoaderConfig:
    class Params(TypedDict, total=False):
        comments: Sequence[str|tuple[str,str]]
        lineends: str
        json_level: Literal[0,1,5]
        escape_seq: str
        loose_mode: bool
        frid_basic: Iterable[BasicTypeSpec]
        frid_mixin: Mapping[str,MixinTypeSpec]|Iterable[MixinTypeSpec]
        parse_real: Callable[[str],int|float|None]
        parse_date: Callable[[str],DateTypes|None]
        parse_blob: Callable[[str],BlobTypes|None]
        parse_expr: Callable[[str,str],FridValue]
        parse_misc: Callable[[str,str],FridValue]

    def __init__(self, config: Params):
        self.comments = config.pop('comments', ())
        if not all(item if isinstance(item, str) else (
            isinstance(item, tuple) and len(item) == 2
            and all(isinstance(x, str) for x in item)
        ) for item in self.comments):
            raise ValueError(f"Invalid comments configuration: {self.comments}")

        self.lineends = config.pop('lineends', "\n\v\f")
        self.json_level = config.pop('json_level', 0)
        self.escape_seq = config.pop('escape_seq', None)
        self.loose_mode = config.pop('loose_mode', False)
        self.frid_basic = config.pop('frid_basic', ())

        frid_mixin = config.pop('frid_mixin', None)
        self.frid_mixin: Mapping[str,MixinTypeSpec] = {}
        if isinstance(frid_mixin, Mapping):
            self.frid_mixin.update(cast(Mapping[str,MixinTypeSpec], frid_mixin))
        elif frid_mixin is not None:
            for entry in frid_mixin:
                data = entry.data if isinstance(entry, ValueArgs) else entry
                for key in data.frid_keys():
                    self.frid_mixin[key] = entry

        self.parse_real = config.pop('parse_real', None)
        self.parse_date = config.pop('parse_date', None)
        self.parse_blob = config.pop('parse_blob', None)
        self.parse_expr = config.pop('parse_expr', None)
        self.parse_misc = config.pop('parse_misc', None)

class FridLoader:
    """This class loads data in buffer into Frid-allowed data structures.

    Constructor arguments (all optional):
    - `buffer`: the optional buffer for the (initial parts) of the data stream.
    - `length`: an upper bound of total length of the data including all data
      loaded and not yet loaded, which must be greater than or equal to `len(buffer)`,
      equal if all text is loaded; in other words, this is the upper bound of length
      from the begining (outset) of the buffer to the end of stream.
      The default is the buffer length if buffer is given or a big number,
      if the length is unknown.
    - `outset`: the position of beginning of the buffer; hence, `outset+length`
      is an upper bound of total length (and equals once the total length is known).
    - `offset`: the inital offset in the buffer.

    Keywoard parameters:
    - `comments`: a list of comment specifications; each of them can be
        + A string: the openning string of comments to the line ends (see `lineends`)
        + A pair of strings: first is the opening string and the second is the closing
          string of comments.
    - `lineends`: the characters that are considered as line ends.
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
    - `parse_expr`: callback to parse data in parentheses; must return a FridValue.
      The function accepts an additional path parameter for path in the tree.
      Note that if the user wants to preserve the original expression, the return
      value can be a FridBasic.
    - `parse_misc`: Callback to parse any unparsable data; must return a Frid
      compatible type. The function accepts an additional path parameter for path
      in the tree.
    """

    Params = FridLoaderConfig.Params
    def __init__(
            self, /, buffer: str|None=None, length: int|None=None, outset: int=0, offset: int=0,
            *args, init_path: str|None=None, **kwargs: Unpack[Params],
    ):
        # self.length is an upper bound of length of buffer, not include outset.
        self.buffer = buffer or ""
        self.length = length if length is not None else 1<<62 if buffer is None else len(buffer)
        self.outset = outset   # Intial position of the buffer since start of stream.
        self.offset = offset
        self.anchor: list[int] = []   # A place where the locations between 0 & offset are saved
        self.pstack: list[str] = [init_path] if init_path else []
        self.config = FridLoaderConfig(kwargs)
        self.decode = StringEscapeDecode(
            EXTRA_ESCAPE_PAIRS + ''.join(x + x for x in ALLOWED_QUOTES),
            '\\', ('x', 'u', 'U')
        )
        super().__init__(*args, **kwargs)  # type: ignore

    def alert(self, error: str, *, offset: int|None=None):
        if offset is None:
            offset = self.offset
        warn(error + " (path=" + ''.join(self.pstack) + "): "
             + self.buffer[max(offset-32, 0):offset]
             + '\u274e' + self.buffer[offset:(offset+32)])
    def error(self, error: str|BaseException, *, offset: int|None=None) -> NoReturn:
        """Raise an FridParseError at the current `index` with the given `error`."""
        if offset is None:
            offset = self.offset
        pos = self.outset + offset
        end = "*" if self.length > len(self.buffer) + (1<<60) else self.outset + self.length
        msg = (f"@{pos} ({self.offset}/{len(self.buffer)}) in [{self.outset}:{end}] (path="
               + ''.join(self.pstack) + "): " + str(error))
        if offset >= self.length:
            if isinstance(error, BaseException):
                raise FridTruncError(self.buffer, offset, msg) from error
            raise FridTruncError(self.buffer, offset, msg)
        if isinstance(error, BaseException):
            raise FridParseError(self.buffer, offset, msg) from error
        raise FridParseError(self.buffer, offset, msg)

    def fetch(self) -> None:
        """Fetchs more data into the buffer from the back stream.
        The data in buffer before offset may be removed to save memory,
        so the updated offset may be decremented.
        Also self.anchor may also be changed if not None. Bytes after any
        anchor or offset, whichever is smaller, are preserved.
        By default this function raise an FridParseError, because
        the base class loads from the fixed string buffer.
        """
        tot_len = self.length + self.outset
        buf_end = self.outset + len(self.buffer)
        self.error(
            f"Stream ends at ${buf_end} when parsing at {self.offset}; "
            f"length: {self.length},{tot_len}, buffer {self.outset}-{buf_end}; path: "
            + ''.join(self.pstack),
            offset=self.length
        )

    def parse_prime_str(self, s: str, default: T, /) -> FridPrime|T:
        """Parses unquoted string or non-string prime types.
        - `s`: The input string, already stripped.
        - Returns the `default` if the string is not a simple unquoted value
          (including empty string)
        """
        if not s:
            return default
        if self.config.json_level:
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
            if self.config.parse_blob is not None:
                return self.config.parse_blob(s)
            return base64url_decode(s.rstrip('.'))
            # if not s.endswith('.'):
            #     return base64.urlsafe_b64decode(s)
            # data = s[:-2] + "==" if s.endswith('..') else s[:-1] + "="
            # return base64.urlsafe_b64decode(data)
        if self.config.parse_date:
            t = self.config.parse_date(s)
            if t is not None:
                return t
        else:
            t = parse_datetime(s)
            if t is not None:
                return t
        if self.config.parse_real:
            r = self.config.parse_real(s)
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
        if self.config.frid_basic:
            for t in self.config.frid_basic:
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

    def peek_fixed_size(self, nchars: int) -> str:
        """Peeks a string with a fixed size given by `nchars`.
        - Returns the string with these number of chars, or shorter if end of
          stream is reached.
        """
        while len(self.buffer) < min(self.offset + nchars, self.length):
            self.fetch()
        while True:
            try:
                if self.offset >= self.length:
                    return ''
                return self.buffer[self.offset:min(self.offset + nchars, self.length)]
            except IndexError:
                self.fetch()

    def skip_fixed_size(self, nchars: int) -> None:
        """Skips a number of characters without checking the content."""
        self.offset += nchars
        if self.offset > self.length:
            self.error(f"Trying to pass beyound the EOS at {self.length}",
                       offset=self.length)

    def skip_comments(self) -> str|None:
        """Skip the comments in pairs."""
        content = []
        for item in self.config.comments:
            if isinstance(item, tuple):
                (opening, closing) = item
            else:
                assert isinstance(item, str)
                opening = item
                closing = None
            token = self.peek_fixed_size(len(opening))
            if token != opening:
                continue
            self.skip_fixed_size(len(opening))
            while True:
                if closing is None:
                    end_idx = str_find_any(self.buffer, self.config.lineends, self.offset)
                else:
                    end_idx = self.buffer.find(closing, self.offset)
                if end_idx >= 0:
                    assert end_idx >= self.offset
                    content.append(self.buffer[self.offset:end_idx])
                    if closing is not None:
                        end_idx += len(closing)
                    self.offset = end_idx
                    return ''.join(content)
                if len(self.buffer) >= self.length:
                    if closing is None:
                        # If the closing is a newline, it is optional at end
                        self.offset = self.length
                        return ''.join(content)
                    self.error(("Expecting '" + str_encode_nonprints(closing)
                                + " after '" + str_encode_nonprints(opening) + "'"))
                self.fetch()
        return None

    def skip_characters(self, char_set: str) -> None:
        while True:
            try:
                while self.offset < self.length and self.buffer[self.offset] in char_set:
                    self.offset += 1
                break
            except IndexError:
                self.fetch()

    def skip_whitespace(self) -> None:
        """Skips the all following whitespaces including comments."""
        while True:
            try:
                while self.offset < self.length and self.buffer[self.offset].isspace():
                    self.offset += 1
                old_pos = self.outset + self.offset
                self.skip_comments()
                if self.offset >= self.length:
                    return
                new_pos = self.outset + self.offset
                if new_pos <= old_pos: # No progress
                    break
            except IndexError:
                self.fetch()

    def skip_prefix_str(self, prefix: str) -> None:
        """Skips the `prefix` if it matches, or raise an ParseError."""
        while len(self.buffer) < min(self.offset + len(prefix), self.length):
            self.fetch()
        if not self.buffer.startswith(prefix, self.offset):
            self.error(f"Stream ends while expecting '{prefix}' at {self.offset}",
                       offset=self.length)
        self.offset += len(prefix)

    def scan_prime_data(self, empty: Any='', accept=NO_QUOTE_CHARS) -> FridValue:
        """Scans the unquoted data that are identifier chars plus the est given by `accept`."""
        # For loose mode, scan to the first , or : or any close delimiters.
        if self.config.loose_mode:
            data = self.scan_data_until(")]},:", True)
            start = self.offset - len(data)
        else:
            while True:
                offset = self.offset
                try:
                    while offset < self.length:
                        c = self.buffer[offset]
                        if not is_quote_free_char(c) and c not in accept:
                            break
                        offset += 1
                    break
                except IndexError:
                    self.fetch()
            data = self.buffer[self.offset:offset]
            start = self.offset
            self.offset = offset
        data = data.strip()
        if not data:
            return empty
        value = self.parse_prime_str(data, ...)
        if value is ...:
            if self.config.loose_mode:
                return data
            self.error(f"Fail to parse unquoted value {data}", offset=start)
        return value

    def scan_data_until(
            self, char_set: str, allow_missing: bool=False,
            *, paired="{}[]()", quotes=ALLOWED_QUOTES, escape='\\',
    ) -> str:
        while True:
            try:
                ending = str_find_any(self.buffer, char_set, self.offset, self.length,
                                      paired=paired, quotes=quotes, escape=escape)
                if ending < 0:
                    if len(self.buffer) < self.length:
                        self.fetch()
                        continue
                    if allow_missing:
                        value = self.buffer[self.offset:]
                        self.offset = len(self.buffer)
                        return value
                    self.error(f"Fail to find '{char_set}'")
                value = self.buffer[self.offset:ending]
                self.offset = ending
                return value
            except IndexError:
                self.fetch()
            except ValueError as exc:
                self.error(exc)

    def scan_escape_str(self, stop: str) -> str:
        """Scans a text string with escape sequences."""
        while True:
            try:
                (count, value) = self.decode(self.buffer, stop, self.offset, self.length)
                if count < 0:
                    self.fetch()
                    continue
                break
            except IndexError:
                self.fetch()
            except ValueError as exc:
                self.error(exc)
        self.offset += count
        return value

    def scan_quoted_seq(
            self, quotes: str, check_mixin: bool=False,
    ) -> FridPrime|FridBeing|FridMixin:
        """Scan a sequence of quoted strings."""
        out = []
        while True:
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if not token or token not in quotes:
                break
            self.skip_fixed_size(len(token))
            value = self.scan_escape_str(token)
            out.append(value)
            self.skip_prefix_str(token)
        data = ''.join(out)
        if self.config.escape_seq and data.startswith(self.config.escape_seq):
            data = data[len(self.config.escape_seq):]
            if not data:
                return PRESENT
            if data.endswith("()"):
                name = data[:-2]
                if is_frid_identifier(name):
                    return self.construct_mixin(name, (), {})
            elif check_mixin and is_frid_identifier(data):
                return DummyMixin(data)
            out = self.parse_prime_str(data, ...)
            if out is not ...:
                return out
        return data

    def construct_mixin(
            self, name: str, args: FridArray, kwds: StrKeyMap, *, offset: int|None=None
    ) -> FridMixin:
        entry = self.config.frid_mixin.get(name)
        if entry is None:
            keys = ", ".join(self.config.frid_mixin.keys())
            self.error(f"Cannot find constructor '{name}' in {{{keys}}}",
                       offset=(self.offset if offset is None else offset))
        if not isinstance(entry, ValueArgs):
            return entry.frid_from(FridNameArgs(name, args, kwds))
        return entry.data.frid_from(FridNameArgs(name, args, kwds), *entry.args, **entry.kwds)
    def try_mixin_in_seq(
            self, data: list[FridSeqVT], *, parent_checking: bool=False
    ) -> FridMixin|list[FridSeqVT]:
        if not data:
            return data
        first = data[0]
        if not isinstance(first, DummyMixin):
            return data
        # If the first entry is already a dummy with arguments, construct it to the real one
        if first.args is not None:
            data[0] = self.construct_mixin(first.name, first.args, {})
            return data
        # If the first entry is just a mixin name, then construct a dummy include the rest
        if parent_checking:
            return DummyMixin(first.name, data[1:])
        # Otherwise construct a real mixin with the rest of the list as positional argument
        return self.construct_mixin(first.name, data[1:], {})
    def try_mixin_in_map(self, data: dict[str,FridMapVT]) -> FridMixin|dict[str,FridMapVT]:
        if not self.config.escape_seq:
            return data
        first = data.get('')
        if not isinstance(first, DummyMixin):
            return data
        data.pop('')
        return self.construct_mixin(first.name, first.args or (), data)

    def scan_naked_list(
            self, stop: str='', sep: str=',', check_mixin: bool=False,
    ) -> list[FridSeqVT]|FridMixin:
        out: list[FridSeqVT] = []
        value = ...
        for i in itertools.count(0):
            self.pstack.append(str(i) + ']')
            try:
                value = self.scan_frid_value(
                    empty=...,
                    # Only check for mixin for the first item (`not out``) and with escape
                    check_mixin=(not out and bool(self.config.escape_seq))
                )
            finally:
                self.pstack.pop()
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if token in stop:  # Empty is also a sub-seq
                break
            if token == sep[0]:
                self.skip_fixed_size(1)
            elif self.config.loose_mode and not is_frid_prime(value):
                self.alert("Loose mode: adding missing ','")
            else:
                self.error(f"Unexpected '{token}' after list entry #{len(out)}")
            assert not isinstance(value, FridBeing)
            out.append(value if value is not ... else '')
        # The last entry that is not an empty string will be added to the data.
        if value is not ...:
            assert not isinstance(value, FridBeing)
            out.append(value)
        # Check if this is a mixin (only if caller does not ask for a mixin)
        return self.try_mixin_in_seq(out, parent_checking=check_mixin)

    def scan_naked_dict(self, stop: str='', sep: str=",:") -> StrKeyMap|Set|FridMixin:
        out: dict[FridPrime,FridMapVT] = {}
        empty_entry = False
        while True:
            # Empty string is represented using MISSING
            key = self.scan_frid_value(empty=MISSING)
            if key is not MISSING and not is_frid_prime(key):
                self.error(f"Invalid key type {type(key).__name__} of a map")
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if token == sep[0]:
                # Seeing item separator without key/value separator
                if key is MISSING:
                    if out or empty_entry:
                        self.error("An empty key follows other entries")
                    empty_entry = True
                elif key in out:
                    self.error(f"Existing key '{key}' of a map")
                # Using value PRESENT if key is non-empty
                self.skip_fixed_size(len(token))
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
                self.error(f"A key '{key}' follows an empty entry")
            if key is MISSING:
                key = ''
            if key in out:
                self.error(f"Existing key '{key}' of a map")
            if token != sep[1]:
                self.error(f"Expect '{sep[1]}' after key '{key}' of a map")
            # With value, key must be string
            if not isinstance(key, str):
                self.error(f"Invalid key type {type(key).__name__} of a map")
            self.skip_fixed_size(1)
            self.pstack.append(key)
            try:
                value = self.scan_frid_value(
                    check_mixin=(not key and bool(self.config.escape_seq))
                )
            finally:
                self.pstack.pop()
            out[key] = value
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if token in stop:  # Empty is also a sub-seq
                break
            if token == sep[0]:
                self.skip_fixed_size(1)
            elif self.config.loose_mode and not (
                is_frid_prime(value) or isinstance(value, FridBeing)
            ):
                self.alert("Loose mode: adding missing ','")
            else:
                self.error(f"Expect '{sep[0]}' after the value for '{key}'")
        # Convert into a set if non-empty and all values are PRESENT
        if not out and empty_entry:
            return set()
        assert not empty_entry  # Cannot have empty entry following other entries
        if out and all(v is PRESENT for v in out.values()):
            return set(out.keys())
        if not is_frid_skmap(out):
            self.error("Not a set but keys are not all string")
        # Now we check if this is a mixin
        if self.config.escape_seq:
            x = self.try_mixin_in_map(cast(dict[str,FridMapVT], out))
            if x is not out:
                return x
        return out

    def scan_naked_args(
            self, stop: str='', sep: str=",="
    ) -> tuple[list[FridValue],dict[str,FridValue]]:
        args = []
        kwds = {}
        for i in itertools.count(0):
            self.pstack.append('$' + str(i))
            try:
                name = self.scan_frid_value()
            finally:
                self.pstack.pop()
            if not name:
                break
            self.skip_whitespace()
            if self.offset >= self.length:
                self.error(f"Unexpected ending after '{name}' of a map")
            token = self.peek_fixed_size(1)
            if token == sep[0]:
                self.skip_fixed_size(1)
                if kwds:
                    self.error("Unnamed argument following keyword argument")
                args.append(name)
                continue
            if token in stop:
                if kwds:
                    self.error("Unnamed argument following keyword argument")
                args.append(name)
                break
            if token != sep[1]:
                self.error(f"Expect '{sep[1]}' after key '{name}' of a map")
            if not isinstance(name, str):
                self.error(f"Invalid name type {type(name).__name__} of a map")
            self.skip_fixed_size(1)
            self.pstack.append(name)
            try:
                value = self.scan_frid_value()
            finally:
                self.pstack.pop()
            if name in kwds:
                self.error(f"Existing key '{name}' of a map")
            kwds[name] = value
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if token in stop:
                break
            if token == sep[0]:
                self.skip_fixed_size(1)
            elif self.config.loose_mode and not is_frid_prime(value):
                self.alert("Loose mode: adding missing ','")
            else:
                self.error(f"Expect '{sep[0]}' after the value for '{name}'")
        return (args, kwds)

    def scan_frid_value(self, empty: Any='', check_mixin: bool=False) -> FridValue|FridBeing:
        """Load the text representation."""
        self.skip_whitespace()
        if self.offset >= self.length:
            return empty
        token = self.peek_fixed_size(1)
        if token == '[':
            self.skip_fixed_size(1)
            self.pstack.append('[')
            try:
                value = self.scan_naked_list(']', check_mixin=check_mixin)
            finally:
                self.pstack.pop()
            self.skip_prefix_str(']')
            return value
        if token == '{':
            self.skip_fixed_size(1)
            self.pstack.append('/')
            try:
                value = self.scan_naked_dict('}')
            finally:
                self.pstack.pop()
            self.skip_prefix_str('}')
            return value
        if token in ALLOWED_QUOTES:
            return self.scan_quoted_seq(quotes=ALLOWED_QUOTES, check_mixin=bool(check_mixin))
        if token == '(' and self.config.parse_expr is not None:
            self.skip_fixed_size(1)
            value = self.scan_data_until(')')
            self.skip_prefix_str(')')
            self.pstack.append('(')
            try:
                return self.config.parse_expr(value, ''.join(self.pstack))
            finally:
                self.pstack.pop()
        # Now scan regular non quoted data
        self.anchor.append(self.offset)
        try:
            value = self.scan_prime_data(empty=empty)
            if self.offset >= self.length or not isinstance(value, str):
                return value
            dist = self.offset - self.anchor[-1]
            self.skip_whitespace()
            token = self.peek_fixed_size(1)
            if self.config.frid_mixin and token == '(' and is_frid_identifier(value):
                self.skip_fixed_size(1)
                name = value
                self.pstack.append(value + '(')
                try:
                    (args, kwds) = self.scan_naked_args(')')
                finally:
                    self.pstack.pop()
                self.skip_prefix_str(')')
                return self.construct_mixin(name, args, kwds, offset=self.anchor[-1])
            self.offset = self.anchor[-1] + dist
            return value
        except FridParseError:
            self.offset = self.anchor[-1]
            if self.config.parse_misc:
                value = self.scan_data_until(",)]}")
                return self.config.parse_misc(value, ''.join(self.pstack))
            raise
        finally:
            self.anchor.pop()

    def scan(
            self, stop: str='',
            *, top_dtype: Literal['list','dict','args']|None=None, until_eol: str|bool=False,
    ) -> FridValue|ValueArgs[str]:
        match top_dtype:
            case None:
                value = self.scan_frid_value()
                if isinstance(value, FridBeing):
                    self.error("PRESENT or MISSING is only supported for map values")
            case 'list':
                value = self.scan_naked_list(stop=stop)
            case 'dict':
                value = self.scan_naked_dict(stop=stop)
            case 'args':
                (args, kwds) = self.scan_naked_args(stop=stop)
                value = ValueArgs(''.join(self.pstack), *args, **kwds)
            case _:
                self.error(f"Invalid input {top_dtype}")
        # Skip to the end of the line (lineends in the comments are ignored)
        lineends = until_eol if isinstance(until_eol, str) else '\n\v\f'
        while True:
            self.skip_characters(char_set=' \r\t')
            comments = self.skip_comments()
            if comments is None:
                break
        # Check if the following character is a newline
        if self.offset < self.length:
            c = self.peek_fixed_size(1)
            if c in lineends:
                self.skip_fixed_size(1)
            elif until_eol:
                self.error("Trailing data at the end of line")
        return value
    def load(
            self, top_dtype: Literal['list','dict','args']|None=None
    ) -> FridValue|ValueArgs[str]:
        value = self.scan(top_dtype=top_dtype)
        if self.offset < 0:
            self.error("Offset is negative")
        if self.offset < self.length:
            self.skip_whitespace()
            if self.offset < self.length:
                self.error(f"Trailing data at {self.offset}")
        if isinstance(value, FridBeing):
            self.error("PRESENT or MISSING is only supported for map values")
        return value

@overload
def load_frid_str(s: str, *args, init_path: str='', top_dtype: None=None,
                  **kwargs: Unpack[FridLoader.Params]) -> FridValue: ...
@overload
def load_frid_str(s: str, *args, init_path: str='', top_dtype: Literal['list'],
                  **kwargs: Unpack[FridLoader.Params]) -> FridArray: ...
@overload
def load_frid_str(s: str, *args, init_path: str='', top_dtype: Literal['dict'],
                  **kwargs: Unpack[FridLoader.Params]) -> StrKeyMap: ...
@overload
def load_frid_str(s: str, *args, init_path: str='', top_dtype: Literal['args'],
                  **kwargs: Unpack[FridLoader.Params]) -> ValueArgs[str]: ...
def load_frid_str(
        s: str, *args, init_path: str='', top_dtype: Literal['list','dict','args']|None=None,
        **kwargs: Unpack[FridLoader.Params]
) -> FridValue|ValueArgs[str]:
    return FridLoader(s, *args, init_path=init_path, **kwargs).load(top_dtype=top_dtype)

@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: None=None,
        **kwargs: Unpack[FridLoader.Params]
) -> tuple[FridValue,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['list'],
        **kwargs: Unpack[FridLoader.Params]
) -> tuple[FridArray,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['dict'],
        **kwargs: Unpack[FridLoader.Params]
) -> tuple[StrKeyMap,int]: ...
@overload
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['args'],
        **kwargs: Unpack[FridLoader.Params]
) -> tuple[ValueArgs[str],int]: ...
def scan_frid_str(
        s: str, start: int, *args, init_path: str='', end_chars: str='',
        until_eol: str|bool=False, top_dtype: Literal['list','dict','args']|None=None,
        **kwargs: Unpack[FridLoader.Params]
) -> tuple[FridValue|ValueArgs[str],int]:
    """Note: this function will raise TruncError if the string ends prematurely.
    For other parsing issues, a regular ParseError is returned.
    """
    loader = FridLoader(s, offset=start, *args, init_path=init_path, **kwargs)
    value = loader.scan(end_chars, top_dtype=top_dtype, until_eol=until_eol)
    return (value, loader.offset)


class FridTextIOLoader(FridLoader):
    def __init__(self, t: TextIO, page: int = 16384,
                 *, init_path: str='', **kwargs: Unpack[FridLoader.Params]):
         # Do not pass any positional parameters; using default
        super().__init__("", 1<<62, init_path=init_path, **kwargs)
        self.file: TextIO|None = t
        self.page: int = page
    def __bool__(self):
        self.skip_whitespace()
        return self.offset >= self.length
    def __call__(self, *, end_chars: str='', until_eol: str|bool=False,
                top_dtype: Literal['list','dict']|None=None, **kwargs):
        return self.scan(end_chars, top_dtype=top_dtype, until_eol=until_eol)
    def fetch(self) -> None:
        if self.file is None:
            return super().fetch()  # Just raise reaching end exception
        half_page = self.page >> 1
        cutoff = self.offset - half_page # Keep the past page
        if cutoff > half_page:
            if self.anchor and cutoff > (min_anchor := min(self.anchor)):
                cutoff = min_anchor
            if cutoff > half_page:
                # Remove some of the past text
                self.buffer = self.buffer[cutoff:]
                self.outset += cutoff
                self.offset -= cutoff
                for i in range(len(self.anchor)):
                    self.anchor[i] -= cutoff
        data = self.file.read(self.page)
        self.buffer += data
        if len(data) < self.page:
            self.length = len(self.buffer)
            self.file = None
        # print(f"Loaded {len(data)}B, anchor={self.anchor} offset={old_offset}->{self.offset} "
        #       f"index={start}->{index} buffer=[{len(self.buffer)}]: "
        #       f"{self.buffer[:index]}\u2728{self.buffer[index:]}")

@overload
def load_frid_tio(t: TextIO, *args, init_path: str='', top_dtype: None=None,
                  **kwargs: Unpack[FridLoader.Params]) -> FridValue: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='', top_dtype: Literal['list'],
                  **kwargs: Unpack[FridLoader.Params]) -> FridArray: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='', top_dtype: Literal['dict'],
                  **kwargs: Unpack[FridLoader.Params]) -> StrKeyMap: ...
@overload
def load_frid_tio(t: TextIO, *args, init_path: str='', top_dtype: Literal['args'],
                  **kwargs: Unpack[FridLoader.Params]) -> ValueArgs[str]: ...
def load_frid_tio(
        t: TextIO, *args, init_path: str='',
        top_dtype: Literal['list','dict','args']|None=None, **kwargs: Unpack[FridLoader.Params]
) -> FridValue|ValueArgs[str]:
    """Loads the frid data from the text I/O stream `t`.
    - `*args` and `**kwargs` are passed to the constructor of `FridTextIOLoader`.
    - `init_path` is passed to `FridTextIOLoader.load()` as `path`.
    - `top_dtype` is passed to `FridTextIOLoader.load()`.
    """
    return FridTextIOLoader(t, *args, init_path=init_path, **kwargs).load(top_dtype=top_dtype)

def open_frid_tio(
        t: TextIO, *args, **kwargs: Unpack[FridLoader.Params]
) -> Callable[...,FridValue|ValueArgs[str]]:
    """Scans possibly multiple data from the text I/O stream `t`.
    - `*args` and `**kwargs` are passed to the constructor of `FridTextIOLoader`.
    - `init_path` is passed to `FridTextIOLoader.load()` as `path`.
    - `top_dtype` is passed to `FridTextIOLoader.load()`.
    Returns an instance of FridTextIOLoader as a functor.
    """
    return FridTextIOLoader(t, *args, **kwargs)
