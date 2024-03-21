import re, math, base64
from datetime import date, time, datetime, timezone, timedelta
from typing import Any, TypeVar

from .dtypes import FridArray, FridPrime, FridValue, StrKeyMap
from .checks import is_frid_identifier, is_identifier_char
from .errors import FridError
from .finder import scan_transforms

DEFAULT_PRIME_STR_CHARS = "!?@#$%^&*/"

quote_free_re = re.compile(r"[A-Za-z_](?:[\w\s@.+-]*\w)?")  # Quote free strings
plain_list_re = re.compile(r"\[[\w\s@,.+-]*\]")   # Plain list with quote free entries
whitespace_re = re.compile(r"\s")

date_only_re_str = r"(\d\d\d\d)-([01]\d)-([0-3]\d)"
time_zone_re_str = r"[+-](\d\d)(:?(\d\d))|Z"
time_only_re_str = r"([012]\d):([0-5]\d)(:?:([0-6]\d)(?:.(\d+))?)?(" + time_zone_re_str + ")?"
time_curt_re_str = r"([012]\d):?([0-5]\d)(:?:?([0-6]\d)(?:.(\d+))?)?(" + time_zone_re_str + ")?"
date_time_regexp = re.compile(date_only_re_str + r"\s*[Tt_ ]\s*" + time_curt_re_str)
date_only_regexp = re.compile(date_only_re_str)
time_only_regexp = re.compile(time_only_re_str)
time_curt_regexp = re.compile(time_curt_re_str)

T = TypeVar('T')

class ParserError(FridError):
    def __init__(self, s: str, index: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_string = s
        self.error_offset = index
    def to_frid(self, with_trace: bool=False) -> dict[str,str|int|list[str]]:
        # TODO: put the string an index in
        return super().to_frid(with_trace)

def parse_time_only(s, m: re.Match|None=None) -> time|None:
    """Parse ISO time string, where the colon between hour and second are time is optional.
    - Returns the Python time object or None if it fails to parse.
    Since we support Python 3.10, the new feature in 3.11 may not be available.
    """
    if m is None:
        m = time_curt_regexp.fullmatch(s)
        if m is None:
            return None
    fs_str = m.group(4)   # Fractional second
    if len(fs_str) > 6:
        fs_str = fs_str[:6]
    micros = int(fs_str)
    if len(fs_str) < 6:
        micros *= 10 ** (6 - len(fs_str))
    tz_str = m.group(5)
    if not tz_str:
        tzinfo = None
    elif tz_str == 'Z':
        tzinfo = timezone.utc
    else:
        tdelta = timedelta(hours=int(m.group(5)), minutes=int(m.group(6) or 0))
        tzinfo = timezone(-tdelta if tz_str[0] == '-' else tdelta)
    return time(int(m.group(1)), int(m.group(2)), int(m.group(3) or 0), micros, tzinfo=tzinfo)

def parse_date_time(s) -> datetime|date|time|None:
    """Parses a date or time or date with time in extended ISO format.
    - Returns the Python datetime/date/time object, or None if it fails to parse.
    """
    if s.startswith('0T') or s.startswith('0t'):
        s = s[2:]
        if m := time_curt_regexp.match(s):
            return parse_time_only(s, m)
        return None
    if date_time_regexp.fullmatch(s):
        (d_str, _, t_str) = s.partition('T')
        t_val = parse_time_only(t_str)
        if t_val is None:
            return None
        return datetime.combine(date.fromisoformat(d_str), t_val)
    if date_only_regexp.fullmatch(s):
        return date.fromisoformat(s)
    if m := time_only_regexp.fullmatch(s):
        return parse_time_only(s, m)
    return None

def parse_prime_str(s: str, default: T, /, *, json: bool=False) -> FridPrime|T:
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
    if json:
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
    if (t := parse_date_time(s)) is not None:
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

_quoted_origin_set = "\n\t\r\v\f\b\a\x27\0"
_quoted_target_set = "ntrvfbae\0"
_quoted_common_set = "\\\"\'`"

def _scan_escape_seq(s: str, start: int, bound: int, prefix: str) -> tuple[int,str]:
    index = len(prefix)
    c = s[start + index]
    j = _quoted_target_set.find(c)
    if j >= 0:
        return (index + 1, _quoted_origin_set[j])
    j = _quoted_common_set.find(c)
    if j >= 0:
        return (index + 1, c)
    if c == 'x':
        n = 2
    elif c == 'u':
        n = 4
    elif c == 'U':
        n = 8
    else:
        raise ParserError(s, start, f"Invalid escape sequence \\{c}")
    # The 2 or 4 or 8 hex chars to load
    index += 1
    n = 8 if c == 'U' else 4
    if start + index + n > bound:
        raise ParserError(s, start, f"Less than {n} letters for \\{c}")
    try:
        codepoint = int(s[(start + index):(start + index + 4)], 16)
        return (index + n, chr(codepoint))
    except ValueError as exc:
        raise ParserError(s, start, "Invalid unicode spec for \\{c}") from exc

def skip_whitespace(s: str, start: int) -> int:
    index = start
    while index < len(s) and s[index].isspace():
        index += 1
    return index

def _scan_ending_seq(s: str, start: int, bound: int, ending: str):
    return (len(ending), "")  # Ending string does not contribute to the data

def scan_prime_data(s: str, start: int, accept=DEFAULT_PRIME_STR_CHARS):
    index = start
    bound = len(s)
    while index < bound:
        c = s[index]
        if not is_identifier_char(c) and c not in accept:
            break
    value = parse_prime_str(s[start:index].strip(), ...)
    if value is ...:
        raise ValueError("Fail to parse unquoted value")
    return (index - start, value)

def scan_quoted_str(s: str, start: int, quote: str) -> tuple[int,str]:
    """Scan quoted string until the end-quote."""
    return scan_transforms(s, [
        ('\\', _scan_escape_seq), (quote, _scan_ending_seq)
    ], start)

def scan_naked_list(s: str, start: int, end: str='', sep: str=',') -> tuple[int,FridArray]:
    out = []
    index = start
    while True:
        (count, value) = scan_multi_data(s, index)
        if count < 0:
            raise ParserError(s, index, f"Could not load entry #{len(out)} of a list")
        index = skip_whitespace(s, start + count)
        if index >= len(s) or s[index] in end:
            break
        out.append(value)
        if s[index] != sep:
            raise ParserError(s, index, f"Unexpected '{s[index]}' after {len(out)}-th entry")
    return (index - start, out)

def scan_naked_dict(s: str, start: int, end: str='', sep: str=",:") -> tuple[int,StrKeyMap]:
    out = {}
    index = start
    while True:
        (count, key) = scan_frid_value(s, index)
        if count < 0:
            raise ParserError(s, index, f"Could not load the key of entry #{len(out)} of a map")
        if not isinstance(key, str):
            raise ParserError(s, index, f"Invalid key type {type(key).__name__}")
        if key in out:
            raise ParserError(s, index, f"Existing key '{key}'")
        index = skip_whitespace(s, start + count)
        if index >= len(s):
            raise ParserError(s, index, f"Unexpected ending after the key '{key}' of a map")
        if s[index] != sep[1]:
            raise ParserError(s, index, f"Expect '{sep[1]}' after the key '{key}' of a map")
        index += 1
        (count, value) = scan_multi_data(s, index)
        if count < 0:
            raise ParserError(s, index, f"Could not load the value of '{key}' of a map")
        out[key] = value
        index = skip_whitespace(s, start + count)
        if index >= len(s) or s[index] in end:
            break
        if s[index] != sep[0]:
            raise ParserError(s, index, f"Expect '{sep[0]}' after the value for '{key}'")
    return (index - start, out)

def scan_expression(s: str, start: int, end: str='', name: str|None=None) -> tuple[int,FridValue]:
    raise NotImplementedError

def scan_frid_value(s: str, start: int, prev: Any=...) -> tuple[int,FridValue]:
    """Load the text representation."""
    index = skip_whitespace(s, start)
    bound = len(s)
    if index >= len(s):
        return (len(s) - index, '')
    c = s[index]
    index += 1
    match c:
        case '[':
            if prev is not ...:
                raise ParserError(s, index, f"list after a value of {type(prev)}")
            ending = ']'
            (count, value) = scan_naked_list(s, index, ending)
            name = "array"
        case '{':
            if prev is not ...:
                raise ParserError(s, index, f"map after a value of {type(prev)}")
            ending = '}'
            (count, value) = scan_naked_dict(s, index, ending)
            name = "map"
        case '"' | '\'' | '`':
            if prev is ... or not isinstance(prev, str):
                raise ParserError(s, index, f"quoted string after a value of {type(prev)}")
            ending = c
            (count, value) = scan_quoted_str(s, index, ending)
            if isinstance(prev, str):
                value = prev + value
            name = "quoted string"
        case '(':
            if prev is ... or not is_frid_identifier(prev):
                raise ParserError(s, index, f"expression after a value of {type(prev)}")
            ending = ')'
            (count, value) = scan_expression(s, index, ending, None if prev is ... else prev)
            name = "expression"
        case _:
            if prev is ... or not isinstance(prev, str):
                raise ParserError(s, index, f"data after a value of {type(prev)}")
            (count, value) = scan_prime_data(s, index)
            if isinstance(prev, str):
                if not isinstance(value, str):
                    raise ParserError(s, index, f"{type(value)} after a string")
                value = prev + value
    if count < 0:
        raise ParserError(s, index, f"Couldn't parse {name}")
    index += count
    if index >= bound or s[index] not in ending:
        raise ParserError(s, index, f"Expecting '{ending}'")
    return (count + 1, value)

def scan_multi_data(s: str, start: int, end: str=''):
    index = start
    (count, value) = scan_frid_value(s, index)
    while count > 0:
        index = skip_whitespace(s, index + count)
        if s[index] in end:
            break
        (count, value) = scan_frid_value(s, index, value)
    return (index - start, value)


# def dump_single_line(data: FridValue) -> str:
#     """Dump the data into a single line of string that can be loaded later."""
#     if data is None:
#         return '.'
#     if isinstance(data, bool):  # Do bool first as it is subtyping int
#         return '+' if data else '-'
#     if isinstance(data, (int, float)):
#         return str(data)
#     if isinstance(data, str):
#         if not data or quote_free_re.fullmatch(data):
#             return data
#     elif isinstance(data, Sequence) and not isinstance(data, BlobTypes):
#         result = '[' + ','.join(dump_single_line(x) for x in data) + ']'
#         if plain_list_re.fullmatch(result):
#             return result
#     return whitespace_re.sub(' ', pyjson5.encode(data))

# def dump_pretty_line(data: FridValue) -> str:
#     """Dumps the data into a sinelg line mainly for human consumption."""
#     if data is None:
#         return 'null'
#     if isinstance(data, str):
#         return data
#     if isinstance(data, bool):  # Do bool first as it is subtyping int
#         return 'true' if data else 'false'
#     if isinstance(data, Sequence) and not isinstance(data, BlobTypes):
#         return '[' + ", ".join(dump_pretty_line(x) for x in data) + ']'
#     if isinstance(data, Mapping):
#         return '{' + ", ".join(k + ": " + dump_pretty_line(v) for k, v in data.items()) + '}'
#     return str(data)

# _text_variable_re = re.compile(r"\$\{\s*([\w.]+)\s*\}")
# def format_pretty(s: str, default: str|None=None, /, **kwargs: FridValue):
#     return _text_variable_re.sub((lambda m: (
#         dump_pretty_line(kwargs.get(m.group(1), m.group()))
#         if default is None else default
#     )), s)
