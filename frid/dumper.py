import math, base64
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone

from .checks import is_frid_identifier, is_frid_quote_free
from .dtypes import BlobTypes, DateTypes, FridPrime, FridValue, StrKeyMap
from .dtypes import JsonLevel, timeonly, dateonly

JSON_KEYWORDS = (
    'true', 'false', 'null',
)
JSON1_ESCAPE_CHARS = "\n\t\r\b"
JSON1_AFTER_ESCAPE = "ntrb"
JSON5_ESCAPE_CHARS = JSON1_ESCAPE_CHARS + "\v\f0"
JSON5_AFTER_ESCAPE = JSON1_AFTER_ESCAPE + "vf0"
EXTRA_ESCAPE_CHARS = JSON5_ESCAPE_CHARS + "\a\x27"
EXTRA_AFTER_ESCAPE = JSON5_AFTER_ESCAPE + "ae"

def print_frid_real(data: float, *, path: str, json: JsonLevel=None) -> str:
    if not json:
        # Frid format
        if math.isnan(data):
            return "+." if data >= 0 else "-."
        if math.isinf(data):
            return "++" if data >= 0 else "--"
        return str(data)
    if json == 5:
        if math.isnan(data):
            return "NaN"
        if math.isinf(data):
            return "+Infinity" if data >= 0 else "-Infinity"
        return str(data)
    if json is True:
        if math.isnan(data):
            raise ValueError(f"NaN is not supported by JSON at {path}")
        if math.isinf(data):
            raise ValueError(f"NaN is not supported by JSON at {path}")
        return str(data)
    if isinstance(json, str):
        if math.isnan(data):
            out = "+." if data >= 0 else "-."
        elif math.isinf(data):
            out = "++" if data >= 0 else "--"
        else:
            return str(data)
        return '"' + json + out + '"'
    raise ValueError(f"Invalid {json=} at {path=}")

def print_date_time(data: DateTypes, *, path: str, json: JsonLevel=None) -> str:
    if isinstance(data, timeonly|datetime):
        if data.tzinfo is timezone.utc:
            out = data.replace(tzinfo=None).isoformat() + 'Z'
        else:
            out = data.isoformat() # TODO timespec
    elif isinstance(data, dateonly):
        out = data.isoformat()
    else:
        return "??"
    if not json:
        return out
    if isinstance(json, str):
        return '"' + json + out + '"'
    raise ValueError(f"Date formats are not supported for {json=} at {path=}: {out}")

def print_frid_blob(
        data: BlobTypes, *, path: str, json: JsonLevel=None
) -> str:
    # TODO: support line splitting and indentation
    out = base64.urlsafe_b64decode(data).decode()
    if not out.endswith("="):
        out = ".." + out
    elif out.endswith("=="):
        out = ".." + out[:-2] + ".."
    else:
        out = ".." + out[:-1] + "."
    if not json:
        return out
    if isinstance(json, str):
        return '"' + json + out + '"'
    raise ValueError(f"Blob types are not supported by {json=} at {path=}")

def print_prime_str(
        data: FridValue, *, path: str, json: JsonLevel=False,
        print_int: Callable[[int],str]=str, print_float: Callable[[float],str]|None=None,
        print_date: Callable[[DateTypes],str]|None=None,
        **kwargs
) -> str|None:
    if json:
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
    if isinstance(data, int):
        return print_int(data)
    if isinstance(data, float):
        if print_float is not None:
            return print_float(data)
        return print_frid_real(data, path=path, json=json)
    if isinstance(data, DateTypes):
        if print_date is not None:
            return print_date(data)
        return print_date_time(data, path=path, json=json)
    if isinstance(data, BlobTypes):
        return print_frid_blob(data, path=path, json=json)
    if json:
        return None
    if isinstance(data, str) and is_frid_quote_free(data):
        return data
    return None

class TransTable:
    def __init__(self, source: str, target: str, *, escape: str='\\', quotes: str='\"',
                 no_utf: int=0, with_2=False, with_8=False):
        assert escape
        assert len(source) == len(target)
        self._map: dict[int,str] = {ord(escape[0]): escape + escape}
        for s, t in zip(source, target):
            self._map[ord(s)] = escape + t
        for q in quotes:
            self._map[ord(q)] = escape + q
        self._escape = escape
        self._no_utf = no_utf
        self._with_2 = with_2
        self._with_8 = with_8
    def __getitem__(self, cp: int):
        assert cp >= 0
        if cp < 256:
            data = self._map.get(cp)
            if data is not None:
                return data
            if cp >= 0x20 and cp < 0x7f:
                return chr(cp)
            if not self._no_utf:
                c = chr(cp)
                if c.isprintable():
                    return c
            if self._with_2:
                return self._escape + 'x' + format(cp, "02x")
        elif not self._no_utf:
            c = chr(cp)
            if c.isprintable():
                return c
        if cp < 0x10000:
            return self._escape + 'u' + format(cp, "04x")
        if self._with_8:
            return self._escape + 'U' + format(cp, "08x")
        cpx = cp - 0x10000
        assert cpx < 0x100000
        # Return a surrogate pair
        return (self._escape + 'u' + chr((cpx >> 12) + 0xD800)
                + self._escape + 'u' + chr((cpx & 0x3ff) + 0xDC00))

_json1_trans_table = TransTable(JSON1_ESCAPE_CHARS, JSON1_AFTER_ESCAPE,
                                with_2=False, with_8=False)
_json5_trans_table = TransTable(JSON1_ESCAPE_CHARS, JSON1_AFTER_ESCAPE,
                                with_2=False, with_8=False)
_extra_trans_table = TransTable(EXTRA_ESCAPE_CHARS, EXTRA_AFTER_ESCAPE,
                                with_2=True, with_8=True)
_json1_trans_ascii = TransTable(JSON1_ESCAPE_CHARS, JSON1_AFTER_ESCAPE,
                                no_utf=True, with_2=False, with_8=False)
_json5_trans_ascii = TransTable(JSON1_ESCAPE_CHARS, JSON1_AFTER_ESCAPE,
                                no_utf=True, with_2=False, with_8=False)
_extra_trans_ascii = TransTable(EXTRA_ESCAPE_CHARS, EXTRA_AFTER_ESCAPE,
                                no_utf=True, with_2=True, with_8=True)

def push_quoted_str(r: list[str], data: str, quote: str='\"',
                    *, path: str, json: JsonLevel=None, ascii_only=False, **kwargs):
    """Push a quoted string into the list (without quotes themselves)."""
    if not json:
        table = _extra_trans_ascii if ascii_only else _extra_trans_table
    elif json == 5:
        table = _json5_trans_ascii if ascii_only else _json5_trans_table
    else:
        table = _json1_trans_ascii if ascii_only else _json1_trans_table
    r.append(data.translate(table))

def push_prime_data(r: list[str], data: FridPrime, *, path: str, json: JsonLevel=None):
    s = print_prime_str(data, path=path, json=json)
    if s is None:
        raise ValueError(f"Invalid data type {type(data)}")
    r.append(s)

def push_naked_list(r: list[str], data: Iterable[FridValue], sep: str=', ',
                    *, path: str="", **kwargs):
    for i, x in enumerate(data):
        if i > 0:
            r.append(sep)
        push_frid_value(r, x, path=(path + '[' + str(i) + ']'), **kwargs)
    # TODO: add a separator at the end if newline is ensured except for JSON format

def _is_quote_free_key(key: str, json: JsonLevel=None, ascii_only: bool=False):
    if ascii_only and not key.isascii():
        return False
    if not json:
        return is_frid_identifier(key)
    if json != 5:
        return False
    # JSON 5 identifiers, first not ECMAScript keywords but not in Python
    if key in JSON_KEYWORDS:
        return False
    key = key.replace('$', '_')  # Handle $ the same way as _
    # Use python identifiers as it is generally more restrictive
    return key.isidentifier()

def push_naked_dict(r: list[str], data: StrKeyMap, sep: str=',:',
                    *, path: str="", json: JsonLevel=None, ascii_only: bool=False, **kwargs):
    for i, (k, v) in enumerate(data.items()):
        if not isinstance(k, str):
            raise ValueError(f"Key is not a string: {k}")
        if _is_quote_free_key(k, json, ascii_only):
            r.append(k)
        else:
            push_quoted_str(r, k, path=path, json=json, ascii_only=ascii_only)
        r.append(sep[1])
        push_frid_value(r, v, path=path, json=json)
        r.append(sep[0])
    # TODO: add a separator at the end if newline is ensured except for JSON format

def push_frid_value(r: list[str], data: FridValue,
                    *, path: str, json: JsonLevel=None, ascii_only: bool=False):
    x = print_prime_str(data, path=path, json=json)
    if x is not None:
        r.append(x)
    elif isinstance(data, str):
        push_quoted_str(r, data, path=path, json=json, ascii_only=ascii_only)
    elif isinstance(data, Mapping):
        r.append('{')
        push_naked_dict(r, data, path=path, json=json)
        r.append('}')
    elif isinstance(data, Iterable):
        r.append('[')
        push_naked_list(r, data, path=path, json=json)
        r.append(']')
    else:
        raise ValueError(f"Invalid type {type(data)}")

def dump_frid_value(data: FridValue, json: JsonLevel=None) -> str:
    out: list[str] = []
    push_frid_value(out, data, path="", json=json)
    return ''.join(out)

