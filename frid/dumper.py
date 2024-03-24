import math, base64
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .typing import BlobTypes, FridMixin, FridPrime, FridValue, StrKeyMap, JsonLevel
from .chrono import DateTypes, strfr_datetime
from .guards import is_frid_identifier, is_frid_quote_free
from .pretty import PrettyPrint, PPTokenType
from .strops import StringEscape

JSON_NONIDENTIFIERS = (
    'true', 'false', 'null',
)
JSON1_ESCAPE_PAIRS = "\nn\tt\rr\bb"
JSON5_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\vv\ff\00"
EXTRA_ESCAPE_PAIRS = JSON1_ESCAPE_PAIRS + "\aa\x27e"

class FridDumper(PrettyPrint):
    def __init__(self, *args, json_level: JsonLevel=None, ascii_only: bool=False,
                 print_real: Callable[[int|float,str],str|None]|None=None,
                 print_date: Callable[[DateTypes,str],str|None]|None=None,
                 print_user: Callable[[Any,str],str|None]|None=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.json_level = json_level
        self.ascii_only = ascii_only
        self.print_real = print_real
        self.print_date = print_date
        self.print_user = print_user
        if not json_level:
            pairs = EXTRA_ESCAPE_PAIRS
            encode_hex = ['x', 'u', 'U']
        elif json_level == 5:
            pairs = JSON5_ESCAPE_PAIRS
            encode_hex = ['x', 'u', None]
        else:
            pairs = JSON1_ESCAPE_PAIRS
            encode_hex = [None, 'u', None]
        if ascii_only:
            self.str_escape = StringEscape(pairs, '\\', encode_hex=[None, None, None])
        else:
            self.str_escape = StringEscape(pairs, '\\', encode_hex=encode_hex)

    def print(self, token: str, ttype: PPTokenType, /):
        """Default token print behavior:
        - Do not show optional separator.
        - Add an space after required separator.
        """
        if ttype not in (PPTokenType.OPT_0, PPTokenType.OPT_1):
            self._print(token)
        if ttype in (PPTokenType.SEP_0, PPTokenType.SEP_1):
            self._print(' ')

    def real_to_str(self, data: int|float, path: str, /) -> str:
        if isinstance(data, int):
            return str(data)
        if not self.json_level:
            # Frid format
            if math.isnan(data):
                return "+." if data >= 0 else "-."
            if math.isinf(data):
                return "++" if data >= 0 else "--"
            return str(data)
        if self.json_level == 5:
            if math.isnan(data):
                return "NaN"
            if math.isinf(data):
                return "+Infinity" if data >= 0 else "-Infinity"
            return str(data)
        if self.json_level is True:
            if math.isnan(data):
                raise ValueError(f"NaN is not supported by JSON at {path}")
            if math.isinf(data):
                raise ValueError(f"NaN is not supported by JSON at {path}")
            return str(data)
        if isinstance(self.json_level, str):
            if math.isnan(data):
                out = "+." if data >= 0 else "-."
            elif math.isinf(data):
                out = "++" if data >= 0 else "--"
            else:
                return str(data)
            return '"' + self.json_level + out + '"'
        raise ValueError(f"Invalid {self.json_level=} at {path=}")

    def date_to_str(self, data: DateTypes, path: str, /) -> str:
        out = strfr_datetime(data)
        if out is None:
            raise ValueError(f"Unsupported datetime type {type(data)} at {path=}")
        if not self.json_level:
            return out
        if isinstance(self.json_level, str):
            return '"' + self.json_level + out + '"'
        raise ValueError(f"Unsupported data for json={self.json_level} at {path=}: {out}")

    def blob_to_str(self, data: BlobTypes, path: str) -> str:
        # TODO: support line splitting and indentation
        out = base64.urlsafe_b64decode(data).decode()
        if not out.endswith("="):
            out = ".." + out
        elif out.endswith("=="):
            out = ".." + out[:-2] + ".."
        else:
            out = ".." + out[:-1] + "."
        if not self.json_level:
            return out
        if isinstance(self.json_level, str):
            return '"' + self.json_level + out + '"'
        raise ValueError(f"Blobs are unsupported by json={self.json_level} at {path=}")

    def prime_data_to_str(self, data: FridValue, path: str, /) -> str|None:
        if self.json_level:
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
            if self.print_real is not None and (out := self.print_real(data, path)) is not None:
                return out
            return self.real_to_str(data, path)
        if isinstance(data, DateTypes):
            if self.print_date is not None and (out := self.print_date(data, path)) is not None:
                return out
            return self.date_to_str(data, path)
        if isinstance(data, BlobTypes):
            return self.blob_to_str(data, path)
        if self.json_level:
            return None
        if isinstance(data, str) and is_frid_quote_free(data):
            return data
        if self.print_user is not None and (out := self.print_user(data, path)) is not None:
            return out
        return None

    def print_quoted_str(self, data: str, path: str, /, as_key: bool=False, quote: str='\"'):
        """Push a quoted string into the list (without quotes themselves)."""
        self.print(self.str_escape.encode(data, quote),
                   PPTokenType.LABEL if as_key else PPTokenType.ENTRY)

    def print_prime_data(self, data: FridPrime, path: str, /):
        s = self.prime_data_to_str(data, path)
        if s is None:
            raise ValueError(f"Invalid data type {type(data)}")
        self.print(s, PPTokenType.ENTRY)

    def print_naked_list(self, data: Iterable[FridValue], path: str="", /, sep: str=','):
        for i, x in enumerate(data):
            if i > 0:
                self.print(sep, PPTokenType.SEP_0)
            self.print_frid_value(x, path + '[' + str(i) + ']')
        if i > 0 and self.json_level == 5 or not self.json_level:
            self.print(sep[0], PPTokenType.OPT_0)

    def _is_unquoted_key(self, key: str):
        if self.ascii_only and not key.isascii():
            return False
        if not self.json_level:
            return is_frid_identifier(key)
        if self.json_level != 5:
            return False
        # JSON 5 identifiers, first not ECMAScript keywords but not in Python
        if key in JSON_NONIDENTIFIERS:
            return False
        key = key.replace('$', '_')  # Handle $ the same way as _
        # Use python identifiers as it is generally more restrictive
        return key.isidentifier()

    def print_naked_dict(self, data: StrKeyMap, path: str="", /, sep: str=',:'):
        for i, (k, v) in enumerate(data.items()):
            if not isinstance(k, str):
                raise ValueError(f"Key is not a string: {k}")
            if self._is_unquoted_key(k):
                self.print(k, PPTokenType.LABEL)
            else:
                self.print_quoted_str(k, path, as_key=True)
            self.print(sep[1], PPTokenType.SEP_1)
            self.print_frid_value(v, path)
            self.print(sep[0], PPTokenType.SEP_0)
        if i > 0 and self.json_level == 5 or not self.json_level:
            self.print(sep[0], PPTokenType.OPT_0)

    def print_frid_mixin(self, data: FridMixin, path: str, /):
        (name, args, kwas) = data.frid_repr()
        if not self.json_level:
            assert is_frid_identifier(name)
            self.print(name, PPTokenType.ENTRY)
            self.print('(', PPTokenType.START)
            self.print_naked_list(args, path, ',')
            self.print(',', PPTokenType.SEP_0)
            self.print_naked_dict(kwas, path, ',=')
            self.print(')', PPTokenType.CLOSE)
            return
        if not isinstance(self.json_level, str):
            raise ValueError(f"FridMixin is not supported with pure JSON/JSON5: {path}")
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
        x = self.prime_data_to_str(data, path)
        if x is not None:
            self.print(x, PPTokenType.ENTRY)
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
        else:
            raise ValueError(f"Invalid type {type(data)}")

