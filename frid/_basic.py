import sys, math, string
from random import Random
from collections.abc import Callable, Collection, Mapping, Sequence
from enum import Flag
from typing import Concatenate, Generic, ParamSpec, TypeVar, cast, overload

from .typing import (
    PRESENT, MISSING, BlobTypes, DateTypes, FridPrime, FridBasic, FridBeing, MissingType,
    FridMixin, FridArray, FridMapVT, FridSeqVT, FridValue, StrKeyMap
)
from .guards import is_list_like
from .chrono import timeonly, datetime, dateonly
from .lib import str_scan_sub
from ._dumps import dump_frid_str

P = ParamSpec('P')
T = TypeVar('T')
PrimitiveCompFunc = Callable[Concatenate[T,FridValue,P],bool]
RecursiveCompFunc = Callable[Concatenate[T,FridValue,Callable[...,bool],P],bool]

class FridCompare(Generic[P]):
    """Compares data trees in a flexiable and configurable way."""
    def __init__(
            self, *, default: bool=False,
            compare_none: PrimitiveCompFunc[None,P]|None=None,
            compare_bool: PrimitiveCompFunc[bool,P]|None=None,
            compare_real: PrimitiveCompFunc[int|float,P]|None=None,
            compare_text: PrimitiveCompFunc[str,P]|None=None,
            compare_blob: PrimitiveCompFunc[BlobTypes,P]|None=None,
            compare_date: PrimitiveCompFunc[DateTypes,P]|None=None,
            compare_list: RecursiveCompFunc[FridArray,P]|None=None,
            compare_dict: RecursiveCompFunc[StrKeyMap,P]|None=None
    ):
        self._default: bool = default
        self._compare_none: PrimitiveCompFunc[None,P] = compare_none or self.is_none
        self._compare_bool: PrimitiveCompFunc[bool,P] = compare_bool or self.equal_item
        self._compare_real: PrimitiveCompFunc[int|float,P] = compare_real or self.equal_item
        self._compare_text: PrimitiveCompFunc[str,P] = compare_text or self.equal_item
        self._compare_blob: PrimitiveCompFunc[BlobTypes,P] = compare_blob or self.equal_item
        self._compare_date: PrimitiveCompFunc[DateTypes,P] = compare_date or self.equal_item
        self._compare_list: RecursiveCompFunc[FridArray,P] = compare_list or self.equal_list
        self._compare_dict: RecursiveCompFunc[StrKeyMap,P] = compare_dict or self.equal_dict

    def __call__(self, d1: FridValue, d2: FridValue,
                 /, *args: P.args, **kwargs: P.kwargs) -> bool:
        if d1 is None:
            return self._compare_none(d1, d2, *args, **kwargs)
        if isinstance(d1, bool):
            return self._compare_bool(d1, d2, *args, **kwargs)
        if isinstance(d1, int|float):
            return self._compare_real(d1, d2, *args, **kwargs)
        if isinstance(d1, str):
            return self._compare_text(d1, d2, *args, **kwargs)
        if isinstance(d1, BlobTypes):
            return self._compare_blob(d1, d2, *args, **kwargs)
        if isinstance(d1, DateTypes):
            return self._compare_date(d1, d2, *args, **kwargs)
        if isinstance(d1, Sequence):
            return self._compare_list(d1, d2, self, *args, **kwargs)
        if isinstance(d1, Mapping):
            return self._compare_dict(d1, d2, self, *args, **kwargs)
        return self._default

    @staticmethod
    def is_none(d1: None, d2: FridValue,
                /, *args: P.args, **kwargs: P.kwargs) -> bool:
        return d2 is None

    @staticmethod
    def equal_item(d1: str|int|float|DateTypes|BlobTypes, d2: FridValue,
                   /, *args: P.args, **kwargs: P.kwargs) -> bool:
        return d1 == d2

    @staticmethod
    def equal_list(d1: FridArray, d2: FridValue, /, comparator: Callable[...,bool],
                   *args: P.args, **kwargs: P.kwargs) -> bool:
        if not isinstance(d2, Sequence):
            return False
        return len(d1) == len(d2) and all(
            comparator(x, d2[i], *args, **kwargs) for i, x in enumerate(d1)
        )

    @staticmethod
    def equal_dict(d1: StrKeyMap, d2: FridValue, /, comparator: Callable[...,bool],
                   *args: P.args, **kwargs: P.kwargs) -> bool:
        if not isinstance(d2, Mapping):
            return False
        return len(d1) == len(d2) and all(
            k in d2 and comparator(v, d2[k], *args, **kwargs) for k, v in d1.items()
        )

    @staticmethod
    def is_submap(d1: StrKeyMap, d2: FridValue, /, comparator: Callable[...,bool],
                  *args: P.args, **kwargs: P.kwargs) -> bool:
        """Returns true iff `d2` is a submap of `d1`."""
        if not isinstance(d2, Mapping):
            return False
        return all(
            k in d2 and comparator(v, d2[k], *args, **kwargs) for k, v in d1.items()
        )
class FridReplace:
    """Replaces delimited variables in template strings into their values."""
    def __init__(self, prefix: str="${", suffix: str="}",
                 *, present: str='?', missing: str=''):
        self.prefix = prefix
        self.suffix = suffix
        self.present = present
        self.missing = missing
    def textuate(self, data: FridValue|FridBeing) -> str:
        """Convert data to text in the case it is in the middle of a string.
        This method can be overridden by a derived class
        """
        if isinstance(data, str):
            return data
        if isinstance(data, FridBeing):
            return self.present if data else self.missing
        return dump_frid_str(data)

    def evaluate(self, expr: str, values: StrKeyMap) -> FridValue|FridBeing:
        """Evaluate an expression against the values."""
        expr = expr.strip()
        # Currently only handles the wildcard as the end of variable
        if expr.endswith('*'):
            expr = expr[:-1]
            return {k[len(expr):]: v for k, v in values.items()
                    if k.startswith(expr) and v is not MISSING}
        return values.get(expr, MISSING)

    def sub_text(self, s: str, values: StrKeyMap) -> FridValue|FridBeing:
        """Return the string `s` with placeholder variable replaced with values.
        If a variable does not exist in `values`
        - Returns MISSING if the template contains only a single variable;
        - Returns as is if template contains more than a single variable.
        """
        if s.startswith(self.prefix) and s.endswith(self.suffix):
            name = s[2:-1]
            return self.evaluate(name, values)
        def _transform(s: str, start: int, bound: int, prefix: str):
            index = start + len(prefix)
            end = s.find(self.suffix, index, bound)
            if end < 0:
                assert len(s) == bound
                raise ValueError(f"Missing '{self.suffix}' at {index}")
            expr = s[index:end]
            return (len(self.suffix) + end - start,
                    self.textuate(self.evaluate(expr, values)))
        return str_scan_sub(s, {self.prefix: _transform})[1]
    _T = TypeVar('_T', bound=FridValue)
    @overload
    def sub_data(self, data: StrKeyMap, values: StrKeyMap) -> dict[str,FridMapVT]: ...
    @overload
    def sub_data(self, data: FridArray, values: StrKeyMap) -> list[FridSeqVT]: ...
    @overload  # Note: some analyzer cannot handle this overload as str is a squence
    def sub_data(self, data: str, values: StrKeyMap) -> FridValue: ...  # type: ignore
    @overload
    def sub_data(self, data: _T, values: StrKeyMap) -> _T: ...
    def sub_data(self, data: FridValue, values: StrKeyMap) -> FridValue|FridBeing:
        """Substitute the placeholders in data (only for its values).
        The placeholders are escaped with `${......}` (only for string value).
        The enclosed string `......` is used as the key to get the actual value
        in `values`.
        """
        if isinstance(data, str):
            return self.sub_text(data, values)
        if isinstance(data, BlobTypes):
            return data
        if isinstance(data, Mapping):
            # We get rid of MISSING here
            return {k: v if isinstance(v, FridBeing) else self.sub_data(v, values)
                    for k, v in data.items() if v is not MISSING}
        if isinstance(data, Sequence):
            # Special handling for array: array return value do "splice"
            out = []
            for v in data:
                r = self.sub_data(v, values)
                if isinstance(r, FridBeing):
                    out.append(self.present if r else self.missing)
                elif is_list_like(r):
                    out.extend(r)
                else:
                    out.append(r)
            return out
        return data
    def __call__(self, data: FridValue, values: StrKeyMap|None=None,
                 /, **kwargs: FridValue|FridBeing) -> FridValue:
        if values:
            kwargs.update(values)
        result = self.sub_data(data, kwargs)
        if isinstance(result, FridBeing):
            return self.present if result else self.missing
        return result

class MingleFlags(Flag):
    NONE = 0
    BOOL = 0x1
    REAL = 0x2
    TEXT = 0x4
    BLOB = 0x8
    LIST = 0x10
    DICT = 0x20
    SET = 0x40
    LIST_AS_SET = 0x80
    ALL = BOOL | REAL | TEXT | BLOB | LIST | DICT | SET

def frid_sizeof(data: FridValue|FridBeing) -> int:
    if data is None or isinstance(data, bool|FridBeing):
        return 0   # Do not count these singletons
    size = sys.getsizeof(data)
    if isinstance(data, Mapping):
        size += sum(sys.getsizeof(k) + frid_sizeof(v) for k, v in data.items())
    elif isinstance(data, Collection) and not isinstance(data, str|BlobTypes):
        size += sum(frid_sizeof(v) for v in data)
    return size

def frid_mingle(old: T|MissingType, new: T, *, depth: int=16, flags=MingleFlags.ALL) -> T:
    if old is MISSING:
        return new
    if isinstance(new, bool):
        if flags & MingleFlags.BOOL:
            return bool(old) or new
    elif isinstance(new, int|float):
        if flags & MingleFlags.REAL and isinstance(old, int|float|bool):
            return old + new
    elif isinstance(new, Mapping):
        if flags & MingleFlags.DICT and isinstance(old, Mapping):
            if not new:
                return old
            d = dict(old)
            for k, v in new.items():
                old_v = d.get(k, MISSING)
                if depth > 0:
                    v = frid_mingle(old_v, v, depth=(depth - 1), flags=flags)
                if v is not MISSING:
                    d[k] = v
            return cast(T, d)
    elif isinstance(new, str):
        if flags & MingleFlags.TEXT and isinstance(old, str):
            if not new:
                return old
            return old + new
    elif isinstance(new, BlobTypes):
        if flags & MingleFlags.BLOB and isinstance(old, BlobTypes):
            if not new:
                return old
            return bytes(old) + new
    elif isinstance(new, Sequence):
        if flags & MingleFlags.LIST:
            if isinstance(old, Sequence) and not isinstance(old, str|BlobTypes):
                if not new:
                    return old
                out = list(old)
            else:
                out = [old]
            if flags & MingleFlags.LIST_AS_SET:
                out.extend(x for x in new if x not in old)
            else:
                out.extend(new)
            return cast(T, out)
    return new

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


_CHAR_POOL = string.printable + "\u03b1\u03b2\u03b3\u2020\u2021\u2022" #"\U00010330"
def _random_str(rng: Random, for_key=False):
    n = rng.randint(0, 20)
    if for_key and rng.randint(0, 1):
        # Generate within the
        return "".join(rng.choices("abcxyz+-._", k=n))
    return "".join(rng.choices(_CHAR_POOL, k=n))

def frid_random(rng: Random, depth: int=1, *, for_json: int=0):
    r = rng.randint(0, 32 if depth > 0 else 20)
    match r:
        case 0:
            return None
        case 1:
            return True
        case 2:
            return False
        case 3:
            if for_json:
                return None
            return datetime.now().replace(microsecond=0)
        case 4:
            if for_json:
                return True
            return dateonly.today()
        case 5:
            if for_json:
                return False
            return datetime.now().time().replace(microsecond=0)
        case 6 | 7 | 8 | 9:
            return _random_str(rng)
        case 10 | 11:
            if for_json:
                return _random_str(rng)
            return _random_str(rng).encode()
        case 12 | 13 | 14:
            return rng.randint(-10000, 10000)
        case 15:
            # Cannot use NaN as NaN != NaN
            if for_json == 1:
                return rng.choice([1.0, 0.0, -1.0, math.e, math.pi]) # no infinity
            return rng.choice([math.inf, 1.0, 0.0, -1.0, math.e, math.pi, -math.inf])
        case 16 | 17 | 18 | 19:
            return math.ldexp(rng.random() - 0.5, rng.randint(-40, 40))
        case 20 | 21 | 22 | 23 | 24 | 25:
            return [frid_random(rng, depth - 1, for_json=for_json)
                    for _ in range(rng.randint(0, 8))]
        case 26 | 27 | 28 | 29 | 30 | 31:
            return {
                _random_str(rng, True):
                    frid_random(rng, depth - 1, for_json=for_json)
                for _ in range(rng.randint(0, 8))
            }
