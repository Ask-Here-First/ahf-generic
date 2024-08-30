from collections.abc import Callable, Mapping, Sequence
from functools import partial
from enum import Flag
from typing import Concatenate, Generic, ParamSpec, TypeVar, cast, overload

from .typing import MISSING, BlobTypes, DateTypes, FridBasic, FridBeing, FridMixin, FridTypeSize, MissingType
from .typing import FridArray, FridMapVT, FridSeqVT, FridValue, StrKeyMap
from .chrono import dateonly, timeonly, datetime
from .guards import is_list_like
from .strops import str_transform
from .dumper import dump_frid_str

P = ParamSpec('P')
T = TypeVar('T')
PrimitiveCompFunc = Callable[Concatenate[T,FridValue,P],bool]
RecursiveCompFunc = Callable[Concatenate[T,FridValue,Callable[...,bool],P],bool]

class Comparator(Generic[P]):
    """A flexiable data comparitor."""
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
class Substitute:
    """Substitutes delimited variables in template strings into their values."""
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
        return str_transform(s, {self.prefix: _transform})[1]
    _T = TypeVar('_T', bound=FridValue)
    @overload
    def sub_data(self, data: StrKeyMap, values: StrKeyMap) -> dict[str,FridMapVT]: ...
    @overload
    def sub_data(self, data: FridArray, values: StrKeyMap) -> list[FridSeqVT]: ...
    @overload
    def sub_data(self, data: str, values: StrKeyMap) -> FridValue: ...
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

class MergeFlags(Flag):
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

def frid_merge(old: T|MissingType, new: T, *, depth: int=16, flags=MergeFlags.ALL) -> T:
    if old is MISSING:
        return new
    if isinstance(new, bool):
        if flags & MergeFlags.BOOL:
            return bool(old) or new
    elif isinstance(new, int|float):
        if flags & MergeFlags.REAL and isinstance(old, int|float|bool):
            return old + new
    elif isinstance(new, Mapping):
        if flags & MergeFlags.DICT and isinstance(old, Mapping):
            if not new:
                return old
            d = dict(old)
            for k, v in new.items():
                old_v = d.get(k, MISSING)
                if depth > 0:
                    v = frid_merge(old_v, v, depth=(depth - 1), flags=flags)
                if v is not MISSING:
                    d[k] = v
            return cast(T, d)
    elif isinstance(new, str):
        if flags & MergeFlags.TEXT and isinstance(old, str):
            if not new:
                return old
            return old + new
    elif isinstance(new, BlobTypes):
        if flags & MergeFlags.BLOB and isinstance(old, BlobTypes):
            if not new:
                return old
            return bytes(old) + new
    elif isinstance(new, Sequence):
        if flags & MergeFlags.LIST:
            if isinstance(old, Sequence) and not isinstance(old, str|BlobTypes):
                if not new:
                    return old
                out = list(old)
            else:
                out = [old]
            if flags & MergeFlags.LIST_AS_SET:
                out.extend(x for x in new if x not in old)
            else:
                out.extend(new)
            return cast(T, out)
    return new

def frid_type_size(data: FridValue) -> FridTypeSize:
    if data is None:
        return ('null', 0)
    if isinstance(data, str):
        return ('text', len(data))
    if isinstance(data, bool):
        return ('bool', 0)
    if isinstance(data, int|float):
        return ('real', 0)
    if isinstance(data, BlobTypes):
        return ('blob', len(data))
    if isinstance(data, dateonly|timeonly|datetime):
        return ('date', 0)
    if isinstance(data, Mapping):
        return ('dict', len(data))
    if isinstance(data, Sequence):
        return ('list', len(data))
    if isinstance(data, FridMixin|FridBasic):
        return ('frid', 0)
    return ('', -1)


def _callable_name(func: Callable) -> str:
    # if hasattr(func, '__qualname__'):
    #     return func.__qualname__
    if hasattr(func, '__name__'):
        return func.__name__
    if hasattr(func, '__class__'):  # pragma: no cover
        return func.__class__.__name__ + "()"
    return str(func)  # pragma: no cover

def get_qual_name(data) -> str:
    """Return the data's qualified name."""
    if hasattr(data, '__qualname__'):
        return data.__qualname__
    return type(data).__qualname__

def get_type_name(data) -> str:
    """Return the data type name."""
    if isinstance(data, type):  # If data is already a type, return its type name
        return data.__name__
    # Or return its type's type name
    return type(data).__name__

def get_func_name(func: Callable) -> str:
    """Returns the proper function names for regular or partial functions."""
    if not isinstance(func, partial):
        return _callable_name(func) + "()"
    if not func.args and not func.keywords:
        return _callable_name(func.func) + "()"
    name = _callable_name(func.func) + "("
    if func.args:
        name += ','.join(str(x) for x in func.args) + ",..."
    else:
        name += "..."
    if func.keywords:
        name += ',' + ','.join(str(k) + '=' + str(v) for k, v in func.keywords.items()) + ",..."
    return name + ")"
