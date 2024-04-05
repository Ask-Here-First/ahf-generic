from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Concatenate, Generic, ParamSpec, TypeVar, overload

from .typing import BlobTypes, DateTypes, FridArray, FridMapVT, FridSeqVT, FridValue, StrKeyMap
from .guards import is_list_like
from .strops import str_transform
from .dumper import dump_into_str

P = ParamSpec('P')
T = TypeVar('T')
CompareFunc = Callable[Concatenate[T,FridValue,P],bool]
RecursiveFunc = Callable[Concatenate[T,FridValue,Callable[...,bool],P],bool]

class Comparator(Generic[P]):
    def __init__(
            self, *, default: bool=False,
            compare_none: CompareFunc[None,P]|None=None,
            compare_bool: CompareFunc[bool,P]|None=None,
            compare_real: CompareFunc[int|float,P]|None=None,
            compare_text: CompareFunc[str,P]|None=None,
            compare_blob: CompareFunc[BlobTypes,P]|None=None,
            compare_date: CompareFunc[DateTypes,P]|None=None,
            compare_list: RecursiveFunc[FridArray,P]|None=None,
            compare_dict: RecursiveFunc[StrKeyMap,P]|None=None
    ):
        self._default: bool = default
        self._compare_none: CompareFunc[None,P] = compare_none or self.is_none
        self._compare_bool: CompareFunc[bool,P] = compare_bool or self.equal_item
        self._compare_real: CompareFunc[int|float,P] = compare_real or self.equal_item
        self._compare_text: CompareFunc[str,P] = compare_text or self.equal_item
        self._compare_blob: CompareFunc[BlobTypes,P] = compare_blob or self.equal_item
        self._compare_date: CompareFunc[DateTypes,P] = compare_date or self.equal_item
        self._compare_list: RecursiveFunc[FridArray,P] = compare_list or self.equal_list
        self._compare_dict: RecursiveFunc[StrKeyMap,P] = compare_dict or self.equal_dict

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
    def __init__(self, prefix: str="${", suffix: str="}", default: FridValue=None):
        self.prefix = prefix
        self.suffix = suffix
        self.default = default
    def textuate(self, data: FridValue) -> str:
        """Convert data to text in the case it is in the middle of a string.
        This method can be overridden by a derived class
        """
        if isinstance(data, str):
            return data
        return dump_into_str(data)

    def evaluate(self, expr: str, values: StrKeyMap, default) -> FridValue:
        """Evaluate an expression against the values."""
        expr = expr.strip()
        if expr.endswith('*'):
            expr = expr[:-1]
            return {k[len(expr):]: v for k, v in values.items() if k.startswith(expr)}
        v = values.get(expr, default)
        if v is ...:
            return default
        return v

    def sub_text(self, s: str, values: StrKeyMap|None, default) -> FridValue:
        """Return the string `s` with placeholder variable replaced with values.
        If a variable does not exist in `values`
        - Returns `...` (ellipsis)  if the template contains only a single variable;
        - Returns as is if template contains more than a single variable.
        """
        if not values:
            return s
        if s.startswith(self.prefix) and s.endswith(self.suffix):
            name = s[2:-1]
            return self.evaluate(name, values, default)
        def _transform(s: str, start: int, bound: int, prefix: str):
            index = start + len(prefix)
            end = s.find(self.suffix, index, bound)
            if end < 0:
                if len(s) < bound:
                    raise IndexError(f"Search ends at {index}")
                raise ValueError(f"Missing '}}' at {index}")
            expr = s[index:end]
            return (len(self.suffix) + end, self.textuate(self.evaluate(expr, values, default)))
        return str_transform(s, {self.prefix: _transform})[1]
    _T = TypeVar('_T', bound=FridValue)
    @overload
    def sub(self, data: StrKeyMap, values: StrKeyMap|None=None) -> dict[str,FridMapVT]: ...
    @overload
    def sub(self, data: FridArray, values: StrKeyMap|None=None) -> list[FridSeqVT]: ...
    @overload
    def sub(self, data: str, values: StrKeyMap|None=None) -> FridValue: ...
    @overload
    def sub(self, data: _T, values: StrKeyMap|None=None) -> _T: ...
    def sub(self, data: FridValue, values: StrKeyMap|None=None) -> FridValue:
        """Substitute the placeholders in data (only for its values).
        The placeholders are escaped with `${......}` (only for string value).
        The enclosed string `......` is used as the key to get the actual value
        in `values`.
        """
        if isinstance(data, str):
            return self.sub_text(data, values, self.default)
        if isinstance(data, Mapping):
            return {k: ... if v is ... else self.sub(v, values) for k, v in data.items()}
        if isinstance(data, Sequence):
            # Special handling for array: array return value do "splice"
            out = []
            for v in data:
                r = self.sub(v, values)
                if r is ...:
                    continue
                if is_list_like(r):
                    out.extend(r)
                else:
                    out.append(r)
            return out
        return data
    __call__ = sub

def _callable_name(func: Callable) -> str:
    if hasattr(func, '__qualname__'):
        return func.__qualname__
    if hasattr(func, '__name__'):
        return func.__name__
    if hasattr(func, '__class__'):
        return func.__class__.__name__ + "()"
    return str(func)

def get_type_name(data) -> str:
    """Return the data type name."""
    return type(data).__name__

def get_func_name(func: Callable) -> str:
    """Returns the proper function names for regular or partial functions."""
    if not isinstance(func, partial):
        return _callable_name(func)
    if not func.args and not func.keywords:
        return _callable_name(func.func)
    name = _callable_name(func.func).removesuffix("()") + "("
    if func.args:
        name += ','.join(str(x) for x in func.args) + ",..."
    else:
        name += "..."
    if func.keywords:
        name += ',' + ','.join(str(k) + '=' + str(v) for k, v in func.keywords.items()) + ",..."
    return name + ")"
