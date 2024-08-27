from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Literal, TypeGuard, TypeVar, overload

from .typing import (
    BlobTypes, DateTypes, FridBasic, FridPrime, FridBeing,
    FridSeqVT, FridMapVT, FridArray, FridMixin, FridValue, StrKeyMap
)

K = TypeVar('K')
V = TypeVar('V')

def is_text_list_like(data, /) -> TypeGuard[Sequence[str]]:
    """Type guard for a sequence of string elements."""
    if not isinstance(data, Sequence) or isinstance(data, str|BlobTypes):
        return False
    return all(isinstance(x, str) for x in data)

def is_blob_list_like(data, /) -> TypeGuard[Sequence[BlobTypes]]:
    """Type guard for a sequence of binary elements."""
    if not isinstance(data, Sequence) or isinstance(data, str|BlobTypes):
        return False
    return all(isinstance(x, BlobTypes) for x in data)

@overload
def is_list_like(
        data, /, etype: None=None, *, allow_none: bool=False
) -> TypeGuard[Sequence]: ...
@overload
def is_list_like(
        data, /, etype: type[V], *, allow_none: Literal[False]=False
) -> TypeGuard[Sequence[V]]: ...
@overload
def is_list_like(
        data, /, etype: type[V], *, allow_none: Literal[True]|bool
) -> TypeGuard[Sequence[V|None]]: ...
@overload
def is_list_like(
        data, /, etype: Callable[[Any],TypeGuard[V]], *, allow_none: Literal[False]=False
) -> TypeGuard[Sequence[V]]: ...
@overload
def is_list_like(
        data, /, etype: Callable[[Any],TypeGuard[V]], *, allow_none: Literal[True]|bool
) -> TypeGuard[Sequence[V|None]]: ...
def is_list_like(
        data, /, etype: Callable[[Any],bool]|type|None=None, *, allow_none: bool=False
) -> TypeGuard[Sequence]:
    """Type guard for a sequence type.
    Arguments:
    - `data`: the input data to be type-checked.
    - `etype`: the type for individual elements (default: any).
    - `allow_none`: if set to true, the element values is allowed to be None
      in addition to the given `etype`.
    """
    if not isinstance(data, Sequence):
        return False
    if isinstance(data, str|BlobTypes):
        return False
    if etype is None:
        return True
    if isinstance(etype, type):
        if not allow_none:
            return all(isinstance(x, etype) for x in data)
        return all(isinstance(x, etype) or x is None for x in data)
    if callable(etype):
        if not allow_none:
            return all(etype(x) for x in data)
        return all(etype(x) or x is None for x in data)
    raise ValueError(f"Invalid etype specification: {etype}")

@overload
def is_dict_like(
        data, /, vtype: None=None, *, allow_none=False
) -> TypeGuard[Mapping]: ...
@overload
def is_dict_like(
        data, /, vtype: type[V], ktype: type[K]=str, *, allow_none: Literal[False]=False
) -> TypeGuard[Mapping[K,V]]: ...
@overload
def is_dict_like(
        data, /, vtype: type[V], ktype: type[K]=str, *, allow_none: Literal[True]|bool
) -> TypeGuard[Mapping[K,V|None]]: ...
@overload
def is_dict_like(
        data, /, vtype: Callable[[Any],TypeGuard[V]], ktype: type[K]=str,
        *, allow_none: Literal[False]=False
) -> TypeGuard[Mapping[K,V]]: ...
@overload
def is_dict_like(
        data, /, vtype: Callable[[Any],TypeGuard[V]], ktype: type[K]=str,
        *, allow_none: Literal[True]
) -> TypeGuard[Mapping[K,V|None]]: ...
@overload
def is_dict_like(
        data, /, vtype: Callable[[Any],bool], ktype: type[K]=str,
        *, allow_none: Literal[False]=False
) -> TypeGuard[Mapping[K,Any]]: ...
@overload
def is_dict_like(
        data, /, vtype: Callable[[Any],bool], ktype: type[K]=str,
        *, allow_none: Literal[True]|bool
) -> TypeGuard[Mapping[K,Any]]: ...
def is_dict_like(
        data, /, vtype: Callable[[Any],bool]|type|None=None,
        ktype: type=str, *, allow_none=False
) -> TypeGuard[Mapping]:
    """Type guard for a map type.
    Arguments:
    - `data`: the input data to be type-checked.
    - `vtype`: the type for values (default: any).
    - `ktype`: the type for keys (default: `str`)
    - `allow_none`: if set to true, the element values is allowed to be None
      in addition to the `etype`.
    """
    if not isinstance(data, Mapping):
        return False
    if ktype is not None and not all(isinstance(x, ktype) for x in data.keys()):
        return False
    if vtype is None:
        return True
    if isinstance(vtype, type):
        if not allow_none:
            return all(isinstance(x, vtype) for x in data.values())
        return all(isinstance(x, vtype) or x is None for x in data.values())
    if callable(vtype):
        if not allow_none:
            return all(vtype(x) for x in data.values())
        return all(vtype(x) or x is None for x in data.values())
    raise ValueError(f"Invalid etype specification: {vtype}")


@overload
def as_type(dtype: type[V], data, /, allow_none: Literal[False]=False) -> V: ...
@overload
def as_type(dtype: type[V], data, /, allow_none: Literal[True]|bool) -> V|None: ...
def as_type(dtype: type[V], data, /, allow_none: bool=False) -> V|None:
    if data is None and allow_none:
        return None
    if not isinstance(data, dtype):
        raise ValueError(f"Expecting type {dtype}, got {type(data).__name__}")
    return data
@overload
def get_as_type(dtype: type[V], map: StrKeyMap, /, key: str, default: V|None=None,
                *, allow_none: Literal[False]=False) -> V: ...
@overload
def get_as_type(dtype: type[V], map: StrKeyMap, /, key: str, default: V|None=None,
                *, allow_none: Literal[True]|bool) -> V|None: ...
def get_as_type(
        dtype: type[V], map: StrKeyMap, /, key: str, default=None, *, allow_none: bool=False
) -> V|None:
    return as_type(dtype, map.get(key, default), allow_none=allow_none)

def as_kv_pairs(
        data: Sequence[tuple[K,V]]|Mapping[K,V]|Iterable
) -> Sequence[tuple[K,V]]:
    """Converts the `data` to a sequence of key value pairs.
    - If the `data` is a map, convert it to a list of pairs.
    - If the `data` is already a sequence, return it as is.
    - If the `data` is an iterable but not a sequence, convert it to a list.
    The last operation will avoid the issue of non-repeatble iterable like
    generators by reloading it to a list.
    """
    if isinstance(data, Mapping):
        return list(data.items())
    if isinstance(data, Sequence):
        return data
    if isinstance(data, Iterable):
        return list(data)
    raise ValueError(f"The input is not an iterable: {type(data)}")


def is_frid_prime(data) -> TypeGuard[FridPrime]:
    return data is None or isinstance(data, str|BlobTypes|DateTypes|int|float|bool|FridBasic)
def is_frid_seqvt(data) -> TypeGuard[FridSeqVT]:
    return is_frid_value(data)
def is_frid_array(data) -> TypeGuard[FridArray]:
    return is_list_like(data, is_frid_seqvt)
def is_frid_mapvt(data) -> TypeGuard[FridMapVT]:
    return is_frid_value(data) or isinstance(data, FridBeing)
def is_frid_skmap(data) -> TypeGuard[StrKeyMap]:
    return is_dict_like(data, is_frid_mapvt)
def is_frid_value(data) -> TypeGuard[FridValue]:
    return (is_frid_prime(data) or is_frid_array(data) or is_frid_skmap(data)
            or isinstance(data, FridMixin) or (isinstance(data, set) and all(
                is_frid_value(v) for v in data
            )))
FridMixin._is_frid_value = is_frid_value

def is_identifier_head(c: str) -> bool:
    """Returns if `c` can be first character of an indentifier."""
    return c.isalpha() or c in "_"

def is_identifier_char(c: str) -> bool:
    """Returns if `c` can be middle character of an indentifier."""
    return c.isalnum() or c in "_.+-"

def is_identifier_tail(c: str) -> bool:
    """Returns if `c` can be last character of an indentifier."""
    return c.isalnum() or c in "_"

def is_frid_identifier(s) -> TypeGuard[str]:
    """Returns if `s` is a valid identifier in FRID.
    An FRID identifier must:
    - Start with a letter or _,
    - Contain letters, digits, and other characters in this list`._+-`,
    - But not end with `.+-`.
    """
    if not s or not isinstance(s, str):
        return False
    if s[0] == '.' and len(s) >= 2:
        s = s[1:]   # If starting with . check the reset if it is an identifier
    return is_identifier_head(s[0]) and all(
        is_identifier_char(c) for c in s[1:-1]
    ) and is_identifier_tail(s[-1])

def is_quote_free_head(c: str) -> bool:
    """Returns if `c` can be first character of a quote-free string."""
    return c.isalpha() or c in "_$"

def is_quote_free_char(c: str) -> bool:
    """Returns if `c` can be middle character of a quote-free string."""
    return c.isalnum() or c in " _.+-$@%"

def is_quote_free_tail(c: str) -> bool:
    """Returns if `c` can be last character of a quote-free string."""
    return c.isalnum() or c in "_.+-$%"

def is_frid_quote_free(data) -> TypeGuard[str]:
    """Returns if a data value is a string that does not need to be quoted.
    An FRID quote free value must:
    - Start with a letter or `_$`,
    - Contain letters, digits, and other characters in this list `._+-@$` or single spaces,
    - But not end with `$@`.
    """
    if not data or not isinstance(data, str):
        return False
    if data[0] == '.' and len(data) >= 2:
        data = data[1:]   # If starting with . check the reset if it is an identifier
    return is_quote_free_head(data[0]) and all(
        is_quote_free_char(c) for c in data[1:-1]
    ) and is_quote_free_tail(data[-1]) and "  " not in data
