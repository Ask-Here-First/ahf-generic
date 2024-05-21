from collections.abc import Iterable, Mapping, Sequence
from enum import Flag
from typing import Any, TypeGuard, TypeVar, cast

from ..typing import MISSING, FridBeing, FridValue, MissingType
from ..guards import is_frid_array, is_frid_skmap, is_list_like


VStoreKey = str|tuple[str|int,...]
KeySearch = str|tuple[str|int|None,...]|None
VSListSel = int|slice|tuple[int,int]|None
VSDictSel = str|Iterable[str]|None
VStoreSel = VSListSel|VSDictSel
BulkInput = Mapping[VStoreKey,FridValue]|Sequence[tuple[VStoreKey,FridValue]]|Iterable

class VSPutFlag(Flag):
    UNCHECKED = 0       # Special value to skip all the checks
    ATOMICITY = 0x80    # Only for bulk writes: the write has to be all successful to no change
    NO_CREATE = 0x40    # Do not create a new entry if the key is missing
    NO_CHANGE = 0x20    # Do not change existing entry; skip all following if set
    KEEP_BOTH = 0x10    # Keep both existing data and new data, using frid_merge()
    # TODO additional flags to pass to for frid_merge()

    def __bool__(self):
        return bool(self.value)

_K = TypeVar('_K')
_T = TypeVar('_T')

def check_flags(flags: VSPutFlag, total_count: int, exist_count: int) -> bool:
    """Checking if keys exists to decide if the atomic put_bulk operation can succeed."""
    if flags & VSPutFlag.ATOMICITY and flags & (VSPutFlag.NO_CREATE | VSPutFlag.NO_CHANGE):
        if flags & VSPutFlag.NO_CREATE:
            return exist_count >= total_count
        if flags & VSPutFlag.NO_CHANGE:
            return exist_count <= 0
        # TODO: what to do for other flags: no need to check if result is not affected
    return True

def match_key(key: VStoreKey, pat: KeySearch) -> bool:
    if pat is None:
        return True
    if isinstance(pat, str|int):
        if isinstance(key, str|int):
            return str(key) == str(pat)
        if isinstance(key, tuple):
            return len(key) == 1 and str(key[0]) == str(pat)
        return False
    if isinstance(pat, tuple):
        if isinstance(key, str|int):
            return len(pat) == 1 and (pat[0] is None or str(key) == str(pat[0]))
        if isinstance(key, tuple):
            return len(key) == len(pat) and all(
                p is None or str(k) == str(p) for k, p in zip(key, pat)
            )
    return False

def is_list_sel(sel) -> TypeGuard[VSListSel]:
    return isinstance(sel, int|slice) or (
        isinstance(sel, tuple) and len(sel) == 2
        and isinstance(sel[0], int) and isinstance(sel[1], int)
    )

def is_dict_sel(sel) -> TypeGuard[VSDictSel]:
    return isinstance(sel, str) or is_list_like(sel, str)

def is_straight(sel: VSListSel) -> bool:
    """Returns true if the selection indexes is a straight (consecutive indexes)."""
    return not isinstance(sel, slice) or sel.step is None or sel.step == 1

def list_bounds(sel: VSListSel) -> tuple[int,int]:
    """Returns the index (may be negative) of the first and the last element."""
    if isinstance(sel, int):
        return (sel, sel)
    if isinstance(sel, tuple):
        (index, until) = sel
        return (index, until - 1)
    if isinstance(sel, slice):
        if sel.step and sel.step < 0:
            return ((sel.stop or 0) + 1, sel.start)
        return (sel.start or 0, (sel.stop or 0) - 1)
    raise ValueError(f"Invalid list selector type {type(sel)}: {sel}")

def fix_indexes(sel: tuple[int,int], val_len: int):
    """Fixes the pair of indexes to handle negative indexes.
    - `val_len`: the length of the value, needed for negative indexes.
    """
    (index, until) = sel
    if not len(sel) == 2 or not isinstance(index, int) or not isinstance(until, int):
        raise ValueError(f"Invalid selector: {sel}")
    if index < 0:
        index += val_len
        if index < 0:
            index = 0
    if until <= 0:
        until += val_len
        if until < 0:
            until = 0
    return (index, until)

def list_concat(seq1: Iterable[_T]|None, seq2: Iterable[_T]|None) -> Iterable[_T]:
    if seq2 is None:
        return [] if seq1 is None else seq1
    if seq1 is None:
        return seq2
    list0 = list(seq1)
    list0.extend(seq2)
    return list0

def dict_concat(map1: Mapping[_K,_T]|None, map2: Mapping[_K,_T]|None) -> Mapping[_K,_T]:
    """Merges the two dictionary into a single one."""
    if map1 is None:
        return map2 if map2 is not None else {}
    if map2 is None:
        return map1
    m = dict(map1)
    m.update(map2)
    return m

def list_remove_all(seq: list, item) -> int:
    index: int = 0
    count: int = 0
    while index < len(seq):
        if seq[index] == item:
            seq.pop(index)
            count += 1
        else:
            index += 1
    return count

def list_select(
    val: Sequence[_T], sel: int|slice|tuple[int,int]
) -> Sequence[_T]|_T|MissingType:
    """Gets the selected elements in a sequence."""
    if isinstance(sel, int):
        return val[sel] if 0 <= sel < len(val) else MISSING
    if isinstance(sel, slice):
        return val[sel]
    if isinstance(sel, tuple) and len(sel) == 2:
        (index, until) = fix_indexes(sel, len(val))
        return val[index:until]
    raise ValueError(f"Invalid selector type {type(sel)}")

def dict_select(
        val: Mapping[str,_T], sel: str|Iterable[str]
) -> Mapping[str,_T]|_T|MissingType:
    """Gets the selected elements in a mapping."""
    if sel is None:
        return val
    if isinstance(sel, str):
        return val.get(sel, MISSING)
    if isinstance(sel, Iterable):
        return {k: v for k in val if not isinstance((v := val.get(k, MISSING)), FridBeing)}
    raise ValueError(f"Invalid selector type {type(sel)}")

def frid_select(val: FridValue, sel: VStoreSel) -> FridValue|MissingType:
    """Returns sublist/subdict of `val` according to the selector `sel`."""
    if sel is None:
        return val
    if isinstance(val, Mapping):
        out = dict_select(val, cast(str|Iterable[str], sel))
    elif isinstance(val, Sequence):
        out = list_select(val, cast(int|slice|tuple[int,int], sel))
    else:
        raise ValueError(f"Selector is not None for data type {type(val)}")
    if out is MISSING:
        return MISSING
    assert not isinstance(out, FridBeing)
    return out

def list_delete(val: list, sel: int|slice|tuple[int,int]) -> int:
    """Deletes the selected items in the list.
    - Returns the number of items deleted.
    """
    if isinstance(sel, int):
        if 0 <= sel < len(val):
            del val[sel]
            return 1
        return 0
    old_len = len(val)
    if isinstance(sel, slice):
        del val[sel]
        return len(val) - old_len
    if isinstance(sel, tuple):
        (index, until) = fix_indexes(sel, len(val))
        del val[index:until]
        return len(val) - old_len
    raise ValueError(f"Invalid sequence selector type {type(sel)}")

def dict_delete(val: dict[str,Any], sel: str|Iterable[str]) -> int:
    """Deletes the selected items in the dict.
    - Returns the number of items deleted.
    """
    if isinstance(sel, str):
        return 0 if val.pop(sel, MISSING) is MISSING else 1
    return sum(bool(val.pop(k, MISSING) is not MISSING) for k in sel)

def frid_delete(data: _T, sel: VStoreSel) -> tuple[_T|list|dict[str,Any],int]:
    """Deletes sublist/subdict of `val` according to the selector `sel`.
    - Returns a pair of updated data and number of deleted items.
    This function will change data if the data is a list of a dict.
    """
    if sel is None:
        return (data, 0)
    if is_frid_skmap(data):
        new_dict = data if isinstance(data, dict) else dict(data)
        cnt = dict_delete(new_dict, cast(str|Iterable[str], sel))
        return (new_dict, cnt)
    if is_frid_array(data):
        new_list = data if isinstance(data, list) else list(data)
        cnt = list_delete(new_list, cast(int|slice|tuple[int,int], sel))
        return (new_list, cnt)
    raise ValueError(f"Data type {type(data)} does not support partial removal")
