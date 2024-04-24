from collections.abc import Iterable, Mapping, Sequence
from enum import Flag
from typing import Any, TypeGuard, TypeVar

from ..typing import MISSING, FridBeing, FridValue, MissingType
from ..guards import is_list_like


VStoreKey = str|tuple[str|int,...]
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

def list_select(
    val: Sequence[_T], sel: int|slice|tuple[int,int]
) -> Sequence[_T]|_T|MissingType:
    """Gets the selected elements in a sequence."""
    if isinstance(sel, int):
        return val[sel] if 0 <= sel < len(val) else MISSING
    if isinstance(sel, slice):
        return val[sel]
    if isinstance(sel, tuple) and len(sel) == 2:
        (index, until) =fix_indexes(sel, len(val))
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
@staticmethod
def _list_delete(val: list, sel: int|slice|tuple[int,int]) -> int:
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

def _dict_delete(val: dict[str,Any], sel: str|Iterable[str]) -> int:
    """Deletes the selected items in the dict.
    - Returns the number of items deleted.
    """
    if isinstance(sel, str):
        return 0 if val.pop(sel, MISSING) is MISSING else 1
    return sum(bool(val.pop(k, MISSING) is not MISSING) for k in sel)

