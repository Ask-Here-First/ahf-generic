"""The Frid Value Store."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence, Mapping
from contextlib import AbstractContextManager, AbstractAsyncContextManager
from enum import Flag
from typing import Any, TypeGuard, TypeVar, overload

from ..typing import MISSING, BlobTypes, FridTypeSize
from ..typing import FridArray, FridBeing, FridSeqVT, FridValue, MissingType, StrKeyMap
from ..guards import as_kv_pairs, is_frid_array, is_frid_skmap, is_list_like

VStoreKey = str|tuple[str|int,...]
VSListSel = int|slice|tuple[int,int]|None
VSDictSel = str|Iterable[str]|None
VStoreSel = VSListSel|VSDictSel
VStorePutBulkData = Mapping[VStoreKey,FridValue]|Sequence[tuple[VStoreKey,FridValue]]|Iterable

_T = TypeVar('_T')
_Self = TypeVar('_Self', bound='_BaseStore')  # TODO: remove this in 3.11

class VSPutFlag(Flag):
    UNCHECKED = 0       # Special value to skip all the checks
    ATOMICITY = 0x80    # Only for bulk writes: the write has to be all successful to no change
    NO_CREATE = 0x40    # Do not create a new entry if the key is missing
    NO_CHANGE = 0x20    # Do not change existing entry; skip all following if set
    KEEP_BOTH = 0x10    # Keep both existing data and new data, using frid_merge()
    # TODO additional flags to pass to for frid_merge()

class _BaseStore(ABC):
    @abstractmethod
    def substore(self: _Self, name: str, *args: str) -> _Self:
        """Returns a substore ValueStore as given by a list of names."""
        raise NotImplementedError  # pragma: no cover

    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        """Returns an reentrant lock for desired concurrency."""
        raise NotImplementedError  # pragma: no cover
    def finalize(self):
        """Calling to finalize this store before drop the reference."""
        raise NotImplementedError  # pragma: no cover
    def get_meta(self, keys: Iterable[VStoreKey]) -> Mapping[VStoreKey,FridTypeSize]:
        """Gets the meta data of a list of `keys` and returns a map for existing keys.
        Notes: There is no atomicity guarantee for this method.
        """
        raise NotImplementedError  # pragma: no cover
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        """Gets the value of the given `key` in the value store.
        - If `sel` is specified, uses the selection rule to select the partial data to return.
        - If the value of the key is missing, returns MISSING.
        There are a number of type specific get methods (get_{text,blob,list,dict}()).
        By default, those methods will call get_frid() method and then verify
        the type of return data; however implementations may choose to implement
        those methods separately, or even call those functions using `sel` as a hint.
        """
        raise NotImplementedError  # pragma: no cover
    def put_frid(self, key: VStoreKey, val: FridValue, /, flags=VSPutFlag.UNCHECKED) -> bool:
        """Puts the value `val` into the store for the given `key`.
        - Returns true iff the storage changes.
        """
        raise NotImplementedError  # pragma: no cover
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        """Deletes the data associated with the given `key` from the store.
        - Returns true iff the storage changes.
        """
        raise NotImplementedError  # pragma: no cover
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        """Returns the data associated with a list of keys in the store."""
        raise NotImplementedError  # pragma: no cover
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        """Puts the data in the into the store.
        - `data`: either a key/value pairs or a list of tuple of key/value pairs
        """
        raise NotImplementedError  # pragma: no cover
    def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
        """Deletes the keys from the storage and returns the number of keys deleted.
        - Returns the number of keys deleted from the store.
        """
        raise NotImplementedError  # pragma: no cover

    def get_text(self, key: VStoreKey, /, alt: _T=None) -> str|_T:
        """Gets the text value associated with the given `key`.
        - If the entry exists but is not of text type, it can either return
          the string representation of the value, or reaise an exception.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        """Gets the blob value associated with the given `key`.
        - If the entry exists but is not of blob type, it can either return
          the binary representation of the value, or reaise an exception.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover
    @overload
    def get_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None=None,
                 /, alt: _T=None) -> FridArray|_T: ...
    def get_list(self, key: VStoreKey, sel: VSListSel=None, /, alt: _T=None) -> FridValue|_T:
        """Gets the list value associated with the given `key`.
        - If the selector `sel` is specified, it will be applied to the value.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover
    @overload
    def get_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                 /, alt: _T=None) -> StrKeyMap|_T: ...
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None, /, alt: _T=None) -> FridValue|_T:
        """Gets the dict value associated with the given `key`.
        - If the selector `sel` is specified, it will be applied to the value.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover

    # The following are the helper methods
    @staticmethod
    def _check_flags(flags: VSPutFlag, total_count: int, exist_count: int) -> bool:
        """Checking if keys exists to decide if the atomic put_bulk operation can succeed."""
        if flags & VSPutFlag.ATOMICITY and flags & (VSPutFlag.NO_CREATE | VSPutFlag.NO_CHANGE):
            if flags & VSPutFlag.NO_CREATE:
                return exist_count >= total_count
            if flags & VSPutFlag.NO_CHANGE:
                return exist_count <= 0
            # TODO: what to do for other flags: no need to check if result is not affected
        return True
    @staticmethod
    def _is_list_sel(sel) -> TypeGuard[VSListSel]:
        return isinstance(sel, int|slice) or (
            isinstance(sel, tuple) and len(sel) == 2
            and isinstance(sel[0], int) and isinstance(sel[1], int)
        )
    @staticmethod
    def _is_dict_sel(sel) -> TypeGuard[VSDictSel]:
        return isinstance(sel, str) or is_list_like(sel, str)
    @staticmethod
    def _consecutive(sel: VSListSel) -> bool:
        """Returns true if the selection is the consecutive indexes."""
        return not isinstance(sel, slice) or sel.step is None or sel.step == 1
    @staticmethod
    def _list_bounds(sel: VSListSel) -> tuple[int,int]:
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
    @staticmethod
    def _fix_indexes(sel: tuple[int,int], val_len: int):
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
    @staticmethod
    def _list_select(
        val: Sequence[_T], sel: int|slice|tuple[int,int]
    ) -> Sequence[_T]|_T|MissingType:
        """Gets the selected elements in a sequence."""
        if isinstance(sel, int):
            return val[sel] if 0 <= sel < len(val) else MISSING
        if isinstance(sel, slice):
            return val[sel]
        if isinstance(sel, tuple) and len(sel) == 2:
            (index, until) = __class__._fix_indexes(sel, len(val))
            return val[index:until]
        raise ValueError(f"Invalid selector type {type(sel)}")
    @staticmethod
    def _dict_select(
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
            (index, until) = __class__._fix_indexes(sel, len(val))
            del val[index:until]
            return len(val) - old_len
        raise ValueError(f"Invalid sequence selector type {type(sel)}")
    @staticmethod
    def _dict_delete(val: dict[str,Any], sel: str|Iterable[str]) -> int:
        """Deletes the selected items in the dict.
        - Returns the number of items deleted.
        """
        if isinstance(sel, str):
            return 0 if val.pop(sel, MISSING) is MISSING else 1
        return sum(bool(val.pop(k, MISSING) is not MISSING) for k in sel)

class ValueStore(_BaseStore):
    def finalize(self):
        pass
    @abstractmethod
    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        # Make the getter abstract
        raise NotImplementedError  # pragma: no cover
    def put_frid(self, key: VStoreKey, val: FridValue, /, flags=VSPutFlag.UNCHECKED) -> bool:
        raise NotImplementedError  # pragma: no cover
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        raise NotImplementedError  # pragma: no cover
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        with self.get_lock():
            return [v if (v := self.get_frid(k)) is not MISSING else alt for k in keys]
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        with self.get_lock():
            meta = self.get_meta(k for k, _ in pairs)
            if not self._check_flags(flags, len(pairs), len(meta)):
                return 0
            # If Atomicity for bulk is set and any other flags are set, we need to check
            return sum(int(self.put_frid(k, v, flags)) for k, v in pairs)
    def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
        """Deletes the keys from the storage and returns the number of keys deleted.
        - Returns the number of keys deleted from the store.
        """
        with self.get_lock():
            return sum(int(self.del_frid(k)) for k in keys)
    def get_text(self, key: VStoreKey, /, alt: _T=None) -> str|_T:
        data = self.get_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, str), type(data)
        return data
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        data = self.get_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, BlobTypes), type(data)
        return data
    def get_list(self, key: VStoreKey, sel: VSListSel=None, /, alt: _T=None) -> FridValue|_T:
        data = self.get_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, int):
            assert is_frid_array(data), type(data)
        return data
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None, /, alt: _T=None) -> FridValue|_T:
        data = self.get_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert is_frid_skmap(data), type(data)
        return data

class AsyncStore(_BaseStore):
    # Override all methods if signature is different
    async def finalize(self):
        pass
    @abstractmethod
    def get_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|MissingType:
        raise NotImplementedError  # pragma: no cover
    async def put_frid(self, key: VStoreKey, val: FridValue,
                       /, flags=VSPutFlag.UNCHECKED) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def get_bulk(self, keys: Iterable[VStoreKey],
                       /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        async with self.get_lock():
            return [v if (v := await self.get_frid(k)) is not MISSING else alt for k in keys]
    async def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        async with self.get_lock():
            meta = await self.get_meta(k for k, _ in pairs)
            if not self._check_flags(flags, len(pairs), len(meta)):
                return 0
            count = 0
            for k, v in pairs:
                if await self.put_frid(k, v, flags):
                    count += 1
            return count
    async def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        async with self.get_lock():
            count = 0
            for k in keys:
                if await self.del_frid(k):
                    count += 1
            return count
    async def get_text(self, key: VStoreKey, alt: _T=None) -> str|_T:
        data = await self.get_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, str), type(data)
        return data
    async def get_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        data = await self.get_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, BlobTypes), type(data)
        return data
    @overload
    async def get_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def get_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None,
                        /, alt: _T=None) -> FridArray|_T: ...
    async def get_list(self, key: VStoreKey, sel: VSListSel, /, alt: _T=None) -> FridValue|_T:
        data = await self.get_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, int):
            assert is_frid_array(data), type(data)
        return data
    @overload
    async def get_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def get_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                        /, alt: _T=None) -> StrKeyMap|_T: ...
    async def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=None) -> FridValue|_T:
        data = await self.get_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert is_frid_skmap(data), type(data)
        return data

