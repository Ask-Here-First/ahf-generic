"""The Frid Value Store."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager, AbstractAsyncContextManager
from typing import TypeVar, overload


from ..typing import MISSING, BlobTypes, FridTypeSize
from ..typing import FridArray, FridSeqVT, FridValue, MissingType, StrKeyMap
from ..guards import as_kv_pairs, is_frid_array, is_frid_skmap
from . import utils
from .utils import VSPutFlag, VSListSel, VSDictSel, VStoreKey, VStoreSel, BulkInput


_T = TypeVar('_T')
_Self = TypeVar('_Self', bound='_BaseStore')  # TODO: remove this in 3.11

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
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
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
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
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

class ValueStore(_BaseStore):
    def finalize(self):
        pass
    @abstractmethod
    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_meta(self, *args: VStoreKey, keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
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
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        with self.get_lock():
            meta = self.get_meta(keys=(k for k, _ in pairs))
            if not utils.check_flags(flags, len(pairs), len(meta)):
                return 0
            # If Atomicity for bulk is set and any other flags are set, we need to check
            return sum(int(self.put_frid(k, v, flags)) for k, v in pairs)
    def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
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
    async def get_meta(self, *args: VStoreKey,
                       keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
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
    async def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        async with self.get_lock():
            meta = await self.get_meta(keys=(k for k, _ in pairs))
            if not utils.check_flags(flags, len(pairs), len(meta)):
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

