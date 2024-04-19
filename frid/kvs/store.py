"""The Frid Value Store."""

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Collection, Iterable, Sequence
from contextlib import AbstractContextManager, AbstractAsyncContextManager
from enum import Flag
from typing import Any, Mapping, TypeVar, overload

from ..typing import MISSING, BlobTypes, FridTypeSize
from ..typing import FridArray, FridBeing, FridSeqVT, FridValue, MissingType, StrKeyMap
from ..guards import as_kv_pairs, is_frid_array, is_frid_skmap

VStoreKey = str|tuple[str|int,...]
VSListSel = int|slice|tuple[int,int]|None
VSDictSel = str|Iterable[str]|None
VStoreSel = VSListSel|VSDictSel
VStorePutBulkData = Mapping[VStoreKey,FridValue]|Sequence[tuple[VStoreKey,FridValue]]|Iterable

_T = TypeVar('_T')

class VSPutFlag(Flag):
    UNCHECKED = 0       # Special value to skip all the checks
    ATOMICITY = 0x80    # Only for bulk writes: the write has to be all successful to no change
    NO_CREATE = 0x40    # Do not create a new entry if the key is missing
    NO_CHANGE = 0x20    # Do not change existing entry; skip all following if set
    KEEP_BOTH = 0x10    # Keep both existing data and new data, using frid_merge()
    # TODO additional flags to pass to for frid_merge()

class ValueStore(ABC):
    @abstractmethod
    def substore(self, name: str, *args: str) -> 'ValueStore':
        """Returns a substore ValueStore as given by a list of names."""
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        """Returns an reentrant lock for desired concurrency."""
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_meta(self, keys: Iterable[VStoreKey]) -> Mapping[VStoreKey,FridTypeSize]:
        """Gets the meta data of a list of `keys` and returns a map for existing keys.
        Notes: There is no atomicity guarantee for this method.
        """
        raise NotImplementedError  # pragma: no cover
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        """Gets the value of the given `key` in the value store.
        - If `sel` is specified, use the selection rule to select the partial data to return.
        - If the value of the key is missing, return MISSING.
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
        with self.get_lock():
            return [v if (v := self.get_frid(k)) is not MISSING else alt for k in keys]
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        """Puts the data in the into the store.
        - `data`: either a key/value pairs or a list of tuple of key/value pairs
        """
        pairs = as_kv_pairs(data)
        with self.get_lock():
            if not self._check_atomic(flags, pairs):
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
    @overload
    def get_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None=None,
                 /, alt: _T=None) -> FridArray|_T: ...
    def get_list(self, key: VStoreKey, sel: VSListSel=None, /, alt: _T=None) -> FridValue|_T:
        data = self.get_frid(key, sel)
        if data is MISSING:
            return alt
        assert is_frid_array(data), type(data)
        return data
    @overload
    def get_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                 /, alt: _T=None) -> StrKeyMap|_T: ...
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None, /, alt: _T=None) -> FridValue|_T:
        data = self.get_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert is_frid_skmap(data), type(data)
        return data

    @abstractmethod
    def aget_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def aget_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
    async def aget_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|MissingType:
        raise NotImplementedError  # pragma: no cover
    async def aput_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def adel_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def aget_bulk(self, keys: Iterable[VStoreKey],
                        /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        async with self.aget_lock():
            return [v if (v := await self.aget_frid(k)) is not MISSING else alt for k in keys]
    async def aput_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        async with self.aget_lock():
            if not self._check_atomic(flags, pairs):
                return 0
            count = 0
            for k, v in pairs:
                if await self.aput_frid(k, v, flags):
                    count += 1
            return count
    async def adel_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        async with self.aget_lock():
            count = 0
            for k in keys:
                if await self.adel_frid(k):
                    count += 1
            return count
    async def aget_text(self, key: VStoreKey, alt: _T=None) -> str|_T:
        data = await self.aget_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, str), type(data)
        return data
    async def aget_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        data = await self.aget_frid(key)
        if data is MISSING:
            return alt
        assert isinstance(data, BlobTypes), type(data)
        return data
    @overload
    async def aget_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def aget_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None,
                        /, alt: _T=None) -> FridArray|_T: ...
    async def aget_list(self, key: VStoreKey, sel: VSListSel, /, alt: _T=None) -> FridValue|_T:
        data = await self.aget_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, int):
            assert is_frid_array(data), type(data)
        return data
    @overload
    async def aget_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def aget_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                        /, alt: _T=None) -> StrKeyMap|_T: ...
    async def aget_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=None) -> FridValue|_T:
        data = await self.aget_frid(key, sel)
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert is_frid_skmap(data), type(data)
        return data

    def _check_atomic(self, flags: VSPutFlag, pairs: Collection[tuple[VStoreKey,Any]]) -> bool:
        """Checking if keys exists to decide if the atomic bulk operation can succeed."""
        if flags & VSPutFlag.ATOMICITY and flags & (VSPutFlag.NO_CREATE | VSPutFlag.NO_CHANGE):
            count = len(self.get_meta(k for k, _ in pairs))
            if flags & VSPutFlag.NO_CREATE:
                return count >= len(pairs)
            if flags & VSPutFlag.NO_CHANGE:
                return count <= 0
            # TODO: what to do for other flags: no need to check if result is not affected
        return True

class AsyncToSyncStoreMixin(ValueStore):
    """This mixin converts the sync value store API to an async one.

    This mixin should only be used to the implementation that are generally
    considered as non-blocking (e.g., in memory or fast disk.)
    Assuming there is already a sync version of the class calls MySyncStore
    that implements ValueStore, one can just use
    ```
        class MyAsyncStore(AsyncToSyncValueStoreMixin, MySyncStore):
            pass
    ```
    """
    async def aget_lock(self, name: str|None=None):
        raise NotImplementedError  # pragma: no cover --- not going to be used
    async def aget_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return self.get_meta(keys)
    async def aget_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|FridBeing:
        return self.get_frid(key, sel)
    async def aput_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return self.put_frid(key, val, flags)
    async def adel_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return self.del_frid(key, sel)
    async def aget_bulk(self, keys: Iterable[VStoreKey],
                        /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        return self.get_bulk(keys, alt)
    async def aput_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        return self.put_bulk(data, flags)
    async def adel_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return self.del_bulk(keys)

class SyncToAsyncStoreMixin(ValueStore):
    """This mixin converts the async value store API to a sync one with asyncio.run().

    Assuming there is already a sync version of the class calls MySyncStore
    that implements ValueStore, one can just use
    ```
        class MyAsyncStore(AsyncToSyncValueStoreMixin, MySyncStore):
            pass
    ```
    """
    def get_lock(self, name: str|None=None):
        raise NotImplementedError  # pragma: no cover --- not going to be used
    def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return asyncio.run(self.aget_meta(keys))
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|FridBeing:
        return asyncio.run(self.aget_frid(key, sel))
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return asyncio.run(self.aput_frid(key, val, flags))
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return asyncio.run(self.adel_frid(key, sel))
    def get_bulk(self, keys: Iterable[VStoreKey],
                        /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        return asyncio.run(self.aget_bulk(keys, alt))
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        return asyncio.run(self.aput_bulk(data, flags))
    def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return asyncio.run(self.adel_bulk(keys))
