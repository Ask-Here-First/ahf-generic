from collections.abc import Iterable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import TypeVar

from ..typing import MISSING, BlobTypes, FridBeing, FridSeqVT, FridTypeSize, FridValue
from .store import VSDictSel, VSListSel, VSPutFlag, VStoreKey, VStorePutBulkData, VStoreSel
from .store import AsyncToSyncStoreMixin, SyncToAsyncStoreMixin, ValueStore


_T = TypeVar('_T')

class ProxyStore(ValueStore):
    def __init__(self, store: ValueStore):
        self._store = store

    def substore(self, name: str, *args: str) -> 'ValueStore':
        return self._store.substore(name, *args)

    def get_lock(self, name: str|None=None):
        return self._store.get_lock(name)
    def finalize(self):
        return self._store.finalize()
    def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return self._store.get_meta(keys)
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|FridBeing:
        return self._store.get_frid(key, sel)
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return self._store.put_frid(key, val, flags)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return self._store.del_frid(key, sel)
    def get_bulk(self, keys: Iterable[VStoreKey],
                        /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        return self._store.get_bulk(keys, alt)
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        return self._store.put_bulk(data, flags)
    def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return self._store.del_bulk(keys)
    def get_text(self, key: VStoreKey, /, alt: _T=None) -> str|_T:
        return self._store.get_text(key, alt)
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        return self._store.get_blob(key, alt)
    def get_list(self, key: VStoreKey, sel: VSListSel=None, /, alt: _T=None) -> FridValue|_T:
        return self._store.get_list(key, sel, alt)
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None, /, alt: _T=None) -> FridValue|_T:
        return self._store.get_dict(key, sel, alt)

    def aget_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        return self._store.aget_lock(name)
    async def afinalize(self):
        return await self._store.afinalize()
    async def aget_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return await self._store.aget_meta(keys)
    async def aget_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> FridValue|FridBeing:
        return await self._store.aget_frid(key, sel)
    async def aput_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return await self._store.aput_frid(key, val, flags)
    async def adel_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return await self._store.adel_frid(key, sel)
    async def aget_bulk(self, keys: Iterable[VStoreKey],
                        /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        return await self._store.aget_bulk(keys, alt)
    async def aput_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        return await self._store.aput_bulk(data, flags)
    async def adel_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return await self._store.adel_bulk(keys)
    async def aget_text(self, key: VStoreKey, alt: _T=None) -> str|_T:
        return await self._store.aget_text(key, alt)
    async def aget_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        return await self._store.aget_blob(key, alt)
    async def aget_list(self, key: VStoreKey, sel: VSListSel, /, alt: _T=None) -> FridValue|_T:
        return await self._store.aget_list(key, sel, alt)
    async def aget_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=None) -> FridValue|_T:
        return await self._store.aget_dict(key, sel, alt)

class AsyncToSyncProxyStore(AsyncToSyncStoreMixin, ProxyStore):
    pass

class SyncToASyncProxyStore(SyncToAsyncStoreMixin, ProxyStore):
    pass

