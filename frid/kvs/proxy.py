import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping
from concurrent.futures import Executor
from contextlib import AbstractAsyncContextManager
from typing import Concatenate, ParamSpec, TypeVar

from ..typing import MISSING, BlobTypes, FridBeing, FridTypeName, FridTypeSize, FridValue
from ..autils import collect_async_iterable
from . import utils
from .store import AsyncStore, ValueStore
from .utils import KeySearch, VSDictSel, VSListSel, VSPutFlag, VStoreKey, VStoreSel, BulkInput


_T = TypeVar('_T')
_P = ParamSpec('_P')

class ValueProxyStore(ValueStore):
    def __init__(self, store: ValueStore):
        self._store = store
    def substore(self, name: str, *args: str):
        return self.__class__(self._store.substore(name, *args))
    def get_lock(self, name: str|None=None):
        return self._store.get_lock(name)
    def finalize(self, depth=0):
        if depth > 0:
            return self._store.finalize(depth - 1)
    def get_keys(self, pat: KeySearch=None, /) -> Iterable[VStoreKey]:
        return self._store.get_keys(pat)
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return self._store.get_meta(*args, keys=keys)
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|FridBeing:
        return self._store.get_frid(key, sel, dtype)
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return self._store.put_frid(key, val, flags)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return self._store.del_frid(key, sel)
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridValue|_T]:
        return self._store.get_bulk(keys, alt)
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        return self._store.put_bulk(data, flags)
    def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return self._store.del_bulk(keys)
    def get_text(self, key: VStoreKey, /, alt: _T=None) -> str|_T:
        return self._store.get_text(key, alt)
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        return self._store.get_blob(key, alt)
    def get_list(self, key: VStoreKey, sel: VSListSel=None,
                 /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        return self._store.get_list(key, sel, alt)
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                 /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        return self._store.get_dict(key, sel, alt)

class AsyncProxyStore(AsyncStore):
    def __init__(self, store: AsyncStore):
        self._store = store
    def substore(self, name: str, *args: str):
        return self.__class__(self._store.substore(name, *args))

    def get_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        return self._store.get_lock(name)
    async def finalize(self, depth=0):
        if depth > 0:
            return await self._store.finalize(depth - 1)
    def get_keys(self, pat: KeySearch=None, /) -> AsyncIterable[VStoreKey]:
        return self._store.get_keys(pat)
    async def get_meta(self, *args: VStoreKey,
                       keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return await self._store.get_meta(*args, keys=keys)
    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                       /, dtype: FridTypeName='') -> FridValue|FridBeing:
        return await self._store.get_frid(key, sel, dtype)
    async def put_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return await self._store.put_frid(key, val, flags)
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return await self._store.del_frid(key, sel)
    async def get_bulk(self, keys: Iterable[VStoreKey],
                       /, alt: _T=MISSING) -> list[FridValue|_T]:
        return await self._store.get_bulk(keys, alt)
    async def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        return await self._store.put_bulk(data, flags)
    async def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return await self._store.del_bulk(keys)
    async def get_text(self, key: VStoreKey, alt: _T=None) -> str|_T:
        return await self._store.get_text(key, alt)
    async def get_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        return await self._store.get_blob(key, alt)
    async def get_list(self, key: VStoreKey, sel: VSListSel,
                       /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        return await self._store.get_list(key, sel, alt)
    async def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        return await self._store.get_dict(key, sel, alt)

AsyncRunType = Callable[Concatenate[Callable[...,_T],_P],Awaitable[_T]]
class ValueProxyAsyncStore(AsyncStore):
    """This proxy converts the sync value store API to an async one.
    """
    def __init__(self, store: ValueStore, *, executor: Executor|AsyncRunType|bool=False):
        super().__init__()
        self._store = store
        if isinstance(executor, Executor):
            self._executor = executor
            self._asyncrun: AsyncRunType = self._loop_run
        elif isinstance(executor, Callable):
            self._executor = None
            self._asyncrun = executor
        elif executor:
            self._executor = None
            self._asyncrun = self._loop_run
        else:
            self._executor = None
            self._asyncrun = self._run_func
    def substore(self, name: str, *args: str):
        return self.__class__(self._store.substore(name, *args))

    @staticmethod
    async def _run_func(func: Callable[...,_T], *args) -> _T:
        return func(*args)
    async def _loop_run(self, call: Callable[...,_T], *args) -> _T:
        return await asyncio.get_running_loop().run_in_executor(self._executor, call, *args)
    async def finalize(self, depth=0):
        if depth > 0:
            return await self._asyncrun(self._store.finalize, depth - 1)
    def get_lock(self, name: str|None=None):
        return self._store.get_lock(name)
    async def get_keys(self, pat: KeySearch=None, /) -> AsyncIterable[VStoreKey]:
        for key in self._store.get_keys(pat):
            yield key
    async def get_meta(self, *args: VStoreKey,
                       keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return await self._asyncrun(self._store.get_meta, *utils.list_concat(args, keys))
    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                       /, dtype: FridTypeName='') -> FridValue|FridBeing:
        return await self._asyncrun(self._store.get_frid, key, sel, dtype)
    async def put_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return await self._asyncrun(self._store.put_frid, key, val, flags)
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return await self._asyncrun(self._store.del_frid, key, sel)
    async def get_bulk(self, keys: Iterable[VStoreKey],
                       /, alt: _T=MISSING) -> list[FridValue|_T]:
        return await self._asyncrun(self._store.get_bulk, keys, alt)
    async def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        return await self._asyncrun(self._store.put_bulk, data, flags)
    async def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return await self._asyncrun(self._store.del_bulk, keys)
    async def get_text(self, key: VStoreKey, alt: _T=None) -> str|_T:
        return await self._asyncrun(self._store.get_text, key, alt)
    async def get_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        return await self._asyncrun(self._store.get_blob, key, alt)
    async def get_list(self, key: VStoreKey, sel: VSListSel,
                       /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        return await self._asyncrun(self._store.get_list, key, sel, alt)
    async def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        return await self._asyncrun(self._store.get_dict, key, sel, alt)

class AsyncProxyValueStore(ValueStore):
    """This proxy converts the async value store API to a sync one with an event loop.
    """
    def __init__(self, store: AsyncStore, *, loop: asyncio.AbstractEventLoop|None=None):
        super().__init__()
        self._store = store
        if loop is not None:
            self._loop_owner = False
            self._loop = loop
        else:
            # print("Creating a new event loop")
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
            self._loop_owner = True
    def __del__(self):
        self._del_loop()
    def _del_loop(self):
        if self._loop_owner:
            self._loop.close()
            self._loop_owner = False
    def substore(self, name: str, *args: str):
        return self.__class__(self._store.substore(name, *args))

    def get_lock(self, name: str|None=None):
        raise NotImplementedError  # pragma: no cover --- not going to be used
    def finalize(self, depth=0):
        if depth > 0:
            result = self._loop.run_until_complete(self._store.finalize(depth - 1))
        self._del_loop()
        return result
    def get_keys(self, pat: KeySearch=None, /) -> Iterable[VStoreKey]:
        return self._loop.run_until_complete(collect_async_iterable(self._store.get_keys(pat)))
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return self._loop.run_until_complete(self._store.get_meta(*args, keys=keys))
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|FridBeing:
        return self._loop.run_until_complete(self._store.get_frid(key, sel, dtype))
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> int|bool:
        return self._loop.run_until_complete(self._store.put_frid(key, val, flags))
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> int|bool:
        return self._loop.run_until_complete(self._store.del_frid(key, sel))
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridValue|_T]:
        return self._loop.run_until_complete(self._store.get_bulk(keys, alt))
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        return self._loop.run_until_complete(self._store.put_bulk(data, flags))
    def del_bulk(self, keys: Iterable[VStoreKey], /) -> int:
        return self._loop.run_until_complete(self._store.del_bulk(keys))
    def get_text(self, key: VStoreKey, /, alt: _T=None) -> str|_T:
        return self._loop.run_until_complete(self._store.get_text(key, alt))
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        return self._loop.run_until_complete(self._store.get_blob(key, alt))
    def get_list(self, key: VStoreKey, sel: VSListSel=None,
                 /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        return self._loop.run_until_complete(self._store.get_list(key, sel, alt))
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                 /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        return self._loop.run_until_complete(self._store.get_dict(key, sel, alt))
