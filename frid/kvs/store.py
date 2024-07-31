"""The Frid Value Store."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, Iterable, Mapping
from contextlib import AbstractContextManager, AbstractAsyncContextManager
from typing import TypeVar, overload


from ..typing import MISSING, BlobTypes, FridTypeName, FridTypeSize, FridValue, MissingType
from ..guards import as_kv_pairs
from . import utils
from .utils import KeySearch, VSPutFlag, VSListSel, VSDictSel, VStoreKey, VStoreSel, BulkInput


_T = TypeVar('_T')
_Self = TypeVar('_Self', bound='_BaseStore')  # TODO: remove this in 3.11

class _BaseStore(ABC):
    @classmethod
    def from_url(cls: type[_Self], url: str, /, *args, **kwargs) -> _Self:
        """Create an store accorind to an URL.
        - Optional positional arguments may be needed but they are not passed
          unchecked to constructor.
        - Some eyword arguments are passed unchecked to the constructor.
        """
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def substore(self: _Self, name: str, *args: str) -> _Self:
        """Returns a substore ValueStore as given by a list of names."""
        raise NotImplementedError  # pragma: no cover

    def finalize(self, depth=0):
        """Calling to finalize this store before drop the reference.
        - `depth`: only effective for proxies: if depth > 0, call
          `finalize(depth - 1)` of the backend.
        """
        raise NotImplementedError  # pragma: no cover
    def get_lock(self, name: str|None=None, /) -> AbstractContextManager:
        """Returns an reentrant lock for desired concurrency."""
        raise NotImplementedError  # pragma: no cover
    def get_keys(self, pat: KeySearch=None, /):
        """Returns a list of keys according to the key search string."""
        raise NotImplementedError  # pragma: no cover
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        """Gets the meta data of a list of `keys` and returns a map for existing keys.
        Notes: There is no atomicity guarantee for this method.
        """
        raise NotImplementedError  # pragma: no cover
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|MissingType:
        """Gets the value of the given `key` in the value store.
        - If `sel` is specified, uses the selection rule to select the partial data to return.
        - If `dtype` is set, use it as a type hint for the data type
          (it can return a different type though)
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
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridValue|_T]:
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
    def get_list(self, key: VStoreKey, sel: VSListSel=None,
                 /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        """Gets the list value associated with the given `key`.
        - If the selector `sel` is specified, it will be applied to the value.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                 /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        """Gets the dict value associated with the given `key`.
        - If the selector `sel` is specified, it will be applied to the value.
        - Returns the `alt` value if the entry is missing.
        """
        raise NotImplementedError  # pragma: no cover

class ValueStore(_BaseStore):
    @classmethod
    def from_url(cls: type[_Self], url: str, /, *args, **kwargs) -> _Self:
        raise NotImplementedError  # pragma: no cover
    def finalize(self, depth=0):
        pass
    @abstractmethod
    def get_lock(self, name: str|None=None, /) -> AbstractContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_keys(self, pat: KeySearch=None, /) -> Iterable[VStoreKey]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_meta(self, *args: VStoreKey, keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|MissingType:
        # Make the getter abstract
        raise NotImplementedError  # pragma: no cover
    def put_frid(self, key: VStoreKey, val: FridValue, /, flags=VSPutFlag.UNCHECKED) -> bool:
        raise NotImplementedError  # pragma: no cover
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        raise NotImplementedError  # pragma: no cover
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridValue|_T]:
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
        data = self.get_frid(key, dtype='text')
        if data is MISSING:
            return alt
        assert isinstance(data, str), type(data)
        return data
    def get_blob(self, key: VStoreKey, /, alt: _T=None) -> BlobTypes|_T:
        data = self.get_frid(key, dtype='blob')
        if data is MISSING:
            return alt
        assert isinstance(data, BlobTypes), type(data)
        return data
    @overload
    def get_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None=None,
                 /, alt: _T=None) -> list[FridValue]|_T: ...
    def get_list(self, key: VStoreKey, sel: VSListSel=None,
                 /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        data = self.get_frid(key, sel, dtype='list')
        if data is MISSING:
            return alt
        if not isinstance(sel, int):
            assert isinstance(data, list)
        return data
    @overload
    def get_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    def get_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                 /, alt: _T=None) -> dict[str,FridValue]|_T: ...
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                 /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        data = self.get_frid(key, sel, dtype='dict')
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert isinstance(data, dict)
        return data

class AsyncStore(_BaseStore):
    @classmethod
    async def from_url(cls: type[_Self], url: str, /, *args, **kwargs) -> _Self:
        raise NotImplementedError  # pragma: no cover
    # Override all methods if signature is different
    async def finalize(self, depth=0):
        pass
    @abstractmethod
    def get_lock(self, name: str|None=None, /) -> AbstractAsyncContextManager:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def get_keys(self, pat: KeySearch=None, /) -> AsyncIterable[VStoreKey]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def get_meta(self, *args: VStoreKey,
                       keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                       /, dtype: FridTypeName='') -> FridValue|MissingType:
        raise NotImplementedError  # pragma: no cover
    async def put_frid(self, key: VStoreKey, val: FridValue,
                       /, flags=VSPutFlag.UNCHECKED) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        raise NotImplementedError  # pragma: no cover
    async def get_bulk(self, keys: Iterable[VStoreKey],
                       /, alt: _T=MISSING) -> list[FridValue|_T]:
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
        data = await self.get_frid(key, dtype='text')
        if data is MISSING:
            return alt
        assert isinstance(data, str), type(data)
        return data
    async def get_blob(self, key: VStoreKey, alt: _T=None) -> BlobTypes|_T:
        data = await self.get_frid(key, dtype='blob')
        if data is MISSING:
            return alt
        assert isinstance(data, BlobTypes), type(data)
        return data
    @overload
    async def get_list(self, key: VStoreKey, sel: int, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def get_list(self, key: VStoreKey, sel: slice|tuple[int,int]|None=None,
                        /, alt: _T=None) -> list[FridValue]|_T: ...
    async def get_list(self, key: VStoreKey, sel: VSListSel=None,
                       /, alt: _T=None) -> list[FridValue]|FridValue|_T:
        data = await self.get_frid(key, sel, dtype='list')
        if data is MISSING:
            return alt
        if not isinstance(sel, int):
            assert isinstance(data, list)
        return data
    @overload
    async def get_dict(self, key: VStoreKey, sel: str, /, alt: _T=None) -> FridValue|_T: ...
    @overload
    async def get_dict(self, key: VStoreKey, sel: Iterable[str]|None=None,
                       /, alt: _T=None) -> dict[str,FridValue]|_T: ...
    async def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                       /, alt: _T=None) -> dict[str,FridValue]|FridValue|_T:
        data = await self.get_frid(key, sel, dtype='dict')
        if data is MISSING:
            return alt
        if not isinstance(sel, str):
            assert isinstance(data, dict)
        return data

