"""This class implement a basic value store that retrives the whole data then do selection.
It will derive a memory based store from there
"""
import asyncio, threading
from dataclasses import dataclass, field
from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TypeVar, cast

from ..typing import MISSING, PRESENT, FridBeing, BlobTypes, MissingType
from ..typing import FridTypeSize, FridValue
from ..autils import AsyncReentrantLock
from ..guards import is_frid_array
from ..helper import frid_merge, frid_type_size
from ..strops import escape_control_chars
from ..dumper import dump_into_str
from ..loader import load_from_str
from .store import VSPutFlag, VStoreKey, VStoreSel, ValueStore

_T = TypeVar('_T')

class SimpleValueStore(ValueStore):
    """Simple value store are stores that always handles each item as a whole."""
    @abstractmethod
    def _get(self, key: str) -> FridValue|MissingType:
        """Get the whole data from the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _put(self, key: str, val: FridValue) -> bool:
        """Write the whole data into the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _rmw(self, key: str, mod: Callable[...,tuple[FridValue|FridBeing,_T]],
             *args, **kwargs) -> _T:
        """The read-modify-write process for the value of the `key` in the store.
        - `mod`: the callback function to be called with:
            + The current value as the first argument (or MISSING);
            + The values of `*args` and `**kwargs` are passed as the rest of the arguments;
            + It returns a tuple of updated value and the return value to pass on.
            + The updated value will be written to to the store with the given `key`,
              except if the updated value is of FridBeing type:
                + If it is PRESENT, keep the original;
                + If it is MISSING, delete the key.
        - This method returns the second return value of `mod()` as is.
        """
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _del(self, key: str) -> bool:
        """Delete the data in the store associated to the given `key`.
        - Returns boolean to indicate if the key is deleted (or if the store is changed).
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    async def _aget(self, key: str) -> FridValue|MissingType:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _aput(self, key: str, val: FridValue) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _armw(self, key: str, mod: Callable[...,tuple[FridValue|FridBeing,_T]],
                    *args, **kwargs) -> _T:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _adel(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    def _encode(self, val: FridValue) -> FridValue:
        return val
    def _decode(self, val: FridValue) -> FridValue:
        return val

    def _key(self, key: VStoreKey) -> str:
        """Generate string based key depending if the key is tuple or named tuple.
        For tuple or named tuple, the generated key is just joined with a TAB.
        """
        if isinstance(key, str):
            return key
        if isinstance(key, tuple):
            # Using the DEL key to escape
            return '\t'.join(escape_control_chars(str(k), '\x7f') for k in key)
        raise ValueError(f"Invalid key type {type(key)}")

    def _get_sel(self, val: FridValue, sel: VStoreSel) -> FridValue|MissingType:
        """Gets selection for an general value."""
        if sel is None:
            return val
        val = self._decode(val)
        if isinstance(val, Mapping):
            out = self._dict_select(val, cast(str|Iterable[str], sel))
        elif isinstance(val, Sequence):
            out = self._list_select(val, cast(int|slice|tuple[int,int], sel))
        else:
            raise ValueError(f"Selector is not None for data type {type(val)}")
        if out is MISSING:
            return MISSING
        assert not isinstance(out, FridBeing)
        return self._encode(out)
    def _add(self, old: FridValue|MissingType, new: FridValue,
             flags: VSPutFlag) -> tuple[FridValue|FridBeing,bool]:
        """Adds or replaces the `new` value into the `old` values depending on the `flags.
        - Returns a pair: the updated value (with PRESENT for no change and MISSING for delete),
          and a boolean value for whether or not the store will be changed.
        """
        if old is MISSING:
            return (MISSING, False) if flags & VSPutFlag.NO_CREATE else (new, True)
        if flags & VSPutFlag.NO_CHANGE:
            return (PRESENT, False)
        if flags & VSPutFlag.KEEP_BOTH:
            # TODO: frid_merge() to accept more flags
            return (self._encode(frid_merge(self._decode(old), new)), True)
        return (self._encode(new), True)
    def _del_sel(self, val: FridValue, sel: VStoreSel) -> tuple[FridValue,int]:
        """Deletes the selected items in general. Note it will try to delete in place.
        - Returns a pair: the updated value and the number of items deleted.
        """
        assert sel is not None
        val = self._decode(val)
        if isinstance(val, Mapping):
            if not isinstance(val, dict):
                val = dict(val)
            cnt = self._dict_delete(val, cast(str|Iterable[str], sel))
        elif is_frid_array(val):
            if not isinstance(val, list):
                val = list(val)
            cnt = self._list_delete(val, cast(int|slice|tuple[int,int], sel))
        else:
            raise ValueError(f"Data type {type(val)} does not support partial removal")
        return (self._encode(val), cnt)

    def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        key = self._key(key)
        with self.get_lock(key):
            data = self._get(key)
            if data is MISSING:
                return MISSING
            return self._get_sel(data, sel)
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> bool:
        key = self._key(key)
        with self.get_lock(key):
            if flags == VSPutFlag.UNCHECKED:
                return self._put(key, self._encode(val))
            return self._rmw(key, self._add, val, flags)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key(key)
        with self.get_lock(key):
            if sel is None:
                return self._del(key)
            return bool(self._rmw(key, self._del_sel, sel))

    async def aget_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        key = self._key(key)
        async with self.aget_lock(key):
            data = await self._aget(key)
            if data is MISSING:
                return MISSING
            return self._get_sel(data, sel)
    async def aput_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> bool:
        key = self._key(key)
        async with self.aget_lock(key):
            if flags == VSPutFlag.UNCHECKED:
                return await self._aput(key, self._encode(val))
            return await self._armw(key, self._add, val, flags)
    async def adel_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key(key)
        async with self.aget_lock(key):
            if sel is None:
                return await self._adel(key)
            return bool(await self._armw(key, self._del_sel, sel))

class BinaryValueStore(SimpleValueStore):
    """This store encodes the data into a binary string."""
    def _encode(self, val: FridValue) -> BlobTypes:
        return dump_into_str(val).encode('utf-8')
    def _decode(self, val: BlobTypes) -> FridValue:
        return load_from_str(bytes(val).decode('utf-8'))

class MemoryValueStore(SimpleValueStore):
    @dataclass
    class StoreMeta:
        store: dict[str,FridValue] = field(default_factory=dict)
        tlock: threading.RLock = field(default_factory=threading.RLock)
        alock: asyncio.Lock = field(default_factory=AsyncReentrantLock)
    StorageType = dict[tuple[str,...],StoreMeta]

    def __init__(self, storage: StorageType|None=None, names: tuple[str,...]=()):
        super().__init__()
        self._storage = storage if storage is not None else {}
        self._meta = self._storage.setdefault(names, self.StoreMeta())
        self._data = self._meta.store
    def all_data(self) -> Mapping[str,FridValue]:
        return self._data

    def substore(self, name: str, *args: str) -> 'MemoryValueStore':
        return __class__(self._storage, (name, *args))

    def get_lock(self, name: str|None=None):
        return self._meta.tlock
    def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return {k: frid_type_size(v) for k in keys
                if (v := self._get(self._key(k))) is not MISSING}
    def aget_lock(self, name: str|None=None):
        return self._meta.alock
    async def aget_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return self.get_meta(keys)

    def _get(self, key: str, /) -> FridValue|MissingType:
        return self._data.get(key, MISSING)
    def _rmw(self, key: str, mod: Callable[...,tuple[FridValue|FridBeing,_T]],
             *args, **kwargs) -> _T:
        old_data = self._data.get(key, MISSING)
        (data, out) = mod(old_data, *args, **kwargs)
        if not isinstance(data, FridBeing):
            self._data[key] = data
        elif data is MISSING and old_data is not MISSING:
            del self._data[key]
        return out
    def _put(self, key: str, data: FridValue) -> bool:
        self._data[key] = data
        return True
    def _del(self, key: str) -> bool:
        return self._data.pop(key, MISSING) is not MISSING

    async def _aget(self, key: str) -> FridValue|MissingType:
        return self._get(key)
    async def _aput(self, key: str, val: FridValue) -> bool:
        return self._put(key, val)
    async def _armw(self, key: str, mod: Callable[...,tuple[FridValue|FridBeing,_T]],
                    *args, **kwargs) -> _T:
        return self._rmw(key, mod, *args, **kwargs)
    async def _adel(self, key: str) -> bool:
        return self._del(key)
