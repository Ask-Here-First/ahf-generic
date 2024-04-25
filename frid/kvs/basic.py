"""This class implement a basic value store that retrives the whole data then do selection.
It will derive a memory based store from there
"""
import asyncio, threading
from dataclasses import dataclass, field
from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Concatenate, Generic, ParamSpec, TypeVar, cast

from ..typing import MISSING, PRESENT, BlobTypes, FridArray, FridBeing, MissingType, StrKeyMap
from ..typing import FridTypeSize, FridValue
from ..autils import AsyncReentrantLock
from ..guards import is_frid_array, is_frid_skmap
from ..helper import frid_merge, frid_type_size
from ..strops import escape_control_chars
from ..dumper import dump_into_str
from ..loader import load_from_str
from . import utils
from .store import AsyncStore, ValueStore
from .utils import VSPutFlag, VStoreKey, VStoreSel

_T = TypeVar('_T')
_E = TypeVar('_E')   # The encoding type
_P = ParamSpec('_P')

class _SimpleBaseStore(Generic[_E]):
    """Simple value store are stores that always handles each item as a whole."""
    @abstractmethod
    def _encode(self, val: FridValue, /) -> _E:
        raise NotImplementedError
    @abstractmethod
    def _decode(self, val: _E, /) -> FridValue:
        raise NotImplementedError

    def _get(self, key: str, /) -> _E|MissingType:
        """Get the whole data from the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    def _put(self, key: str, val: _E, /) -> bool:
        """Write the whole data into the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    def _rmw(self, key: str,
             mod: Callable[Concatenate[_E|FridBeing,_P],tuple[_E|FridBeing,_T]],
             /, *args: _P.args, **kwargs: _P.kwargs) -> _T:
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
    def _del(self, key: str, /) -> bool:
        """Delete the data in the store associated to the given `key`.
        - Returns boolean to indicate if the key is deleted (or if the store is changed).
        """
        raise NotImplementedError  # pragma: no cover

    def _key(self, key: VStoreKey, /) -> str:
        """Generate string based key depending if the key is tuple or named tuple.
        For tuple or named tuple, the generated key is just joined with a TAB.
        """
        if isinstance(key, str):
            return key
        if isinstance(key, tuple):
            # Using the DEL key to escape
            return '\t'.join(escape_control_chars(str(k), '\x7f') for k in key)
        raise ValueError(f"Invalid key type {type(key)}")

    def _get_sel(self, val: _E, sel: VStoreSel, /) -> FridValue|MissingType:
        """Gets selection for an general value."""
        data = self._decode(val)
        if sel is None:
            return data
        if isinstance(data, Mapping):
            out = utils.dict_select(data, cast(str|Iterable[str], sel))
        elif isinstance(data, Sequence):
            out = utils.list_select(data, cast(int|slice|tuple[int,int], sel))
        else:
            raise ValueError(f"Selector is not None for data type {type(val)}")
        if out is MISSING:
            return MISSING
        assert not isinstance(out, FridBeing)
        return out
    def _combine(self, old: _E|FridBeing, new: FridValue,
                 /, flags: VSPutFlag) -> tuple[_E|FridBeing,bool]:
        """Combines the `new` value into the `old` values depending on the `flags`.
        - Returns a pair: the updated value (with PRESENT for no change and MISSING for delete),
          and a boolean value for whether or not the store will be changed.
        """
        assert old is not PRESENT
        if old is MISSING:
            if flags & VSPutFlag.NO_CREATE:
                return (MISSING, False)
            return (self._encode(new), True)
        if flags & VSPutFlag.NO_CHANGE:
            return (PRESENT, False)
        if flags & VSPutFlag.KEEP_BOTH:
            # TODO: frid_merge() to accept more flags
            return (self._encode(frid_merge(self._decode(old), new)), True)
        return (self._encode(new), True)
    def _del_sel(self, val: _E|FridBeing, sel: VStoreSel) -> tuple[_E|FridBeing,int]:
        """Deletes the selected items in general. Note it will try to delete in place.
        - Returns a pair: the updated value and the number of items deleted.
        """
        assert val is not PRESENT
        assert sel is not None
        if val is MISSING:
            return (MISSING, 0)
        data = self._decode(val)
        if is_frid_skmap(data):
            if not isinstance(data, dict):
                data = dict(data)
            cnt = utils.dict_delete(data, cast(str|Iterable[str], sel))
        elif is_frid_array(data):
            if not isinstance(data, list):
                data = list(data)
            cnt = utils.list_delete(data, cast(int|slice|tuple[int,int], sel))
        else:
            raise ValueError(f"Data type {type(data)} does not support partial removal")
        if cnt == 0:
            return (PRESENT, 0)
        return (self._encode(data), cnt)

class SimpleValueStore(_SimpleBaseStore[_E], ValueStore):
    @abstractmethod
    def _get(self, key: str) -> _E|MissingType:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _put(self, key: str, val: _E) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _rmw(self, key: str,
             mod: Callable[Concatenate[_E|FridBeing,_P],tuple[_E|FridBeing,_T]],
             *args, **kwargs) -> _T:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _del(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

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
            return self._rmw(key, self._combine, val, flags)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key(key)
        with self.get_lock(key):
            if sel is None:
                return self._del(key)
            return bool(self._rmw(key, self._del_sel, sel))

class SimpleAsyncStore(_SimpleBaseStore[_E], AsyncStore):
    @abstractmethod
    async def _get(self, key: str) -> _E|MissingType:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _put(self, key: str, val: _E) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _rmw(self, key: str,
                   mod: Callable[Concatenate[_E|FridBeing,_P],tuple[_E|FridBeing,_T]],
                   *args, **kwargs) -> _T:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _del(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        key = self._key(key)
        async with self.get_lock(key):
            val = await self._get(key)
            if val is MISSING:
                return MISSING
            return self._get_sel(val, sel)
    async def put_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> bool:
        key = self._key(key)
        async with self.get_lock(key):
            if flags == VSPutFlag.UNCHECKED:
                return await self._put(key, self._encode(val))
            return await self._rmw(key, self._combine, val, flags)
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key(key)
        async with self.get_lock(key):
            if sel is None:
                return await self._del(key)
            return bool(await self._rmw(key, self._del_sel, sel))

class MemoryValueStore(SimpleValueStore[FridValue]):
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

    def _encode(self, val: FridValue) -> FridValue:
        return val
    def _decode(self, val: FridValue) -> FridValue:
        return val

    def get_lock(self, name: str|None=None):
        return self._meta.tlock
    def get_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        return {k: frid_type_size(v) for k in keys
                if (v := self._get(self._key(k))) is not MISSING}

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

class BinaryStoreMixin:
    def __init__(self, *, frid_prefix: bytes=b'',
                 text_prefix: bytes|None=None, blob_prefix: bytes|None=None,
                 list_prefix: bytes|None=None, dict_prefix: bytes|None=None, **kwargs):
        super().__init__(**kwargs)
        self._frid_prefix = frid_prefix
        self._text_prefix = text_prefix
        self._blob_prefix = blob_prefix
        self._list_prefix = list_prefix
        self._dict_prefix = dict_prefix
        decoders: list[tuple[bytes,Callable[[bytes],FridValue]]] = []
        if frid_prefix is not None:
            decoders.append((frid_prefix, self._decode_frid))
        if text_prefix is not None:
            decoders.append((text_prefix, self._decode_text))
        if blob_prefix is not None:
            decoders.append((blob_prefix, self._decode_blob))
        if list_prefix is not None:
            decoders.append((list_prefix, self._decode_list))
        if dict_prefix is not None:
            decoders.append((dict_prefix, self._decode_dict))
        decoders.sort(reverse=True, key=lambda x: len(x[0]))
        self._decoders = decoders

    def _collide_with_prefix(self, b: bytes) -> bool:
        for prefix, _ in self._decoders:
            if not prefix:
                return False
            if b.startswith(prefix):
                return True
        return False

    def _encode(self, data: FridValue, append=False, /) -> bytes:
        if isinstance(data, str):
            if self._text_prefix is not None:
                b = data.encode('utf-8')
                if append:
                    return b
                if self._text_prefix:
                    return self._text_prefix + b
                if not self._collide_with_prefix(b):
                    return b
        elif isinstance(data, BlobTypes):
            if self._blob_prefix is not None:
                if append:
                    return data
                if self._blob_prefix:
                    return self._blob_prefix + self._encode_blob(data)
                if not self._collide_with_prefix(data):
                    return data
        elif is_frid_array(data):
            if self._list_prefix is not None:
                b = self._encode_list(data)
                if append:
                    return b
                return self._list_prefix + b
        elif is_frid_skmap(data):
            if self._dict_prefix is not None:
                b = self._encode_dict(data)
                if append:
                    return b
                return self._dict_prefix + b
        if self._frid_prefix is None:
            raise ValueError(f"Do not know how to encode type {type(data)}")
        b = self._encode_frid(data)
        if append:
            return b
        return self._frid_prefix + b
    def _encode_frid(self, data: FridValue, /) -> bytes:
        return dump_into_str(data).encode('utf-8')
    def _encode_blob(self, data: BlobTypes, /) -> bytes:
        return bytes(data)
    def _encode_text(self, data: str, /) -> bytes:
        return data.encode('utf-8')
    def _encode_list(self, data: FridArray, /) -> bytes:
        out: list[bytes] = [dump_into_str(item).encode('utf-8') for item in data]
        out.append(b'')
        return b'\n'.join(out)
    def _encode_dict(self, data: StrKeyMap, /) -> bytes:
        out: list[bytes] = []
        for k, v in data.items():
            line = escape_control_chars(k, '\x7f')
            line += "\t" + (v.strfr() if isinstance(v, FridBeing) else dump_into_str(v))
            out.append(line.encode('utf-8'))
        out.append(b"")
        return b'\n'.join(out)

    def _decode(self, val: bytes, /) -> FridValue:
        if not isinstance(val, BlobTypes): # pragma: no cover -- should not happen
            raise ValueError(f"Incorrect encoded type {type(val)}; expect binary")
        if isinstance(val, memoryview|bytearray):  # pragma: no cover -- should not happen
            val = bytes(val)
        for prefix, decode in self._decoders:
            if val.startswith(prefix):
                return decode(val[len(prefix):])
        raise ValueError(f"Invalid byte encoding of {len(val)} bytes")
    def _decode_frid(self, val: bytes, /) -> FridValue:
        return load_from_str(val.decode('utf-8'))
    def _decode_text(self, val: bytes, /) -> str:
        return val.decode('utf-8')
    def _decode_blob(self, val: bytes, /) -> bytes:
        return val
    def _decode_list(self, val: bytes, /) -> FridArray:
        return [load_from_str(line.decode()) for line in val.splitlines()]
    def _decode_dict(self, val: bytes, /) -> StrKeyMap:
        out = {}
        for line in val.splitlines():
            (key_str, tab_str, val_str) = line.decode('utf-8').partition('\t')
            key = escape_control_chars(key_str, '\x7f')
            if tab_str:
                being = FridBeing.parse(val_str)
                if being is None:
                    out[key] = load_from_str(val_str)
                elif being:
                    out[key] = PRESENT
                else:
                    out.pop(key, MISSING)
            else:
                out[key] = PRESENT
        return out
