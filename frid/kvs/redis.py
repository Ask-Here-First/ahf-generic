import os, traceback
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar, cast, overload
from logging import error

import redis
from redis import asyncio as aredis

from ..typing import MISSING, FridBeing, FridTypeName, MissingType
from ..typing import FridArray, FridTypeSize, FridValue, StrKeyMap
from ..guards import as_kv_pairs, is_frid_array, is_frid_skmap, is_list_like
from ..strops import escape_control_chars, revive_control_chars
from ..helper import frid_merge, frid_type_size
from . import utils
from .store import ValueStore, AsyncStore
from .basic import BinaryStoreMixin
from .utils import KeySearch, VSDictSel, VSListSel, VStoreSel, BulkInput, VSPutFlag, VStoreKey
from .utils import match_key

_T = TypeVar('_T')

class _RedisBaseStore(BinaryStoreMixin):
    NAMESPACE_SEP = '\t'
    def __init__(self, name_prefix: str='', *, frid_prefix: bytes=b'#!',
                 text_prefix: bytes|None=b'', blob_prefix: bytes|None=b'#=',
                 **kwargs):
        super().__init__(frid_prefix=frid_prefix, text_prefix=text_prefix,
                         blob_prefix=blob_prefix, **kwargs)
        self._name_prefix = name_prefix
    @classmethod
    def _redis_args(cls, kwargs: dict[str,Any]) -> dict[str,Any]:
        env_set = {
            'FRID_REDIS_HOST': 'host',
            'FRID_REDIS_PORT': 'port',
            'FRID_REDIS_USER': 'username',
            'FRID_REDIS_PASS': 'password',
        }
        out = {}
        for key, val in env_set.items():
            data = kwargs.pop(val, os.getenv(key))
            if data is not None:
                out[val] = data
        return out
    @classmethod
    def _build_name_prefix(cls, base: str, name: str, *args: str) -> str:
        prefix = name + cls.NAMESPACE_SEP
        if base:
            prefix = base + cls.NAMESPACE_SEP + prefix
        if args:
            prefix += cls.NAMESPACE_SEP.join(args) + cls.NAMESPACE_SEP
        return prefix

    def _key_name(self, key: VStoreKey):
        if isinstance(key, tuple):
            key = '\t'.join(escape_control_chars(str(k), '\x7f') for k in key)
        return self._name_prefix + key
    def _key_list(self, keys: Iterable[VStoreKey]) -> list[str]:
        return [self._key_name(k) for k in keys]

    @overload
    def _check_type(self, data, typ: type[_T], default: None=None) -> _T|None: ...
    @overload
    def _check_type(self, data, typ: type[_T], default: _T) -> _T: ...
    def _check_type(self, data, typ: type[_T], default: _T|None=None) -> _T|None:
        if not isinstance(data, typ):  # pragma: no cover -- should not happen
            # TODO: generic code to log current or given stacktrace or exception
            trace = '\n'.join(traceback.format_list(traceback.extract_stack()))
            error(f"Incorrect Redis return type {type(data)}; expecting {typ}, at\n{trace}\n")
            return default
        return data
    def _check_bool(self, data) -> bool:
        if data is None:
            return False   # Redis-py actually returns None for False sometimes
        return self._check_type(data, bool, False)
    def _check_text(self, data) -> str|None:
        if data is None:
            return None  # pragma: no cover -- should not happen
        if isinstance(data, str):
            return data  # pragma: no cover -- should not happen
        if isinstance(data, bytes):
            return data.decode()
        if isinstance(data, (memoryview, bytearray)):  # pragma: no cover -- should not happen
            return bytes(data).decode()
        raise ValueError(f"Incorrect Redis type {type(data)}; expect string") # pragma: no cover
        return None  # pragma: no cover
    def _revive_key(self, data, pat: KeySearch) -> VStoreKey|None:
        text = self._check_text(data)
        if text is None:
            return None
        if not text.startswith(self._name_prefix):
            return None
        items = text[len(self._name_prefix):].split('\t')
        key = tuple(revive_control_chars(x, '\x7f') for x in items)
        if not match_key(key, pat):
            return None
        if len(key) == 1:
            return key[0]
        return key

class RedisValueStore(_RedisBaseStore, ValueStore):
    URL_SCHEME = 'redis'
    def __init__(self, *args, _redis: redis.Redis|None=None, **kwargs):
        self._redis = redis.Redis(**self._redis_args(kwargs)) if _redis is None else _redis
        super().__init__(*args, **kwargs)
    @classmethod
    def from_url(cls, url: str, *args, **kwargs) -> 'RedisValueStore':
        # Allow passing an URL through but the content is not checked
        assert url.startswith('redis://')
        redis_kwargs = cls._redis_args(kwargs)
        return cls(*args, _redis=redis.Redis.from_url(url, **redis_kwargs), **kwargs)
    def wipe_all(self) -> int:
        """This is mainly for testing."""
        keys = self._redis.keys(self._name_prefix + "*")
        if not isinstance(keys, Iterable):  # pragma: no cover
            error(f"Redis.keys() returns a type {type(keys)}")
            return -1
        if not keys:
            return 0
        return self._check_type(self._redis.delete(*keys), int, -1)
    def finalize(self, depth=0):
        self._redis.close()
    def substore(self, name: str, *args: str) -> 'RedisValueStore':
        return self.__class__(_redis=self._redis, name_prefix=self._build_name_prefix(
            self._name_prefix, name, *args
        ))

    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        return self._redis.lock((name or "*GLOBAL*") + "\v*LOCK*")
    def _get_name_meta(self, name: str) -> FridTypeSize|None:
        t = self._check_text(self._redis.type(name))
        if t == 'list':
            return ('list', self._check_type(self._redis.llen(name), int, 0))
        if t == 'hash':
            return ('dict', self._check_type(self._redis.hlen(name), int, 0))
        data: bytes|None = self._redis.get(name) # type: ignore
        if data is None:
            return None
        return frid_type_size(self._decode(data))
    def get_keys(self, pat: KeySearch=None, /) -> Iterator[VStoreKey]:
        # TODO: speed up to convert KeySearch to a minimal superset Redis pattern
        keys = self._redis.keys()
        if not isinstance(keys, Iterable):
            return
        for k in keys:
            key = self._revive_key(k, pat)
            if key is not None:
                yield key
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return {k: v for k in utils.list_concat(args, keys)
                if (v := self._get_name_meta(self._key_name(k))) is not None}
    def get_list(self, key: VStoreKey, sel: VSListSel=None,
                 /, alt: _T=MISSING) -> list[FridValue]|FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            seq: Sequence = self._redis.lrange(redis_name, 0, -1)  # type: ignore
            return [self._decode_frid(x) for x in seq]
        if isinstance(sel, int):
            val: bytes = self._redis.lindex(redis_name, sel)  # type: ignore
            return self._decode_frid(val) if val is not None else alt
        (first, last) = utils.list_bounds(sel)
        seq = self._redis.lrange(redis_name, first, last)  # type: ignore
        assert isinstance(seq, Sequence)
        if isinstance(sel, slice) and sel.step is not None and sel.step != 1:
            seq = seq[::sel.step]
        return [self._decode_frid(x) for x in seq]
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                 /, alt: _T=MISSING) -> dict[str,FridValue]|FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            map: Mapping = self._redis.hgetall(redis_name) # type: ignore
            return {k.decode(): self._decode_frid(v) for k, v in map.items()}
        if isinstance(sel, str):
            val: bytes|None = self._redis.hget(redis_name, sel) # type: ignore
            return self._decode_frid(val) if val is not None else alt
        if isinstance(sel, Sequence):
            if not isinstance(sel, list):
                sel = list(sel)  # pragma: no cover
            seq = self._redis.hmget(redis_name, sel) # type: ignore
            assert is_list_like(seq)
            return {k: self._decode_frid(v) for i, k in enumerate(sel)
                    if (v := seq[i]) is not None}
        raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")  # pragma: no cover
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|MissingType:
        if dtype == 'list' or (sel is not None and utils.is_list_sel(sel)):
            return self.get_list(key, cast(VSListSel, sel))
        if dtype == 'dict' or (sel is not None and utils.is_dict_sel(sel)):
            return self.get_dict(key, cast(VSDictSel, sel))
        redis_name = self._key_name(key)
        if not dtype:
            t = self._check_text(self._redis.type(redis_name)) # Just opportunisitic; no lock
            if t == 'list':
                return self.get_list(key, cast(VSListSel, sel))
            if t == 'hash':
                return self.get_dict(key, cast(VSDictSel, sel))
        data: bytes|None = self._redis.get(redis_name) # type: ignore
        return self._decode(data) if data is not None else MISSING
    def put_list(self, key: VStoreKey, val: FridArray, /, flags=VSPutFlag.UNCHECKED) -> bool:
        redis_name = self._key_name(key)
        encoded_val = [self._encode_frid(x) for x in val]
        if flags & VSPutFlag.KEEP_BOTH and not (flags & VSPutFlag.NO_CHANGE):
            if not encoded_val:  # Do nothing if the data is empty
                return False
            if flags & VSPutFlag.NO_CREATE:
                result = self._redis.rpushx(redis_name, *encoded_val)  # type: ignore
            else:
                result = self._redis.rpush(redis_name, *encoded_val)
        else:
            with self.get_lock(redis_name):
                if self._redis.exists(redis_name):
                    if flags & VSPutFlag.NO_CHANGE:
                        return False
                    self._redis.delete(redis_name)
                    retval = True
                else:
                    if flags & VSPutFlag.NO_CREATE:
                        return False
                    retval = False
                if not encoded_val:
                    return retval
                result = self._redis.rpush(redis_name, *encoded_val)
        return bool(self._check_type(result, int, 0))
    def put_dict(self, key: VStoreKey, val: StrKeyMap, /, flags=VSPutFlag.UNCHECKED) -> bool:
        redis_name = self._key_name(key)
        encoded_val = {k: v.strfr() if isinstance(v, FridBeing) else self._encode_frid(v)
                       for k, v in val.items() if v is not MISSING}
        if flags & VSPutFlag.KEEP_BOTH and not (
            flags & (VSPutFlag.NO_CHANGE | VSPutFlag.NO_CREATE)
        ):
            if not encoded_val:
                return False
            self._redis.hset(redis_name, mapping=encoded_val)
            return bool(encoded_val)  # Note result contains only the number of entries added
        with self.get_lock(redis_name):
            if self._redis.exists(redis_name):
                if flags & VSPutFlag.NO_CHANGE:
                    return False
                self._redis.delete(redis_name)
                retval = True
            else:
                if flags & VSPutFlag.NO_CREATE:
                    return False
                retval = False
            if not encoded_val:
                return retval
            result = self._redis.hset(redis_name, mapping=encoded_val)
        return bool(self._check_type(result, int, 0))
    def put_frid(self, key: VStoreKey, val: FridValue, /, flags=VSPutFlag.UNCHECKED) -> bool:
        if is_frid_array(val):
            return self.put_list(key, val, flags)
        if is_frid_skmap(val):
            return self.put_dict(key, val, flags)
        redis_name = self._key_name(key)
        nx = bool(flags & VSPutFlag.NO_CHANGE)
        xx = bool(flags & VSPutFlag.NO_CREATE)
        if flags & VSPutFlag.KEEP_BOTH:
           with self.get_lock():
               data: bytes|None = self._redis.get(redis_name) # type: ignore
               return self._check_bool(self._redis.set(redis_name, self._encode(
                   frid_merge(self._decode(data), val, depth=0) if data is not None else val
               ), nx=nx, xx=xx))
        return self._check_bool(self._redis.set(
            redis_name, self._encode(val), nx=nx, xx=xx
        ))
    def del_list(self, key: VStoreKey, sel: VSListSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            return bool(self._check_type(self._redis.delete(redis_name), int, 0))
        (first, last) = utils.list_bounds(sel)
        if utils.is_straight(sel):
            if last == -1:
                return self._check_bool(self._redis.ltrim(redis_name, 0, first - 1))
            if first == 0:
                return self._check_bool(self._redis.ltrim(redis_name, last + 1, -1))
        with self.get_lock(redis_name):
            data = self._redis.lrange(redis_name, 0, -1)
            if not data:
                return False
            assert isinstance(data, list)
            if utils.list_delete(data, sel):
                self._redis.delete(redis_name)
                if data:
                    self._redis.rpush(redis_name, *data)
                return True
            return False
    def del_dict(self, key: VStoreKey, sel: VSDictSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            result = self._redis.delete(redis_name)
        elif isinstance(sel, str):
            result = self._redis.hdel(redis_name, sel)
        elif isinstance(sel, Sequence):
            assert is_list_like(sel, str)
            if not sel:
                return False
            if not isinstance(sel, list):
                sel = list(sel)
            result = self._redis.hdel(redis_name, *sel)
        else:
            raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")# pragma: no cover
        return bool(self._check_type(result, int, 0))
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is not None:
            if utils.is_list_sel(sel):
                return self.del_list(key, sel)
            if utils.is_dict_sel(sel):
                return self.del_dict(key, sel)
            raise ValueError(f"Invalid selector type {type(sel)}: {sel}")  # pragma: no cover
        return bool(self._check_type(self._redis.delete(redis_name), int, 0))
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridValue|_T]:
        redis_keys = self._key_list(keys)
        data = self._redis.mget(redis_keys)
        if not isinstance(data, Iterable):
            return [alt] * len(redis_keys)
        return [self._decode(x) if x is not None else alt for x in data]
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        req = {self._key_name(k): self._encode(v) for k, v in pairs}
        if flags == VSPutFlag.UNCHECKED:
            return len(pairs) if self._check_bool(self._redis.mset(req)) else 0
        elif flags & VSPutFlag.NO_CHANGE and flags & VSPutFlag.ATOMICITY:
            return len(pairs) if self._check_bool(self._redis.msetnx(req)) else 0
        else:
            return super().put_bulk(data, flags)
    def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
        # No need to lock, assuming redis delete is atomic
        return self._check_type(self._redis.delete(
            *(self._key_name(k) for k in keys)
        ), int, 0)

class RedisAsyncStore(_RedisBaseStore, AsyncStore):
    def __init__(self, *args, _aredis: aredis.Redis|None=None, **kwargs):
        self._aredis = aredis.Redis(**self._redis_args(kwargs)) if _aredis is None else _aredis
        super().__init__(*args, **kwargs)
    @classmethod
    async def from_url(cls, url: str, *args, **kwargs) -> 'RedisAsyncStore':
        # Allow passing an URL through but the content is not checked
        assert url.startswith('redis://')
        redis_kwargs = cls._redis_args(kwargs)
        return cls(*args, _aredis=aredis.Redis.from_url(url, **redis_kwargs), **kwargs)
    def substore(self, name: str, *args: str) -> 'RedisAsyncStore':
        return self.__class__(_aredis=self._aredis, name_prefix=self._build_name_prefix(
            self._name_prefix, name, *args
        ))
    async def finalize(self, depth=0):
        await self._aredis.aclose()
    async def wipe_all(self) -> int:
        """This is mainly for testing."""
        keys = await self._aredis.keys(self._name_prefix + "*")
        if not isinstance(keys, Iterable):  # pragma: no cover
            error(f"Redis.keys() returns a type {type(keys)}")
            return -1
        if not keys:
            return 0
        return self._check_type(await self._aredis.delete(*keys), int, -1)

    def get_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        return self._aredis.lock((name or "*GLOBAL*") + "\v*LOCK*")
    async def _get_name_meta(self, name: str) -> FridTypeSize|None:
        t = await self._aredis.type(name)
        if t is None:
            return None
        t = self._check_text(t)
        if t == 'list':
            result = await self._aredis.llen(name) # type: ignore
            return ('list', self._check_type(result, int, 0))
        if t == 'hash':
            result = await self._aredis.hlen(name)  # type: ignore
            return ('dict', self._check_type(result, int, 0))
        data: bytes|None = await self._aredis.get(name)
        if data is None:
            return None
        return frid_type_size(self._decode(data))
    async def get_keys(self, pat: KeySearch) -> AsyncIterator[VStoreKey]:
        # TODO: speed up to convert KeySearch to a minimal superset Redis pattern
        keys = await self._aredis.keys()
        if not isinstance(keys, Iterable):
            return
        for k in keys:
            key = self._revive_key(k, pat)
            if key is not None:
                yield key
    async def get_meta(self, *args: VStoreKey,
                       keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return {k: v for k in utils.list_concat(args, keys)
                if (v := await self._get_name_meta(self._key_name(k))) is not None}
    async def get_list(self, key: VStoreKey, sel: VSListSel=None,
                        /, alt: _T=MISSING) -> list[FridValue]|FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            seq: Sequence = await self._aredis.lrange(redis_name, 0, -1) # type: ignore
            return [self._decode_frid(x) for x in seq]
        if isinstance(sel, int):
            val: bytes|None = await self._aredis.lindex(redis_name, sel)  # type: ignore
            return self._decode_frid(val) if val is not None else alt
        (first, last) = utils.list_bounds(sel)
        seq = await self._aredis.lrange(redis_name, first, last) # type: ignore
        assert isinstance(seq, Sequence)
        if isinstance(sel, slice) and sel.step is not None and sel.step != 1:
            seq = seq[::sel.step]
        return [self._decode_frid(x) for x in seq]
    async def get_dict(self, key: VStoreKey, sel: VSDictSel=None,
                        /, alt: _T=MISSING) -> dict[str,FridValue]|FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            map = await self._aredis.hgetall(redis_name) # type: ignore
            return {k.decode(): self._decode_frid(v) for k, v in map.items()}
        if isinstance(sel, str):
            val: bytes = await self._aredis.hget(redis_name, sel) # type: ignore
            return self._decode_frid(val) if val is not None else alt
        if isinstance(sel, Sequence):
            if not isinstance(sel, list):
                sel = list(sel)  # pragma: no cover
            seq = await self._aredis.hmget(redis_name, sel) # type: ignore
            assert is_list_like(seq)
            return {k: self._decode_frid(v) for i, k in enumerate(sel)
                    if (v := seq[i]) is not None}
        raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")  # pragma: no cover
    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                       /, dtype: FridTypeName='') -> FridValue|MissingType:
        if dtype == 'list' or (sel is not None and utils.is_list_sel(sel)):
            return await self.get_list(key, cast(VSListSel, sel))
        if dtype == 'dict' or (sel is not None and utils.is_dict_sel(sel)):
            return await self.get_dict(key, cast(VSDictSel, sel))
        redis_name = self._key_name(key)
        if not dtype:
            t = self._check_text(await self._aredis.type(redis_name)) # Just opportunisitic; no lock
            if t == 'list':
                return await self.get_list(key, cast(VSListSel, sel))
            if t == 'hash':
                return await self.get_dict(key, cast(VSDictSel, sel))
        data = await self._aredis.get(redis_name)
        return self._decode(data) if data is not None else MISSING
    async def put_list(self, key: VStoreKey, val: FridArray,
                       /, flags=VSPutFlag.UNCHECKED) -> bool:
        redis_name = self._key_name(key)
        encoded_val = [self._encode_frid(x) for x in val]
        if flags & VSPutFlag.KEEP_BOTH and not (flags & VSPutFlag.NO_CHANGE):
            if not encoded_val:
                return False
            if flags & VSPutFlag.NO_CREATE:
                result = await self._aredis.rpushx(redis_name, *encoded_val) # type: ignore
            else:
                result = await self._aredis.rpush(redis_name, *encoded_val) # type: ignore
        else:
            async with self.get_lock(redis_name):
                if await self._aredis.exists(redis_name):
                    if flags & VSPutFlag.NO_CHANGE:
                        return False
                    await self._aredis.delete(redis_name)
                    retval = True
                else:
                    if flags & VSPutFlag.NO_CREATE:
                        return False
                    retval = False
                if not encoded_val:
                    return retval
                result = await self._aredis.rpush(redis_name, *encoded_val) # type: ignore
        return bool(self._check_type(result, int, 0))
    async def put_dict(
            self, key: VStoreKey, val: StrKeyMap, /, flags=VSPutFlag.UNCHECKED
    ) -> bool:
        redis_name = self._key_name(key)
        encoded_val = {k: v.strfr() if isinstance(v, FridBeing) else self._encode_frid(v)
                       for k, v in val.items() if v is not MISSING}
        if flags & VSPutFlag.KEEP_BOTH and not (
            flags & (VSPutFlag.NO_CHANGE | VSPutFlag.NO_CREATE)
        ):
            if not encoded_val:
                return False
            await self._aredis.hset(redis_name, mapping=encoded_val)  # type: ignore
            return bool(encoded_val)  # Note result contains only the number of entries added
        async with self.get_lock(redis_name):
            if await self._aredis.exists(redis_name):
                if flags & VSPutFlag.NO_CHANGE:
                    return False
                await self._aredis.delete(redis_name)
                retval = True
            else:
                if flags & VSPutFlag.NO_CREATE:
                    return False
                retval = False
            if not encoded_val:
                return retval
            result = await self._aredis.hset(redis_name, mapping=encoded_val) # type: ignore
        return bool(self._check_type(result, int, 0))
    async def put_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> bool:
        if is_frid_array(val):
            return await self.put_list(key, val, flags)
        if is_frid_skmap(val):
            return await self.put_dict(key, val, flags)
        redis_name = self._key_name(key)
        nx = bool(flags & VSPutFlag.NO_CHANGE)
        xx = bool(flags & VSPutFlag.NO_CREATE)
        if flags & VSPutFlag.KEEP_BOTH:
           async with self.get_lock():
               data = await self._aredis.get(redis_name)
               return self._check_bool(await self._aredis.set(redis_name, self._encode(
                   frid_merge(self._decode(data), val, depth=0) if data is not None else val
               ), nx=nx, xx=xx))
        return self._check_bool(await self._aredis.set(
            redis_name, self._encode(val), nx=nx, xx=xx
        ))
    async def del_list(self, key: VStoreKey, sel: VSListSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            return bool(self._check_type(await self._aredis.delete(redis_name), int, 0))
        (first, last) = utils.list_bounds(sel)
        if utils.is_straight(sel):
            if last == -1:
                result = await self._aredis.ltrim(redis_name, 0, first - 1) # type: ignore
                return self._check_bool(result)
            if first == 0:
                result = await self._aredis.ltrim(redis_name, last + 1, -1) # type: ignore
                return self._check_bool(result)
        async with self.get_lock(redis_name):
            result = await self._aredis.lrange(redis_name, 0, -1) # type: ignore
            if not result:
                return False
            assert isinstance(result, list)
            if utils.list_delete(result, sel):
                await self._aredis.delete(redis_name)
                if result:
                    await self._aredis.rpush(redis_name, *result) # type: ignore
                return True
            return False
    async def del_dict(self, key: VStoreKey, sel: VSDictSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            result = await self._aredis.delete(redis_name)
        elif isinstance(sel, str):
            result = await self._aredis.hdel(redis_name, sel) # type: ignore
        elif isinstance(sel, Sequence):
            assert is_list_like(sel, str)
            if not sel:
                return False
            if not isinstance(sel, list):
                sel = list(sel)
            result = await self._aredis.hdel(redis_name, *sel) # type: ignore
        else:   # pragma: no cover
            raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")
        return bool(self._check_type(result, int, 0))
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is not None:
            if utils.is_list_sel(sel):
                return await self.del_list(key, sel)
            if utils.is_dict_sel(sel):
                return await self.del_dict(key, sel)
            raise ValueError(f"Invalid selector type {type(sel)}: {sel}")  # pragma: no cover
        return bool(self._check_type(await self._aredis.delete(redis_name), int, 0))
    async def get_bulk(self, keys: Iterable[VStoreKey],
                       /, alt: _T=MISSING) -> list[FridValue|_T]:
        redis_keys = self._key_list(keys)
        data = await self._aredis.mget(redis_keys)
        if not isinstance(data, Iterable):
            return [alt] * len(redis_keys)
        return [self._decode(x) if x is not None else alt for x in data]
    async def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        req = {self._key_name(k): self._encode(v) for k, v in pairs}
        if flags == VSPutFlag.UNCHECKED:
            return len(pairs) if self._check_bool(await self._aredis.mset(req)) else 0
        elif flags & VSPutFlag.NO_CHANGE and flags & VSPutFlag.ATOMICITY:
            return len(pairs) if self._check_bool(await self._aredis.msetnx(req)) else 0
        else:
            return await super().put_bulk(data, flags)
    async def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
        # No need to lock, assuming redis delete is atomic
        return self._check_type(await self._aredis.delete(
            *(self._key_name(k) for k in keys)
        ), int, 0)
