import traceback
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from collections.abc import Iterable, Mapping, Sequence
from typing import TypeVar, cast, overload
from logging import error

import redis

from ..typing import MISSING, BlobTypes, MissingType
from ..typing import FridArray, FridSeqVT, FridTypeSize, FridValue, StrKeyMap
from ..guards import as_kv_pairs, is_frid_array, is_frid_skmap, is_list_like
from ..strops import escape_control_chars
from ..helper import frid_merge, frid_type_size
from ..dumper import dump_into_str
from ..loader import load_from_str
from .store import VSDictSel, VSListSel, VStoreSel
from .store import ValueStore, VStorePutBulkData, VSPutFlag, VStoreKey

_T = TypeVar('_T')

class RedisValueStore(ValueStore):
    NAMESPACE_SEP = '\t'
    def __init__(self, host: str|None=None, port: int=0,
                 username: str|None=None, password: str|None=None,
                 *, parent: 'RedisValueStore|None'=None, name_prefix: str='',
                 frid_prefix: bytes=b'#!', blob_prefix: bytes=b'#='):
        super().__init__()
        if isinstance(parent, __class__):
            self._redis = parent._redis
        else:
            assert host is not None
            self._redis = redis.StrictRedis(host=host, port=port,
                                            username=username, password=password)
        self._name_prefix = name_prefix
        self._frid_prefix = frid_prefix
        self._blob_prefix = blob_prefix

    def substore(self, name: str, *args: str) -> 'RedisValueStore':
        prefix = name + self.NAMESPACE_SEP
        if self._name_prefix:
            prefix = self._name_prefix + self.NAMESPACE_SEP + prefix
        if args:
            prefix += self.NAMESPACE_SEP.join(args) + self.NAMESPACE_SEP
        return __class__(parent=self, name_prefix=prefix)

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
        if isinstance(data, typ):
            return data
        # TODO: generic code to log current or given stacktrace or exception
        trace = '\n'.join(traceback.format_list(traceback.extract_stack()))
        error(f"Incorrect Redis return type {type(data)}; expecting {typ}, at\n{trace}\n")
        return default
    def _check_bool(self, data) -> bool:
        if data is None:
            return False   # Redis-py actually returns None for False sometimes
        return self._check_type(data, bool, False)
    def _check_text(self, data) -> str|None:
        if data is None:
            return None
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            return data.decode()
        if isinstance(data, (memoryview, bytearray)):
            return bytes(data).decode('utf-8')
        error(f"Incorrect Redis return type {type(data)}; expecting string")
        return None

    def _encode_frid(self, data) -> bytes:
        if isinstance(data, BlobTypes):
            return self._blob_prefix + data
        if isinstance(data, str):
            b = data.encode('utf-8')
            if not b.startswith(self._blob_prefix) and not b.startswith(self._frid_prefix):
               return b
        return self._frid_prefix + dump_into_str(data).encode('utf-8')
    def _decode_frid(self, data, alt: _T=MISSING) -> FridValue|_T:
        if data is None:
            return alt
        if not isinstance(data, BlobTypes):
            error(f"Incorrect Redis return type {type(data)}; expecting binary")
            return alt
        if not isinstance(data, bytes):
            data = bytes(data)
        if data.startswith(self._frid_prefix):
            return load_from_str(data[len(self._frid_prefix):].decode('utf-8'))
        if data.startswith(self._blob_prefix):
            return data[len(self._blob_prefix):]
        return data.decode('utf-8')
    def _decode_list(self, data) -> list[FridValue]:
        if not isinstance(data, Iterable):
            return []
        # It should not have None is data, so it does not matter what alt is
        return [self._decode_frid(x, None) for x in data]
    def _decode_dict(self, data) -> dict[str,FridValue]|None:
        out = {}
        for k, v in as_kv_pairs(data):
            key = self._check_text(k)
            val = self._decode_frid(v)
            if key is not None and val is not MISSING:
                out[key] = val
        return out

    def _delete_list_range(self, name: str, index: int, until: int) -> bool:
        if until == 0:
            return self._check_bool(self._redis.ltrim(name, 0, index - 1))
        if index == 0:
            return self._check_bool(self._redis.ltrim(name, until, -1))
        with self.get_lock(name):
            data = self._redis.lrange(name, 0, -1)
            if not data:
                return False
            assert isinstance(data, list)
            if self._list_delete(data, (index, until)):
                self._redis.delete(name)
                self._redis.rpush(name, *data)
                return True
            return False
    def get_lock(self, name: str|None=None) -> AbstractContextManager:
        return self._redis.lock((name or "*GLOBAL*") + "\v*LOCK*")
    def _get_name_meta(self, name: str) -> FridTypeSize|None:
        t = self._check_text(self._redis.type(name))
        if t == 'list':
            return ('list', self._check_type(self._redis.llen(name), int, 0))
        if t == 'hash':
            return ('dict', self._check_type(self._redis.hlen(name), int, 0))
        data: FridValue|MissingType = self._decode_frid(self._redis.get(name))
        if data is MISSING:
            return None
        return frid_type_size(data)
    def get_meta(self, keys: Iterable[VStoreKey]) -> Mapping[VStoreKey,FridTypeSize]:
        results = self._redis.keys()
        if not isinstance(results, Iterable):
            return {}
        results = set(b.decode('utf-8') for b in results)
        return {k: v for k in keys if (name := self._key_name(k)) in results
                                      and (v := self._get_name_meta(name)) is not None}
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
    def get_list(self, key: VStoreKey, sel: VSListSel=None, /, alt: _T=MISSING) -> FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            return self._decode_list(self._redis.lrange(redis_name, 0, -1))
        if isinstance(sel, int):
            return self._decode_frid(self._redis.lindex(redis_name, sel), alt)
        if isinstance(sel, tuple):
            (index, until) = sel
            # Should not have missing entry here
            return self._decode_list(self._redis.lrange(redis_name, index, until - 1))
        if isinstance(sel, slice):
            if sel.step is not None and sel.step != 1:
                # Todo to support non-continuous slicing by getting the range then slice locally
                raise ValueError(f"Non-continuous slicing is not supported {sel}")
            return self._decode_list(self._redis.lrange(
                redis_name, sel.start or 0, (sel.stop or 0) - 1,
            ))  # Should not have missing entry here
        raise ValueError(f"Invalid list selector type {type(sel)}: {sel}")
    def get_dict(self, key: VStoreKey, sel: VSDictSel=None, /, alt: _T=MISSING) -> FridValue|_T:
        redis_name = self._key_name(key)
        if sel is None:
            return self._decode_dict(self._redis.hgetall(redis_name))
        if isinstance(sel, str):
            return self._decode_frid(self._redis.hget(redis_name, sel), alt)
        if isinstance(sel, Sequence):
            if not isinstance(sel, list):
                sel = list(sel)
            data = self._redis.hmget(redis_name, sel)
            assert is_list_like(data)
            return {k: self._decode_frid(v) for i, k in enumerate(sel)
                    if (v := data[i]) is not None}
        raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")
    def get_frid(self, key: VStoreKey, sel: VStoreSel=None) -> FridValue|MissingType:
        if sel is not None:
            if self._is_list_sel(sel):
                return self.get_list(key, cast(VSListSel, sel))
            if self._is_dict_sel(sel):
                return self.get_dict(key, sel)
            raise ValueError(f"Invalid selector type {type(sel)}: {sel}")
        redis_name = self._key_name(key)
        t = self._check_text(self._redis.type(redis_name)) # Just opportunisitic; no lock
        if t == 'list':
            return self.get_list(key, cast(VSListSel, sel))
        if t == 'dict':
            return self.get_dict(key, sel)
        return self._decode_frid(self._redis.get(redis_name))
    def put_list(self, key: VStoreKey, val: FridArray, /, flags=VSPutFlag.UNCHECKED) -> bool:
        redis_name = self._key_name(key)
        encoded_val = [self._encode_frid(x) for x in val]
        if flags & VSPutFlag.KEEP_BOTH and not (flags & VSPutFlag.NO_CHANGE):
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
                else:
                    if flags & VSPutFlag.NO_CREATE:
                        return False
                result = self._redis.rpush(redis_name, *encoded_val)
        return bool(self._check_type(result, int, 0))
    def put_dict(self, key: VStoreKey, val: StrKeyMap, /, flags=VSPutFlag.UNCHECKED) -> bool:
        redis_name = self._key_name(key)
        if not isinstance(val, dict):
            val = dict(val)
        if flags & VSPutFlag.KEEP_BOTH and not (
            flags & (VSPutFlag.NO_CHANGE | VSPutFlag.NO_CREATE)
        ):
            result = self._redis.hset(redis_name, mapping=val)
        else:
            with self.get_lock(redis_name):
                if self._redis.exists(redis_name):
                    if flags & VSPutFlag.NO_CHANGE:
                        return False
                    self._redis.delete(redis_name)
                else:
                    if flags & VSPutFlag.NO_CREATE:
                        return False
                result = self._redis.hset(redis_name, mapping=val)
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
               data = self._redis.get(redis_name)
               return self._check_bool(self._redis.set(redis_name, self._encode_frid(
                   frid_merge(self._decode_frid(data), val)
               ), nx=nx, xx=xx))
        return self._check_bool(self._redis.set(
            redis_name, self._encode_frid(val), nx=nx, xx=xx
        ))
    def del_list(self, key: VStoreKey, sel: VSListSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            return bool(self._check_type(self._redis.delete(redis_name), int, 0))
        if isinstance(sel, int):
            return self._delete_list_range(redis_name, sel, sel + 1)
        if isinstance(sel, tuple):
            (index, until) = sel
            return self._delete_list_range(redis_name, index, until)
        if isinstance(sel, slice):
            if sel.step is not None and sel.step != 1:
                # Todo to support non-continuous slicing by getting the range then slice locally
                raise ValueError(f"Non-continuous slicing is not supported {sel}")
            return self._delete_list_range(redis_name, sel.start or 0, sel.stop or 0)
        raise ValueError(f"Invalid list selector type {type(sel)}: {sel}")
    def del_dict(self, key: VStoreKey, sel: VSDictSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is None:
            return bool(self._check_type(self._redis.delete(redis_name), int, 0))
        if isinstance(sel, str):
            return bool(self._check_type(self._redis.hdel(redis_name, sel), int, 0))
        if isinstance(sel, Sequence):
            assert is_list_like(sel, str)
            if not isinstance(sel, list):
                sel = list(sel)
            return bool(self._check_type(self._redis.hdel(redis_name, *sel), int, 0))
        raise ValueError(f"Invalid dict selector type {type(sel)}: {sel}")
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        redis_name = self._key_name(key)
        if sel is not None:
            if self._is_list_sel(sel):
                return self.del_list(key, sel)
            if self._is_dict_sel(sel):
                return self.del_dict(key, sel)
            raise ValueError(f"Invalid selector type {type(sel)}: {sel}")
        return bool(self._check_type(self._redis.delete(redis_name), int, 0))
    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        redis_keys = self._key_list(keys)
        data = self._redis.mget(redis_keys)
        if not isinstance(data, Iterable):
            return [alt] * len(redis_keys)
        return [self._decode_frid(x, alt) for x in data]
    def put_bulk(self, data: VStorePutBulkData, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        req = {self._key_name(k): self._encode_frid(v) for k, v in pairs}
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

    def wipe_all(self) -> int:
        """This is mainly for testing."""
        keys = self._redis.keys(self._name_prefix + "*")
        if not isinstance(keys, Iterable):
            return -1
        if not keys:
            return 0
        return self._check_type(self._redis.delete(*keys), int, -1)

    def aget_lock(self, name: str|None=None) -> AbstractAsyncContextManager:
        raise NotImplementedError  # pragma: no cover
    async def aget_meta(self, keys: Iterable[VStoreKey], /) -> Mapping[VStoreKey,FridTypeSize]:
        raise NotImplementedError  # pragma: no cover
