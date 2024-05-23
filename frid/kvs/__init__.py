import asyncio

from .store import ValueStore, AsyncStore
from .proxy import ValueProxyStore, AsyncProxyStore, AsyncProxyValueStore, ValueProxyAsyncStore
from .utils import VStoreKey, VStoreSel, VSPutFlag
from .basic import MemoryValueStore
from .files import FileIOValueStore

_value_store_constructors: dict[str,type[ValueStore]] = {
    'memory': MemoryValueStore,
    'file': FileIOValueStore,
}

_async_store_constructors: dict[str,type[AsyncStore]] = {
}

def _get_scheme(url: str) -> str:
    return url[:url.index('://')]

def create_value_store(url: str|None) -> ValueStore|None:
    if url is None:
        return None
    scheme = _get_scheme(url)
    value_cls = _value_store_constructors.get(scheme)
    if value_cls is not None:
        return value_cls.from_url(url)
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return AsyncProxyValueStore(asyncio.run(async_cls.from_url(url)))
    value_cls = _value_store_constructors.get('')
    if value_cls is not None:
        return value_cls.from_url(url)
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return AsyncProxyValueStore(asyncio.run(async_cls.from_url(url)))
    raise ValueError(f"Storage URL scheme is not supported: {scheme}")

def create_async_store(url: str|None) -> AsyncStore|None:
    if url is None:
        return None
    scheme = _get_scheme(url)
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return asyncio.get_event_loop().run_until_complete(async_cls.from_url(url))
    scheme = _get_scheme(url)
    value_cls = _value_store_constructors.get(scheme)
    if value_cls is not None:
        return ValueProxyAsyncStore(value_cls.from_url(url))
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return asyncio.get_event_loop().run_until_complete(async_cls.from_url(url))
    value_cls = _value_store_constructors.get('')
    if value_cls is not None:
        return ValueProxyAsyncStore(value_cls.from_url(url))
    raise ValueError(f"Storage URL scheme is not supported: {scheme}")

def is_local_store_url(url: str|None) -> bool:
    if url is None:
        return False
    return any(url.startswith(x) for x in ("file://", "memory://"))

__all__ = [
    'ValueStore', 'AsyncStore',
    'ValueProxyStore', 'AsyncProxyStore', 'AsyncProxyValueStore', 'ValueProxyAsyncStore',
    'VStoreKey', 'VStoreSel', 'VSPutFlag',
    'MemoryValueStore', 'FileIOValueStore',
    'create_value_store', 'create_async_store', 'is_local_store_url',
]


try:
    from .redis import RedisValueStore, RedisAsyncStore
    _value_store_constructors['redis'] = RedisValueStore
    _async_store_constructors['redis'] = RedisAsyncStore
except ImportError:
    pass

try:
    from .dbsql import DbsqlValueStore, DbsqlAsyncStore
    _value_store_constructors[''] = DbsqlValueStore
    _async_store_constructors[''] = DbsqlAsyncStore
except ImportError:
    pass
