from typing import Callable
from .store import ValueStore, AsyncStore
from .proxy import ValueProxyStore, AsyncProxyStore, AsyncProxyValueStore, ValueProxyAsyncStore
from .utils import VStoreKey, VStoreSel, VSPutFlag
from .basic import MemoryValueStore
from .files import FileIOValueStore

_value_store_constructors: dict[str,Callable[[str],ValueStore]] = {
    'memory': MemoryValueStore,
    'file': FileIOValueStore,
}

_async_store_constructors: dict[str,Callable[[str],AsyncStore]] = {
    'memory': lambda url: ValueProxyAsyncStore(MemoryValueStore(url)),
    'file': lambda url: ValueProxyAsyncStore(FileIOValueStore(url)),
}

def _get_scheme(url: str) -> str:
    return url[:url.index('://')]

def create_value_store(url: str|None) -> ValueStore|None:
    if url is None:
        return None
    scheme = _get_scheme(url)
    func = _value_store_constructors.get(scheme)
    if func is None:
        func = _value_store_constructors.get('')
        if func is None:
            raise ValueError(f"Storage URL scheme is not supported: {scheme}")
    return func(url)

def create_async_store(url: str|None) -> AsyncStore|None:
    if url is None:
        return None
    scheme = _get_scheme(url)
    func = _async_store_constructors.get(scheme)
    if func is None:
        func = _async_store_constructors.get('')
        if func is None:
            raise ValueError(f"Storage URL scheme is not supported: {scheme}")
    return func(url)

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
