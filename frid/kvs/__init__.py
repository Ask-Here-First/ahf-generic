import asyncio
from urllib.parse import unquote

from ..typing import FridNameArgs
from ..loader import load_frid_str
from .store import ValueStore, AsyncStore
from .proxy import ValueProxyStore, AsyncProxyStore, AsyncProxyValueStore, ValueProxyAsyncStore
from .utils import VStoreKey, VStoreSel, VSPutFlag
from .basic import MemoryValueStore
from .files import FileIOValueStore

_value_store_constructors: dict[str,type[ValueStore]] = {
    MemoryValueStore.URL_SCHEME: MemoryValueStore,
    FileIOValueStore.URL_SCHEME: FileIOValueStore,
}

_async_store_constructors: dict[str,type[AsyncStore]] = {
}

def _get_scheme(url: str) -> str:
    return url[:url.index('://')]

def is_local_store_url(url: str) -> bool:
    return _get_scheme(url) in (
        MemoryValueStore.URL_SCHEME, FileIOValueStore.URL_SCHEME
    )

def is_dbsql_store_url(url: str) -> bool:
    scheme = _get_scheme(url)
    if scheme in _value_store_constructors or scheme in _async_store_constructors:
        return False
    return '' in _value_store_constructors or '' in _async_store_constructors

def _split_url_varargs(url: str) -> FridNameArgs:
    (url, sep, frag) = url.partition('#')
    if not sep:
        return FridNameArgs(url, (), {})
    args = []
    kwds = {}
    for item in frag.split('&'):
        (key, sep, val) = item.partition('=')
        if sep:
            kwds[unquote(key)] = load_frid_str(unquote(val))
        else:
            args.append(load_frid_str(unquote(key)))
    return FridNameArgs(url, args, kwds)

def create_value_store(url: str, *args, **kwargs) -> ValueStore|None:
    name_args = _split_url_varargs(url)
    scheme = _get_scheme(url)
    value_cls = _value_store_constructors.get(scheme)
    if value_cls is not None:
        return value_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        )
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return AsyncProxyValueStore(asyncio.run(async_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        )))
    value_cls = _value_store_constructors.get('')
    if value_cls is not None:
        return value_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        )
    raise ValueError(f"Storage URL scheme is not supported: {scheme}")

async def create_async_store(url: str, *args, **kwargs) -> AsyncStore|None:
    name_args = _split_url_varargs(url)
    scheme = _get_scheme(url)
    async_cls = _async_store_constructors.get(scheme)
    if async_cls is not None:
        return await async_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        )
    scheme = _get_scheme(url)
    value_cls = _value_store_constructors.get(scheme)
    if value_cls is not None:
        return ValueProxyAsyncStore(value_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        ))
    async_cls = _async_store_constructors.get('')
    if async_cls is not None:
        return await async_cls.from_url(
            name_args.name, *name_args.args, *args, **name_args.kwds, **kwargs
        )
    raise ValueError(f"Storage URL scheme is not supported: {scheme}")

__all__ = [
    'ValueStore', 'AsyncStore',
    'ValueProxyStore', 'AsyncProxyStore', 'AsyncProxyValueStore', 'ValueProxyAsyncStore',
    'VStoreKey', 'VStoreSel', 'VSPutFlag',
    'MemoryValueStore', 'FileIOValueStore',
    'create_value_store', 'create_async_store', 'is_local_store_url', 'is_dbsql_store_url',
]


try:
    from .redis import RedisValueStore, RedisAsyncStore
    _value_store_constructors['redis'] = RedisValueStore
    _async_store_constructors['rediss'] = RedisAsyncStore
except ImportError:
    pass

try:
    from .dbsql import DbsqlValueStore, DbsqlAsyncStore
    _value_store_constructors[''] = DbsqlValueStore
    _async_store_constructors[''] = DbsqlAsyncStore
except ImportError:
    pass
