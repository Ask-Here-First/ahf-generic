from abc import abstractmethod
from collections.abc import Collection, Iterable
from typing import TypeVar, Any, overload

_T = TypeVar('_T')
_K = TypeVar('_K')
_V = TypeVar('_V')
_Class = TypeVar('_Class', bound=dict)

class _DictKeysView(Collection[_K]):
    __slots__ = ['data']
    def __init__(self, data: 'TransKeyDict[_K,Any]'):
        self.data = data
    def __iter__(self):
        return (self.data._real_keys.get(k, k) for k in self.data.keys())
    def __reversed__(self):
        return (self.data._real_keys.get(k, k) for k in reversed(self.data.keys()))
    def __len__(self):
        return len(self.data)
    def __contains__(self, key):
        return key in self.data.keys()
class _DictItemsView(Collection[tuple[_K,_V]]):
    __slots__ = ['data']
    def __init__(self, data: 'TransKeyDict[_K,_V]'):
        self.data = data
    def __iter__(self):
        return ((self.data._real_keys.get(k, k), v) for k, v in self.data.items())
    def __reversed__(self):
        return ((self.data._real_keys.get(k, k), v) for k, v in reversed(self.data.items()))
    def __len__(self):
        return len(self.data)
    def __contains__(self, item):
        if not isinstance(item, Collection) or len(item) != 2:
            return False
        (k, v) = item
        return (self.data._trans_key(k), v) in self.data.items()


class TransKeyDict(dict[_K,_V]):
    __slots__ = ['_real_keys']

    def __init__(self, *args, **kwargs):
        self._real_keys = {}   # Only store the keys that are transformed to a differen object
        self.update(*args, **kwargs)

    @abstractmethod
    def _trans_key(self, key: _K|Any, /):
        raise NotImplementedError

    def __iter__(self):
        return (self._real_keys.get(k, k) for k in self.keys())

    def __getitem__(self, key: _K, /) -> _V:
        return super().__getitem__(self._trans_key(key))

    def __setitem__(self, key: _K, value: _V, /):
        real_key = self._trans_key(key)
        if real_key is not key:
            self._real_keys[real_key] = key
        return super().__setitem__(real_key, value)

    def __delitem__(self, key: _K, /):
        real_key = self._trans_key(key)
        self._real_keys.pop(real_key, None)
        return super().__delitem__(real_key)

    def __contains__(self, key, /) -> bool:
        return super().__contains__(self._trans_key(key))

    def __or__(self, other):
        return self.__class__(self).__ior__(other)

    def __ior__(self, other):
        self.update(other)
        return self

    def clear(self):
        self._real_keys.clear()
        return super().clear()

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls: type[_Class], keys: Iterable, /, value=None) -> _Class:
        return cls((key, value) for key in keys)

    @overload
    def get(self, key, default: None=None, /) -> _V|None: ...
    @overload
    def get(self, key, default: _T, /) -> _V|_T: ...
    def get(self, key, default: _T|None=None, /) -> _V|_T|None:
        return super().get(self._trans_key(key), default)

    def items(self): # type: ignore -- The return view is not of the same type
        return _DictItemsView(self)

    def keys(self):  # type: ignore -- The return view is not of the same type
        return _DictKeysView(self)

    @overload
    def pop(self, key: _K) -> _V: ...
    @overload
    def pop(self, key: _K, default: _T) -> _V|_T: ...
    __dummy = object()
    def pop(self, key, default=__dummy):
        real_key = self._trans_key(key)
        if default is self.__dummy:
            result = super().pop(real_key)
        else:
            result = super().pop(real_key, default)
        self._real_keys.pop(real_key, None)
        return result

    def popitem(self):
        (key, value) = super().popitem()
        return (self._real_keys.pop(key, key), value)

    ### reversed(self): -- no need to override because it is just reversed(self.keys())?

    def setdefault(self, key: _K, /, default: _V=None) -> _V|None:  # type: ignore
        real_key = self._trans_key(key)
        self._real_keys.setdefault(real_key, key)
        return super().setdefault(real_key, default)

    def update(self, *args, **kwargs):
        # TODO: a better implementation?
        for key, value in dict(*args, **kwargs).items():
            self.__setitem__(key, value)

    ### values(self): (not need to override because the implementation is indentical

class CaseDict(TransKeyDict):
    def _trans_key(self, key, /):
        return key.lower() if isinstance(key, str) and not key.islower() else key