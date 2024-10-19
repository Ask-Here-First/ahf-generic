from abc import abstractmethod
from collections.abc import Collection, ItemsView, Iterable, KeysView, Mapping, Sequence
from typing import TypeVar, Any, overload

_T = TypeVar('_T')
_K = TypeVar('_K')
_V = TypeVar('_V')
_Class = TypeVar('_Class', bound=dict)

class TransKeyDict(dict[_K,_V]):
    __slots__ = ['_real_keys']

    @abstractmethod
    def key_func(self, key: _K|Any, /):
        """The function to transform the key to the actual key used in the dict.
        The derived class must implement this.
        """
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._real_keys = {}   # Only store the keys that are transformed to a differen object
        self.update(*args, **kwargs)

    def __iter__(self):
        return (self._real_keys.get(k, k) for k in self.keys())

    def __reversed__(self):
        return (self._real_keys.get(k, k) for k in reversed(self.keys()))

    def __getitem__(self, key: _K, /) -> _V:
        return super().__getitem__(self.key_func(key))

    def __setitem__(self, key: _K, value: _V, /):
        real_key = self.key_func(key)
        if real_key is key:
            self._real_keys.pop(real_key, None)
        else:
            self._real_keys[real_key] = key
        return super().__setitem__(real_key, value)

    def __delitem__(self, key: _K, /):
        real_key = self.key_func(key)
        self._real_keys.pop(real_key, None)
        return super().__delitem__(real_key)

    def __contains__(self, key, /) -> bool:
        return super().__contains__(self.key_func(key))

    def __eq__(self, other) -> bool:
        if isinstance(other, Mapping):
            items = other.items()
        elif isinstance(other, Iterable):
            items = other
        else:
            return False
        key_set = set()
        for x in items:
            if not isinstance(x, Sequence) or len(x) != 2:
                return False
            (k, v) = x
            k = self.key_func(k)
            if k in key_set or not super().__contains__(k) or v != super().__getitem__(k):
                return False
            key_set.add(k)
        return len(key_set) == super().__len__()

    def __or__(self, other):
        return self.__class__(self).__ior__(other)

    def __ior__(self, other):
        self.update(other)
        return self

    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(dict(self)) + ')'

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
        return super().get(self.key_func(key), default)

    def items(self): # type: ignore -- The return view is not of the same type
        return TransKeyDictItemsView(super().items(), self)

    def keys(self):  # type: ignore -- The return view is not of the same type
        return TransKeyDictKeysView(super().keys(), self)

    @overload
    def pop(self, key: _K) -> _V: ...
    @overload
    def pop(self, key: _K, default: _T) -> _V|_T: ...
    __dummy = object()
    def pop(self, key, default=__dummy):
        real_key = self.key_func(key)
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
        real_key = self.key_func(key)
        if real_key is not key and not super().__contains__(key):
            self._real_keys[real_key] = key
        return super().setdefault(real_key, default)

    def update(self, *args, **kwargs):
        # TODO: a better implementation?
        for key, value in dict(*args, **kwargs).items():
            self.__setitem__(key, value)

    ### values(self): (not need to override because the implementation is indentical

class TransKeyDictKeysView(KeysView[_K]):
    __slots__ = ['data', 'keys']
    def __init__(self, real_keys: KeysView[_K], data: TransKeyDict[_K,Any]):
        self.keys = real_keys
        self.data = data
    def __iter__(self):
        return (self.data._real_keys.get(k, k) for k in self.keys)
    def __reversed__(self):
        return (self.data._real_keys.get(k, k) for k in reversed(self.keys))
    def __len__(self):
        return len(self.keys)
    def __contains__(self, key):
        return self.data.key_func(key) in self.keys
    def __eq__(self, other):
        if not isinstance(other, Iterable):
            return False
        key_set = set()
        for k in other:
            k = self.data.key_func(k)
            if k in key_set:
                return False
            key_set.add(k)
        return len(key_set) == len(self.keys)
    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(list(self)) + ')'

class TransKeyDictItemsView(ItemsView[_K,_V]):
    __slots__ = ['data', 'items']
    def __init__(self, items: ItemsView[_K,_V], data: TransKeyDict[_K,_V]):
        self.data = data
        self.items = items
    def __iter__(self):
        return ((self.data._real_keys.get(k, k), v) for k, v in self.items)
    def __reversed__(self):
        return ((self.data._real_keys.get(k, k), v) for k, v in reversed(self.items))
    def __len__(self):
        return len(self.items)
    def __contains__(self, item):
        if not isinstance(item, Collection) or len(item) != 2:
            return False
        (k, v) = item
        return (self.data.key_func(k), v) in self.items
    def __eq__(self, other):
        return self.data.__eq__(other)
    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(list(self)) + ')'

class CaseDict(TransKeyDict[_K,_V]):
    def key_func(self, key, /):
        if not isinstance(key, str):
            return key
        low_key = key.lower()
        return key if key == low_key else low_key
