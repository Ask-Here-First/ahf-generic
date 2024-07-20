"""This class implement a basic value store that retrives the whole data then do selection.
It will derive a memory based store from there
"""
import asyncio, threading
from dataclasses import dataclass, field
from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Concatenate, Generic, ParamSpec, TypeVar
from urllib.parse import urlparse

from ..typing import MISSING, PRESENT, BlobTypes, MissingType
from ..typing import FridTypeName, FridTypeSize, FridValue, FridArray, FridBeing, StrKeyMap
from ..autils import AsyncReentrantLock
from ..chrono import parse_datetime, strfr_datetime, datetime, timezone
from ..guards import is_frid_array, is_frid_skmap
from ..helper import frid_merge, frid_type_size
from ..strops import escape_control_chars, revive_control_chars
from ..dumper import dump_frid_str
from ..loader import load_frid_str
from .store import AsyncStore, ValueStore
from .utils import KeySearch, VSPutFlag, VStoreKey, VStoreSel, frid_delete, frid_select, list_concat, match_key

_T = TypeVar('_T')
_E = TypeVar('_E')   # The encoding type
_P = ParamSpec('_P')

ModFunc = Callable[Concatenate[_E|MissingType,_P],_E|tuple[_E,Any]|FridBeing]

class _SimpleBaseStore(Generic[_E]):
    """Simple value store are stores that always handles each item as a whole."""
    @abstractmethod
    def _encode(self, data: FridValue, /) -> _E:
        """Encodes the data into a generic encoding type (bytes, string, etc)."""
        raise NotImplementedError
    @abstractmethod
    def _decode(self, val: _E, /) -> FridValue:
        """Decodes the data from a generic encoded `val` (bytes, string, etc)."""
        raise NotImplementedError

    def _get(self, key: str, /) -> _E|MissingType:
        """Get the whole data from the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    def _put(self, key: str, val: _E, /) -> bool:
        """Write the whole data into the store associated to the given `key`."""
        raise NotImplementedError  # pragma: no cover
    def _rmw(self, key: str, mod: ModFunc[_E,_P],
             /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        """The read-modify-write process for the value of the `key` in the store.
        - `mod`: the callback function to be called with:
            + The current value as the first argument (or MISSING);
            + The values of `*args` and `**kwargs` are passed as the rest of the arguments;
            + It returns either of the following:
                + The updated value to be written to the storage;
                + A pair of the updated value and some auxillary data;
                + PRESENT to keep the original;
                + MISSING to delete the key.
        - This method returns True iff the storage is changed.
        """
        raise NotImplementedError  # pragma: no cover
    def _del(self, key: str, /) -> bool:
        """Delete the data in the store associated to the given `key`.
        - Returns boolean to indicate if the key is deleted (or if the store is changed).
        """
        raise NotImplementedError  # pragma: no cover

    def _key_str(self, key: VStoreKey, /) -> str:
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
        return frid_select(self._decode(val), sel)
    def _add_new(self, old: _E|MissingType, new: FridValue) -> _E|FridBeing:
        """Adds the `new` value into the `old` values (which can be MISSING).
        - Return he updated value (with PRESENT for no change and MISSING to delete entry).
        """
        old_data = old if isinstance(old, FridBeing) else self._decode(old)
        assert old_data is not PRESENT
        # TODO: frid_merge() to accept more merge flags
        new_data = frid_merge(old_data, new, depth=0)
        if new_data is old_data:
            return PRESENT
        return self._encode(new_data)
    def _del_sel(self, val: _E|MissingType, sel: VStoreSel) -> _E|FridBeing:
        """Deletes the selected items in general.
        - Return he updated value (with PRESENT for no change and MISSING to delete entry).
        """
        assert val is not PRESENT
        assert sel is not None
        if val is MISSING:
            return MISSING
        data = self._decode(val)
        (data, cnt) = frid_delete(data, sel)
        if cnt == 0:
            return PRESENT
        return self._encode(data)

class SimpleValueStore(_SimpleBaseStore[_E], ValueStore):
    """This is the base class of simple value store that loads and saves records in full."""
    @abstractmethod
    def _get(self, key: str) -> _E|MissingType:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _put(self, key: str, val: _E) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _rmw(self, key: str, mod: ModFunc[_E,_P],
             /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    def _del(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|MissingType:
        key = self._key_str(key)
        with self.get_lock(key):
            data = self._get(key)
            if data is MISSING:
                return MISSING
            return self._get_sel(data, sel)
    def put_frid(self, key: VStoreKey, val: FridValue,
                 /, flags=VSPutFlag.UNCHECKED) -> bool:
        key = self._key_str(key)
        with self.get_lock(key):
            if flags == VSPutFlag.UNCHECKED:
                return self._put(key, self._encode(val))
            return self._rmw(key, self._add_new, flags, val)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key_str(key)
        with self.get_lock(key):
            if sel is None:
                return self._del(key)
            return self._rmw(
                key, self._del_sel, VSPutFlag.NO_CREATE | VSPutFlag.KEEP_BOTH, sel
            )

class SimpleAsyncStore(_SimpleBaseStore[_E], AsyncStore):
    """This is the base class of simple async store that loads and saves records in full."""
    @abstractmethod
    async def _get(self, key: str) -> _E|MissingType:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _put(self, key: str, val: _E) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _rmw(self, key: str, mod: ModFunc[_E,_P],
                   /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        raise NotImplementedError  # pragma: no cover
    @abstractmethod
    async def _del(self, key: str) -> bool:
        raise NotImplementedError  # pragma: no cover

    async def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                       /, dtype: FridTypeName='') -> FridValue|MissingType:
        key = self._key_str(key)
        async with self.get_lock(key):
            val = await self._get(key)
            if val is MISSING:
                return MISSING
            return self._get_sel(val, sel)
    async def put_frid(self, key: VStoreKey, val: FridValue,
                        /, flags=VSPutFlag.UNCHECKED) -> bool:
        key = self._key_str(key)
        async with self.get_lock(key):
            if flags == VSPutFlag.UNCHECKED:
                return await self._put(key, self._encode(val))
            return await self._rmw(key, self._add_new, flags, val)
    async def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        key = self._key_str(key)
        async with self.get_lock(key):
            if sel is None:
                return await self._del(key)
            return await self._rmw(
                key, self._del_sel, VSPutFlag.NO_CREATE | VSPutFlag.KEEP_BOTH, sel,
            )

class MemoryValueStore(SimpleValueStore[FridValue]):
    """Simplest memory based value store with thread locking."""
    URL_SCHEME = 'memory'
    DATA_SPACE = {}    # This is the global data space for all memory stores
    @dataclass
    class StoreMeta:
        store: dict[str,FridValue] = field(default_factory=dict)
        tlock: threading.RLock = field(default_factory=threading.RLock)
        alock: asyncio.Lock = field(default_factory=AsyncReentrantLock)

    DataSpaceType = dict[tuple[str,...],StoreMeta]
    def __init__(self, dataspace: DataSpaceType|None=None, namespace: tuple[str,...]=(), /):
        super().__init__()
        self._storage = dataspace if dataspace is not None else self.DATA_SPACE
        self._meta = self._storage.setdefault(namespace, self.StoreMeta())
        self._data = self._meta.store
    def all_data(self) -> Mapping[str,FridValue]:
        return self._data

    @classmethod
    def from_url(cls, url: str, **kwargs) -> 'MemoryValueStore':
        # Allow passing an URL through but the content is not checked
        # This function allows a path; for example memory:///abc/def means namespace=(abc, def)
        result = urlparse(url)
        if result.scheme != cls.URL_SCHEME:
            raise ValueError("Unsupported URL scheme for memory value store: {result.scheme}")
        if (path := result.path.strip('/')):
            namespace = path.split('/')
            if (value := kwargs.get('namespace')):
                namespace.extend(value)
            kwargs['namespace'] = tuple(namespace)
        if result.netloc or result.query or result.fragment:
            raise ValueError("The URL should just be {cls.URL_SCHEME}://[/name/space]")
        return cls(**kwargs)
    def substore(self, name: str, *args: str) -> 'MemoryValueStore':
        return __class__(self._storage, (name, *args))

    def _encode(self, val: FridValue) -> FridValue:
        return val
    def _decode(self, val: FridValue) -> FridValue:
        return val

    def get_lock(self, name: str|None=None):
        return self._meta.tlock
    def get_keys(self, pat: KeySearch=None, /) -> Iterable[VStoreKey]:
        for k in self._data.keys():
            if match_key(k, pat):
                yield k
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        return {k: frid_type_size(v) for k in list_concat(args, keys)
                if (v := self._get(self._key_str(k))) is not MISSING}

    def _get(self, key: str, /) -> FridValue|MissingType:
        return self._data.get(key, MISSING)
    def _put(self, key: str, data: FridValue) -> bool:
        self._data[key] = data
        return True
    def _rmw(self, key: str, mod: ModFunc[_E,_P],
             /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        old_data = self._data.get(key, MISSING)
        if flags & VSPutFlag.NO_CREATE:
            if old_data is MISSING:
                return False
        elif flags & VSPutFlag.NO_CHANGE:
            if old_data is not MISSING:
                return False
        if not (flags & VSPutFlag.KEEP_BOTH):
            old_data = MISSING
        data = mod(old_data, *args, **kwargs)
        assert not isinstance(data, tuple)
        if not isinstance(data, FridBeing):
            self._data[key] = data
            return True
        if data is MISSING and old_data is not MISSING:
            del self._data[key]
            return True
        return False
    def _del(self, key: str) -> bool:
        return self._data.pop(key, MISSING) is not MISSING

class BinaryStoreMixin:
    """This mixin help encodes data of various types into binary stream.

    The constructor accept a number of optional arguments to allow users
    to configure what prefixes should be used for different kind of types:
    - `frid_prefix`: the prefix use for generic frid representation as
      the default.
    - `text_prefix`: if this prefix is set, text string is encoded in
      UTF-8 with this prefix (i.e., without quotes).
    - `blob_prefix`: if this prefix is set, binary string is encoded as is
      after the prefix.
    - `list_prefix`: if this prefix is set, by default lists are encoded as
      lines (i.e., they are separated by `\n`), where each element is a single
      line frid representation that does not contain and control characters.
    - `dict_prefix`: if this prefix is set, by defeault dicts are encoded as
      lines of key value pair, where the pair is separated by `\t`, and key
      has all below 0x20 control characters escaped, and values is a single
      line frid representation that does not contain and control characters.
    Note that if set, only one of these prefix can be empty string.

    One can also override individual _encode_xxxx() and _decode_xxxx() methods
    to change how data is encoded for these five cases.
    Also _insert_prefix() and _remove_prefix() can be overridden to handle
    prefix matching add/or to extra information after the prefix.
    """
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
        self._decoders = dict(decoders)
        if len(self._decoders) < len(decoders):
            prefix_str = ", ".join(f"'{k}'" for k, _ in decoders)
            raise ValueError(f"Duplicated prefixes {prefix_str}")

    def _remove_header(self, val: bytes, prefix: bytes) -> bytes|None:
        """Removes the `prefix` from the beginning `val` if any.
        - Returns the value striped of the prefix.
        - If the prefix does not match, return None.
        - This method can be overridden to use a different method to
          match and strip the prefix.
        """
        if val.startswith(prefix):
            return val[len(prefix):]
        return None

    def _insert_header(self, val: bytes, prefix: bytes, data: FridValue) -> bytes|None:
        """Inserts the `prefix` to the beginning `val`, for the original `data`.
        - Returns the combined bytes.
        - If can return None if encoding is invalid; in the default
          implementation, it happens only if prefix is empty string
          but the `val` is conflict with other prefixes.
        - This method can be overridden to use a different method to
          insert the prefix and/or extra information.
        """
        if prefix:
            return prefix + val
        # If prefix is empty string, we have to check if it collides with other prefix
        for prefix in self._decoders.keys():
            if prefix and val.startswith(prefix):
                return None
        return val

    def _encode(self, data: FridValue, without_header=False, /) -> bytes:
        """Encodes the data into binary to be written to storage.
        - By default a header is generated unless `without_header` is set.
        """
        if isinstance(data, str):
            if self._text_prefix is not None:
                b = self._encode_text(data)
                if without_header:
                    return b
                if (out := self._insert_header(b, self._text_prefix, data)):
                    return out
        elif isinstance(data, BlobTypes):
            if self._blob_prefix is not None:
                b = self._encode_blob(data)
                if without_header:
                    return b
                if (out := self._insert_header(b, self._blob_prefix, data)):
                    return out
        elif is_frid_array(data):
            if self._list_prefix is not None:
                b = self._encode_list(data)
                if without_header:
                    return b
                if (out := self._insert_header(b, self._list_prefix, data)):
                    return out
        elif is_frid_skmap(data):
            if self._dict_prefix is not None:
                b = self._encode_dict(data)
                if without_header:
                    return b
                if (out := self._insert_header(b, self._dict_prefix, data)):
                    return out
        if self._frid_prefix is None:
            raise ValueError(f"Do not know how to encode type {type(data)}")
        b = self._encode_frid(data)
        if without_header:
            return b
        if (out := self._insert_header(b, self._frid_prefix, data)):
            return out
        raise ValueError(f"Failed to encode string for type {type(data)}")
    def _encode_frid(self, data: FridValue, /) -> bytes:
        """Encodes general frid-supported data.
        - This method is used no specific encoding is specified.
        """
        return dump_frid_str(data).encode()
    def _encode_blob(self, data: BlobTypes, /) -> bytes:
        """Encodes blob (binary data)."""
        return bytes(data)
    def _encode_text(self, data: str, /) -> bytes:
        """Encodes a text string."""
        return data.encode()
    def _encode_list(self, data: FridArray, /) -> bytes:
        """Encodes a list as lines."""
        out: list[bytes] = [dump_frid_str(item).encode() for item in data]
        out.append(b'')
        return b'\n'.join(out)
    def _encode_dict(self, data: StrKeyMap, /) -> bytes:
        """Encodes a dict as lines of key/value pairs.
        - The key/value pairs are separated by the tab.
        - Note that all unprintable ASCII less than 0x20 are escaped for keys,
          and should not appear in values.
        """
        out: list[bytes] = []
        for k, v in data.items():
            line = escape_control_chars(k, '\x7f')
            line += "\t" + (v.strfr() if isinstance(v, FridBeing) else dump_frid_str(v))
            out.append(line.encode())
        out.append(b'')
        return b'\n'.join(out)
    def _remove_dict(self, sel: VStoreSel, /) -> bytes:
        if isinstance(sel, str):
            return (escape_control_chars(sel, '\x7f') + '\t' + MISSING.strfr()).encode()
        assert isinstance(sel, Iterable)
        out: list[bytes] = []
        for k in sel:
            assert isinstance(k, str)
            out.append((escape_control_chars(k, '\x7f') + '\t' + MISSING.strfr()).encode())
        out.append(b'')
        return b'\n'.join(out)

    def _decode(self, val: bytes, /) -> FridValue:
        """Decodes the value from the encoded byte string."""
        if not isinstance(val, BlobTypes): # pragma: no cover -- should not happen
            raise ValueError(f"Incorrect encoded type {type(val)}; expect binary")
        if isinstance(val, memoryview|bytearray):  # pragma: no cover -- should not happen
            val = bytes(val)
        for prefix, decode in self._decoders.items():
            content = self._remove_header(val, prefix)
            # print("<>", prefix, decode, val, "=>", content)
            if content is not None:
                return decode(content)
        raise ValueError(f"Invalid byte encoding of {len(val)} bytes")
    def _decode_frid(self, val: bytes, /) -> FridValue:
        """Decode the value as the generic frid representation."""
        return load_frid_str(val.decode())
    def _decode_text(self, val: bytes, /) -> str:
        """Decode the value as the string representation."""
        return val.decode()
    def _decode_blob(self, val: bytes, /) -> bytes:
        """Decode the value as the binary representation."""
        return val
    def _decode_list(self, val: bytes, /) -> FridArray:
        """Decode the value as the representation for a list."""
        return [load_frid_str(line.decode()) for line in val.splitlines()]
    def _decode_dict(self, val: bytes, /) -> StrKeyMap:
        """Decode the value as the representation for a dict."""
        out = {}
        for line in val.splitlines():
            (key_str, tab_str, val_str) = line.decode().partition('\t')
            key = revive_control_chars(key_str, '\x7f')
            if tab_str:
                being = FridBeing.parse(val_str)
                if being is None:
                    out[key] = load_frid_str(val_str)
                elif being:
                    out[key] = PRESENT
                else:
                    out.pop(key, MISSING)  # MISSING is handled as "delete"
            else:
                out[key] = PRESENT
        return out

class StreamStoreMixin(BinaryStoreMixin, _SimpleBaseStore[bytes]):
    """This is a binary store where some data can be appended if possible.

    Appendable data types includes text, blob, list, and dict.
    """
    def __init__(self, *, header_head=b"#!", header_link=b"@[", header_tail=b"]\f", **kwargs):
        super().__init__(
            frid_prefix=header_head,
            text_prefix=(header_head + b'text' + header_link),
            blob_prefix=(header_head + b'blob' + header_link),
            list_prefix=(header_head + b'list' + header_link),
            dict_prefix=(header_head + b'dict' + header_link),
        )
        self._header_head = header_head
        self._header_link = header_link
        self._header_tail = header_tail
        # The total length of header should be 32 bytes by default
        self._header_size = len(header_head) + len(header_link) + len(header_tail) + 26

    def _create_header(self, typ: str|bytes) -> bytes:
        typ_bytes = typ.encode() if isinstance(typ, str) else typ
        assert len(typ_bytes) == 4, f"{typ_bytes=}"
        now_str = strfr_datetime(datetime.now(timezone.utc), precision=3)
        assert now_str is not None
        tim_bytes = now_str.encode()
        assert len(tim_bytes) == 22
        return self._header_head + typ_bytes + self._header_link + tim_bytes + self._header_tail

    def _insert_header(self, val: bytes, prefix: bytes, data: FridValue) -> bytes|None:
        """Insert the header with the given prefix plus extra data before `val`."""
        now_str = strfr_datetime(datetime.now(timezone.utc), precision=3)
        assert now_str is not None
        tim = now_str.encode()
        assert len(tim) == 22
        if prefix == self._header_head:
            (typ, _) = frid_type_size(data)
        else:
            n = len(self._header_head)
            assert len(prefix) == n + 4 + len(self._header_link)
            typ = prefix[n:(n + 4)]
        result = self._create_header(typ)
        assert len(result) == self._header_size
        return result + val

    def _remove_header(self, val: bytes, prefix: bytes) -> bytes|None:
        if not val.startswith(prefix):
            return None
        if not self._get_header_type(val):
            return None
        return val[self._header_size:]

    ModReturnType = tuple[bytes,bool|None]|FridBeing
    def _add_new(self, old: bytes|MissingType, new: FridValue) -> ModReturnType:
        """Combines the `new` value into the `old` values depending on the `flags`.
        - Returns either of the following:
            + PRESENT: use the existing value without change.
            + a tuple of output bytes and a boolean/none flag about how to use it:
                + If set to True, the output bytes are appended to the original content;
                + If set to False, the output bytes replace the original content;
                + If set to None, the input bytes is not complete; call the API again
                  with complete data.
        """
        if old is not MISSING:
            result = self._can_be_appended(self._get_header_type(old), new)
            if result:
                if not new:
                    return PRESENT
                return (self._encode(new, True), True)
            if len(old) <= self._header_size:
                return (b'', None)
        new_val = super()._add_new(old, new)
        if isinstance(new_val, FridBeing):
            return new_val
        return (new_val, False)
    def _del_sel(self, val: bytes|MissingType, sel: VStoreSel, /) -> ModReturnType:
        if val is not MISSING:
            if isinstance(val, Mapping) and self._get_header_type(val) == 'dict':
                return (self._remove_dict(sel), True)
            if len(val) <= self._header_size:
                return (b'', None)
        new_val = super()._del_sel(val, sel)
        if isinstance(new_val, FridBeing):
            return new_val
        return (new_val, False)

    def _get_header_type(self, old_val: bytes) -> str:
        """Extract the header type (four bytes) fro the header.
        This method check if all parts of the header is correct.
        """
        if len(old_val) < self._header_size:
            raise ValueError(f"Header has only {len(old_val)} bytes, need {self._header_size}")
        n = len(self._header_head)
        if not old_val.startswith(self._header_head):
            raise ValueError(f"Header does not start with '{self._header_head}'")
        if not old_val.startswith(self._header_link, n + 4):
            raise ValueError(f"header does not contain '{self._header_link}' after type")
        m = self._header_size - len(self._header_tail)
        if not old_val.startswith(self._header_tail, m):
            raise ValueError(f"Header does not end with '{self._header_tail}'")
        typ = old_val[n:(n + 4)].decode()
        n += 4 + len(self._header_link)
        if parse_datetime(old_val[n:m].decode()) is None:
            raise ValueError(f"Incorrect timestamp in header '{old_val[n:m]}'")
        return typ

    def _can_be_appended(self, old_type: str, data: FridValue) -> bool:
        """Returns true if the data can be appended to old data of the type `old_type`."""
        if isinstance(data, str):
            return old_type == 'text'
        if isinstance(data, BlobTypes):
            return old_type == 'blob'
        if is_frid_array(data):
            return old_type == 'list'
        if is_frid_skmap(data):
            return old_type == 'dict'
        return False
