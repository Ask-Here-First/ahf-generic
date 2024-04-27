from collections.abc import Iterable, Mapping
import os
from abc import ABC, abstractmethod
from enum import Flag
from logging import error
import time
from typing import BinaryIO, ParamSpec, TypeVar


from ..typing import MISSING, PRESENT, BlobTypes, FridBeing, FridTypeSize, MissingType
from ..helper import frid_type_size
from .utils import VSPutFlag, VStoreKey, list_concat
from .store import ValueStore
from .basic import ModFunc, StreamStoreMixin

_T = TypeVar('_T')
_P = ParamSpec('_P')

class OpenMode(Flag):
    OVERWRITE = 0
    READ_ONLY = 0x80   # If set, all other flags are ignored
    NO_CREATE = 0x40
    NO_CHANGE = 0x20

class AbstractStreamAgent(ABC):
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    @abstractmethod
    def get(self, index: int=0, until: int=0, /) -> bytes|MissingType:
        """Gets the binary content, starting from `index`, to the byte up to `until`.
        - If `until` is zero, read to the end.
        - Neither `index` nor `until` can be negative.
        - Returns None if file is missing to get or empty if file exists but no data.
        """
        raise NotImplementedError
    @abstractmethod
    def put(self, blob: BlobTypes|FridBeing|None, /) -> bool:
        """Puts the text into the store.
        It accept following values for `blob`:
        - A binary string: write the binary string to the current writing position.
        - None: reset the writing position to the beginning (i.e., truncate).
        - PRESENT: use the present value (i.e., rollback)
        - MISSING: delete the present value without any new value.
        """
        raise NotImplementedError

class StreamValueStore(StreamStoreMixin, ValueStore):
    """This is a data store that opens file streams."""
    @abstractmethod
    def _open(self, key: str, mode: OpenMode) -> AbstractStreamAgent:
        """Open a file stream that one can get ant put binary data."""
        raise NotImplementedError

    def _put_flags_to_open_mode(self, flags: VSPutFlag) -> OpenMode:
        """Convert the put flags to open mode for puts."""
        mode = OpenMode.OVERWRITE
        if flags & VSPutFlag.NO_CREATE:
            mode |= OpenMode.NO_CREATE
        if flags & VSPutFlag.NO_CHANGE:
            mode |= OpenMode.NO_CHANGE
        return mode

    def _get(self, key: str) -> BlobTypes|MissingType:
        try:
            with self._open(key, OpenMode.READ_ONLY) as h:
                return h.get()
        except FileNotFoundError:
            return MISSING
    def _put(self, key: str, blob: BlobTypes) -> bool:
        try:
            with self._open(key, OpenMode.OVERWRITE) as h:
                return h.put(blob)
        except Exception:
            error(f"Failed to put to {key}")
            return False
    def _rmw(self, key: str, mod: ModFunc[bytes,_P],
             /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        try:
            with self._open(key, self._put_flags_to_open_mode(flags)) as h:
                b = h.get(0, self._header_size)
                if not flags & VSPutFlag.KEEP_BOTH:
                    b = MISSING
                result = mod(b, *args, **kwargs)
                if isinstance(result, tuple):
                    (_, op) = result
                    if op is None and b is not MISSING:
                        x = h.get(self._header_size)
                        assert x is not MISSING
                        result = mod(b + x, *args, **kwargs)
                if result is PRESENT:
                    return h.put(PRESENT)
                if result is MISSING:
                    return h.put(MISSING)
                assert isinstance(result, tuple)
                (new_val, op) = result
                if op is None:
                    return h.put(PRESENT)
                if op and not h.put(None):
                    return h.put(PRESENT)
                return h.put(new_val)
        except Exception:
            error(f"Failed to write to {key}", exc_info=True)
            return False

class FileIOAgent(AbstractStreamAgent):
    def __init__(self, file: BinaryIO, kvs_path: str, tmp_path: str|None=None,
                 has_data: bool=False):
        self.file = file
        self.kvs_path = kvs_path
        self.tmp_path = tmp_path
        self.has_data = has_data
        self.io_state: FridBeing|None = None   # PRESENT: restore original; MISSING: remove
    def __enter__(self):
        self.file.__enter__()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.__exit__(exc_type, exc_val, exc_tb)
        if self.tmp_path is None:
            return  # This is the read only case
        if self.io_state is None or self.io_state:
            # Replace the file back, assuming for state = PRESENT the file is not chanaged
            os.replace(self.tmp_path, self.kvs_path)
        else:
            os.unlink(self.tmp_path)  # Remove the temp file

    def get(self, index: int=0, until: int=0) -> bytes|None:
        if not self.has_data:
            return None
        fsize = os.fstat(self.file.fileno()).st_size
        if index < 0:
            index = fsize + index
        if until <= 0:
            until = fsize + index
        if index >= until:
            return b''
        if index > 0:
            self.file.seek(index, 0)
        if until < fsize:
            return self.file.read(until - index)
        return self.file.read()

    def put(self, data: BlobTypes|FridBeing|None=None) -> bool:
        if data is None:
            # TODO: save the current file
            self.file.truncate(0)
            return True
        if isinstance(data, FridBeing):
            self.io_state = data
            return not data
        self.file.seek(0, os.SEEK_END)
        if self.file.write(data) == len(data):
            return True
        self.io_state = PRESENT
        return False

class FileIOValueStore(StreamValueStore):
    def __init__(self, root: os.PathLike|str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._root = os.path.abspath(root)
        if not os.path.isdir(self._root):
            os.makedirs(self._root, exist_ok=True)
    def substore(self, name: str, *args: str):
        root = os.path.join(self._root, self._encode_name(name),
                            *(self._encode_name(x) for x in args))
        return self.__class__(root=root)

    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        out = {}
        for k in list_concat(args, keys):
            v = self.get_frid(k)
            if v is not MISSING:
                out[k] = frid_type_size(v)
        return out

    def _encode_name(self, key: str):
        return key  # TODO: need to escape most of non-alnum characters

    def _key_str(self, key: VStoreKey) -> str:
        if isinstance(key, str):
            return self._encode_name(key)
        return os.path.join(*(self._encode_name(str(k)) for k in key))

    def _get_path_pairs(self, key: str) -> tuple[str,str]:
        path = os.path.join(self._root, key)
        dir = os.path.dirname(path)
        if not os.path.isdir(dir):
            os.makedirs(dir)
        return (path + ".kvs", path + ".tmp")

    def _get_read_agent(self, kvs_path, tmp_path):
        count = 60
        while True:
            try:
                return FileIOAgent(open(kvs_path, mode='rb'), kvs_path)
            except FileNotFoundError:
                if os.path.exists(tmp_path):
                    count -= 1
                    if count < 0:
                        raise
                    time.sleep(0.1)
                else:
                    raise
    def _move_or_create(self, old_path, new_path) -> BinaryIO|None:
        """Trying to move the `old_path` to `new_path` atomically.
        - If the `new_path` exists, it will back-off and retry for a period of time.
        - If the `old_path` does not exists, the `new_path` is created
          and kept open. In this case the file object is returned.
        - If the `old_path` exists, rename it to the `new_path` atomically.
          In this case, returns None.
        """
        count = 300
        while True:
            match os.name:
                case 'posix':
                    try:
                        f = open(new_path, "xb")
                    except FileExistsError:
                        if count <= 0:
                            raise
                        # Fall through to back off
                    else:
                        try:
                            os.rename(old_path, new_path)
                        except FileNotFoundError:
                            # old_path does not exist, return created file at new_path
                            return f
                        else:
                            f.close()
                            return None
                case 'nt':
                    try:
                        os.rename(old_path, new_path)
                    except FileNotFoundError:
                        try:
                            f = open(new_path, "xb")
                        except FileExistsError:
                            if count <= 0:
                                raise
                            # Fall through to back off
                        else:
                            if not os.path.exists(old_path):
                                return f
                            f.close()
                            os.unlink(new_path)
                            continue # Try again without waiting
                case _:
                    raise SystemError(f"Unsupported operating system {os.name}")
            count -= 1
            time.sleep(0.1)

    def _open(self, key: str, mode: OpenMode) -> FileIOAgent:
        (kvs_path, tmp_path) = self._get_path_pairs(key)
        if mode & OpenMode.READ_ONLY:
            return self._get_read_agent(kvs_path, tmp_path)
        # If the renaming is successful, the write lock is held
        file = self._move_or_create(kvs_path, tmp_path)
        if mode:
            if file is None:
                # The value exists
                if mode & OpenMode.NO_CHANGE:
                    # Replace the file back
                    os.replace(tmp_path, kvs_path)
                    raise FileExistsError(kvs_path)
            else:
                # The value does not exist
                if mode & OpenMode.NO_CREATE:
                    file.close()
                    raise FileNotFoundError(kvs_path)
        if file is not None:
            return FileIOAgent(file, kvs_path, tmp_path, False)
        return FileIOAgent(open(tmp_path, mode='r+b'), kvs_path, tmp_path, True)

    def _del(self, key: str) -> bool:
        (kvs_path, tmp_path) = self._get_path_pairs(key)
        file = self._move_or_create(kvs_path, tmp_path)
        if file is not None:
            # The value does not exist, just close and remove the newly created file
            file.close()
            os.unlink(tmp_path)
            return False
        # The value exists and was renamed. We can delete now.
        # TODO: backup the content to the history here
        os.unlink(tmp_path)
        return True
