import os, time
from collections.abc import Iterable, Mapping
from abc import ABC, abstractmethod
from enum import Flag
from logging import error
from typing import BinaryIO, ParamSpec, TypeVar
from urllib.parse import quote, unquote, urlparse

from ..typing import MISSING, PRESENT, BlobTypes, FridBeing, FridTypeSize, MissingType
from ..helper import frid_type_size
from ..osutil import url_path_to_os_path
from .utils import KeySearch, VSPutFlag, VStoreKey, list_concat, match_key
from .basic import ModFunc, SimpleValueStore, StreamStoreMixin

_T = TypeVar('_T')
_P = ParamSpec('_P')

class OpenMode(Flag):
    OVERWRITE = 0
    READ_ONLY = 0x80   # If set, all other flags are ignored
    NO_CREATE = 0x40
    NO_CHANGE = 0x20

    def __bool__(self):
        return bool(self.value)

class AbstractStreamAgent(ABC):
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    @abstractmethod
    def get(self, index: int=0, until: int=0, /) -> bytes|None:
        """Gets the binary content, starting from `index`, to the byte up to `until`.
        - If `until` is zero, read to the end of stream.
        - If `index` is negative, the offset is starting from the end (only if the stream
          is seekable).
        - Returns None if file is missing, or empty if file exists but no data in the range.
        """
        raise NotImplementedError
    @abstractmethod
    def put(self, blob: BlobTypes|FridBeing|None, /) -> bool:
        """Puts the text into the store.
        It accepts following values for `blob`:
        - A binary string: write the binary string to the current writing position.
        - None: erase all data and reset the writing position to the beginning
          (i.e., truncate to zero), or alternative delete the current stream and
          create a new stream over it.
        - PRESENT: use the present stream without (further) changes.
        - MISSING: delete the present stream without any new value.
        """
        raise NotImplementedError

class StreamValueStore(StreamStoreMixin, SimpleValueStore):
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
                result = h.get()
                return MISSING if result is None else result
        except FileNotFoundError:
            return MISSING
    def _put(self, key: str, blob: BlobTypes) -> bool:
        try:
            with self._open(key, OpenMode.OVERWRITE) as h:
                if not h.put(None):
                    return False
                return h.put(blob)
        except Exception:
            error(f"Failed to put the value for the key path '{key}'", exc_info=True)
            return False
    def _rmw(self, key: str, mod: ModFunc[bytes,_P],
             /, flags: VSPutFlag, *args: _P.args, **kwargs: _P.kwargs) -> bool:
        try:
            with self._open(key, self._put_flags_to_open_mode(flags)) as h:
                if flags & VSPutFlag.KEEP_BOTH:
                    header = h.get(0, self._header_size)
                    b = MISSING if header is None else header
                else:
                    b = MISSING
                result = mod(b, *args, **kwargs)
                if isinstance(result, tuple):
                    (_, op) = result
                    if op is None and b is not MISSING:
                        x = h.get(self._header_size)
                        assert x is not None
                        result = mod(b + x, *args, **kwargs)
                if result is PRESENT:
                    return h.put(PRESENT)
                if result is MISSING:
                    return h.put(MISSING)
                assert isinstance(result, tuple)
                (new_val, op) = result
                if op is None:
                    return h.put(PRESENT)
                if op:
                    return h.put(new_val)  # Append
                if not h.put(None):
                    return h.put(PRESENT)
                return h.put(new_val)
        except (FileExistsError, FileNotFoundError):
            return False
        except Exception:
            error(f"Failed to write to {key}", exc_info=True)
            return False

class FileIOAgent(AbstractStreamAgent):
    """The implementation of Stream Agent that uses local files."""
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
        assert not os.path.exists(self.tmp_path)

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
        self.file.seek(max(index, 0), os.SEEK_SET)
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


class FileDeleter:
    """A simple context provider that delete the a file on exit."""
    def __init__(self, path: str):
        self._path = path
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            os.unlink(self._path)
        except OSError:
            error(f"Failed to delete {self._path}", exc_info=True)

class FileIOValueStore(StreamValueStore):
    """File based value store."""
    URL_SCHEME = "file"
    SUBSTORE_EXT = ".!"
    LCK_FILE_EXT = ".lck"
    KVS_FILE_EXT = ".kvs"
    TMP_FILE_EXT = ".tmp"
    def __init__(self, root: os.PathLike|str, /, **kwargs):
        super().__init__(**kwargs)
        if isinstance(root, str) and root.startswith("file://"):
            root = url_path_to_os_path(root[7:])
        self._root = os.path.abspath(root)
        if not os.path.isdir(self._root):
            os.makedirs(self._root, exist_ok=True)
    @classmethod
    def from_url(cls, url: str, **kwargs) -> 'FileIOValueStore':
        # Allow passing an URL through but the content is not checked
        result = urlparse(url)
        if result.scheme != cls.URL_SCHEME:
            raise ValueError("Unsupported URL scheme for memory value store: {result.scheme}")
        if result.netloc or result.query or result.fragment:
            raise ValueError(f"The URL should just be {cls.URL_SCHEME}:///[PATH]")
        return cls(url_path_to_os_path(result.path), **kwargs)
    def substore(self, name: str, *args: str):
        root = os.path.join(self._root, self._encode_name(name) + self.SUBSTORE_EXT,
                            *(self._encode_name(x) + self.SUBSTORE_EXT for x in args))
        return self.__class__(root)

    def get_keys(self, pat: KeySearch=None, /) -> Iterable[VStoreKey]: # type: ignore
        for (path, dirs, files) in os.walk(self._root):
            relpath = os.path.relpath(path, self._root)
            if '!' in relpath:
                continue
            if relpath == '.':
                prefix = ()
            else:
                prefix = tuple(self._decode_name(n) for n in os.path.split(relpath))
            existing = set()
            for name in files:
                if name.endswith(self.KVS_FILE_EXT):
                    name = name[:-len(self.KVS_FILE_EXT)]
                elif name.endswith(self.TMP_FILE_EXT):
                    name = name[:-len(self.TMP_FILE_EXT)]
                else:
                    continue
                if name in existing:
                    continue
                existing.add(name)
                key = (*prefix, self._decode_name(name))
                if match_key(key, pat):
                    yield key[0] if len(key) == 1 else key
            # TODO: we can do prefix match for subdirectories to speed up
            # Remove substores from search
            i = len(dirs) - 1
            while i >= 0:
                if dirs[i].endswith(self.SUBSTORE_EXT):
                    del dirs[i]
                i -= 1
    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        out = {}
        for k in list_concat(args, keys):
            v = self.get_frid(k)
            if v is not MISSING:
                out[k] = frid_type_size(v)
        return out
    def get_lock(self, name: str|None=None):
        path = os.path.join(self._root, (name or '') + self.LCK_FILE_EXT)
        self._makedir_parent(path)
        while True:
            try:
                with open(path, "x+b") as f:
                    f.write(self._create_header('lock'))
                return FileDeleter(path)
            except FileExistsError:
                time.sleep(0.1)

    def _encode_name(self, key: str) -> str:
        """Encode string into file system compatible name string."""
        return quote(key, safe='+@')
    def _decode_name(self, file_name: str) -> str:
        """Decode string from file system compatible name string."""
        return unquote(file_name)

    def _key_str(self, key: VStoreKey) -> str:
        if isinstance(key, str):
            return self._encode_name(key)
        return os.path.join(*(self._encode_name(str(k)) for k in key))

    def _get_path_pairs(self, key: str) -> tuple[str,str]:
        """Returns a pair of paths for the given key.
        - The file used to store the value for this key.
        - The temporary file for updating.
        """
        path = os.path.join(self._root, key)
        dir = os.path.dirname(path)
        if not os.path.isdir(dir):
            os.makedirs(dir)
        return (path + self.KVS_FILE_EXT, path + self.TMP_FILE_EXT)

    def _get_read_agent(self, kvs_path: str, tmp_path: str) -> FileIOAgent:
        count = 300
        while True:
            try:
                return FileIOAgent(open(kvs_path, mode='rb'), kvs_path, has_data=True)
            except FileNotFoundError:
                if os.path.exists(tmp_path):
                    count -= 1
                    if count < 0:
                        raise
                    time.sleep(0.1)
                else:
                    raise
    def _makedir_parent(self, path: str):
        """Create the parent directory of the path."""
        parent = os.path.dirname(path)
        os.makedirs(parent, exist_ok=True)
    def _move_or_create(self, old_path: str, new_path: str) -> BinaryIO|None:
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
                    # For posix, since rename always result in replacing silently
                    # We create the destination file in exclusive mode then rename.
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
                    # For Windows, since rename will fail when destination exists,
                    # We do that first. If the old path is missing we create the
                    # new path, but then check the existence of the old path again
                    # to make sure that it is still missing.
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
                            assert not os.path.exists(new_path)
                            continue # Try again without waiting
                    else:
                        return None
                case _:
                    raise SystemError(f"Unsupported operating system {os.name}")
            count -= 1
            time.sleep(0.1)

    def _open(self, key: str, mode: OpenMode) -> FileIOAgent:
        (kvs_path, tmp_path) = self._get_path_pairs(key)
        self._makedir_parent(kvs_path)  # tmp_path is in the same directory
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
                    assert not os.path.exists(tmp_path)
                    raise FileExistsError(kvs_path)
            else:
                # The value does not exist
                if mode & OpenMode.NO_CREATE:
                    file.close()
                    os.unlink(tmp_path)
                    assert not os.path.exists(tmp_path)
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
            assert not os.path.exists(tmp_path)
            return False
        # The value exists and was renamed. We can delete now.
        # TODO: backup the content to the history here
        os.unlink(tmp_path)
        assert not os.path.exists(tmp_path)
        return True
