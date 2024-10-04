from logging import info
import os
from collections.abc import Sequence
from pathlib import Path
from zipfile import ZipFile

from frid.typing import StrKeyMap
from frid.web import HttpError, HttpMixin

class FileRouter:
    _known_mime_type = {
        '.json': "application/json",
        '.html': "text/html",
        ".css": "text/css",
        ".js": "text/javascript",
        '.ico': "image/vnd.microsoft.icon",
        '.png': "image/png",
        '.jpg': "image/jpeg",
        '.jpeg': "image/jpeg",
        '.gif': "image/gif",
        '.yaml': "application/yaml",
    }
    _other_mime_type = "application/octet-stream"

    def __init__(self, *roots: str, index: str="index.html"):
        self._roots: list[Path|tuple[ZipFile,str]] = [
            self._open_path(v) for v in roots
        ] if roots else [Path(os.getcwd())]
        self._index = index
    def roots(self) -> list[str]:
        return [x.filename if isinstance(x, ZipFile) and x.filename is not None else str(x)
                for x in self._roots]
    @staticmethod
    def _open_path(s: str) -> Path|tuple[ZipFile,str]:
        path = Path(s).absolute()
        if path.is_dir():
            return path
        if path.is_file() and path.name.endswith(".zip"):
            return (ZipFile(path), "")
        index = s.find('.zip/')
        if index > 0:
            index += 4
            path = Path(s[:index])
            if path.is_file():
                return (ZipFile(path), s[(index+1):])
        raise ValueError(f"Invalid path {s}: not a directory or a zip file")

    def _get_mime_type(self, suffix):
        return self._known_mime_type.get(suffix, self._other_mime_type)

    def _load_file_in_dir(self, root: Path, path: Sequence[str]) -> tuple[bytes,str]|None:
        file_path = root.joinpath(*path)
        if not file_path.is_relative_to(root):
            info("The path '{file_path}' is not contained under {root}")
            return None
        if not os.path.exists(file_path):
            return None
        if os.path.isdir(file_path):
            file_path = file_path.joinpath(self._index)
            if not os.path.exists(file_path):
                # TODO: do we generate the directory list?
                return None
        if not os.path.isfile(file_path):
            raise HttpError(409, "Not a regular file")
        try:
            with open(file_path, 'rb') as fp:
                data = fp.read()
        except IOError as e:
            raise HttpError(403, str(e))
        return (data, self._get_mime_type(file_path.suffix))

    def _load_file_in_zip(self, zipf: ZipFile, path: Sequence[str],
                          prefix: str="") -> tuple[bytes,str]|None:
        zip_path = prefix + '/'.join(path)
        if zip_path not in zipf.namelist():
            zip_path += '/' + self._index
            if zip_path not in zipf.namelist():
                return None
        try:
            data = zipf.read(zip_path)
        except IOError as e:
            raise HttpError(403, e)
        idx = zip_path.rfind('.')
        if idx >= 0:
            return (data, self._get_mime_type(zip_path[idx:]))
        return (data, self._other_mime_type)

    def get_(self, *path, __: StrKeyMap={}):
        if any(p.startswith('.') for p in path):
            raise HttpError(400, f"Path items cannot start with .: '{'/'.join(path)}'")
        for root in self._roots:
            if isinstance(root, Path):
                result = self._load_file_in_dir(root, path)
            elif isinstance(root, tuple) and len(root) == 2:
                (zipfile, prefix) = root
                if isinstance(zipfile, ZipFile) and isinstance(prefix, str):
                    result = self._load_file_in_zip(zipfile, path, prefix)
                else:
                    raise ValueError(f"Invalid tuple {type(zipfile)} and {type(prefix)}")
            else:
                raise ValueError(f"Invalid root directory type {type(root)}")
            if result is None:
                continue
            (http_body, mime_type) = result
            return HttpMixin(ht_status=200, http_body=http_body, mime_type=mime_type)
        raise HttpError(404, f"File not found: '{'/'.join(path)}'")
