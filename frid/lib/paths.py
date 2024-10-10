import os, sys
from urllib.parse import quote, unquote


def path_to_url_path(path: os.PathLike|str) -> str:
    """Convert OS relative path to URL path (URL encoded, using / as separator)."""
    if not isinstance(path, str):
        path = str(path)
    if sys.platform.startswith('win'):
        is_abs = os.path.isabs(path)
        path = path.replace('\\', '/')
        if is_abs and not path.startswith('/'):
            path = '/' + path
    return quote(path)

def url_path_to_path(path: str) -> str:
    """Convert URL path to OS relative path (URL decoded, using native separator)."""
    path = unquote(path)
    if sys.platform.startswith('win'):
        if len(path) >= 3 and path[0] == '/' and path[2] == ':':
            path = path[1:]
        path = path.replace('/', '\\')
    return path

