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

def find_in_ancestor(name: str, /, start: os.PathLike[str]|str|None=None,
                     depth: int=-1) -> str|None:
    """Find a file with given `name` in one of its ancestor diretories bottom up.
    - `start`: the initial bottommost directory to search (current directory by default)
    - `depth`: the maximum number of directory to go up (0: only the start, -1: infinite).
    """
    if start is None:
        start = os.getcwd()
    curr = os.path.abspath(start)
    while True:
        if os.path.exists(path := os.path.join(curr, name)):
            return path
        if not depth:
            break
        depth -= 1
        parent = os.path.dirname(curr)
        if parent == curr:
            break
        curr = parent
    return None
