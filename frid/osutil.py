import os, sys, importlib
from urllib.parse import quote, unquote

from .loader import load_frid_str

def os_path_to_url_path(path: os.PathLike|str) -> str:
    """Convert OS path to URL path (URL encoded, using / as separator)."""
    if not isinstance(path, str):
        path = str(path)
    if sys.platform.startswith('win'):
        is_abs = os.path.isabs(path)
        path = path.replace('\\', '/')
        if is_abs and not path.startswith('/'):
            path = '/' + path
    return quote(path)

def url_path_to_os_path(path: str) -> str:
    """Convert URL path to OS path (URL decoded, using native separator)."""
    path = unquote(path)
    if sys.platform.startswith('win'):
        if len(path) >= 3 and path[0] == '/' and path[2] == ':':
            path = path[1:]
        path = path.replace('/', '\\')
    return path

def load_data_in_module(name: str, package: str|None=None):
    """Loads the object as defined by `name`.
    - `name`: a string references the object, in the format of either
      `a.b.c:obj` where `a.b.c` is the module path (relative to `package`
      if given), and `obj` is the name of the object in the module
    - `package`: the base package name.
    """
    if ':' in name:
        (p, name) = name.split(':', 1)
        package = p if package is None else package + '.' + p
    elif package is None:
        raise ImportError(f"The name {name} must contain a ':' if package is not set")
    name = name.strip()
    module = importlib.import_module(package)
    index = name.find('(')
    if index >= 0 and name.endswith(')'):
        init_path = name[:index].rstrip()
        call_args = load_frid_str(name[index+1:-1], init_path=init_path, top_dtype='args')
        name = call_args.data
    else:
        call_args = None
    if not hasattr(module, name):
        raise ImportError(f"The member {name} is missing from module {package}")
    obj = getattr(module, name)
    if call_args is not None:
        obj = obj(*call_args.args, **call_args.kwds)
    return obj
