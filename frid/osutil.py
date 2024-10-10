from frid._loads import load_frid_str
import importlib

# TODO: DELETE this file in 0.5.0.

# This is for backward compatibility to be removed at 0.5.0
from .lib import path_to_url_path, url_path_to_path  # noqa: F401

# TODO: move this single function to be defined directly in __init__.py
def load_module_data(name: str, package: str|None=None):
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

# For backward compatibility
load_data_in_module = load_module_data
os_path_to_url_path = path_to_url_path
url_path_to_os_path = url_path_to_path
