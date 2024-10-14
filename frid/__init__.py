import importlib

from .typing import FridError, get_func_name, get_qual_name, get_type_name
from ._basic import FridCompare, FridReplace, MingleFlags, frid_mingle, frid_redact
from ._loads import load_frid_str, load_frid_tio, scan_frid_str, open_frid_tio
from ._loads import FridParseError, FridTruncError
from ._dumps import dump_frid_str, dump_frid_tio, dump_args_str, dump_args_tio

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

# For Json-like compatibility but do not include them in public symbols
loads = load_frid_str
dumps = dump_frid_str
load = load_frid_tio
dump = dump_frid_tio

__all__ = [
    # From typing
    'FridError', 'get_func_name', 'get_type_name', 'get_qual_name',
    # From _basic
    'FridCompare', 'FridReplace', "MingleFlags", 'frid_mingle', 'frid_redact',
    # From _loads
    'load_frid_str', 'load_frid_tio', 'scan_frid_str', 'open_frid_tio',
    # From _dumps
    'FridParseError', 'FridTruncError',
    'dump_frid_str', 'dump_frid_tio', 'dump_args_str', 'dump_args_tio',

    # Here
    'load_module_data',
]

# Backward compatibility; to be removed in 0.5.0
MergeFlags = MingleFlags
frid_merge = frid_mingle
Comparator = FridCompare
Substitute = FridReplace
