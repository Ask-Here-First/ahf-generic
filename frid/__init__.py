from .typing import FridError, FridValue, get_func_name, get_qual_name, get_type_name
from ._basic import FridCompare, FridReplace, MingleFlags, frid_mingle, frid_redact, frid_sizeof
from ._loads import load_frid_str, load_frid_tio, scan_frid_str, open_frid_tio
from ._loads import FridParseError, FridTruncError
from ._dumps import dump_frid_str, dump_frid_tio, dump_args_str, dump_args_tio
from ._utils import load_module_data

__version__ = "0.5.1"

# For Json-like compatibility but do not include them in public symbols
loads = load_frid_str
dumps = dump_frid_str
load = load_frid_tio
dump = dump_frid_tio

__all__ = [
    # From typing
    'FridError', 'FridValue', 'get_func_name', 'get_type_name', 'get_qual_name',
    # From _basic
    'FridCompare', 'FridReplace', "MingleFlags", 'frid_mingle', 'frid_redact', 'frid_sizeof',
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
