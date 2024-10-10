from .typing import FridError, get_func_name, get_qual_name, get_type_name
from ._basic import Comparator, Substitute, MergeFlags, frid_merge, frid_redact
from ._loads import load_frid_str, load_frid_tio, scan_frid_str, open_frid_tio
from ._loads import FridParseError, FridTruncError
from ._dumps import dump_frid_str, dump_frid_tio, dump_args_str, dump_args_tio

from .osutil import load_module_data  # TODO: to to moved here

# For Json-like compatibility but do not include them in public symbols
loads = load_frid_str
dumps = dump_frid_str
load = load_frid_tio
dump = dump_frid_tio

__all__ = [
    # From typing
    'FridError', 'get_func_name', 'get_type_name', 'get_qual_name',
    # From _basic
    'Comparator', 'Substitute', "MergeFlags", 'frid_merge', 'frid_redact',
    # From _loads
    'load_frid_str', 'load_frid_tio', 'scan_frid_str', 'open_frid_tio',
    # From _dumps
    'FridParseError', 'FridTruncError',
    'dump_frid_str', 'dump_frid_tio', 'dump_args_str', 'dump_args_tio',

    # Here
    'load_module_data',
]
