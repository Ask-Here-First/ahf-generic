import os, sys, logging, signal, inspect, faulthandler
from collections.abc import Callable, Generator, Sequence
from typing import Any, Literal, cast

from frid.typing import get_qual_name


LOG_LINE_FMT = "%(asctime)s %(levelname).1s {%(process)d} %(message)s (%(filename)s:%(lineno)d)"
LOG_TIME_FMT = "%Y-%m-%dT%H%M%S"

StrLogLevel = Literal['critical','error','warning','info','debug','trace']
_log_levels: dict[str,int] = {
    'trace': 0, 'debug': logging.DEBUG, 'info': logging.INFO,
    'warn': logging.WARNING, 'warning': logging.WARNING,
    'error': logging.ERROR, 'critical': logging.CRITICAL
}

def set_root_logging(
        level: str|int|None=None, *, format=LOG_LINE_FMT, datefmt=LOG_TIME_FMT, **kwargs
) -> StrLogLevel:
    """Set the default logging level and a default uniform format.
    - The log level accepts a number and a lower case string as one of
      `trace`, `debug`, `info`, `warning`, `error`, `critical`
    - Returns the log level in lower case string.
    """
    if level is None:
        level = os.getenv('FRID_LOG_LEVEL', 'warn')
        if level.isnumeric():
            level = int(level)
    if isinstance(level, str):
        level = _log_levels.get(level)
        if level is None:
            print(f"Invalid FRID_LOG_LEVEL={level}", file=sys.stderr)
            level = logging.WARNING
    logging.basicConfig(level=level, format=format, datefmt=datefmt, **kwargs)
    return get_loglevel_str(level)

def get_loglevel_str(level: int|None=None) -> StrLogLevel:
    """Gets the given log level's string representation."""
    # There is no trace in python logging:
    if level is None:
        level = logging.getLogger().level
    if level < 10:
        return 'trace'
    # Round to a multiple of 10
    return cast(StrLogLevel, logging.getLevelName(level // 10 * 10).lower())

def use_signal_trap(
        signums: signal.Signals|Sequence[signal.Signals]=signal.SIGTERM,
        handler: Callable|None=None, *args, **kwargs
):
    """Use the signal trap for a number of signals in a Python program.
    - For those fault signals (SIGSEGV, SIGFPE, SIGABRT, SIGBUS), install
      a handler to Python tracebacks (handled by faulthandler.enanble())
    - For another signals in `signums`, install a handler that calls
      `handler` with `handler(*args, **kwargs)`.
    - By default, the handler calls `sys.exit`, with exit code 1 (or args[0]).
    - If the function is called with no-argument, sys.exit(1) is called
      with only SIGTERM.
    """
    if handler is None:
        handler = sys.exit
        args = ((args[0] if args else 1),)
        kwargs = {}
    def signal_handler(signum, frame):
        handler(*args, **kwargs)
    faulthandler.enable()
    if isinstance(signums, int):
        signal.signal(signal.SIGTERM, signal_handler)
    elif signums is not None:
        for sig in signums:
            signal.signal(sig, signal_handler)


def iter_stack_info(skip: int=1) -> Generator[tuple[str,int,str],Any,None]:
    """Returns an iterator of triplets of stack frames start """
    start_frame = inspect.currentframe()
    if start_frame is None:
        return
    try:
        frame = start_frame
        while frame is not None:
            if skip > 0:
                skip -= 1
                frame = frame.f_back
                continue
            filename = frame.f_code.co_filename
            line_num = frame.f_lineno
            function = frame.f_code.co_name
            mod = inspect.getmodule(frame)
            if mod is not None:
                mod_name = mod.__name__
            else:
                mod_name = ''
            try:
                if 'self' in frame.f_locals:
                    cls_name = get_qual_name(frame.f_locals['self'])
                elif 'cls' in frame.f_locals:
                    cls_name = get_qual_name(frame.f_locals['cls'])
                else:
                    cls_name = None
            except AttributeError:
                cls_name = None
            if function != "<module>":
                if cls_name is not None:
                    name = mod_name + ':' + cls_name + '.' + function
                else:
                    name = mod_name + ':' + function
            else:
                if cls_name is not None:
                    name = mod_name + ':' + cls_name
                else:
                    name = mod_name
            yield (filename, line_num, name)
            frame = frame.f_back
    finally:
        del start_frame
    return None

def get_caller_info(depth: int=1, *, squash_file: bool=False,
                    skip_module: str|None=None) -> tuple[str,int,str]|None:
    """Gets the caller's information: a triplet of file name, line number, and function name.
    - `depth`: number of additional call frames to go back.
        + With `depth=0`, it returns the information of caller itself.
        + By default, with `depth=1`, it returns the caller of the caller (which is desired).
    - `squash_file`: if set to true, caller frames from the same file will be
       squashed into one.
    - `skip_module`: Skip this module and its submodules at the beginning.
    """
    last = None
    for file, line, name in iter_stack_info(skip=2):  # First one should be the caller
        if skip_module is not None and (name == skip_module or (
            name.startswith(skip_module) and name[len(skip_module)] in '.:'
        )):
            continue
        if squash_file and file == last:
            continue
        last = file
        if depth == 0:
            return (file, line, name)
        depth -= 1
    return None

def warn(msg: str, *args, skip_module: str|None='frid', **kwargs):
    stack_info = get_caller_info(skip_module=skip_module)
    if stack_info is not None:
        (file, line, name) = stack_info
        if file.startswith('<') and file.endswith('>'):
            msg += f" ({name})"   # Python system file
        else:
            msg += f" ({os.path.basename(file)}:{line})"
    logging.warning(msg, *args, **kwargs)
