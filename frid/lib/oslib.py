import os, sys, logging, signal, inspect, faulthandler
from collections.abc import Callable, Sequence
from typing import Literal, cast


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

def get_caller_info(depth: int=1, *, squash: bool=False) -> tuple[str,int,str]|None:
    """Gets the caller's information: a triplet of file name, line number, and function name.
    - `depth`: number of additional call frames to go back.
        + With `depth=0`, it returns the information of caller itself.
        + By default, with `depth=1`, it returns the caller of the caller (which is desired).
    - `squash`: if set to true, caller frames from the same file will be
       squashed into one.
    """
    current_frame = inspect.currentframe()
    if current_frame is None:
        return None
    try:
        last_filename = current_frame.f_code.co_filename
        frame = current_frame.f_back  # Start with the caller
        depth += 1            # One more level because the start is this method
        while frame is not None:
            filename = frame.f_code.co_filename
            line_num = frame.f_lineno
            function = frame.f_code.co_name
            if not squash or filename != last_filename:
                depth -= 1
                if depth <= 0:
                    return (filename, line_num, function)
            frame = frame.f_back
            last_filename = filename
    finally:
        del current_frame
    return None
