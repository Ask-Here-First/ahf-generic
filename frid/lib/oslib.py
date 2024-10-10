
import os, sys, logging, signal, faulthandler
from collections.abc import Callable, Sequence
from typing import Literal, cast


LOG_LINE_FMT = "%(asctime)s %(levelname)1s {%(process)d} %(message)s (%(filename)s:%(lineno)d)"
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
