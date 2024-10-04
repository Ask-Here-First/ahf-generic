import os, traceback
from collections.abc import Sequence
from types import TracebackType

from .typing import FridMixin, FridNameArgs
from .helper import get_qual_name, get_type_name
from .guards import is_text_list_like

FRID_ERROR_VENUE = os.getenv('FRID_ERROR_VENUE')

class FridError(FridMixin, Exception):
    """The base class of errors that is compatible with Frid.
    The error can be constructed in three ways:
    - Construct with a single error message string.
    - Construct with a error message and a stack trace, which will replace
      the current stack trace.
    - Construct with `raise FridError("error") from exc` in which case
      the exc with be chained.
    """
    def __init__(self, *args, trace: TracebackType|Sequence[str]|None=None,
                 cause: BaseException|str|None=None, notes: Sequence[str]|None=None,
                 venue: str|None=None):
        if args and isinstance(args[0], BaseException):
            exc = args[0]
            super().__init__(*exc.args, *args[1:])
            if trace is None:
                trace = exc.__traceback__
            if cause is None:
                cause = get_qual_name(exc)
        else:
            super().__init__(*args)
        self.notes: list[str] = list(notes) if notes else []
        self.cause: BaseException|str|None = cause
        self.venue: str|None = venue
        if trace is None:
            self.trace = None
        elif isinstance(trace, TracebackType):
            self.trace = None
            self.with_traceback(trace)
        elif is_text_list_like(trace):
            self.trace = list(trace)
            self.with_traceback(None)
        else:
            raise ValueError(f"Invalid trace type {type(trace)}")

    @classmethod
    def frid_from(cls, data: FridNameArgs, /):
        # The `trace` and `cause` are not accepting TrackbackType and BaseException;
        # and `error` is passed as the first argument.
        assert data.name in cls.frid_keys()
        error = data.kwds.get('error')
        trace = data.kwds.get('trace')
        cause = data.kwds.get('cause')
        notes = data.kwds.get('notes')
        venue = data.kwds.get('venue')
        assert trace is None or is_text_list_like(trace)
        assert cause is None or isinstance(cause, str)
        assert notes is None or is_text_list_like(notes)
        assert venue is None or isinstance(venue, str)
        return FridError(error, trace=trace, cause=cause, notes=notes, venue=venue)

    def frid_dict(self) -> dict[str,str|int|list[str]]:
        """Convert the error into a dictionary"""
        out: dict[str,str|int|list[str]] = {'error': str(self)}
        trace = []
        if self.trace is not None:
            trace.extend(self.trace)
            trace.append("")
        if self.__traceback__ is not None:
            trace.extend(traceback.format_exception(self))
        if self.__cause__:
            out['cause'] = str(self.__cause__)
        elif self.cause is not None:
            out['cause'] = str(self.cause)
            if isinstance(self.cause, BaseException):
                trace.append("Caused by:")
                trace.extend(traceback.format_exception(self.cause))
        if trace:
            out['trace'] = trace
        if self.notes:
            out['notes'] = self.notes
        if FRID_ERROR_VENUE is not None:
            out['venue'] = FRID_ERROR_VENUE
        return out

    def frid_repr(self) -> FridNameArgs:
        return FridNameArgs(get_type_name(self), (), self.frid_dict())
