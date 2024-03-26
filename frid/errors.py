import os
from collections.abc import Sequence
import traceback
from types import TracebackType

from .typing import FridMixin
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
                 cause: BaseException|None=None, notes: Sequence[str]|None):
        super().__init__(*args)
        self.notes: list[str] = list(notes) if notes else []
        self.cause = cause
        if isinstance(trace, TracebackType):
            self.trace = None
            self.with_traceback(trace)
        elif is_text_list_like(trace):
            self.trace = list(trace)
            self.with_traceback(None)
        else:
            raise ValueError(f"Invalid trace type {type(trace)}")

    @classmethod
    def frid_from(cls, name: str, *args, error: str, trace: Sequence[str], **kwas):
        assert name in cls.frid_keys()
        return FridError(error, trace=trace, **kwas)

    def frid_repr(self) -> dict[str,str|int|list[str]]:
        out: dict[str,str|int|list[str]] = {'error': str(self)}
        trace = []
        if self.trace is not None:
            trace.extend(self.trace)
            trace.append("")
        trace.extend(traceback.format_exception(self))
        if self.__cause__:
            out['cause'] = str(self.__cause__)
        elif self.cause is not None:
            out['cause'] = str(self.cause)
            trace.append("Caused by:")
            trace.extend(traceback.format_exception(self.cause))
        if trace:
            out['trace'] = trace
        if self.notes:
            out['notes'] = self.notes
        if FRID_ERROR_VENUE is not None:
            out['venue'] = FRID_ERROR_VENUE
        return out
