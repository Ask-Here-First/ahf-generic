from collections.abc import Mapping, Sequence
import traceback
from types import TracebackType

from .typing import FridMixin
from .guards import is_text_list_like


class FridError(FridMixin, Exception):
    """The base class of errors that is compatible with Frid.
    The error can be constructed in three ways:
    - Construct with a single error message string.
    - Construct with a error message and a stack trace, which will replace
      the current stack trace.
    - Construct with `raise FridError("error") from exc` in which case
      the exc with be chained.
    """
    def __init__(self, *args, trace: TracebackType|Sequence[str]|None=None):
        super().__init__(*args)
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
    def frid_from(cls, name, *args, error: str, trace: Sequence[str], **kwas):
        assert name in cls.frid_keys()
        return FridError(kwas)

    def frid_repr(self) -> dict[str,str|int|list[str]]:
        out: dict[str,str|int|list[str]] = {'error': str(self)}
        if self.trace is not None:
            out['trace'] = self.trace
        out['trace'] = traceback.format_exception(self)
        if self.__cause__:
            out['cause'] = str(self.__cause__)
        # TODO: notes? genre? maker?
        return out

class HttpError(FridError):
    """An HttpError with an status code.
    - The constructor requires the http status code as the first argment
      before the error message.
    - Optionally an HTTP text can be given by `http_text` for construction.
    - Users can also specify `headers` as a dict.
    """
    def __init__(self, http_code: int=500, /, *args, http_text: str|None=None,
                 headers: Mapping[str,str]|None=None, **kwargs):
        self.http_code = http_code
        self.http_text = http_text
        self.headers = headers

    def frid_repr(self) -> dict[str,str|int|list[str]]:
        out = super().frid_repr()
        out['http_code'] = self.http_code
        if self.http_text is not None:
            out['http_text'] = self.http_text
        return out

