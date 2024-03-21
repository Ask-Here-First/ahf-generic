from collections.abc import Mapping
import traceback
from types import TracebackType

from .dtypes import FridMixin, FridValue


class FridError(Exception, FridMixin):
    """The base class of errors that is compatible with Frid.
    The error can be constructed in three ways:
    - Construct with a single error message string.
    - Construct with a error message and a stack trace, which will replace
      the current stack trace.
    - Construct with `raise FridError("error") from exc` in which case
      the exc with be chained.
    """
    def __init__(self, *args, trace: TracebackType|None=None):
        super().__init__(*args)
        if trace is not None:
            self.with_traceback(trace)

    def to_frid(self, with_trace: bool=False) -> dict[str,str|int|list[str]]:
        out: dict[str,str|int|list[str]] = {'error': str(self)}
        if with_trace:
            out['trace'] = traceback.format_exception(self)
        if self.__cause__:
            out['cause'] = str(self.__cause__)
        # TODO: notes? genre? maker?
        return out

    def __frid__(self) -> FridValue:
        return self.to_frid()

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

    def to_frid(self, with_trace: bool=False) -> dict[str,str|int|list[str]]:
        out = super().to_frid(with_trace=with_trace)
        out['http_code'] = self.http_code
        if self.http_text is not None:
            out['http_text'] = self.http_text
        return out

