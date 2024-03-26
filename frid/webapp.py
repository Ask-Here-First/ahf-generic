import json
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Literal
from urllib.parse import unquote_plus

from .typing import BlobTypes, FridValue
from .errors import FridError
from .loader import load_from_str
from .dumper import dump_into_str


JSON_ESCAPE_SEQ = "#!"
FRID_MIME_TYPE = "text/vnd.askherefirst.frid"

ShortMimeType = Literal['text','html','form','blob','json','frid']
InputHttpHead = Mapping[str|bytes,str|bytes]|Iterable[tuple[str|bytes,str|bytes]]

def parse_http_query(qs: str) -> tuple[list[tuple[str,str]|str],dict[str,FridValue]]:
    if not qs:
        return ([], {})
    if qs.startswith('?'):
        qs = qs[1:]
    qsargs: list[tuple[str,str]|str] = []
    kwargs: dict[str,FridValue] = {}
    for i, x in enumerate(qs.split('&')):
        if '=' not in x:
            qsargs.append(unquote_plus(x))
            continue
        (k, v) = x.split('=', 1)
        uk = unquote_plus(k)
        uv = unquote_plus(v)
        qsargs.append((uk, uv))
        # If the first character is percentage encoded, retreat whole thing as stting
        kwargs[uk] = load_from_str(uv) if v and v[0] != '%' else uv
    return (qsargs, kwargs)

class HttpMixin:
    """This is a generic mixin class that stores HTTP data.

    It can also be constructed to hold data for either an HTTP request or
    an HTTP response. Constructor arguments (all optional and keyword):
    - `ht_status`: the HTTP status code; for response, it defaults
      to 200/204 with data, or 500 for error.
    - `http_body`: the raw binary body; for responses it is used if specified; otherwise
      it it generated from `http_data`.
    - `mime_type`: the mime_type but one can use one of the following shortcuts:
        + `text`, `blob`, `html`, `json`, `frid`; they will be converted to right MIME types.
        + The default is `json` for responses.
    - `http_data`: the data supported by Frid; will be dumped into string:
        + To frid format if mime_type is frid;
        + To JSON format if unset, but with default escapes.
        + To JSON5 format with escapes when dump to stream in lines.
    """
    def __init__(self, /, *args, ht_status: int=0, http_head: Mapping[str,str]|None=None,
                 http_body: BlobTypes|None=None, mime_type: str|ShortMimeType|None=None,
                 http_data: FridValue|AsyncIterator[FridValue]=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.ht_status = ht_status
        self.http_body = http_body
        self.http_data = http_data
        self.mime_type = mime_type
        self.http_head = dict(http_head) if http_head is not None else {}

    @classmethod
    def from_request(cls, rawdata: bytes|None, headers: InputHttpHead,
                     *args, **kwargs) -> 'HttpMixin':
        """Processing the HTTP headers and data """
        items = headers.items() if isinstance(headers, Mapping) else headers
        http_head: dict[str,str] = {}
        for key, val in items:
            # Convert them into string
            if isinstance(key, bytes):
                key = key.decode()
            elif not isinstance(key, str):
                key = str(key)
            if isinstance(val, bytes):
                val = val.decode()
            elif not isinstance(val, str):
                val = str(key)
            key = key.lower()  # Always using lower cases
        # Extract content type
        encoding: str = 'utf-8'
        mime_type = http_head.get('content-type')
        if mime_type is not None and ';' in mime_type:
            (mime_type, other) = mime_type.split(';', 1)
            mime_type = mime_type.strip().lower()
            if '=' in other:
                (key, val) = other.split('=', 1)
                if key.strip().lower() == 'charset':
                    encoding = val.strip().lower()
        # Decoding the data if any
        if rawdata is not None:
            match mime_type:
                case 'text/plain':
                    http_data = rawdata.decode(encoding)
                    mime_type = 'text'
                case 'text/html':
                    http_data = rawdata.decode(encoding)
                    mime_type = 'html'
                case 'application/x-binary' | 'application/octet-stream':
                    http_data = rawdata
                    mime_type = 'blob'
                case 'application/x-www-form-urlencoded':
                    http_data = parse_http_query(rawdata.decode(encoding))
                    mime_type = 'form'
                case 'application/json' | 'text/json':
                    http_data = json.loads(rawdata.decode(encoding))
                    mime_type = 'json'
                case _:
                    if mime_type == FRID_MIME_TYPE:
                        http_data = load_from_str(rawdata.decode(encoding))
                        mime_type = 'frid'
                    else:
                        http_data = None  # mime_type unchanged
        return cls(*args, http_head=http_head, mime_type=mime_type, http_body=rawdata,
                   http_data=http_data, **kwargs)

    @staticmethod
    async def _streaming(stream: AsyncIterator[FridValue]):
        async for item in stream:
            yield dump_into_str(item, json_level=5)

    def gen_response(self) -> 'HttpMixin':
        """Update ht_status, http_body, and http_head according to http_data."""
        # Convert data to body if http_body is not set
        if self.http_body is not None:
            return self
        if self.http_data is None:
            if not self.ht_status:
                self.ht_status = 204
            return self
        if isinstance(self.http_data, bytes):
            body = self.http_data
            mime_type = 'blob'
        elif isinstance(self.http_data, str):
            body = self.http_data.encode()
            mime_type = 'text'
        elif isinstance(self.http_data, AsyncIterator):
            body = self._streaming(self.http_data)
            mime_type = "text/event-stream"
        elif self.mime_type == 'json':
            body = dump_into_str(self.http_data, json_level=False).encode()
            mime_type = self.mime_type
        elif self.mime_type == 'frid':
            body = dump_into_str(self.http_data).encode()
            mime_type = self.mime_type
        else:
            body = dump_into_str(self.http_data, json_level=JSON_ESCAPE_SEQ).encode()
            mime_type = 'json'
        self.http_body = body
        # Check mime type for Content-Type if it is missing in http_head
        if 'content-type' not in self.http_head:
            if self.mime_type is not None:
                mime_type = self.mime_type # OVerriding the content's mime_type
            if mime_type is not None:
                match mime_type:
                    case 'text':
                        mime_type = 'text/plain'
                    case 'json':
                        mime_type = 'application/json'
                    case 'html':
                        mime_type = 'text/html'
                    case 'blob':
                        mime_type = 'application/octet-stream'
                    case 'form':
                        mime_type = 'application/x-www-form-urlencoded'
                    case 'frid':
                        mime_type = FRID_MIME_TYPE
                self.http_head['content-type'] = mime_type + ";charset=utf-8"
        # Update the status with 200
        if not self.ht_status:
            self.ht_status = 204 if body is None else 200
        return self

class HttpError(HttpMixin, FridError):
    """An HttpError with an status code.
    - The constructor requires the http status code as the first argment
      before the error message.
    - Optionally an HTTP text can be given by `http_text` for construction.
    - Users can also specify `headers` as a dict.
    """
    def __init__(self, ht_status: int, *args, **kwargs):
        super().__init__(*args, ht_status=ht_status, **kwargs)
    def frid_repr(self) -> dict[str,str|int|list[str]]:
        out = super().frid_repr()
        out['ht_status'] = self.ht_status
        return out

