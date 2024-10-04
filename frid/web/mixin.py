import os, json
from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Any, Literal
from urllib.parse import unquote
from email.message import Message

from ..typing import BlobTypes, FridValue
from ..errors import FridError
from ..guards import is_frid_value
from ..loader import load_frid_str
from ..dumper import dump_frid_str


DEF_ESCAPE_SEQ = os.getenv('FRID_ESCAPE_SEQ', "#!")
FRID_MIME_TYPE = "text/vnd.askherefirst.frid"

ShortMimeType = Literal['text','html','form','blob','json','frid']
InputHttpHead = (
    Mapping[str,str]|Mapping[bytes,bytes]|Iterable[tuple[str|bytes,str|bytes]]|Message
)

def parse_url_value(v: str) -> FridValue:
    """Parse a single value in URL.
    - Returns a pair; first is the string form and the second is parsed as frid if possible
    """
    v2 = v.lstrip('+').replace('+', ' ')  # Convert + to space except for leading +
    value = unquote(v[:(len(v) - len(v2))] + v2)
    try:
        return load_frid_str(value)
    except Exception:
        return value

def parse_url_query(qs: str) -> tuple[list[tuple[str,str]|str],dict[str,FridValue]]:
    """Parse the URL query string (or www forms) into key value pairs.
    - Returns two data structures as a pair:
        + A list of original key value pairs of strings, URI decoded, but not evaluated.
        + A dict of with the same original decoded key, but the values are evaluated.
    """
    # About space encoding and plus handling - Current situations (verified in Chrome)
    # - encodeURIComponent() encoding both with %
    # - decodeURIComponent() does not convert + to space
    # - URLSearchParams() does encode space to + and decode + back to space
    # - For parsing forms data, one should do plus to space conversion
    # Hence the current strategy is:
    # - Keep + as + in keys
    # - Keep leading + in value as +, but convert all remaining + chars into space.
    if not qs:
        return ([], {})
    if qs.startswith('?'):
        qs = qs[1:]
        if not qs:
            return ([], {})
    qsargs: list[tuple[str,str]|str] = []
    kwargs: dict[str,FridValue] = {}
    for item in qs.split('&'):
        if '=' not in item:
            qsargs.append(unquote(item))
            continue
        (k, v) = item.split('=', 1)
        key = unquote(k)
        qsargs.append((key, unquote(v)))
        kwargs[key] = parse_url_value(v)
    return (qsargs, kwargs)


class HttpMixin:
    """The generic mixin class that stores additional HTTP data.

    It can also be constructed standalone to hold data for either an HTTP
    request or an HTTP response. Constructor arguments (all optional/keyword):
    - `ht_status`: the HTTP status code; default to 0.
    - `http_head`: the headers as str-to-str map.
    - `http_body`: the raw binary body to send, or an async generator of
      strings in the case of streamming (need unicode strings not binary).
    - `mime_type`: the mime_type with one of the following shortcuts:
      `text`, `blob`, `html`, `json`, `frid`.
    - `http_data`: the data as supported by Frid.
    """
    def __init__(self, /, *args, ht_status: int=0, http_head: Mapping[str,str]|None=None,
                 http_body: BlobTypes|None=None, mime_type: str|ShortMimeType|None=None,
                 http_data: FridValue|AsyncIterable[FridValue|Any]=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.ht_status: int = ht_status
        self.http_body: BlobTypes|AsyncIterable[BlobTypes]|None = http_body
        self.http_data: FridValue|AsyncIterable[FridValue|Any]|None = http_data
        self.mime_type: str|ShortMimeType|None = mime_type
        self.http_head: dict[str,str] = dict(http_head) if http_head is not None else {}

    @classmethod
    def from_request(cls, rawdata: bytes|None, headers: InputHttpHead,
                     *args, **kwargs) -> 'HttpMixin':
        """Processing the HTTP request headers and data and create an object.
        - `rawdata` the HTTP body data (from POST or PUT, for example)
        - `headers` the HTTP request headers.
        It will construct a HttpMixin with:
        - `ht_status` is not set.
        - `http_body` is the same as the `rawdata`.
        - `http_data` is parsed `http_body` depending on the constent type
          and encoding. Supported types: `text`, `html`, `blob`, `form`,
          `json` and `frid`, where `form` is www-form-urlencoded parsed
          into a dictionary with their value evaluated.
        - `mime_type`: from Content-Type header with aobve shortcuts or original
          MIME-type (with `;charset=...` removed) if it does not match.
        - `http_head` the HTTP request headers loaded into a str-to-str dict,
          with all keys in lower cases.
        """
        items = headers.items() if isinstance(headers, Mapping|Message) else headers
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
            # Always using lower cases
            http_head[key.lower()] = val
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
        http_data = None
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
                    http_data = parse_url_query(rawdata.decode(encoding))
                    mime_type = 'form'
                case 'application/json' | 'text/json':
                    http_data = json.loads(rawdata.decode(encoding))
                    mime_type = 'json'
                case _:
                    if not mime_type:  # If not specified try to parse as json
                        try:
                            http_data = json.loads(rawdata.decode(encoding))
                            mime_type = 'json'
                        except Exception:
                            pass
                    elif mime_type == FRID_MIME_TYPE:
                        http_data = load_frid_str(rawdata.decode(encoding))
                        mime_type = 'frid'
        return cls(*args, http_head=http_head, mime_type=mime_type, http_body=rawdata,
                   http_data=http_data, **kwargs)

    @staticmethod
    async def _streaming(stream: AsyncIterable[FridValue|tuple[str,FridValue]]):
        """This is an agent iterator that convert data to string."""
        async for item in stream:
            prefix = b''
            if isinstance(item, tuple) and len(item) == 2:
                (event, item) = item
                if isinstance(event, str):
                    prefix = b"event: " + event.encode() + b"\n"
            if item is None:
                if prefix:
                    yield prefix + b'\n'
            elif is_frid_value(item):
                yield prefix + b"data: " + dump_frid_str(
                    item, json_level=5, escape_seq=DEF_ESCAPE_SEQ
                ).encode() + b"\n\n"
            else:
                if not prefix:
                    prefix = b"event: other\n"
                yield prefix + b'\n'.join(
                    b"data: " + x.encode() for x in str(item).splitlines()
                ) + b"\n\n"

    def set_response(self) -> 'HttpMixin':
        """Update other HTTP fields according to http_data.
        - Returns `self` for chaining, so one can just do
          `var = HttpMixin(...).set_response()`
        The following fields will be updated if not present:
        - `http_body`: the content of the body in binary dump `http_data`:
            + Bytes will be dumped as is, with `blob` type;
            + Strings will be dumped by UTF-8 encoding, with `text` type;
            + `http_body` will be an async generator of strings if
              `http_data` is an async generator of objects; the object
              are dumpped when available in JSON5 format with escaping;
            + Other data types will be dumpped with `dump_into_str()`,
              with option depending `mime_type` setting: `=json` using
              builtin json dumps(), `=frid` in frid format, or default
              using json dump with escaping.
        - `mime_type`: estimated from the type of `http_data.
        - `ht_status`: set to 200 if it has a body or 204 otherwise.
        """
        # Convert data to body if http_body is not set
        mime_type: str|None = None
        if self.http_body is None:
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
            elif isinstance(self.http_data, AsyncIterable):
                body = self._streaming(self.http_data)
                mime_type = "text/event-stream"
            elif self.mime_type == 'json':
                body = json.dumps(self.http_data).encode() # TODO do escape
                mime_type = self.mime_type
            elif self.mime_type == 'frid':
                body = dump_frid_str(self.http_data).encode()
                mime_type = self.mime_type
            else:
                body = dump_frid_str(self.http_data, json_level=1,
                                     escape_seq=DEF_ESCAPE_SEQ).encode()
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
            self.ht_status = 204 if self.http_body is None else 200
        # No body or Content-Length if status code is 1xx, 204, 304
        if self.ht_status < 200 or self.ht_status in (204, 304):
            self.http_body = None
            return self
        if self.http_body is None:
            self.http_body = b''
            self.http_head['content-length'] = "0"
        elif isinstance(self.http_body, BlobTypes):
            self.http_head['content-length'] = str(len(self.http_body))
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
    def set_response(self) -> 'HttpError':
        self.http_data = self.frid_dict()  # Only show the keyword part of this error
        super().set_response()
        return self

