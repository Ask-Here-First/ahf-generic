import os, json, itertools
from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Any, Literal
from urllib.parse import unquote
from email.message import Message

from ..typing import MISSING, BlobTypes, FridValue, MissingType
from ..typing import FridError
from ..guards import is_frid_value
from ..lib.dicts import CaseDict
from .._loads import load_frid_str
from .._dumps import dump_frid_str


DEF_ESCAPE_SEQ = os.getenv('FRID_ESCAPE_SEQ', "#!")
FRID_MIME_TYPE = "text/vnd.frid"

ShortMimeLabel = Literal['text','html','form','blob','frid','yaml','json','json5']
_mime_label_types: dict[ShortMimeLabel,list[str]] = {
    'text': ["text/plain"],
    'html': ["text/html", "text/xhtml", "application/xhtml+xml", "application/html+xml"],
    'blob': ["application/octet-stream", "application/x-binary", "application/binary",
             "binary/octet-stream"],
    'form': ["application/x-www-form-urlencoded"],
    'frid': [FRID_MIME_TYPE, "text/frid", "application/frid", "application/x-frid"],
    'yaml': ["application/yaml", "text/yaml", "application/x-yaml", "text/vnd.yaml"],
    'json': ["application/json", "text/json"],
    'json5': ['application/json5', "text/json5"],
}
mime_label_type = {k: v[0] for k, v in _mime_label_types.items()}
mime_type_label = dict(itertools.chain.from_iterable(
    ((x, k) for x in v) for k, v in _mime_label_types.items()
))

HttpInputHead = (
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

def parse_url_query(qs: str|None) -> tuple[list[tuple[str,str]|str],dict[str,FridValue]]:
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

def parse_http_body(http_body: bytes, mime_type: str|None,
                    encoding: str) -> tuple[FridValue,ShortMimeLabel|None]:
    """Parse the HTTP body using mime_type as hints.
    - Returns a pair of parsed data and a short MIME label if it is of
      a compatible content type.
    """
    mime_label = mime_type_label.get(mime_type) if mime_type else None
    match mime_label:
        case 'text' | 'html':
            data = http_body.decode(encoding)
        case 'blob':
            data = http_body
        case 'form':
            data = parse_url_query(http_body.decode(encoding))
        case 'json':
            data = json.loads(http_body.decode(encoding))
        case 'json5':
            data = load_frid_str(http_body.decode(encoding), json_level=5)
        case 'frid':
            data = load_frid_str(http_body.decode(encoding))
        case _:
            data = http_body
            if not mime_type:  # If not specified, try to parse as json
                try:
                    data = json.loads(http_body.decode(encoding))
                    mime_label = 'json'
                except Exception:
                    pass
    return (data, mime_label)

def build_http_body(http_data: FridValue, mime_type: str|None=None) -> tuple[bytes,str]:
    """Build HTTP body from the data, using Python type and `mime_type` as hints.
    - `mime_type` can be a short label, but usually it is not set (=None).
    - For now we always use UTF-8 encoding; user has no way to set to other encoding.
    - Returns a pair of `http_body` in bytes and the actual mime_type string,
      which won't be None.
    """
    if mime_type is None:
        if isinstance(http_data, bytes):
            body = http_data
            mime_label: ShortMimeLabel = 'blob'
        elif isinstance(http_data, str):
            body = http_data.encode()
            mime_label = 'text'
            # Detecting HTML. The whole string may be very long，so we just get the end portions
            if http_data[-64:].rstrip().lower().endswith("</html>"):
                line = http_data[:64].lstrip().lower()
                if line.startswith("<html>"):
                    body = b"<!DOCTYPE html>\n" + body  # Add DOCTYPE
                    mime_label = 'html'
                elif line.startswith("<!doctype html>"):
                    mime_label = 'html'
        else:
            # For all other data types without mime, dump as JSON with extended escape sequence
            body = dump_frid_str(http_data, json_level=1, escape_seq=DEF_ESCAPE_SEQ).encode()
            mime_label = 'json'
        return (body, mime_label_type[mime_label])
    match mime_type_label.get(mime_type, mime_type):
        case 'json':
            body = json.dumps(http_data).encode() # TODO do escape
        case 'json5':
            body = dump_frid_str(http_data, json_level=5).encode()
        case 'frid':
            body = dump_frid_str(http_data).encode()
        case 'yaml':
            raise ValueError("YAML is not supported")
        case _:   # including text and blob
            if isinstance(http_data, str):
                body = http_data.encode()
            elif isinstance(http_data, bytes):
                body = http_data
            else:
                body = dump_frid_str(http_data).encode()
    # Convert all labels to type
    mime_type = mime_label_type.get(mime_type, mime_type)
    return (body, mime_type)

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
    def __init__(
            self, /, *args, ht_status: int=0, http_head: Mapping[str,str]|None=None,
            http_body: BlobTypes|None=None, mime_type: str|ShortMimeLabel|None=None,
            http_data: FridValue|AsyncIterable[FridValue|Any]|MissingType=MISSING, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ht_status: int = ht_status
        self.http_body: BlobTypes|AsyncIterable[BlobTypes]|None = http_body
        self.http_data: FridValue|AsyncIterable[FridValue|Any]|MissingType = http_data
        self.mime_type: str|ShortMimeLabel|None = mime_type
        self.http_head: CaseDict[str,str] = (
            CaseDict() if http_head is None else http_head if isinstance(http_head, CaseDict)
            else CaseDict(http_head)
        )

    @classmethod
    def from_request(cls, rawdata: bytes|None, headers: HttpInputHead,
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
        - `mime_type`: from Content-Type header converted to a short label
          as defined above or original MIME-type (with `;charset=...` removed)
          if there is no matching short label.
        - `http_head` the HTTP request headers loaded into a str-to-str dict,
          with all keys in lower cases.
        """
        items = headers.items() if isinstance(headers, Mapping|Message) else headers
        http_head: CaseDict[str,str] = CaseDict()
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
            http_head[key] = val
        # Extract content type
        encoding: str = 'utf-8'
        mime_type = http_head.get('Content-Type')
        if mime_type is not None and ';' in mime_type:
            (mime_type, other) = mime_type.split(';', 1)
            mime_type = mime_type.strip().lower()
            if '=' in other:
                (key, val) = other.split('=', 1)
                if key.strip().lower() == 'charset':
                    encoding = val.strip().lower()
        # Decoding the data if any
        if rawdata is None:
            (http_data, mime_label) = (MISSING, None)
        else:
            (http_data, mime_label) = parse_http_body(rawdata, mime_type, encoding)
        return cls(*args, http_head=http_head, mime_type=(mime_label or mime_type),
                   http_body=rawdata, http_data=http_data, **kwargs)

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
        if self.http_body is None:
            if self.http_data is MISSING:
                if not self.ht_status:
                    self.ht_status = 204
                return self
            if isinstance(self.http_data, AsyncIterable):
                self.http_body = self._streaming(self.http_data)
                # Directly set the content type here
                mime_type = self.mime_type or "text/event-stream"
            else:
                (self.http_body, mime_type) = build_http_body(self.http_data, self.mime_type)
        else:
            mime_type = self.mime_type
        # Set Content-Type using mime_type or mime_label if it is missing in http_head
        if 'Content-Type' not in self.http_head and mime_type:
            if mime_type_label.get(mime_type) not in (None, 'blob'):
                mime_type += "; charset=utf-8"  # All non blob labels are of text format
            self.http_head['Content-Type'] = mime_type
        # Update the status with 200
        if not self.ht_status:
            self.ht_status = 204 if self.http_body is None else 200
        # No body or Content-Length if status code is 1xx, 204, 304
        if self.ht_status < 200 or self.ht_status in (204, 304):
            self.http_body = None
            return self
        if self.http_body is None:
            self.http_body = b''
            self.http_head['Content-Length'] = "0"
        elif isinstance(self.http_body, BlobTypes):
            self.http_head['Content-Length'] = str(len(self.http_body))
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
    def frid_args(self) -> tuple[str|BlobTypes|float,...]:
        return (self.ht_status, *FridError.frid_args(self))
    def __insert_ht_status(self, s: str):
        i = s.find('(')
        if i < 0:
            return s
        i += 1
        if i + 1 < len(s) and s[i+1] == ')':
            return s[:i] + str(self.ht_status) + s[i:]
        return s[:i] + str(self.ht_status) + ',' + s[i:]
    def to_str(self):
        return self.__insert_ht_status(FridError.to_str(self))
    def __repr__(self):
        return self.__insert_ht_status(FridError.__repr__(self))
