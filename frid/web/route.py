import sys, inspect, dataclasses, traceback
from logging import info
from collections.abc import AsyncIterable, Iterable, Mapping, Callable, Sequence
from typing import Any, Literal, TypedDict, TypeVar
from functools import partial
from fnmatch import fnmatch
from socket import gethostname

from ..typing import Unpack   # Python 3.11 only feature
from ..typing import FridNameArgs, FridValue, MissingType, MISSING
from ..typing import get_type_name, get_func_name
from ..guards import is_frid_value
from ..lib import CaseDict
from .._basic import frid_redact
from .._dumps import dump_args_str
from .._utils import load_module_data
from .mixin import HttpError, HttpMixin, HttpInputHead, parse_url_query, parse_url_value
from .files import FileRouter

# - If call type is a string, it is call with (call_type, data, *opargs, **kwargs)
# - If call type is true, it is call with (data, *opargs, **kwargs)
# - If call type is false, it is call just with just (*opargs, **kwargs)
HttpMethod = Literal['GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS','CONNECT','TRACE']
# With other "quasi methods"; 'kind' is a more relaxed term
MethodKind = HttpMethod|Literal[':websocket:']
# The internal unified data handling type that is HTTP-agnostic
CallOpType = Literal['get','set','put','add','del']  # GSPAD
_call_op_type_map: dict[str,CallOpType] = {
    'HEAD': 'get', 'GET': 'get', 'POST': 'set', 'PUT': 'put',
    'PATCH': 'add', 'DELETE': 'del'
}


HTTP_SUPPORTED_METHODS = ('HEAD', 'GET', 'PUT', 'POST', 'DELETE', 'PATCH')
HTTP_METHODS_WITH_BODY = ('POST', 'PUT', 'PATCH')

class HttpRouted(TypedDict):
    optype: CallOpType|None                 # The operation type
    router: str                             # This is actually ApiRoute.prefix
    action: str                             # This is actually ApiRoute.medial
    vpargs: Sequence[FridValue]             # Variable positional args,from ApiRoute.suffix
    qsargs: Sequence[tuple[str,str]|str]    # Query string, percentage decoded pairs
    kwargs: Mapping[str,FridValue]          # Keyward arguments, processing from query string

_T = TypeVar('_T')
class HttpInput(TypedDict, total=False):
    method: MethodKind      # One of the five calls
    routed: HttpRouted      # How the HTTP request is routed
    client: str             # The client IP address (port information is removed)
    server: str             # The server address (optional [:port]); from HTTP Host
    # Infromation about the URL
    path: str               # The path to in the URL
    qstr: str               # The query string in the URL, without '?'
    # Information in the headers
    head: Mapping[str,str]  # The headers of the call
    auth: str               # The auth string from the header or elsewhere
    want: Sequence[str]     # A list of MIME types for most wanted to least wanted (from Accept)
    # Information in the HTTP body
    body: bytes             # The body of the call
    data: FridValue         # The data of the call, process from body (typically as json)
    mime: str               # The mime-type of the body

@dataclasses.dataclass(kw_only=True)
class ApiRoute:
    """The class containing information to make an API call through an URL.

    The URL is split into the following fields of this class:

    - `method`: the HTTP method.
    - `pfrags`: The fragments (prefix, medial, suffix) of the path.
    - `qsargs`: The query string, percentage decoded, saved as a list of string or a pair
       of strings.

    The path can be reconstructed by joining `prefix`, `action` (if not None), and `suffix`.
    Other fields with processed arguments:

    - `router`: the router object. It is usually a user-defined class object.
    - `action`: the actual callable action to invoke, the router object itself
       or one of its methods.
    - `vpargs`: the variable positional arguments for the callee, processed from `suffix`.
    - `kwargs`: the keyward arguments for the callee, processed from `qsargs`.
    - `numfpa`: the number of fixed positional arguments, one of 0, 1, and, 2.

    The `action` is called with `numfpa` number of position arguments, followed by `*vpargs`
    and then the keyword arguments given by `kwargs`.
    - If `numfpa` is 1, the request data is passed as the first argument (or None)
    - If `numfpa` is 2, the `optype` and request data is passed as the first two arguments
    - Additional keyword arguments tried to be passed:
        + `_http`: an HttpInfo dict for request information if the user accepts it
        + `_data`: if `action` is None (a callable router) but the API call is not `get`
          and there is data (which can be None).
        + `_call`: if the API call is not 'get'; the callee should use a default value
          of `get` if accepting it.
    """
    # HTTP request information
    method: MethodKind
    prefix: str
    medial: str
    suffix: str
    vpargs: list[FridValue]
    qsargs: list[tuple[str,str]|str]
    kwargs: dict[str,FridValue]
    # Routing information
    router: Any                 # The router object determined by self.prefix
    action: Callable            # The action method determined by self.medial
    numfpa: Literal[0,1,2]

    request: dataclasses.InitVar[HttpMixin]         # Input request
    no_args: bool = dataclasses.field(init=False, default=False)   # Do not pass args to action

    def __post_init__(self, request: HttpMixin):
        if inspect.isclass(self.router):
           # This is a class object; must instantiate.
            http_input = self.to_http_input(request)
            try:
                router = self.router(*self.vpargs, **self.kwargs, _http=http_input)
            except TypeError:
                router = self.router(*self.vpargs, **self.kwargs)
            if self.router is self.action:
                if not callable(router):
                    raise HttpError(403, f"[{self.prefix}]: router not callable {type(router)}")
                self.action = router
            else:
                # Must get the bound version (previous unbound)
                self.action = getattr(router,  self.action.__name__)
            self.router = router
            self.no_args = True

    def __call__(self, req: HttpMixin, **kwargs: Unpack[HttpInput]):
        client = kwargs.get('client')
        msg = self.get_log_str(req, client)
        info(msg)
        # Generates the HttpInput structure users might need
        ## It is now possible with have AsyncIterable data with websock
        # assert not isinstance(req.http_data, AsyncIterable)
        try:
            args = self._get_vpargs(req.http_data)
            kwds = self._get_kwargs(req.http_data)
            if not self.no_args:
                # First try with _http argument
                try:
                    return self.action(*args, **kwds, _http=self.to_http_input(req))
                except TypeError:
                    pass
            return self.action(*args, **kwds)
        except TypeError as exc:
            traceback.print_exc()
            return HttpError(400, "Bad args: " + msg, cause=exc)
        except Exception as exc:
            traceback.print_exc()
            return self.as_http_error(exc, req, client=client)
    def to_http_input(self, req: HttpMixin, **kwargs: Unpack[HttpInput]) -> HttpInput:
        """Converts this route to an HttpInput object.
        - With extra data supplied by `request` and `kwargs`.
        """
        auth = req.http_head.get('Authorization')
        if isinstance(auth, str):
            pair = auth.split()
            if len(pair) == 2 and pair[0] == "Bearer":
                auth = pair[1]
        # The data can now be an AsyncIterable with websocket
        # assert not isinstance(req.http_data, AsyncIterable)
        http_input: HttpInput = {
            'method': self.method,
            'routed': {
                'optype': _call_op_type_map.get(self.method),
                'router': self.prefix, 'action': self.medial,
                'vpargs': self.vpargs, 'qsargs': self.qsargs, 'kwargs': self.kwargs,
            },
            'server': req.http_head.get('Host') or gethostname(),
            **kwargs,  # kwargs contains: client, path, qstr,
            'head': req.http_head,
            'want': [
                x.split(';')[0].strip() for x in accept.split(',') if x.strip()
            ] if (accept := req.http_head.get('Accept')) is not None else []
        }
        if is_frid_value(req.http_data):
            http_input['data'] = req.http_data
        if req.mime_type is not None:
            http_input['mime'] = req.mime_type
        if req.http_body is not None:
            assert not isinstance(req.http_body, AsyncIterable)
            http_input['body'] = req.http_body
        if auth is not None:
            http_input['auth'] = auth
        return http_input
    def as_http_error(self, exc: Exception, req: HttpMixin, client: str|None) -> HttpError:
        if isinstance(exc, HttpError):
            return exc
        status = 500
        # This part is for backward compatibility
        for name in ('http_status', 'ht_status', 'http_code'):
            if hasattr(exc, name):
                s = getattr(exc, name)
                if isinstance(s, int) and s > 0:
                    status = s
                    break
        return HttpError(status, "Crashed: " + self.get_log_str(req, client), cause=exc)
    def _get_vpargs(self, data: _T|MissingType) -> tuple[FridValue|_T,...]:
        vpargs = () if self.no_args else tuple(self.vpargs)
        data_arg = None if data is MISSING else data
        match self.numfpa:
            case 0:
                return vpargs
            case 1:
                return (data_arg, *vpargs)
            case 2:
                return (_call_op_type_map[self.method], data_arg, *vpargs)
            case _:
                raise ValueError(f"Invalid value of numfpa={self.numfpa}")
    def _get_kwargs(self, data: _T|MissingType) -> Mapping[str,_T|FridValue]:
        # Only when the router is directly callable we need the extra parameters
        if self.router is not self.action:
            return {} if self.no_args else self.kwargs
        out: dict[str,_T|FridValue] = {} if self.no_args else dict(self.kwargs)
        call = _call_op_type_map.get(self.method)
        if self.numfpa <= 1 and call is not None and call != 'get':
            out['_call'] = call
        if self.numfpa == 0 and data is not MISSING:
            out['_data'] = data
        return out
    def get_log_str(self, req: HttpMixin, client: str|None=None):
        if isinstance(req.http_data, AsyncIterable):
            data = get_type_name(req.http_data)
        else:
            assert is_frid_value(req.http_data) or req.http_data is MISSING, type(req.http_data)
            data = MISSING if req.http_data is MISSING else frid_redact(req.http_data, 0)
        return f"[{client}] ({self.prefix}) {self.method} " + dump_args_str(FridNameArgs(
            self.medial, self._get_vpargs(data), self._get_kwargs(data)
        ))

class ApiRouteManager:
    """The base route management class.

    Constructor arguments:
    - `routes`: (optional) a map from the URL path prefixes to router objects.
      The values can be router objects/functors themselves, or a string to
      specify where the router can be loaded.
        + Object routers are the class path (`package:ClassName`) followed
          by constructing parameters enclosed in `()`, specified in Frid format.
        + Functor routers are just the function path (`package:FunctionName`)
    - `assets`: (optional) specifies the static asset files on disk.
        + A single directory path on disk, or a path within a zip file (e.g.
          `myzip.zip/dir1/dir2`. The prefix is assumed to be root (`''`).
        + A list of such paths. The prefix is assumed to be root (`''`).
        + A map from paths to URL path prefixes.
      For each unique prefixes, a single file router is created.
    - `accept_origins`: the list of origins that can be accepted.
      One can use hostname only (no https:// or http://) and use glob patterns.
      The header 'Access-Control-Allow-Origin' is set if the origin is in the list.
    - `set_connection`: if not None, `Connection: keep-alive` (for true value) or
      `Connection: close` (for false value) is added to the header.

    Note that for file router, the same prefix can have only one router;
    however, a file router can be served from multiple directories or paths
    in zip files, allowing overlay between them.
    """
    _route_prefixes: Mapping[MethodKind,Sequence[str]] = {
        'HEAD': ['get_', 'run_'],
        'GET': ['get_', 'run_'],
        'POST': ['set_', 'post_', 'run_'],
        'PUT': ['put_', 'run_'],
        'PATCH': ['add_', 'patch_', 'run_'],
        'DELETE': ['del_', 'delete_', 'run_'],
    }
    _num_fixed_args: Mapping[str,Literal[0,1,2]] = {
        'get_': 0, 'set_': 1, 'put_': 1, 'add_': 1, 'del_': 0, 'run_': 2,
        'post_': 1, 'patch_': 1, 'delete_': 0,
    }
    _rprefix_revmap: Mapping[str,Sequence[MethodKind]] = {
        'get_': ['GET'], 'set_': ['POST'], 'put_': ['PUT'], 'add_': ['PATCH'],
        'del_': ['DELETE'], 'run_': ['GET', 'HEAD', 'POST', 'PUT', 'PATCH'],
        'post_': ['PUT'], 'patch_': ['PATCH'], 'delete_': ['DELETE'],
    }
    _common_headers = {
        'Cache-Control': "no-cache",
        # 'Connection': "keep-alive",
        'Content-Encoding': "none",
    }  # TODO: add CORS & cache constrol headers
    _localhost_list = [
        "localhost", "127.0.0.1", "[::1]",
    ]

    @classmethod
    def __init_subclass__(cls):
        try:
            import markdown
            cls._markdown: Callable[[str],str]|None = markdown.Markdown(
                extensions=['tables', 'fenced_code']
            ).convert
        except ImportError:
            cls._markdown = None
    def __init__(
            self, routes: Mapping[str,str|Any]|None=None,
            assets: str|Iterable[str]|Mapping[str,str]|None=None,
            *, accept_origins: Sequence[str]|None=None, set_connection: bool|None=True,
    ):
        self.accept_origins = accept_origins if accept_origins else []
        self.set_connection = set_connection
        self._registry = {}
        if isinstance(assets, str):
            self._registry[''] = FileRouter(assets)
        elif isinstance(assets, Mapping):
            roots: dict[str,list[str]] = {}
            for k, v in assets.items():
                if v in roots:
                    roots[v].append(k)
                else:
                    roots[v] = [k]
            for k, v in roots.items():
                self._registry[k] = FileRouter(*v)
        elif assets is not None:
            self._registry[''] = FileRouter(*assets)
        if routes is not None:
            self._registry.update(
                (k, (load_module_data(v) if isinstance(v, str) else v))
                for k, v in routes.items()
            )
        info("Current routes:")
        for k, v in self._registry.items():
            if isinstance(v, FileRouter):
                r = ' | '.join(v.roots())
            elif k.endswith('/'):
                r = get_type_name(v)
            else:
                r = get_func_name(v)
            info(f"|   {k or '[ROOT]'} => {r}")
    def create_route(self, method: MethodKind, path: str, qstr: str|None, request: HttpMixin,
                     *, router=None, prefix: str|None=None) -> ApiRoute|HttpError:
        if router is None or prefix is None:
            # The fetch optional; not performed if both router and prefix are given
            result = self.fetch_router(path, qstr)
            if isinstance(result, HttpError):
                return result
            if router is None:
                router = result[0]
            if prefix is None:
                prefix = result[1]
        suffix = path[len(prefix):]
        if prefix.endswith('/'):
            result = self.fetch_action(router, method, prefix, suffix, qstr)
            if isinstance(result, HttpError):
                return result
            (action, medial, suffix, numfpa) = result
        elif callable(router):
            action = router
            # Special case if prefix is empty and suffix == '/', set it to member
            if not prefix and suffix == '/':
                medial = "/"
                suffix = ""
            else:
                medial = ""
                suffix = path[len(prefix):]
            # Always pass a single data pbject for non-HTTP route
            numfpa = 0 if method in HTTP_SUPPORTED_METHODS else 1
        else:
            return HttpError(403, f"[{prefix}]: the router is not callable: {type(router)}")
        # Parse the query string
        (qsargs, kwargs) = parse_url_query(qstr)
        if suffix:
            if suffix == '/':
                url = prefix + medial + ('' if qstr is None else '?' + qstr)
                return HttpError(307, http_head={'location': url})
            if suffix[0] == '/':
                args = suffix[1:].split('/')
                leading = '/'
            else:
                args = suffix.split('/')
                leading = ''
            if not all(item for item in args):
                url = prefix + medial + leading + '/'.join(item for item in args if item) + (
                    '' if qstr is None else '?' + qstr
                )
                return HttpError(307, http_head={'location': url})
            vpargs = [parse_url_value(item) for item in args]
        else:
            vpargs = []
        assert path == prefix + medial + suffix
        try:
            return ApiRoute(
                method=method, prefix=prefix, medial=medial, suffix=suffix,
                vpargs=vpargs, qsargs=qsargs, kwargs=kwargs,
                router=router, action=action, numfpa=numfpa,
                request=request,
            )
        except Exception as exc:
            if isinstance(exc, HttpError):
                return exc
            return HttpError(400, exc)
    def fetch_router(self, path: str, qstr: str|None) -> tuple[Any,str]|HttpError:
        """Fetch the router object in the registry that matches the
        longest prefix of path.
        - Returns the router object and its prefix. If it does not match,
          return (None, None)
        """
        router = self._registry.get(path)
        if router is not None:
            return (router, path)
        if not path.endswith('/') and self._registry.get(path + '/'):
            url = path + "/" if qstr is None else path + "/?" + qstr
            return HttpError(307, http_head={'location': url})
        index = path.rfind('/')
        while index >= 0:
            prefix = path[:(index+1)]
            router = self._registry.get(prefix)
            if router is not None:
                return (router, prefix)
            prefix = path[:index]
            router = self._registry.get(prefix)
            if router is not None:
                return (router, prefix)
            index = path.rfind('/', 0, index)
        return HttpError(404, f"Cannot find the route for {path}")
    @classmethod
    def fetch_action(
        cls, router, method: MethodKind, prefix: str, suffix: str, qstr: str|None
    ) -> tuple[Callable,str,str,Literal[0,1,2]]|HttpError:
        """Find the end point in the router according to the path.
        - First try using prefixes concatenated with the first path element as names;
        - Then try the prefixes themselves.
        """
        if suffix and suffix[0] != '/':
            index = suffix.find('/')
            if index > 0:
                medial = suffix[:index]
                new_suffix = suffix[index:]
            else:
                medial = suffix
                new_suffix = ""
            # Special actions when this medial string starting with '-'
            if medial.startswith('-'):
                return (partial(cls.special_action, method, router, prefix, medial),
                        medial, new_suffix, 2)
            # Search for medials
            for rp in cls._route_prefixes[method]:
                full_name = rp + medial
                if not hasattr(router, full_name):
                    continue
                action = getattr(router, full_name)
                if not callable(action):
                    continue
                return (action, medial, new_suffix, cls._num_fixed_args[rp])
        for rp in cls._route_prefixes[method]:
            if not hasattr(router, rp):
                continue
            action = getattr(router, rp)
            if not callable(action):
                continue
            return (action, '', suffix, cls._num_fixed_args[rp])
        return HttpError(405, f"[{prefix}]: no action matches '{suffix}'")

    @classmethod
    def search_actions(cls, router) -> list[tuple[str,Sequence[MethodKind],Callable]]:
        out: list[tuple[str,Sequence[MethodKind],Callable]] = []
        for name in dir(router):
            try:
                attr = getattr(router, name)
            except AttributeError:
                continue
            if not callable(attr):
                continue
            index = name.find('_')
            if index <= 0:
                continue
            index += 1
            http_methods = cls._rprefix_revmap.get(name[:index])
            if http_methods is None:
                continue
            out.append((name[index:], http_methods, attr))
        out.sort(key=(lambda x: x[0]))  # In alphabet order with empty action name at the first
        return out
    @classmethod
    def special_action(cls, method: str, router, prefix: str, medial: str,
                       *args, **kwargs):
        if method != 'GET':
            return HttpError(405, f"[{prefix}]: the special action {medial} is for GET only")
        match medial:
            case '-h'|'--help':
                doc = ["# " + prefix + "\n\n"]
                if router.__doc__:
                    doc.append(router.__doc__)
                if prefix.endswith('/'):
                    for name, methods, action in cls.search_actions(router):
                        doc.append("## " + "/".join(methods) + " " + prefix + name + "\n\n")
                        if action.__doc__:
                            doc.append(action.__doc__)
                if cls._markdown is None:
                    return HttpMixin(http_data="\n\n".join(doc), mime_type='text')
                return HttpMixin(http_data=cls._markdown("\n\n".join(doc)), mime_type='html')
            case '-l'|'--list':
                if not prefix.endswith('/'):
                    return {'': ['GET', '...']}  # TODO: use inspect to exclude HTTP methods
                out: dict[str,list[MethodKind]] = {}
                for name, methods, _ in cls.search_actions(router):
                    value = out.get(name)
                    if value is None:
                        out[name] = list(methods)
                    else:
                        out[name].extend(x for x in methods if x not in value)
                return out
        return HttpError(404, f"[{prefix}]: unsupported special command '{medial}'")

    def handle_options(self, path: str, qstr: str|None) -> HttpMixin:
        if path != '*':
            result = self.fetch_router(path, qstr)
            if isinstance(result, HttpError):
                return result
        return HttpMixin(ht_status=200, http_head={
            # TODO find out what methods are suppoted
            'Access-Control-Allow-Methods': ", ".join(HTTP_SUPPORTED_METHODS) + ", OPTIONS",
            'Access-Control-Allow-Headers':
                "X-Requested-With, Content-Type, Authorization, Accept",
            'Access-Control-Max-Age': "1728000",
        })
    def origin_allowed(self, origin: str) -> bool:
        if not self.accept_origins:
            return False
        origin_no_scheme = split[1] if len(split := origin.split("://")) == 2 else origin
        for pat in self.accept_origins:
            if not pat:
                continue
            name = origin if "://" in pat else origin_no_scheme
            # Patter can not be starting with [ or ending with ] because it is IPv6 host
            if '*' in pat or '?' in pat or ('[' in pat[1:] and ']' in pat[:-1]):
                if fnmatch(name, pat):
                    return True
            else:
                if pat == name:
                    return True
        return False
    def update_headers(self, response: HttpMixin, request: HttpMixin):
        """Adding extra headers to response; mostly for CORS, cache, and access control."""
        headers = response.http_head
        headers.update(self._common_headers)
        host = request.http_head.get('Host')
        assert isinstance(host, str)
        if ':' in host:
            host = host.split(':')[0]
        origin = request.http_head.get('Origin')
        if origin and (host in self._localhost_list or self.origin_allowed(origin)):
            headers['Access-Control-Allow-Origin'] = origin
        if isinstance(response.http_data, AsyncIterable):
            headers['Access-Control-Allow-Credentials'] = "true"
        if isinstance(response.http_data, AsyncIterable):
            headers['X-Accel-Buffering'] = "no"
        if self.set_connection is not None:
            headers['Connection'] = "keep-alive" if self.set_connection else "close"
        return headers

    def handle_request(
            self, method: MethodKind, data: bytes|None, headers: HttpInputHead,
            *, path: str, qstr: str|None, client: str|tuple[str,int]|None,
    ) -> tuple[HttpMixin,HttpMixin|FridValue]:
        """Create a request object and run the route.
        - Returns a pair of (request, result), where request is an HttpMixin
          object and the result is whatever the route returns (if called) or
          an HttpError.
        """
        try:
            request = HttpMixin.from_request(data, headers)
        except Exception as exc:
            return (HttpMixin.from_request(None, headers),
                    HttpError(400, "Parsing input", cause=exc))
        if method == 'OPTIONS':
            return (request, self.handle_options(path, qstr))
        if method not in HTTP_SUPPORTED_METHODS:
            return (HttpMixin.from_request(None, headers),
                    HttpError(405, f"Bad method {method}: {method} {path}"))
        # Run the routes
        route = self.create_route(method, path, qstr, request)
        if isinstance(route, HttpError):
            return (request, route)
        route_args = self.get_route_args(path, qstr, client)
        try:
            return (request, route(request, **route_args))
        except HttpError as exc:
            return (request, exc)
        except Exception as exc:
            traceback.print_exc()
            return (request, route.as_http_error(exc, request, client=route_args.get('client')))
    def process_result(self, request: HttpMixin, result: HttpMixin|FridValue) -> HttpMixin:
        """Process the result of the route execution and returns a response.
        - The response is an object of HttpMixin with body already prepared.
        """
        if isinstance(result, HttpMixin):
            response = result
        else:
            ht_status = 200
            http_head = CaseDict[str,str]()
            mime_type: str|None = None
            if isinstance(result, tuple):
                if not 2 <= len(result) <= 3:
                    return HttpError(500, f"Invalid length of tuple: {len(result)}")
                if isinstance(result[1], int):
                    ht_status = result[1]
                elif isinstance(result[1], str):
                    mime_type = result[1]
                else:
                    return HttpError(500, f"Invalid second item of returned tuple: {result[1]}")
            assert not isinstance(request.http_data, AsyncIterable)
            response = HttpMixin(http_data=result, ht_status=ht_status, http_head=http_head,
                                 mime_type=mime_type)
        self.update_headers(response, request)
        response.set_response()
        return response

    @classmethod
    def get_route_args(cls, path: str, qstr: str|None, client: str|tuple[str,int]|None) -> HttpInput:
        kwargs: HttpInput = {'path': path}
        if qstr is not None:
            kwargs['qstr'] = qstr
        if client is None:
            return kwargs
        if isinstance(client, tuple):
            kwargs['client'] = client[0]
        else:
            kwargs['client'] = str(client)
        return kwargs

def echo_router(*args, _data: FridValue|MissingType=MISSING,
                _call: str='get', _http: HttpInput={}, **kwds):
    args = list(args)
    if _call == 'get':
        if not kwds:
            return args  # Args can be empty
        if not args:
            return kwds
        return {'.call': "get", '.args': args, '.kwds': kwds, '.http': _http}
    if isinstance(_data, Mapping):
        out = dict(_data)
    else:
        out = {}
        if _data is not MISSING:
            out['.data'] = _data
    out['.call'] = _call
    out['.http'] = _http
    if args:
        out['.args'] = args
    if kwds:
        out['.kwds'] = kwds
    return out

def load_command_line_args() -> tuple[dict[str,str],str|list[str]|dict[str,str]|None,str,int]:
    if len(sys.argv) < 2:
        argv0 = sys.argv[0] if sys.argv else "??"
        print(f"Usage: python3 {argv0} [HOST:]PORT [ROOT] [NAME=MODULE...]")
        sys.exit()
    if ':' in sys.argv[1]:
        (host, port) = sys.argv[1].split(':', 1)
        port = int(port)
    else:
        host = ''
        port = int(sys.argv[1])
    assets = []
    routes = {}
    for item in sys.argv[2:]:
        if '=' in item:
            (name, value) = item.split('=', 1)
            if not name.startswith('/'):
                name = '/' + name
            routes[name] = value
        else:
            if assets is not None:
                print(f"The root directory is already specified: {assets}", file=sys.stderr)
                sys.exit(1)
            assets.add(item)
    return (routes, assets, host, port)
