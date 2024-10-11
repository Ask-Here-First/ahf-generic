import sys, traceback
from logging import info
from dataclasses import dataclass
from collections.abc import AsyncIterable, Iterable, Mapping, Callable, Sequence
from typing import Any, Literal, TypedDict
if sys.version_info >= (3, 11):
    from typing import NotRequired, Unpack
else:
    from typing_extensions import NotRequired, Unpack  # noqa: F401

from ..osutil import load_module_data
from ..typing import get_type_name, get_func_name
from ..typing import FridNameArgs, FridValue, MissingType, MISSING
from ..guards import is_frid_value
from .._basic import frid_redact
from .._dumps import dump_args_str
from .mixin import HttpError, HttpMixin, InputHttpHead, parse_url_query, parse_url_value
from .files import FileRouter


# WEBHOOK_BASE_PATH = "/hooks"

# - If call type is a string, it is call with (call_type, data, *opargs, **kwargs)
# - If call type is true, it is call with (data, *opargs, **kwargs)
# - If call type is false, it is call just with just (*opargs, **kwargs)
ApiCallType = Literal['get','set','put','add','del']
_api_call_types: dict[str,ApiCallType] = {
    'HEAD': 'get', 'GET': 'get', 'POST': 'set', 'PUT': 'put',
    'PATCH': 'add', 'DELETE': 'del'
}


HTTP_SUPPORTED_METHODS = ('HEAD', 'GET', 'PUT', 'POST', 'DELETE', 'PATCH')
HTTP_METHODS_WITH_BODY = ('POST', 'PUT', 'PATCH')

class HttpInfo(TypedDict, total=False):
    path: str               # The path to in the URL
    qstr: str               # The query string in the URL
    frag: list[str]         # Three element list after path fragmentation
    call: ApiCallType       # One of the five calls
    head: dict[str,str]     # The headers of the call
    mime: str               # The mime-type of the body
    body: bytes             # The body of the call
    data: FridValue         # The data of the call
    auth: str               # The auth string from the header or elsewhere
    peer: str               # The peer IP address (no port)

@dataclass
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
    - If `numfpa` is 2, the `actype` and request data is passed as the first two arguments
    - Additional keyword arguments tried to be passed:
        + `_http`: an HttpInfo dict for request information if the user accepts it
        + `_data`: if `action` is None (a callable router) but the API call is not `get`
          and there is data (which can be None).
        + `_call`: if the API call is not 'get'; the callee should use a default value
          of `get` if accepting it.
    """
    method: str
    pfrags: list[str]
    qsargs: list[tuple[str,str]|str]
    router: Any
    action: Callable
    vpargs: list[FridValue]
    kwargs: dict[str,FridValue]
    numfpa: Literal[0,1,2]

    def __call__(self, req: HttpMixin, **kwargs: Unpack[HttpInfo]):
        # Fetch authorization status
        peer = kwargs.get('peer')
        auth = req.http_head.get('authorization')
        if isinstance(auth, str):
            pair = auth.split()
            if len(pair) == 2 and pair[0] == "Bearer":
                auth = pair[1]
        # with_auth = self._auth_key is None or self._auth_key == auth_key # TODO: change to id
        # Get the the route information
            # Read body if needed
        msg = self.get_log_str(req, peer)
        info(msg)
        # Generates the HttpInfo structure users might need
        assert not isinstance(req.http_data, AsyncIterable)
        http_info: HttpInfo = {
            'call': _api_call_types[self.method], **kwargs, 'head': req.http_head,
            'frag': self.pfrags,
        }
        if req.http_data is not MISSING:
            http_info['data'] = req.http_data
        if req.mime_type is not None:
            http_info['mime'] = req.mime_type
        if req.http_body is not None:
            assert not isinstance(req.http_body, AsyncIterable)
            http_info['body'] = req.http_body
        if auth is not None:
            http_info['auth'] = auth
        try:
            args = self._get_vpargs(req.http_data)
            kwds = self._get_kwargs(req.http_data)
            try:
                return self.action(*args, **kwds, _http=http_info)
            except TypeError:
                pass
            return self.action(*args, **kwds)
        except TypeError as exc:
            traceback.print_exc()
            return HttpError(400, "Bad args: " + msg, cause=exc)
        except Exception as exc:
            traceback.print_exc()
            return self.to_http_error(exc, req, peer=peer)
    def to_http_error(self, exc: Exception, req: HttpMixin, peer: str|None) -> HttpError:
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
        return HttpError(status, "Crashed: " + self.get_log_str(req, peer), cause=exc)
    def _get_vpargs(self, data: FridValue|MissingType) -> tuple[FridValue,...]:
        if data is MISSING:
            data = None    # Pass data as None if it is MISSING
        match self.numfpa:
            case 0:
                return tuple(self.vpargs)
            case 1:
                return (data, *self.vpargs)
            case 2:
                return (_api_call_types[self.method], data, *self.vpargs)
            case _:
                raise ValueError(f"Invalid value of numfpa={self.numfpa}")
    def _get_kwargs(self, data: FridValue|MissingType):
        if self.router is not self.action or self.method == 'GET':
            return self.kwargs
        kwargs = dict(self.kwargs)
        kwargs['_call'] = _api_call_types[self.method]
        if data is not MISSING:
            kwargs['_data'] = data
        return kwargs
    def get_log_str(self, req: HttpMixin, peer: str|None=None):
        assert is_frid_value(req.http_data) or req.http_data is MISSING, type(req.http_data)
        (prefix, medial, _) = self.pfrags
        data = MISSING if req.http_data is MISSING else frid_redact(req.http_data, 0)
        return f"[{peer}] ({prefix}) {self.method} " + dump_args_str(FridNameArgs(
            medial, self._get_vpargs(data), self.kwargs
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
      The header 'Access-Control-Allow-Origin' is set if the origin is in the list.
    - `set_connection`: if not None, `Connection: keep-alive` (for true value) or
      `Connection: close` (for false value) is added to the header.

    Note that for file router, the same prefix can have only one router;
    however, a file router can be served from multiple directories or paths
    in zip files, allowing overlay between them.
    """
    _route_prefixes = {
        'HEAD': ['get_', 'run_'],
        'GET': ['get_', 'run_'],
        'POST': ['set_', 'post_', 'run_'],
        'PUT': ['put_', 'run_'],
        'PATCH': ['add_', 'patch_', 'run_'],
        'DELETE': ['del_', 'delete_', 'run_'],
    }
    _num_fixed_args: dict[str,Literal[0,1,2]] = {
        'get_': 0, 'set_': 1, 'put_': 1, 'add_': 1, 'del_': 0, 'run_': 2,
        'post_': 1, 'patch_': 1, 'delete_': 0,
    }
    _common_headers = {
        'Cache-Control': "no-cache",
        # 'Connection': "keep-alive",
        'Content-Encoding': "none",
        'Access-Control-Allow-Headers': "X-Requested-With",
        'Access-Control-Max-Age': "1728000",
    }  # TODO: add CORS & cache constrol headers

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
    def create_route(self, method: str, path: str, qstr: str|None) -> ApiRoute|HttpError:
        assert isinstance(path, str)
        result = self.fetch_router(path, qstr)
        if isinstance(result, HttpError):
            return result
        if result is None:
            return HttpError(404, f"Cannot find the path router for {path}")
        (router, prefix) = result
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
            numfpa = 0
        else:
            raise HttpError(403, f"[{prefix}]: the router is not callable")
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
        return ApiRoute(
            method=method, pfrags=[prefix, medial, suffix], qsargs=qsargs,
            router=router, action=action, vpargs=vpargs, kwargs=kwargs, numfpa=numfpa
        )
    def fetch_router(self, path: str, qstr: str|None) -> tuple[str,str]|HttpError|None:
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
        return None
    @classmethod
    def fetch_action(
        cls, router, method: str, prefix: str, suffix: str, qstr: str|None
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

    def handle_options(self, path: str, qstr: str|None) -> HttpMixin:
        if path == '*':
            return HttpMixin(ht_status=203, http_head={
                'access-control-allow-methods': ", ".join(HTTP_SUPPORTED_METHODS) + ", OPTIONS"
            })
        result = self.fetch_router(path, qstr)
        if isinstance(result, HttpError):
            return result
        if result is None:
            return HttpError(404, f"Invalid request OPTIONS {path}")
        return HttpMixin(ht_status=203, http_head={
            # TODO find out what methods are suppoted
            'access-control-allow-methods': "GET, POST, PUT, DELETE, PATCH, OPTIONS"
        })
    def update_headers(self, response: HttpMixin, request: HttpMixin):
        """Adding extra headers to response; mostly for CORS, cache, and access control."""
        headers = response.http_head
        headers.update(self._common_headers)
        host = request.http_head.get('host')
        assert isinstance(host, str)
        if ':' in host:
            host = host.split(':')[0]
        origin = request.http_head.get('origin')
        if origin and (origin in self.accept_origins or host in ('127.0.0.1', 'localhost')):
            headers['Access-Control-Allow-Origin'] = origin
        if isinstance(response.http_data, AsyncIterable):
            headers['X-Accel-Buffering'] = "no"
        if self.set_connection is not None:
            headers['Connection'] = "keep-alive" if self.set_connection else "close"
        return headers

    def handle_request(
            self, method: str, data: bytes|None, headers: InputHttpHead,
            *, path: str, qstr: str|None, peer: str|tuple[str,int]|None,
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
        route = self.create_route(method, path, qstr)
        if isinstance(route, HttpError):
            return (request, route)
        kwargs: HttpInfo = {'path': path}
        if qstr is not None:
            kwargs['qstr'] = qstr
        if peer is not None:
            if not isinstance(peer, str):
                peer = peer[0]
            kwargs['peer'] = peer
        try:
            return (request, route(request, **kwargs))
        except HttpError as exc:
            return (request, exc)
        except Exception as exc:
            traceback.print_exc()
            return (request, route.to_http_error(exc, request, peer=peer))
    def process_result(self, request: HttpMixin, result: HttpMixin|FridValue) -> HttpMixin:
        """Process the result of the route execution and returns a response.
        - The response is an object of HttpMixin with body already prepared.
        """
        if isinstance(result, HttpMixin):
            response = result
        else:
            ht_status = 200
            http_head: dict[str,str] = {}
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

def echo_router(*args, _data: FridValue|MissingType=MISSING,
                _call: str='get', _http: HttpInfo={}, **kwds):
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
            if '(' not in value and ')' not in value:
                value += "()"
            routes[name] = value
        else:
            if assets is not None:
                print(f"The root directory is already specified: {assets}", file=sys.stderr)
                sys.exit(1)
            assets.add(item)
    return (routes, assets, host, port)
