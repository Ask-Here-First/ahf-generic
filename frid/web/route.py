import sys, traceback
from logging import info
from dataclasses import dataclass
from collections.abc import AsyncIterable, Mapping, Callable, Sequence
from typing import Any, Literal
if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired  # noqa: F401

from ..dumper import dump_args_str, frid_redact
from ..guards import is_frid_value
from ..helper import get_type_name
from ..typing import FridNameArgs, FridValue
from ..osutil import load_data_in_module
from .mixin import HttpError, HttpMixin, InputHttpHead, parse_url_query, parse_url_value
from .files import FileRouter

"""
REST API convention.
- get_PATH: for GET method only
- set_PATH: for POST with state changes; take precedence over post_PATH
- del_PATH: for DELETE
- fix_PATH: for PATCH
- post_PATH: for POST, with or without state changes.
- delete_PATH: for DELETE
- put_PATH: for PUT
- run_PATH: for GET, POST, PUT, PATCH, mainly for queries that accepts both methods
"""

# WEBHOOK_BASE_PATH = "/hooks"

# - If call type is a string, it is call with (call_type, data, *opargs, **kwargs)
# - If call type is true, it is call with (data, *opargs, **kwargs)
# - If call type is false, it is call just with just (*opargs, **kwargs)
ApiCallType = Literal['get','set','put','add','del']|bool


HTTP_SUPPORTED_METHODS = ('HEAD', 'GET', 'PUT', 'POST', 'DELETE', 'PATCH')
HTTP_METHODS_WITH_BODY = ('POST', 'PUT', 'PATCH')

@dataclass
class ApiRoute:
    """The class containing information to make an API call through an URL.

    The URL is split into the following fields of this class:

    - `method`: the HTTP method
    - `prefix`: (str) mapped to an object, which may or may not be callable.
       Empty string for object mapped to root, or otherwise must starts with /.
       Also it must ends with a '/'.
    - `action`: (str|None) An action, as a callable attribute of the object.
       This variable can be None if the object itself is callable and will be called directly.
    - `suffix`: (str) the path after the action, as a string; can be empty; without leading /.
    - `qsargs` :: The query string, percentage decoded, saved as a list of string or a pair
       of strings.

    The path can be reconstructed by joining `prefix`, `action` (if not None), and `suffix`.
    Other fields with processed arguments:

    - `router`: the router object. It is usually a user-defined class object.
    - `callee`: the actual callable to invoke, can be the router object itself or its method.
    - `vpargs`: the variable positional arguments for the callee, processed from `suffix`.
    - `kwargs`: the keyward arguments for the callee, processed from `qsargs`.
    - `nodata`: the callee is called without request data.

    When a callee is called, the post data is the first argument, except for `get_` and `del_`
    methods. The `opargs` then follows as positional argments and `kwargs` as keyword arguments.
    """
    method: str
    prefix: str
    action: str|None
    suffix: str
    qsargs: list[tuple[str,str]|str]
    router: Any
    callee: Callable
    vpargs: list[FridValue]
    kwargs: dict[str,FridValue]
    actype: ApiCallType

    def __call__(self, req: HttpMixin, *, peer: tuple[str,int]|str|None=None, **kwargs):
        # Fetch authorization status
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
        try:
            assert not isinstance(req.http_data, AsyncIterable)
            return self.callee(*self._get_vpargs(req.http_data), **self.kwargs, __={
                'head': req.http_head, 'body': req.http_body, 'type': req.mime_type,
                'call': (self.prefix, self.action), 'auth': auth, 'peer': peer,
                **kwargs
            })
        except TypeError as exc:
            traceback.print_exc()
            return HttpError(400, "Bad args: " + msg, cause=exc)
        except Exception as exc:
            traceback.print_exc()
            return self.to_http_error(exc, req, peer)
    def to_http_error(self, exc: Exception, req: HttpMixin,
                      peer: tuple[str,int]|str|None) -> HttpError:
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
    def _get_vpargs(self, data: FridValue) -> tuple[FridValue,...]:
        if isinstance(self.actype, bool):
            return (data, *self.vpargs) if self.actype else tuple(self.vpargs)
        return (self.actype, data, *self.vpargs)
    def get_log_str(self, req: HttpMixin, peer: tuple[str,int]|str|None=None):
        if peer is None:
            peer = "??"
        elif not isinstance(peer, str):
            peer = peer[0]
        assert is_frid_value(req.http_data)
        return f"[{peer}] ({self.prefix}) {self.method} " + dump_args_str(FridNameArgs(
            self.action or "", self._get_vpargs(frid_redact(req.http_data, 0)), self.kwargs
        ))

class ApiRouteManager:
    """The base route management class.

    Constructor arguments:
    - `routes`: (optional) a map from the URL path prefixes to router objects.
    - `assets`: (optional) a map from directory or zip file paths on the disk
      to URL path prefixes. For each unique prefixes, it creates a file router.
    Note that the same prefix can have only one router; however, a file router
    can be served from multiple directories or zip files, allowing overlay.
    When serving resources, the router with the longest matching prefix is used.
    """
    _route_prefixes = {
        'HEAD': ['get_', 'run_'],
        'GET': ['get_', 'run_'],
        'POST': ['set_', 'post_', 'run_'],
        'PUT': ['put_', 'run_'],
        'PATCH': ['add_', 'patch_', 'run_'],
        'DELETE': ['del_', 'delete_', 'run_'],
    }
    _api_call_types: dict[str,ApiCallType] = {
        'HEAD': 'get', 'GET': 'get', 'POST': 'set', 'PUT': 'put',
        'PATCH': 'add', 'DELETE': 'del',
        'get_': False, 'set_': True, 'put_': True, 'add_': True, 'del_': False,
        'post_': True, 'patch_': True, 'delete_': False,
    }
    _common_headers = {
        'Cache-Control': "no-cache",
        'Connection': "keep-alive",
        'Content-Encoding': "none",
        'Access-Control-Allow-Headers': "X-Requested-With",
        'Access-Control-Max-Age': "1728000",
    }  # TODO: add CORS & cache constrol headers

    def __init__(
            self, routes: Mapping[str,Any]|None=None, assets: str|Mapping[str,str]|None=None,
            *, accept_origins: Sequence[str]|None=None,
    ):
        self.accept_origins = accept_origins if accept_origins else []
        self._registry = {}
        if isinstance(assets, str):
            self._registry[''] = FileRouter(assets)
        elif isinstance(assets, Mapping):
            roots: dict[str,list[str]] = {}
            for k, v in assets.items():
                v = v.rstrip('/')
                if v in roots:
                    roots[v].append(k)
                else:
                    roots[v] = [k]
            for k, v in roots.items():
                self._registry[k] = FileRouter(*v)
        if routes is not None:
            self._registry.update(
                (k.rstrip('/'), (load_data_in_module(v) if isinstance(v, str) else v))
                for k, v in routes.items()
            )
        info("Current routes:")
        for k, v in self._registry.items():
            r = ' | '.join(v.roots()) if isinstance(v, FileRouter) else get_type_name(v)
            info(f"|   {k or '/'} => {r}")
    def create_route(self, method: str, path: str, qstr: str|None) -> ApiRoute|HttpError:
        assert isinstance(path, str)
        (prefix, router) = self.fetch_router(path)
        if prefix is None:
            return HttpError(404, f"Cannot find the path router for {path}")
        suffix = path[len(prefix):] # Should either be empty or starting with '/'
        if not suffix:
            url = path + "/" if qstr is None else path + "/?" + qstr
            return HttpError(307, http_head={'location': url})
        # Find the callee
        if callable(router):
            # If the router itself is callable, just call it without action
            action = None
            callee = router
            actype = False
        else:
            assert suffix[0] == '/'
            prefix += '/'
            suffix = suffix[1:]
            # Find the member match the name
            (action, callee, actype) = self.fetch_member(router, method, suffix)
            if callee is None:
                return HttpError(405, f"[{prefix}]: cannot find {method} '.../{suffix}'")
            if action:
                suffix = suffix[len(action):] # Should still be empty or starting with /
        # Parse the query string
        if isinstance(qstr, str):
            (qsargs, kwargs) = parse_url_query(qstr)
        else:
            qsargs = []
            kwargs = {}
        vpargs = [
            parse_url_value(item) for item in args.split('/')
        ] if (args := suffix.strip('/')) else []
        return ApiRoute(
            method=method, prefix=prefix, action=action, suffix=suffix, qsargs=qsargs,
            router=router, callee=callee, vpargs=vpargs, kwargs=kwargs, actype=actype,
        )
    def fetch_router(self, path: str) -> tuple[str|None,Any]:
        """Fetch the router object in the registry that matches the
        longest prefix of path.
        - Returns a pair of path and the router object. If it does not match,
          return (None, None)
        """
        router = self._registry.get(path)
        if router is not None:
            return (path, router)
        index = path.rfind('/')
        while index >= 0:
            sub_path = path[:index]
            router = self._registry.get(sub_path)
            if router is not None:
                return (sub_path, router)
            index = path.rfind('/', 0, index)
        return (None, None)
    @classmethod
    def fetch_member(
        cls, router, method: str, path: str
    ) -> tuple[str|None,Callable|None,ApiCallType]:
        """Find the end point in the router according to the path.
        - First try using prefixes concatenated with the first path element as names;
        - Then try the prefixes themselves.
        """
        actype = cls._api_call_types[method]
        if path:
            parts = path.split('/', 1)
            for prefix in cls._route_prefixes[method]:
                full_name = prefix + parts[0]
                if hasattr(router, full_name):
                    member = getattr(router, full_name)
                    if callable(member):
                        return (parts[0], member, cls._api_call_types.get(prefix, actype))
        for prefix in cls._route_prefixes[method]:
            if hasattr(router, prefix):
                member = getattr(router, prefix)
                if callable(member):
                    return (None, member, cls._api_call_types.get(prefix, actype))
        return (None, None, False)
    def handle_options(self, path: str) -> HttpMixin:
        if path == '*':
            return HttpMixin(ht_status=203, http_head={
                'access-control-allow-methods': ", ".join(HTTP_SUPPORTED_METHODS) + ", OPTIONS"
            })
        router = self.fetch_router(path)
        if router is None:
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
                    HttpError(400, "ASGi: parsing input", cause=exc))
        if method == 'OPTIONS':
            return (request, self.handle_options(path))
        if method not in HTTP_SUPPORTED_METHODS:
            return (HttpMixin.from_request(None, headers),
                    HttpError(405, f"Bad method {method}: {method} {path}"))
        # Run the routes
        route = self.create_route(method, path, qstr)
        if isinstance(route, HttpError):
            return (request, route)
        return (request, route(request, peer=peer, path=path, qstr=qstr))
    def process_result(self, request: HttpMixin, result: HttpMixin|FridValue) -> HttpMixin:
        """Process the result of the route execution and returns a response.
        - The response is an object of HttpMixin with body already prepared.
        """
        if isinstance(result, HttpMixin):
            response = result
        else:
            assert not isinstance(request.http_data, AsyncIterable)
            response = HttpMixin(http_data=result, ht_status=200)
        self.update_headers(response, request)
        response.set_response()
        return response


class EchoRouter:
    def get_(self, *args, __, **kwds):
        if not kwds:
            return args  # Args can be empty
        if not args:
            return kwds
        return {'.self': "get", '.args': args, '.kwds': kwds}
    def del_(self, *args, __, **kwds):
        return self.run_("del", {}, *args, __=__, **kwds)
    def run_(self, action, data, __, *args, **kwds):
        out = dict(data) if isinstance(data, Mapping) else {'.data': data}
        out['.self'] = action
        if args:
            out['.args'] = args
        if kwds:
            out['.kwds'] = kwds
        return out

def load_command_line_args() -> tuple[dict[str,str],str|dict[str,str]|None,str,int]:
    import logging, faulthandler
    faulthandler.enable()
    logging.basicConfig(level=logging.INFO)

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
    assets = None
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
            assets = item
    return (routes, assets, host, port)
