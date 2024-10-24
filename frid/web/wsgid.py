import sys
import http.client
from collections.abc import AsyncIterable, Callable, Mapping, Sequence
from typing import Any
from logging import info

from .route import ApiRouteManager

class WsgiWebApp(ApiRouteManager):
    """The main ASGi Web App."""

    def __init__(self, *args, accept_origins: Sequence[str]=[], **kwargs):
        super().__init__(*args, **kwargs)
        self.accept_origins = accept_origins
    def __call__(self, env: Mapping[str,Any], start_response: Callable):
        method = env['REQUEST_METHOD']
        headers = {k[5:].lower(): v for k, v in env.items() if k.startswith('HTTP_')}
        if headers.get('transfer_encoding', '').lower() == 'chunked':
            input_data = env['wsgi.input'].read()
        elif (content_length := env.get('CONTENT_LENGTH')):
            # wsgiref.simple_server does not support read() without a positive number
            nb = int(content_length)
            if nb > 0:
                input_data = env['wsgi.input'].read(nb)
            else:
                input_data = b''
        else:
            input_data = None
        response = self.process_result(*self.handle_request(
            method, input_data, headers,
            client=env['REMOTE_ADDR'], path=env['PATH_INFO'], qstr=env.get('QUERY_STRING')
        ))
        reason = http.client.responses.get(response.ht_status, "Unknown Status Code")
        start_response(str(response.ht_status) + " " + reason,
                       list(response.http_head.items()))
        assert not isinstance(response.http_body, AsyncIterable)
        return [] if method == 'HEAD' or response.http_body is None else [response.http_body]

def run_wsgi_server_with_gunicorn(
        routes: dict[str,Any], assets: str|dict[str,str]|list[str]|None,
        host: str, port: int, options: Mapping[str,Any]={},
        *, timeout: int=0, **kwargs
):
    options = {**options, **kwargs}
    quiet = options.pop('quiet', False)

    from ..lib import get_loglevel_str
    # Note gunicorn handles signals as we need so we don't need to do anything
    try:
        from gunicorn.app.base import BaseApplication
    except ImportError as e:
        if quiet:
            info(f"Failed to import gunicorn: {e}")
            sys.exit(1)
        raise

    from six import iteritems
    class ServerApplication(BaseApplication):
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super().__init__()
        def load_config(self):
            assert self.cfg is not None
            config = {key: value for key, value in iteritems(self.options)
                     if key in self.cfg.settings and value is not None}
            for key, value in iteritems(config):
                self.cfg.set(key.lower(), value)
        def load(self):
            return self.application
    server  = ServerApplication(WsgiWebApp(routes, assets), {
        'bind': f"{host}:{port}", 'timeout': timeout,
        # Gunicorn does not have trace level, so use debug instead
        'loglevel': level if (level := get_loglevel_str()) != 'trace' else 'debug',
        **options
    })
    info(f"[WSGi gunicorn server] Starting service at {host}:{port} ...")
    try:
        server.run()
    finally:
        info(f"[WSGi gunicorn server] Completed service at {host}:{port}.")

def run_wsgi_server_with_simple(
        routes: Mapping[str,Any], assets: str|dict[str,str]|list[str]|None,
        host: str, port: int, options: Mapping[str,Any]={}, **kwargs
):
    # options = {**options, **kwargs}

    from ..lib import use_signal_trap
    use_signal_trap()

    # wsgiref.simple_server does not support Connection: ...
    app = WsgiWebApp(routes, assets, set_connection=None)
    from .httpd import NoPrintHttpRequestHandler  # simple_server actually uses http.server
    from wsgiref.simple_server import make_server, WSGIRequestHandler, WSGIServer
    class TestWsgiHandler(WSGIRequestHandler, NoPrintHttpRequestHandler):
        pass
    class TestWsgiServer(WSGIServer, NoPrintHttpRequestHandler):
        pass

    server = make_server(host, port, app,
                         handler_class=TestWsgiHandler, server_class=TestWsgiServer)
    info(f"[WSGi simple server] Starting service at {host}:{port} ...")
    try:
        server.serve_forever()
    finally:
        server.server_close()
        info(f"[WSGi simple server] Completed service at {host}:{port}.")

run_wsgi_server = run_wsgi_server_with_gunicorn

if __name__ == '__main__':
    from .route import load_command_line_args
    run_wsgi_server(*load_command_line_args())
