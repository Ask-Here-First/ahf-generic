from collections.abc import AsyncIterable, Callable, Mapping, Sequence
from typing import Any

from .route import ApiRouteManager

StartResponseCallable = Callable[[str,list[tuple[str,str]]],None]

class WsgiWebApp(ApiRouteManager):
    """The main ASGi Web App."""

    def __init__(self, *args, accept_origins: Sequence[str]=[], **kwargs):
        super().__init__(*args, )
        self.accept_origins = accept_origins
    def __call__(self, env: Mapping[str,Any], start_response: StartResponseCallable):
        method = env['REQUEST_METHOD']
        headers = {k[5:].lower(): v for k, v in env.items() if k.startswith('HTTP_')}
        if headers.get('transfer_encoding', '').lower() == 'chunked' or 'CONTENT_LENGTH' in env:
            input_data = env['wsgi.input'].read()
        else:
            input_data = None
        response = self.process_result(*self.handle_request(
            method, input_data, headers,
            peer=env['REMOTE_ADDR'], path=env['PATH_INFO'], qstr=env.get('QUERY_STRING')
        ))
        start_response(str(response.ht_status), list(response.http_head.items()))
        assert not isinstance(response.http_body, AsyncIterable)
        return [] if method == 'HEAD' or response.http_body is None else [response.http_body]

def run_wsgi_server(routes: dict[str,Any], assets: str|dict[str,str]|str|None,
                    host: str, port: int, *, timeout: int=0, **kwargs):
    from gunicorn.app.base import BaseApplication
    from six import iteritems
    class MyApplication(BaseApplication):
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
    MyApplication(WsgiWebApp(routes, assets), {
        'bind': f"{host}:{port}", 'timeout': timeout, **kwargs
    }).run()

if __name__ == '__main__':
    from .route import load_command_line_args
    run_wsgi_server(*load_command_line_args())
