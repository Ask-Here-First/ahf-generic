from collections.abc import AsyncIterable
from http.server import BaseHTTPRequestHandler

from ..typing import FridValue
from .mixin import HttpMixin, HttpError
from .route import ApiRouteManager

class FridHTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, manager: ApiRouteManager, **kwargs):
        self._manager = manager
        super().__init__(*args, **kwargs)
        self.protocol_version = "HTTP/1.1"
    def handle_request(self, method: str, with_body: bool=True):
        request = self.get_http_input()
        if '?' in self.path:
            (path, qstr) = self.path.split('?', 1)
        else:
            path = self.path
            qstr = None
        route = self._manager.create_route(method, path, qstr)
        if isinstance(route, HttpError):
            return self.send_http_data(route, request, with_body=with_body)
        assert not isinstance(request.http_data, AsyncIterable)
        return self.send_http_data(route(
            request, peer=self.client_address, path=path, qstr=qstr,
        ), request, with_body=with_body)
    def get_http_input(self):
        data_len = int(self.headers.get('Content-Length', 0))
        data = self.rfile.read(data_len) if data_len > 0 else None
        return HttpMixin.from_request(data, headers=self.headers)
    def send_http_data(self, data: HttpMixin|FridValue, req: HttpMixin, with_body: bool=True):
        if not isinstance(data, HttpMixin):
            data = HttpMixin(http_data=data, ht_status=200)
        self._manager.update_headers(data, req, host=self.headers.get('Host'))
        # if isinstance(data, HttpError):
        #     data.set_response()
        #     if with_body:
        #         self.send_error(data.ht_status, explain=dump_frid_str(data, indent=4))
        #     else:
        #         self.send_response(data.ht_status)
        #     return
        data.set_response()
        self.send_response(data.ht_status)
        for k, v in data.http_head.items():
            self.send_header(k, v)
        self.end_headers()
        assert not isinstance(data.http_body, AsyncIterable)
        if data.http_body is not None and with_body:
            self.wfile.write(data.http_body)
    def do_GET(self):
        self.handle_request('GET')
    def do_POST(self):
        self.handle_request('POST')
    def do_PUT(self):
        self.handle_request('PUT')
    def do_PATCH(self):
        self.handle_request('PATCH')
    def do_HEAD(self):
        self.handle_request('HEAD', with_body=False)
    def do_OPTIONS(self):
        self.send_http_data(self._manager.handle_options(self.path), self.get_http_input())

if __name__ == '__main__':
    from .route import load_command_line_args
    (routes, assets, host, port) = load_command_line_args()
    manager = ApiRouteManager(routes, assets)
    class TestHTTPRequestHandler(FridHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, manager=manager, **kwargs)
    from http.server import HTTPServer
    with HTTPServer((host, port), TestHTTPRequestHandler) as httpd:
        print(f"Starting HTTP server at {host}:{port} ...")
        httpd.serve_forever()