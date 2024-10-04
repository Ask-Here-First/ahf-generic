from collections.abc import AsyncIterable
from http.server import BaseHTTPRequestHandler
from typing import Any

from .route import ApiRouteManager

class FridHTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, manager: ApiRouteManager, **kwargs):
        self._manager = manager
        super().__init__(*args, **kwargs)
        self.protocol_version = "HTTP/1.1"
    def do_request(self, method: str, with_body: bool=True):
        # Processing URL parameters and
        (path, qstr) = self.path.split('?', 1) if '?' in self.path else (self.path, None)
        # Read the input data
        if self.headers.get('Transfer-Encoding') == 'chunked':
            raise NotImplementedError("Chunked request cannot be handled")
        if 'Content-Length' in self.headers:
            input_data = self.rfile.read(int(self.headers['Content-Length']))
        else:
            input_data = None
        # Handle the request
        response = self._manager.process_result(*self._manager.handle_request(
            method, input_data, self.headers, path=path, qstr=qstr, peer=self.client_address
        ))
        # Send the response
        self.send_response(response.ht_status)
        for k, v in response.http_head.items():
            self.send_header(k, v)
        self.end_headers()
        assert not isinstance(response.http_body, AsyncIterable)
        if response.http_body is not None and with_body:
            self.wfile.write(response.http_body)
    def do_GET(self):
        self.do_request('GET')
    def do_POST(self):
        self.do_request('POST')
    def do_PUT(self):
        self.do_request('PUT')
    def do_PATCH(self):
        self.do_request('PATCH')
    def do_HEAD(self):
        self.do_request('HEAD', with_body=False)
    def do_OPTIONS(self):
        self.do_request('OPTIONS', with_body=False)

def run_http_server(routes: dict[str,Any], assets: str|dict[str,str]|str|None,
                    host: str, port: int):
    manager = ApiRouteManager(routes, assets)
    class TestHTTPRequestHandler(FridHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, manager=manager, **kwargs)
    from http.server import HTTPServer
    with HTTPServer((host, port), TestHTTPRequestHandler) as httpd:
        print(f"Starting HTTP server at {host}:{port} ...")
        httpd.serve_forever()

if __name__ == '__main__':
    from .route import load_command_line_args
    run_http_server(*load_command_line_args())
