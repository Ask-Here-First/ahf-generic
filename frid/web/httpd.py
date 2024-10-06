import sys
from collections.abc import AsyncIterable, Mapping
from http.server import BaseHTTPRequestHandler
from typing import Any
from logging import info

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
    def do_DELETE(self):
        self.do_request('DELETE')
    def do_PATCH(self):
        self.do_request('PATCH')
    def do_HEAD(self):
        self.do_request('HEAD', with_body=False)
    def do_OPTIONS(self):
        self.do_request('OPTIONS', with_body=False)

def run_http_server(routes: dict[str,Any], assets: str|dict[str,str]|str|None,
                    host: str, port: int, options: Mapping[str,Any]={}, **kwargs):
    if kwargs:
        options = {**options, **kwargs}
    manager = ApiRouteManager(routes, assets)
    class TestHTTPRequestHandler(FridHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, manager=manager, **kwargs)
        def log_message(self, format, *args):
            info(f"{self.address_string()} - {format % args}")
    from http.server import HTTPServer
    class TestHTTPServer(HTTPServer):
        def handle_error(self, request, client_address):
            info(f"HTTP request handler encountered {sys.exc_info()[1]} from {client_address}")
    import signal
    def sigterm_handler(signum, frame):
        sys.exit(1)
    signal.signal(signal.SIGTERM, sigterm_handler)
    with TestHTTPServer((host, port), TestHTTPRequestHandler) as httpd:
        info(f"[HTTP server] Starting service at {host}:{port} ...")
        try:
            httpd.serve_forever()
        finally:
            info(f"[HTTP server] Completed service at {host}:{port}.")

if __name__ == '__main__':
    from .route import load_command_line_args
    run_http_server(*load_command_line_args())
