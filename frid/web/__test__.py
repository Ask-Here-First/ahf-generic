import time, random, unittest
import urllib.error
from collections.abc import Callable, Mapping
from typing import Any, Literal
from contextlib import AbstractContextManager
from logging import info
from pathlib import Path
from urllib.request import urlopen, Request
from multiprocessing import Process

from .._loads import load_frid_str
from .._dumps import dump_frid_str
from ..typing import FridValue, MissingType, MISSING, get_func_name

from .route import HttpInput, echo_router
from .httpd import run_http_server
from .wsgid import run_wsgi_server_with_gunicorn, run_wsgi_server_with_simple
from .asgid import WebsocketTextIterator, WebsocketBlobIterator
from .asgid import WebsocketIterator, run_asgi_server_with_uvicorn

class TestRouter:
    def get_echo(self, *args, _http={}, **kwds):
        return [list(args), kwds]
    def set_echo(self, data, *args, _http={}, **kwds):
        return {'.data': data, '.args': list(args), '.kwds': kwds}
    def put_echo(self, data, *args, _http={}, **kwds):
        return [data, list(args), kwds]
    def del_echo(self, *args, _http={}, **kwds):
        return {'status': "ok", **kwds, '.args': list(args)}
    def run_echo(self, optype, data, *args, _http={}, **kwds):
        return {'optype': optype, '.data': data, '.kwds': kwds, '.args': list(args)}
    def get_(self, *args, _http={}, **kwds):
        return [*args, kwds]
    def put_(self, data, *args, _http={}, **kwds):
        return {'optype': 'put', '.data': data, '.kwds': kwds, '.args': list(args)}

class WebsocketRouter:
    def __init__(self, *, dtype: str|None=None, _http: HttpInput):
        self._http = _http
        self.__mime__ = dtype
    async def _echo_call(self, data: WebsocketIterator):
        assert isinstance(data, WebsocketIterator), type(data)
        async for item in data:
            if isinstance(data, WebsocketBlobIterator|WebsocketTextIterator):
                await data(item)
            else:
                await data({'.data': item, '.text': dump_frid_str(item),
                            '.type': "websocket", '.http': self._http})
    async def _echo_yield(self, data: WebsocketIterator):
        assert isinstance(data, WebsocketIterator), type(data)
        async for item in data:
            if isinstance(data, WebsocketBlobIterator|WebsocketTextIterator):
                yield item
            else:
                yield {'.data': item, '.text': dump_frid_str(item),
                       '.type': "websocket", '.http': self._http}
class WebsocketRouter1(WebsocketRouter):
    async def mix_echo(self, data: WebsocketIterator):
        return await self._echo_call(data)
    async def mix_(self, data: WebsocketIterator):
        return self._echo_yield(data)
class WebsocketRouter2(WebsocketRouter):
    async def __call__(self, data: WebsocketIterator):
        async for item in self._echo_yield(data):
            yield item

ServerType = Callable[[dict[str,Any],dict[str,str]|str|None,str,int],None]

class TestWebAppHelper(unittest.TestCase):
    TEST_HOST = "127.0.0.1"
    TEST_PORT = 8183
    BASE_URL = f"http://{TEST_HOST}:{TEST_PORT}"

    @classmethod
    def start_server(cls, server: ServerType):
        cls.server = server
        cls.process = Process(target=server, args=(
            {
                '/echo': echo_router, '/test/': TestRouter(),
                '/wss1/': WebsocketRouter1, '/wss2': WebsocketRouter2,
            },
            {str(Path(__file__).absolute().parent): ''},
            cls.TEST_HOST, cls.TEST_PORT, {'quiet': True},
        ))
        info(f"Spawning {cls.__name__} {get_func_name(server)} at {cls.BASE_URL} ...")
        cls.process.start()
        time.sleep(0.5)
    def await_server(self):
        for _ in range(120):
            try:
                self.load_page('/non-existing-file')
                raise ValueError("Loaded an non-existing file successfully")
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
                break
            except urllib.error.URLError as e:
                if self.process.exitcode:
                    raise unittest.SkipTest(f"Server exited with code {self.process.exitcode}")
                if not isinstance(e.reason, ConnectionRefusedError):
                    raise  # Connection refused
            time.sleep(1.0)
        info(f"{self.__class__.__name__} {self.server.__name__} at {self.BASE_URL} is ready.")
    @classmethod
    def close_server(cls):
        time.sleep(0.5)
        info(f"Terminaing {cls.__name__} server at {cls.BASE_URL} ...")
        # if cls.process.pid is not None:
        #     os.kill(cls.process.pid, signal.SIGINT)
        #     info(f"Sending SIGINT to process {cls.process.pid}")
        #     time.sleep(0.5)
        # for _ in range(10):
        #     if cls.process.exitcode is None:
        #         break
        #     time.sleep(0.5)
        if cls.process.exitcode is None:
            info("Sending SIGTERM to the process")
            cls.process.terminate()
        cls.process.join()
        info(f"The {cls.__name__} server at {cls.BASE_URL} is terminated.")
        time.sleep(0.5)
    def load_page(self, path: str, data: FridValue|MissingType=MISSING,
                  *, method: str|None=None, raw: bool=False) -> FridValue:
        raw_data = None if data is MISSING else dump_frid_str(data, json_level=1).encode()
        path = self.BASE_URL + path
        headers = {'Content-Type': "application/json"}
        with urlopen(Request(path, raw_data, headers, method=method)) as fp:
            result = fp.read()
            self.last_url = fp.url
            return result if raw else load_frid_str(result.decode(), json_level=1)

    def run_test_test(self):
        test = TestRouter()
        self.assertEqual(self.load_page("/test/echo"), test.get_echo())
        self.assertEqual(self.load_page("/test/echo/4"),
                         test.get_echo(4))
        self.assertEqual(self.load_page("/test/echo/a/3?b=4&c=x"),
                         test.get_echo("a", 3, b=4, c="x"))
        self.assertEqual(self.load_page("/test/echo?a=+"),
                         test.get_echo(a=True))
        self.assertEqual(self.load_page("/test/echo/a/3?b=4&c=x"),
                         test.get_echo("a", 3, b=4, c="x"))
        self.assertEqual(self.load_page("/test/echo", {"x": 1, "y": 2}),
                         test.set_echo({"x": 1, "y": 2}))
        self.assertEqual(
            self.load_page("/test/echo/a/3?b=4&c=x", {"x": 1, "y": 2}, method='PUT'),
            test.put_echo({"x": 1, "y": 2}, "a", 3, b=4, c="x")
        )
        self.assertEqual(
            self.load_page("/test/echo/a", method='DELETE'),
            test.del_echo("a")
        )
        self.assertEqual(
            self.load_page("/test/echo?b=4&c=x", {"x": 1, "y": 2}, method='PATCH'),
            test.run_echo('add', {"x": 1, "y": 2}, b=4, c="x")
        )
        self.assertEqual(self.load_page("/test/other/a/3?b=4&c=x"),
                         test.get_("other", "a", 3, b=4, c="x"))
        self.assertEqual(self.load_page("/test/other", {"x": 1, "y": 2}, method='PUT'),
                         test.put_({"x": 1, "y": 2}, "other"))
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test/xxx", method='DELETE')
        self.assertEqual(ctx.exception.code, 405)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test/", method='DELETE')
        self.assertEqual(ctx.exception.code, 405)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test", method='DELETE')
        self.assertEqual(ctx.exception.code, 307)  # Since urllib handdles redirection

    def _remove_env(self, data: FridValue) -> FridValue:
        if not isinstance(data, Mapping):
            return data
        out = dict(data)
        out.pop('.http', None)
        return out

    def run_echo_test(self):
        test = echo_router
        self.assertEqual(self._remove_env(self.load_page("/echo")), self._remove_env(test()))
        self.assertEqual(self._remove_env(self.load_page("/echo/4")), test(4))
        self.assertEqual(self._remove_env(self.load_page("/echo/a/3?b=4&c=x")),
                         self._remove_env(test("a", 3, b=4, c="x")))
        self.assertEqual(self._remove_env(self.load_page("/echo?a=+")),
                         self._remove_env(test(a=True)))
        self.assertEqual(self._remove_env(self.load_page("/echo/a/3?b=4&c=x")),
                         self._remove_env(test("a", 3, b=4, c="x")))
        self.assertEqual(self._remove_env(self.load_page("/echo", {"x": 1, "y": 2})),
                         self._remove_env(test(_data={"x": 1, "y": 2}, _call="set")))
        self.assertEqual(self._remove_env(
            self.load_page("/echo/a/3?b=4&c=x", {"x": 1, "y": 2}, method='PUT')
        ), self._remove_env(test("a", 3, b=4, c="x", _data={"x": 1, "y": 2}, _call="put")))
        self.assertEqual(self._remove_env(
            self.load_page("/echo/a", method='DELETE')
        ), self._remove_env(test("a", _call="del")))
        self.assertEqual(self._remove_env(
            self.load_page("/echo/a/3?b=4&c=x", {"x": 1, "y": 2}, method='PATCH')
        ), self._remove_env(
            test("a", 3, b=4, c="x", _data={"x": 1, "y": 2}, _call="add")
        ))

    def run_file_test(self):
        file = Path(__file__)
        with open(file, 'rb') as fp:
            data = fp.read()
        self.assertEqual(self.load_page('/' + file.name, raw=True), data)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/")
        self.assertEqual(ctx.exception.code, 404)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/non-existing-file")
        self.assertEqual(ctx.exception.code, 404)

    def run_tests(self):
        self.await_server()
        self.run_test_test()
        self.run_echo_test()
        self.run_file_test()

class TestHttpWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_http_server)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_http_server(self):
        return self.run_tests()

class TestWsgiGunicornWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_wsgi_server_with_gunicorn)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_wsgi_server(self):
        return self.run_tests()

class TestWsgiRefWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_wsgi_server_with_simple)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_wsgi_server(self):
        return self.run_tests()

class TestAsgiUvicornWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_asgi_server_with_uvicorn)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_asgi_server(self):
        return self.run_tests()

    # Node: got this with Uvicorn: DeprecationWarning: remove second argument of ws_handler
    # See https://github.com/encode/uvicorn/discussions/2476
    # (2024-09-30, unsolved as of 2024-10-31)
    class AbstractWebsocket(AbstractContextManager):
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
            return False
        def recv(self) -> str|bytes:
            raise NotImplementedError
        def send(self, data: str|bytes) -> None:
            raise NotImplementedError
        def close(self):
            raise NotImplementedError
    def test_with_websockets(self):
        self.assertTrue(callable(WebsocketRouter))
        try:
            from websockets.sync.client import connect
        except ImportError:
            raise unittest.SkipTest(
                "Skip ASGi websocket tests (using websockets) because it is not installed"
            )
        class TestWebsocket(self.AbstractWebsocket):
            def __init__(self, url):
                self.ws = connect(url)
            def recv(self):
                return self.ws.recv()
            def send(self, data: str|bytes):
                self.ws.send(data)
            def close(self):
                self.ws.close()
        self.run_websocket_test(TestWebsocket)
    def test_with_websocket_client(self):
        # TODO: don't know how to gracefully shutdown initiated by client
        # When client send out all data and send a close, the server should
        # be able to send out all responses before handling the close.
        self.assertTrue(callable(WebsocketRouter))
        try:
            from websocket import WebSocket
        except ImportError:
            raise unittest.SkipTest(
                "Skip ASGi websocket tests (using websocket-client) because it is not installed"
            )
        class TestWebsocket(self.AbstractWebsocket):
            def __init__(self, url: str):
                self.ws = WebSocket()
                self.ws.connect(url)
            def recv(self):
                return self.ws.recv()
            def send(self, data: str|bytes):
                self.ws.send(data)
            def close(self):
                self.ws.close()
        self.run_websocket_test(TestWebsocket)
    def run_websocket_test(self, connect: Callable[[str],AbstractContextManager[AbstractWebsocket]]):
        self.await_server()
        data = [
            None, True, False, 3, 100, 0.5, "Hello, world", {'x': 3}, [3, "abc"],
            {'x': 3, 'y': "b", 'z': [False, True]},
        ]
        for path in ("/wss1/echo", "/wss1/", "/wss2"):
            base_url = f"ws://{self.TEST_HOST}:{self.TEST_PORT}" + path
            test_cases: list[tuple[str|None,Literal[0,1,5]]] = [
                (None, 0), ('text', 0), ('blob', 0),
                ('frid', 0), ('json', 1), ('json5', 5),
            ]
            for data_type, json_level in test_cases:
                url = (base_url if data_type is None else base_url + "?dtype=" + str(data_type))
                with connect(url)  as ws:
                    buffer: str = ""
                    for item in data:
                        u = dump_frid_str(item, json_level=json_level)
                        if data_type == 'blob':
                            u = ws.send(u.encode() + b'\n')
                        else:
                            ws.send(u + '\n')
                        if random.random() < 0.5:
                            v = ws.recv()
                            buffer += (v.decode() if isinstance(v, bytes) else str(v))
                    while buffer.count('\n') < len(data):
                        try:
                            v = ws.recv()
                        except IOError:
                            break
                        buffer += (v.decode() if isinstance(v, bytes) else str(v))
                lines = buffer.splitlines()
                self.assertEqual(len(lines), len(data))
                for line, x in zip(lines, data):
                    v = load_frid_str(line, json_level=json_level)
                    if data_type in ('blob', 'text'):
                        self.assertEqual(v, x)
                        continue
                    self.assertIsInstance(v, Mapping)
                    assert isinstance(v, Mapping)  # For tpying
                    self.assertEqual(v.get('.data'), x)
                    self.assertEqual(v.get('.type'), "websocket")
                    text = v.get('.text')
                    self.assertIsInstance(text, str, v)
                    assert isinstance(text, str)
                    self.assertEqual(load_frid_str(text), x)
