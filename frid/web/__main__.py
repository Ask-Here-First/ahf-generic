import time, logging, unittest
import urllib.error
from collections.abc import Callable, Mapping
from typing import Any
from logging import info
from pathlib import Path
from urllib.request import urlopen, Request
from multiprocessing import Process

from ..loader import load_frid_str
from ..dumper import dump_frid_str
from ..typing import FridValue, MissingType, MISSING

from .route import echo_router
from .httpd import run_http_server
from .wsgid import run_wsgi_server
from .asgid import run_asgi_server

class TestRouter:
    def get_echo(self, *args, _http={}, **kwds):
        return [list(args), kwds]
    def set_echo(self, data, *args, _http={}, **kwds):
        return {'.data': data, '.args': list(args), '.kwds': kwds}
    def put_echo(self, data, *args, _http={}, **kwds):
        return [data, args, kwds]
    def del_echo(self, *args, _http={}, **kwds):
        return {'status': "ok", **kwds, '.args': list(args)}
    def run_echo0(self, action, data, *args, _http={}, **kwds):
        return {'action': action, '.data': list(data), '.kwds': kwds, '.args': args}

ServerType = Callable[[dict[str,Any],dict[str,str]|str|None,str,int],None]

class TestWebAppHelper(unittest.TestCase):
    TEST_HOST = "127.0.0.1"
    TEST_PORT = 8183
    BASE_URL = f"http://{TEST_HOST}:{TEST_PORT}"

    @classmethod
    def start_server(cls, server: ServerType):
        cls.process = Process(target=server, args=(
            {
                '/echo': echo_router, '/test/': TestRouter(),
            },
            {str(Path(__file__).absolute().parent): ''},
            cls.TEST_HOST, cls.TEST_PORT, {
                'log_level': logging.getLevelName(logging.getLogger().level).lower(), # "warning",
            }
        ))
        info(f"Spawning {cls.__name__} {server.__name__} at {cls.BASE_URL} ...")
        cls.process.start()
        time.sleep(0.5)
        for _ in range(60):
            try:
                cls.load_page('/non-existing-file')
                raise ValueError("Loaded an non-existing file successfully")
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
                break
            except urllib.error.URLError as e:
                if not isinstance(e.reason, ConnectionRefusedError):
                    raise  # Connection refused
            time.sleep(1.0)
        info(f"{cls.__name__} {server.__name__} at {cls.BASE_URL} is ready.")
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
    @classmethod
    def load_page(cls, path: str, data: FridValue|MissingType=MISSING,
                  *, method: str|None=None, raw: bool=False) -> FridValue:
        raw_data = None if data is MISSING else dump_frid_str(data, json_level=1).encode()
        path = cls.BASE_URL + path
        with urlopen(Request(path, raw_data, method=method)) as fp:
            result = fp.read()
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
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test/xxxx")
        self.assertEqual(ctx.exception.code, 405)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test/")
        self.assertEqual(ctx.exception.code, 405)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test")
        self.assertEqual(ctx.exception.geturl(), self.BASE_URL + "/test/")
        self.assertEqual(ctx.exception.code, 405)  # Since urllib handdles redirection

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
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.load_page("/test/xxxx")
        self.assertEqual(ctx.exception.code, 405)

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

class TestWsgiWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_wsgi_server)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_wsgi_server(self):
        return self.run_tests()

class TestAsgiWebApp(TestWebAppHelper):
    @classmethod
    def setUpClass(cls):
        cls.start_server(run_asgi_server)
    @classmethod
    def tearDownClass(cls):
        cls.close_server()
    def test_asgi_server(self):
        return self.run_tests()
