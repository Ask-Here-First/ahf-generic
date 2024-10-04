import sys, time, asyncio, inspect, traceback
from logging import info, error
from collections.abc import AsyncIterable, Iterable, Mapping, Callable, Sequence
from typing import Literal, TypedDict
if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired  # noqa: F401

from ..helper import get_type_name
from ..strops import escape_control_chars
from ..typing import FridValue
from .mixin import HttpError, HttpMixin
from .route import ApiRouteManager, HTTP_METHODS_WITH_BODY, HTTP_SUPPORTED_METHODS

class AsgiScopeType(TypedDict):
    type: Literal['http','websocket']
    method: Literal['HEAD','GET','PUT','POST','DELETE','PATCH','OPTIONS','CONNECT','TRACE']
    asgi: Mapping[str,str]
    http_version: str
    scheme: NotRequired[str]
    path: str
    raw_path: bytes
    query_string: bytes
    root_path: str
    headers: Iterable[tuple[bytes,bytes]]
    client: tuple[str,int]
    server: tuple[str,int|None]


class AsgiWebApp(ApiRouteManager):
    """The main ASGi Web App."""

    def __init__(self, *args, accept_origins: Sequence[str]=[],
                 http_ping_time: float=3.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.accept_origins = accept_origins
        self.http_ping_time = http_ping_time

    async def __call__(self, scope: AsgiScopeType, recv: Callable, send: Callable):
        """The main ASGi handler"""
        if scope['type'] != 'http':
            return await self.handle_lifespan(scope, recv, send)
        # Get method and headers and get authrization
        method = scope['method']
        path = scope['path']
        if method not in HTTP_SUPPORTED_METHODS:
            result = HttpError(405, f"Bad method {method}: {method} {path}")
        elif method == 'OPTIONS':
            result = self.handle_options(path)
        else:
            req = await self.get_request_data(scope, recv)
            if isinstance(req, HttpError):
                result = req
            else:
                qstr = scope['query_string'].decode()
                # Note: ASGi cannot distinguish empty query string with and without ?
                # Hence we assume '?' does not exist if query string is empty
                route = self.create_route(method, path, qstr or None)
                if isinstance(route, HttpError):
                    result = route
                else:
                    peer = scope['client']
                    result = route(req, peer=peer, path=path, qstr=qstr, asgi=scope)
                    if inspect.isawaitable(result):
                        try:
                            result = await result
                        except asyncio.TimeoutError as exc:
                            traceback.print_exc()
                            msg =  route.get_log_str(req, peer)
                            result = HttpError(503, "Timeout: " + msg, cause=exc)
                        except Exception as exc:
                            traceback.print_exc()
                            result = route.to_http_error(exc, req, peer)
                    if not isinstance(result, HttpMixin):
                        result = HttpMixin(http_data=result)
        self.update_headers(result, req, host=scope['server'],
                            accept_origins=self.accept_origins)
        result.set_response()
        await send({
            'type': 'http.response.start',
            'status': result.ht_status,
            'headers': [(k.encode('utf-8'), v.encode('utf-8'))
                        for k, v in result.http_head.items()],
        })
        if method == 'HEAD' or result.http_body is None:
            return await send({
                'type': 'http.response.body',
                'body': b'',
            })
        if not isinstance(result.http_body, AsyncIterable):
            return await send({
                'type': 'http.response.body',
                'body': result.http_body,
            })
        return await self.exec_async_send(result.http_body, send, recv)

    async def handle_lifespan(self, scope: AsgiScopeType, recv: Callable, send: Callable):
        while True:
            message = await recv()
            if message['type'] == 'lifespan.startup':
                info("WebApp: starting ASGi server")
                for handler in self._registry.values():
                    if hasattr(handler, 'on_starting'):
                        try:
                            await handler.on_starting()
                        except Exception:
                            error(f"Failed to run {get_type_name(handler)}.on_starting()",
                                  exc_info=True)
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                for handler in reversed(self._registry.values()):
                    if hasattr(handler, 'on_stopping'):
                        try:
                            await handler.on_stopping()
                        except Exception:
                            error(f"Failed to run {get_type_name(handler)}.on_stopping()",
                                  exc_info=True)
                await send({'type': 'lifespan.shutdown.complete'})
                break
        info("WebApp: stopping ASGi server")
    async def get_request_data(self, scope: AsgiScopeType, recv: Callable) -> HttpMixin:
        """Read the body and returns the data. Accepted types:
        - `text/plain': returns decoded string.
        - 'application/x-binary', 'application/octet-stream': return as bytes.
        - 'application/json': Json compatible object (dict, list, bool, Number, None)
        - 'application/x-www-form-urlencoded': form data to a dict only containing
          last values of the same key.
        Returns triplet (data, type, body) where
            + The data is parsed data in Frid-compatible data
            + The type is one of 'json', 'text', 'blob', 'form'.
            + The body is the raw binary data in the body
        """
        if scope['method'] not in HTTP_METHODS_WITH_BODY:
            return HttpMixin.from_request(None, scope['headers'])
        body = []
        more_body = True
        while more_body:
            message = await recv()
            frag = message.get('body')
            if frag:
                body.append(frag)
            more_body = message.get('more_body', False)
        data = b''.join(body)
        try:
            return HttpMixin.from_request(data, scope['headers'])
        except Exception as exc:
            return HttpError(400, "ASGi: parsing input", cause=exc)
    async def send_async_ping(self, state: list[float], delay: float, send: Callable):
        while True:
            current = time.time()
            timeout = state[0] + delay
            if current < timeout:
                await asyncio.sleep(timeout - current)
                continue
            try:
                await send({
                    'type': 'http.response.body',
                    'body': b"event: nudge\n\n",
                    'more_body': True,
                })
            except asyncio.CancelledError:
                pass
            except Exception:
                error("WebApp: ASGi send() got an exception when sending nudge", exc_info=True)
                # TODO: what to do here
            await asyncio.sleep(delay)
    async def send_async_data(self, state: list[float], body: AsyncIterable[FridValue],
                              send: Callable):
        try:
            async for item in body:
                await send({
                    'type': 'http.response.body',
                    'body': item,
                    'more_body': True,
                })
                state[0] = time.time()
        except Exception as exc:
            error("Async iterable gets an exception", exc_info=True)
            msg = "event: error\ndata: " + escape_control_chars(str(exc)) + "\n\n"
            await send({
                'type': 'http.response.body',
                'body': msg.encode(),
            })
            return
        finally:
            state[0] = time.time() + 3600.0  # block ping
        # Ending the end
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': False,
        })
    async def recv_http_close(self, recv):
        while True:
            msg = await recv()
            if msg.get('type') == 'http.disconnect':
                info("WebApp: ASGi recv() got a disconnect message")
                break
    async def exec_async_send(self, body: AsyncIterable[FridValue],
                              send: Callable, recv: Callable):
        # To handle disconnection, see https://github.com/tiangolo/fastapi/discussions/11360
        state = [time.time()]
        ping_task = asyncio.create_task(self.send_async_ping(
            state, self.http_ping_time, send
        ))
        (done, pending) = await asyncio.wait((
            asyncio.create_task(self.send_async_data(state, body, send)),
            asyncio.create_task(self.recv_http_close(recv)),
        ), return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        if ping_task is not None:
            ping_task.cancel()

if __name__ == '__main__':
    from .route import load_command_line_args
    (routes, assets, host, port) = load_command_line_args()
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(
        AsgiWebApp(routes, assets), log_level="info", host=host, port=port
    ))
    server.run()