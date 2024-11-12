import os, abc, sys, time, builtins, asyncio, inspect, traceback
from logging import info, error
from collections.abc import (
    AsyncIterable, AsyncIterator, Iterable, Mapping, Callable, Awaitable, Sequence
)
from typing import Any, Literal, TypeVar, TypedDict

from ..typing import NotRequired   # Python 3.11 only feature
from ..typing import MISSING, MissingType, get_type_name, FridValue
from ..lib import str_encode_nonprints
from .._loads import scan_frid_str, FridTruncError, load_frid_str
from .._dumps import dump_frid_str
from .mixin import HttpError, HttpMixin, mime_type_label
from .route import HttpInput, HttpMethod, MethodKind, ApiRouteManager
from .route import HTTP_METHODS_WITH_BODY

WEBSOCKET_QUASI_METHOD = ":websocket:"  # Quasi method for websocket

class AsgiScopeDict(TypedDict):
    type: Literal['http','websocket','lifespan']
    method: HttpMethod
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


AsgiLifeEventType = Literal['lifespan.startup','lifespan.startup.complete',
                            'lifespan.shutdown','lifespan.shutdown.complete']
AsgiHttpEventType = Literal['http.request','http.response.start','http.response.body',
                            'http.disconnect']
AsgiSockEventType = Literal['websocket.receive','websocket.send','websocket.connect',
                            'websocket.accept', 'websocket.disconnect','websocket.close']

class AsgiEventDict(TypedDict):
    type: AsgiLifeEventType|AsgiHttpEventType|AsgiSockEventType
    code: NotRequired[int]
    reason: NotRequired[str]
    status: NotRequired[int]
    headers: NotRequired[Iterable[tuple[builtins.bytes,builtins.bytes]]]
    text: NotRequired[str]
    bytes: NotRequired[bytes]
    body: NotRequired[builtins.bytes]
    more_body: NotRequired[bool]

AsgiRecvCall = Callable[[],Awaitable[AsgiEventDict]]
AsgiSendCall = Callable[[AsgiEventDict],Awaitable[None]]

_T = TypeVar('_T')

class WebsocketIterator(AsyncIterator[_T]):
    def __init__(self, recv: AsgiRecvCall, send: AsgiSendCall, binary: bool):
        self._recv = recv
        self._send = send
        self._binary = binary
        self._traced = load_frid_str(os.getenv('FRID_TRACE_WEBSOCKET', '-'))
        self.last_msg_type = "websocket.receive"
    def __aiter__(self):
        return self
    async def __anext__(self) -> _T:
        while True:
            msg: AsgiEventDict = await self._recv()
            self.last_msg_type = msg.get('type')
            if self.last_msg_type != "websocket.receive":
                raise StopAsyncIteration
            if self._traced:
                self.print_msg(msg, '>')
            if self._binary:
                data = (msg['bytes'] if 'bytes' in msg else msg.get('text'))
            else:
                data = (msg['text'] if 'text' in msg else msg.get('bytes'))
            if data is None:
                error("Websocket: empty packet received")
                continue
            value = self._decode(data)
            if value is not MISSING:
                return value
    async def __call__(self, data: _T):
        encoded = self._encode(data)
        msg: AsgiEventDict = {'type': "websocket.send"}
        if isinstance(encoded, str):
            msg['text'] = encoded
        elif isinstance(encoded, bytes):
            msg['bytes'] = encoded
        else:
            raise HttpError(500, f"Bad return: {self.__class__}._encode() -> {type(encoded)}")
        if self._traced:
            self.print_msg(msg, '<')
        return await self._send(msg)
    @staticmethod
    def print_msg(msg: AsgiEventDict, char: str):
        if (text := msg.get('text')) is not None:
            print(f"{char}T{char}: " + text.rstrip('\n'))
        if (blob := msg.get('bytes')) is not None:
            print(f"{char}B{char}: " + blob.rstrip(b'\n').decode())
    @abc.abstractmethod
    def _encode(self, data: _T) -> str|bytes:
        raise NotImplementedError
    @abc.abstractmethod
    def _decode(self, data: str|bytes) -> _T|MissingType:
        raise NotImplementedError

class WebsocketTextIterator(WebsocketIterator[str]):
    def __init__(self, recv: AsgiRecvCall, send: AsgiSendCall):
        super().__init__(recv, send, False)
    def _encode(self, data: str) -> str:
        if isinstance(data, str):
            return data
        return str(data)
    def _decode(self, data: str|bytes) -> str|MissingType:
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            return data.decode()
        return str(data)

class WebsocketBlobIterator(WebsocketIterator[bytes]):
    def __init__(self, recv: AsgiRecvCall, send: AsgiSendCall):
        super().__init__(recv, send, True)
    def _encode(self, data: bytes) -> bytes:
        if isinstance(data, bytes):
            return data
        return bytes(data)
    def _decode(self, data: str|bytes) -> bytes|MissingType:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode()
        return bytes(data)

class WebsocketFridIterator(WebsocketIterator[FridValue]):
    def __init__(self, recv: AsgiRecvCall, send: AsgiSendCall, *, json_level: FridValue=0):
        super().__init__(recv, send, False)
        self.buffer: str = ""
        self.json_level: Literal[0,1,5] = (5 if json_level == 5 else 1 if json_level else 0)
    def _encode(self, data: FridValue) -> str:
        return dump_frid_str(data, json_level=self.json_level) + "\n"   # Ends with new line
    def _decode(self, data: str|bytes) -> FridValue|MissingType:
        if isinstance(data, bytes):
            data = data.decode()
        if not isinstance(data, str):
            data = str(data)
        if self.buffer:
            data = self.buffer + data
            self.buffer = ""
        try:
            (value, index) = scan_frid_str(data, 0, json_level=self.json_level)
        except FridTruncError:
            self.buffer = data
            return MISSING
        self.buffer = data[index:]
        return value

class AsgiWebApp(ApiRouteManager):
    """The main ASGi Web App."""
    _route_prefixes: Mapping[MethodKind,Sequence[str]] = {
        WEBSOCKET_QUASI_METHOD: ['mix_', 'wss_', 'ws_', 'websocket_', 'websock_', 'wsock_'],
        **ApiRouteManager._route_prefixes,
    }
    _num_fixed_args: Mapping[str,Literal[0,1,2]] = {
        'mix_': 1, 'wss_': 1, 'websocket_': 1, 'websock_': 1, 'wsock_': 1, 'ws_': 1,
        **ApiRouteManager._num_fixed_args,
    }
    _rprefix_revmap: Mapping[str,Sequence[MethodKind]] = {
        'mix_': [WEBSOCKET_QUASI_METHOD], 'websocket_': [WEBSOCKET_QUASI_METHOD],
        'websock_': [WEBSOCKET_QUASI_METHOD], 'wsock_': [WEBSOCKET_QUASI_METHOD],
        'ws_': [WEBSOCKET_QUASI_METHOD],
        **ApiRouteManager._rprefix_revmap,
    }
    _ht_ws_status_map: Mapping[int,int] = {
        200: 1000,
        400: 1002, 401: 3001, 403: 3003, 404: 1002, 406: 1003, 408: 3008,
        413: 1009, 414: 1009, 415: 1003, 429: 1008,
        500: 1011, 501: 1003, 502: 1014, 503: 1001, 504: 1013,
    }

    def __init__(self, *args, http_ping_time: float=3.0, use_websockets: bool=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_ping_time = http_ping_time
        self.use_websockets = use_websockets

    async def __call__(self, scope: AsgiScopeDict, recv: AsgiRecvCall,
                       send: AsgiSendCall) -> None:
        """The main ASGi handler"""
        match scope['type']:
            case 'http':
                return await self.handle_http(scope, recv, send)
            case 'websocket':
                return await self.handle_sock(scope, recv, send)
            case 'lifespan':
                return await self.handle_life(scope, recv, send)
        error(f"ASGi service: unsupported protocol type {scope['type']}")

    async def handle_http(self, scope: AsgiScopeDict, recv: AsgiRecvCall,
                          send: AsgiSendCall) -> None:
        # Get method and headers and get authrization
        method = scope['method']
        req_data = await self.get_request_data(scope, recv)
        # Note: ASGi cannot distinguish between empty query string (with ?) and
        # missing query string (without ?). Assuming missing always.
        (request, result) = self.handle_request(
            method, req_data, scope['headers'], client=scope['client'],
            path=scope['path'], qstr=(scope['query_string'].decode() or None),
        )
        if inspect.isawaitable(result):
            result = await self.wait_for_result(result, f"{method} {scope['path']}")
        response = self.process_result(request, result)
        await send({
            'type': 'http.response.start',
            'status': response.ht_status,
            'headers': [(k.encode('utf-8'), v.encode('utf-8'))
                        for k, v in response.http_head.items()],
        })
        if method == 'HEAD' or response.http_body is None:
            return await send({
                'type': 'http.response.body',
                'body': b'',
            })
        if not isinstance(response.http_body, AsyncIterable):
            return await send({
                'type': 'http.response.body',
                'body': response.http_body,
            })
        return await self.exec_async_send(response.http_body, send, recv)
    async def wait_for_result(self, result: Awaitable[_T], msg_str: str) -> _T|HttpError:
        try:
            return await result
        except asyncio.TimeoutError as exc:
            traceback.print_exc()
            # msg =  route.get_log_str(request, client=scope['client'])
            return HttpError(504, "Timeout: " + msg_str, cause=exc)
        except HttpError as exc:
            return exc
        except Exception as exc:
            traceback.print_exc()
            # result = route.to_http_error(exc, request, client=scope['client'])
            return HttpError(500, "Crashed: " + msg_str, cause=exc)

    async def handle_sock(self, scope: AsgiScopeDict, recv: AsgiRecvCall,
                          send: AsgiSendCall) -> None:
        if not self.use_websockets:
            error("ASGi Websocket is not enabled for the App")
            return
        request = HttpMixin.from_request(None, scope.get('headers'))
        path = scope['path']
        qstr = scope.get('query_string')
        if qstr is not None:
            qstr = qstr.decode()
        route = self.create_route(WEBSOCKET_QUASI_METHOD, path, qstr, request)
        # Check if the route cannot be created
        if isinstance(route, HttpError):
            # This is before accepting
            return await send({  # Or use websocket 3003? But uvicorn coverted it to 403
                'type': "websocket.close", 'code': route.ht_status, 'reason': str(route),
                'headers': [(k.encode('utf-8'), v.encode('utf-8'))
                            for k, v in route.http_head.items()]
            })
        route_args = self.get_route_args(path, qstr, scope.get('client'))
        http_input = route.to_http_input(request, **route_args)
        await send({'type': "websocket.accept"})   # TODO: headers?
        # Get the expected data type
        data = self.create_ws_data(http_input, route, recv, send)
        # Wait for connect message
        msg = await recv()
        if msg.get('type') != 'websocket.connect':
            error(f"ASGi Websocket: got a message {msg.get('type')} when expecting connect")
            return
        # Calling the parser
        request.http_data = data   # Set to use the WebsocketIterator as the data
        result = route(request, **route_args)
        if inspect.isawaitable(result):
            result = await self.wait_for_result(result, f"Websocket {path}")
        if isinstance(result, HttpError):
            # This is after accepting
            return await send({
                'type': "websocket.close",
                'code': self._ht_ws_status_map.get(result.ht_status, 1008),
                'reason': str(HttpError),
            })
        if isinstance(result, AsyncIterable):
            # This is different from self.process_result() which generates SSE output
            async for item in result:
                await data(item)
        elif result is not None:
            await data(result)
        if data.last_msg_type == "websocket.receive":
            # The route function ends, end close signal
            await send({'type': "websocket.close"})
        elif data.last_msg_type != "websocket.disconnect":
            error(f"ASGi websocket: recevied a message {data.last_msg_type} "
                  "when expecting disconnect")
    def create_ws_data(self, http_input: HttpInput, route,
                       recv: AsgiRecvCall, send: AsgiSendCall) -> WebsocketIterator:
        mime_label = None
        router = route.router
        if hasattr(router, '__mime__'):
            mime_label = router.__mime__
        # Guess from the MIME type the request wants
        if mime_label is None and (want := http_input.get('want')):
            mime_label = next((ml for x in want if (ml := mime_type_label.get(x))), None)
        match mime_label:
            case 'text':
                data = WebsocketTextIterator(recv, send)
            case 'blob':
                data = WebsocketBlobIterator(recv, send)
            case 'frid':
                data = WebsocketFridIterator(recv, send)
            case 'json':
                data = WebsocketFridIterator(recv, send, json_level=1)
            case 'json5':
                data = WebsocketFridIterator(recv, send, json_level=5)
            case _:
                data = WebsocketFridIterator(recv, send)
        return data

    async def handle_life(self, scope: AsgiScopeDict, recv: AsgiRecvCall,
                          send: AsgiSendCall) -> None:
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
    async def get_request_data(self, scope: AsgiScopeDict, recv: Callable) -> bytes|None:
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
            return None
        body = []
        more_body = True
        while more_body:
            message = await recv()
            frag = message.get('body')
            if frag:
                body.append(frag)
            more_body = message.get('more_body', False)
        return b''.join(body)
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
            msg = "event: error\ndata: " + str_encode_nonprints(str(exc)) + "\n\n"
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
        (_, pending) = await asyncio.wait((
            asyncio.create_task(self.send_async_data(state, body, send)),
            asyncio.create_task(self.recv_http_close(recv)),
        ), return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        if ping_task is not None:
            ping_task.cancel()

def run_asgi_server_with_uvicorn(
        routes: dict[str,Any], assets: str|dict[str,str]|list[str]|None,
        host: str, port: int, options: Mapping[str,Any]={}, **kwargs
):
    options = {**options, **kwargs}
    quiet = options.pop('quiet', False)

    from ..lib import use_signal_trap, get_loglevel_str
    try:
        import uvicorn
    except ImportError as e:
        if quiet:
            info(f"Failed to import uvicorn: {e}")
            sys.exit(1)
        raise

    server = uvicorn.Server(uvicorn.Config(
        AsgiWebApp(routes, assets), host=host, port=port,
        # Uvicorn has a "trace" level
        log_level=get_loglevel_str(),
        **options,
    ))

    def handler():
        server.should_exit = True
    use_signal_trap(handler=handler)

    info(f"[ASGi server] Starting service at {host}:{port} ...")
    try:
        server.run()
    finally:
        info(f"[ASGi server] Completed service at {host}:{port}.")

run_asgi_server = run_asgi_server_with_uvicorn

if __name__ == '__main__':
    from .route import load_command_line_args
    run_asgi_server(*load_command_line_args())
