import re
import ssl
import json
import contextlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, AsyncIterable, Optional, Union, Iterable, cast

from aiohttp import web, BasicAuth, ClientSession

from .plugins_api import StartStopFunc
from .common import USER_NAME, encrypt_key, ProcessRequest, AsyncTransportClient


PORT = 55667
DEFAULT_HTTP_CHUNK = 1 << 16
MAX_CONTENT_SIZE = 1 << 30
GET_SETTINGS_HEADER = "ASIO-RPC-GET-SERVER-SETTINGS"


@dataclass
class AIOHttpTransportClient(AsyncTransportClient):
    node: Optional[str]
    api_key: str
    url: Optional[str] = None
    ssl_cert: Optional[Path] = None
    user: str = USER_NAME
    headers: Optional[Dict[str, str]] = None
    port: int = PORT
    rpc_path: str = '/rpc'

    rpc_url: str = field(init=False)
    is_connected: bool = field(default=False, init=False)
    request_in_progress: bool = field(default=False, init=False)
    ssl_context: Optional[ssl.SSLContext] = field(default=None, init=False)
    http_conn: ClientSession = field(init=False)
    auth: BasicAuth = field(init=False)
    post_params: Dict[str, Any] = field(init=False)
    multiplexed = False

    def __post_init__(self) -> None:
        if self.url is None and self.node is None:
            raise ValueError("At least 'url' or 'node' params must be provided")
        if self.url is not None and self.node is not None:
            raise ValueError("Only one from 'url' and 'node' params must be provided")

        if self.node:
            self.rpc_url = f"https://{self.node}:{self.port}{self.rpc_path}"
        else:
            self.rpc_url = cast(str, self.url)

        self.http_conn = ClientSession()
        if self.ssl_cert:
            self.ssl_context: Optional[ssl.SSLContext] = ssl.create_default_context(cadata=self.ssl_cert.open().read())
        self.auth = BasicAuth(login=self.user, password=self.api_key)
        self.post_params = {"ssl": self.ssl_context, "auth": self.auth, "verify_ssl": self.ssl_context is not None}

    async def connect(self) -> None:
        await self.http_conn.__aenter__()
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False
        await self.http_conn.__aexit__(None, None, None)

    def __str__(self) -> str:
        return f"HTTP({self.rpc_path})"

    async def get_settings(self) -> Dict[str, Any]:
        data = b""
        data_iter: AsyncIterable[bytes]
        async with self.make_request(b"", headers={GET_SETTINGS_HEADER: ""}) as data_iter:
            async for chunk in cast(AsyncIterable[bytes], data_iter):
                data += chunk
        return json.loads(data.decode())

    @contextlib.asynccontextmanager   # type: ignore
    async def make_request(self, data: Union[bytes, AsyncIterable[bytes]],
                           headers: Dict[str, str] = None,
                           timeout: Optional[float] = None) -> AsyncIterable[AsyncIterable[bytes]]:

        req_headers = self.headers if self.headers else {}
        if headers:
            req_headers.update(headers)

        async with self.http_conn.post(self.rpc_url, **self.post_params, data=data,
                                       headers=headers, timeout=timeout) as resp:
            yield resp.content.iter_chunked(DEFAULT_HTTP_CHUNK)


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password = encrypt_key(for_check, salt)
    return curr_password == for_check


def basic_auth_middleware(key: str):
    @web.middleware
    async def basic_auth(request, handler):
        if request.path == '/ping':
            return await handler(request)

        auth_info = request.headers.get('Authorization')
        if auth_info:
            auth = BasicAuth.decode(auth_info)
            if auth.login != USER_NAME or not check_key(key, auth.password):
                return await handler(request)

        headers = {'WWW-Authenticate': 'Basic realm="XXX"'}
        return web.HTTPUnauthorized(headers=headers)
    return basic_auth


@dataclass
class AIOHttpTransportServer:
    process_request: ProcessRequest
    ip: str
    ssl_cert: Path
    ssl_key: Path
    api_key_enc: str
    settings: Dict[str, Any]
    port: int = PORT
    on_server_startup: Optional[Iterable[StartStopFunc]] = None
    on_server_shutdown: Optional[Iterable[StartStopFunc]] = None
    rpc_path: str = '/rpc'

    async def handle_rpc(self, request: web.Request) -> web.StreamResponse:
        if GET_SETTINGS_HEADER in request.headers:
            assert (await request.content.read()) == b''
            response = web.Response(status=200, headers={'Content-Encoding': 'identity'},
                                    text=json.dumps(self.settings))
            return response

        code, data_iter = await self.process_request(request.content.iter_chunked(DEFAULT_HTTP_CHUNK))  # type: ignore
        response = web.StreamResponse(status=int(code), headers={'Content-Encoding': 'identity'})

        await response.prepare(request)

        if data_iter:
            if isinstance(data_iter, bytes):
                await response.write(data_iter)
            else:
                async for chunk in data_iter:
                    await response.write(chunk)

        return response

    def make_app(self) -> web.Application:
        auth = basic_auth_middleware(self.api_key_enc)
        app = web.Application(middlewares=[auth], client_max_size=MAX_CONTENT_SIZE)
        app.add_routes([web.post(self.rpc_path, self.handle_rpc)])

        if self.on_server_startup:
            for func in self.on_server_startup:
                app.on_startup.append(func)

        if self.on_server_shutdown:
            for func in self.on_server_shutdown:
                app.on_cleanup.append(func)

        return app

    def make_ssl_context(self) -> ssl.SSLContext:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(str(self.ssl_cert), str(self.ssl_key))
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    def serve_forever(self) -> None:
        web.run_app(self.make_app(), host=self.ip, port=self.port, ssl_context=self.make_ssl_context())

    def __str__(self) -> str:
        return f"HTTPS({self.ip}:{self.port}{self.rpc_path})"

    async def close(self):
        pass
