import re
import ssl
import json
import contextlib
from pathlib import Path
from typing import Dict, Any, AsyncIterable, Optional, Union, AsyncContextManager

from aiohttp import web, BasicAuth, ClientSession

from .common import USER_NAME, encrypt_key, RpcRequestProcessCB, AsyncTransportClient


PORT = 55667
DEFAULT_HTTP_CHUNK = 1 << 16
MAX_CONTENT_SIZE = 1 << 30


class AIOHttpTransportClient(AsyncTransportClient):
    multiplexed = False

    def __init__(self,
                 base_url: str,
                 api_key: str,
                 rpc_path: str = '/rpc',
                 ssl_cert: Path = None,
                 user: str = USER_NAME,
                 headers: Dict[str, str] = None,
                 port: int = PORT) -> None:

        self.rpc_path = rpc_path
        self.headers = headers
        self.rpc_url = base_url if re.match(base_url, "http[s]://") else f"https://{base_url}:{port}{rpc_path}"
        self.api_key = api_key
        self.user = user
        self.request_in_progress = False
        self.http_conn = ClientSession()

        if ssl_cert:
            self.ssl: Optional[ssl.SSLContext] = ssl.create_default_context(cadata=ssl_cert.open().read())
        else:
            self.ssl = None

        self.auth = BasicAuth(login=user, password=self.api_key)

        self.post_params = {"ssl": self.ssl,
                            "auth": self.auth,
                            "verify_ssl": self.ssl is not None}

    async def connect(self) -> None:
        await self.http_conn.__aenter__()

    async def disconnect(self) -> None:
        await self.http_conn.__aexit__(None, None, None)

    def __str__(self) -> str:
        return f"HTTP({self.rpc_path})"

    async def get_settings(self) -> Dict[str, Any]:
        data = b""
        async with self.make_request(b"__get_settings__") as data_iter:
            async for chunk in data_iter:
                data += chunk
        return json.loads(data.decode())

    @contextlib.asynccontextmanager
    def make_request(self, data: Union[bytes, AsyncIterable[bytes]]) -> AsyncIterable[bytes]:
        async with self.http_conn.post(self.rpc_url, **self.post_params, data=data) as resp:
            async for chunk in resp.content.iter_chunked(DEFAULT_HTTP_CHUNK):
                yield chunk


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


class AIOHttpTransportServer:
    def __init__(self,
                 rpc_callback: RpcRequestProcessCB,
                 ip: str,
                 ssl_cert: Path,
                 ssl_key: Path,
                 api_key_enc: Path,
                 port: int = PORT,
                 rpc_path: str = '/rpc') -> None:
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(str(ssl_cert), str(ssl_key))
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        self.ip = ip
        self.port = port
        self.rpc_path = rpc_path
        self.api_key_enc = api_key_enc
        self.rpc_callback = rpc_callback

    async def handle_rpc(self, request: web.Request) -> web.StreamResponse:
        code, data_iter = await self.rpc_callback(request.content.iter_chunked(DEFAULT_HTTP_CHUNK))
        response = web.StreamResponse(status=int(code), headers={'Content-Encoding': 'identity'})

        await response.prepare(request)

        if data_iter:
            if isinstance(data_iter, bytes):
                await response.write(data_iter)
            else:
                async for chunk in data_iter:
                    await response.write(chunk)
        return response

    def serve_forever(self) -> None:
        auth = basic_auth_middleware(self.api_key_enc.open().read())
        app = web.Application(middlewares=[auth], client_max_size=MAX_CONTENT_SIZE)
        app.add_routes([web.post(self.rpc_path, self.handle_rpc)])
        web.run_app(app, host=self.ip, port=self.port, ssl_context=self.ssl_context)

    def __str__(self) -> str:
        return f"HTTPS({self.ip}:{self.port}{self.rpc_path})"

    async def close(self):
        pass


def make_server(**kwargs) -> AIOHttpTransportServer:
    return AIOHttpTransportServer(**kwargs)


async def make_client(**kwargs) -> AsyncContextManager[AIOHttpTransportClient]:
    async with AIOHttpTransportClient(**kwargs).connect() as conn:
        yield conn
