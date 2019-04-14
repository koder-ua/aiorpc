import http
import traceback
from typing import AsyncIterable, Tuple, Union

from aiohttp import web, BasicAuth

from . import rpc
from .aiohttp_transport import make_server as make_http_server
from .plugins_api import ALL_CONFIG_VARS, exposed, exposed_async, on_server_startup, on_server_shutdown
from .common import USER_NAME, encrypt_key, logger, ErrCode


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


async def handle_rpc(serializer: rpc.ISerializer, bstream: rpc.IBlockStream,
                     input_data: AsyncIterable[bytes]) -> Tuple[ErrCode, Union[None, AsyncIterable[bytes]]]:
    packers = {"serializer": serializer, "bstream": bstream}
    try:
        name, args, kwargs = rpc.deserialize(input_data, allow_streamed=True, **packers)
        try:
            if name in exposed_async:
                res = await exposed_async[name](*args, **kwargs)
            elif name in exposed:
                res = exposed[name](*args, **kwargs)
            else:
                return http.HTTPStatus.NOT_FOUND, None
        except Exception as exc:
            return http.HTTPStatus.OK, rpc.serialize(rpc.CALL_FAILED, [exc, traceback.format_exc()], {}, **packers)
        else:
            return http.HTTPStatus.OK, rpc.serialize(rpc.CALL_SUCCEEDED, [res], {}, **packers)
    except:
        logger.exception("During send body")
        raise


def configure(**vals):
    for name, val in vals.items():
        tp, var = ALL_CONFIG_VARS[name]
        assert isinstance(val, tp), f"Value for {name} should have type {tp.__name__}, not {type(val).__name__}"
        var.set(val)


def start_rpc_server(**kwargs):
    server = make_http_server(on_server_startup=on_server_startup, on_server_shutdown=on_server_shutdown, **kwargs)
    server.serve_forever()

