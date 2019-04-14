import functools
import http
import traceback
from typing import AsyncIterable, Tuple, Union

from aiohttp import web, BasicAuth

from . import rpc
from .aiohttp_transport import AIOHttpTransportServer
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


async def handle_rpc(input_data: AsyncIterable[bytes], serializer: rpc.ISerializer,
                     bstream: rpc.IBlockStream) -> Tuple[ErrCode, Union[None, AsyncIterable[bytes]]]:
    packers = {"serializer": serializer, "bstream": bstream}
    try:
        name, args, kwargs = await rpc.deserialize(input_data, allow_streamed=True, **packers)  # type: ignore
        try:
            if name in exposed_async:
                res = await exposed_async[name](*args, **kwargs)
            elif name in exposed:
                res = exposed[name](*args, **kwargs)
            else:
                raise AttributeError(f"Name {name!r} not found")
        except Exception as exc:
            return http.HTTPStatus.OK, rpc.serialize(rpc.CALL_FAILED,  # type: ignore
                [exc, traceback.format_exc()], {}, **packers)  # type: ignore
        else:
            return http.HTTPStatus.OK, rpc.serialize(rpc.CALL_SUCCEEDED, [res], {}, **packers)  # type: ignore
    except:
        logger.exception("During send body")
        raise


def configure(**vals):
    for name, val in vals.items():
        tp, var = ALL_CONFIG_VARS[name]
        assert isinstance(val, tp), f"Value for {name} should have type {tp.__name__}, not {type(val).__name__}"
        var.set(val)


def start_rpc_server(**kwargs) -> None:
    handler = functools.partial(handle_rpc, serializer=rpc.JsonSerializer(), bstream=rpc.SimpleBlockStream())
    AIOHttpTransportServer(on_server_startup=on_server_startup,
                           on_server_shutdown=on_server_shutdown,
                           process_request=handler,  # type: ignore
                           settings={"serializer": "json", "bstream": "simple"},
                           **kwargs).serve_forever()
