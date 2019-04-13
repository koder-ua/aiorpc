from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, AsyncIterator, AsyncContextManager, AsyncIterable

from .interfaces import AsyncTransportClient
from .rpc import deserialize, CALL_FAILED, CALL_SUCCEEDED, serialize, ISerializer, IBlockStream


@dataclass
class BaseAsyncRPC:
    _transport: AsyncTransportClient
    _serializer: ISerializer
    _bstream: IBlockStream
    _max_retry: int = 3
    _retry_timeout: int = 5
    _request_in_progress: bool = field(default=False, init=False)

    @property
    def streamed(self) -> 'StreamedProxy':
        return StreamedProxy(self)

    async def close(self) -> None:
        await self._transport.close()

    def _prepare_call(self, name: str, args: List, kwargs: Dict[str, Any]) -> Tuple[float, AsyncIterator[bytes]]:
        timeout = kwargs.pop("_call_timeout", None)
        return timeout, serialize(name, args, kwargs, self._serializer, self._bstream)

    async def _process_rpc_results(self, data_iter: AsyncIterator[bytes], stream_allowed: bool) -> Any:
        name, args, kwargs = deserialize(data_iter, stream_allowed, self._serializer, self._bstream)
        assert kwargs == {}
        assert len(args) == 1
        if name == CALL_FAILED:
            exc, tb = args[0]
            raise exc from Exception("RPC server traceback:\n" + tb)
        else:
            assert name == CALL_SUCCEEDED
            return args[0]

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]) -> Any:
        if not self._transport.multiplexed:
            assert not self._request_in_progress, "Can't share connection between requests"
        self._request_in_progress = True
        try:
            timeout, data_iter = self._prepare_call(name, args, kwargs)
            assert timeout is None, "Timeout not supported yet"
            async with self._transport.make_request(data_iter) as result_stream:
                return await self._process_rpc_results(result_stream, False)
        finally:
            self._request_in_progress = False

    def _call_streamed(self, name: str, args: List, kwargs: Dict[str, Any]) -> 'StreamedCall':
        if not self._transport.multiplexed:
            assert not self._request_in_progress, "Can't share connection between requests"
        self._request_in_progress = True
        timeout, data_iter = self._prepare_call(name, args, kwargs)
        assert timeout is None, "Can't use timeout with streamed calls"
        return StreamedCall(self._transport.make_request(data_iter), self)

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name, streamed=False)

    def _get_peer_info(self) -> Any:
        raise NotImplementedError()


@dataclass
class StreamedProxy:
    rpc_conn: BaseAsyncRPC

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self.rpc_conn, name, streamed=True)


@dataclass
class StreamedCall:
    req: AsyncContextManager[AsyncIterable[bytes]]
    rpc_conn: BaseAsyncRPC

    def __post_init__(self):
        assert hasattr(self.rpc_conn, "_request_in_progress"), f"{self.rpc_conn} don't have _request_in_progress attr"

    async def __aenter__(self) -> AsyncIterable[bytes]:
        assert self.rpc_conn._request_in_progress, "Request not opened"
        return await self.req.__aenter__()

    async def __aexit__(self, x, y, z) -> None:
        assert self.rpc_conn._request_in_progress, "Request not opened"
        try:
            await self.req.__aexit__(x, y, z)
        finally:
            self.rpc_conn._request_in_progress = False


@dataclass
class RPCModuleProxy:
    rpc_conn: BaseAsyncRPC
    mod_name: str
    streamed: bool

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc_conn, name=f"{self.mod_name}::{name}", streamed=self.streamed)


@dataclass
class RPCFuncProxy:
    rpc_conn: BaseAsyncRPC
    streamed: bool
    name: str

    def __call__(self, *args, **kwargs) -> Any:
        if self.streamed:
            return self.rpc_conn._call_streamed(self.name, list(args), kwargs)
        else:
            return self.rpc_conn(self.name, list(args), kwargs)  # awaitable

