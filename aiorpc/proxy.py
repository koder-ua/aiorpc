import contextlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, Sequence, Optional, AsyncIterable, AsyncContextManager, Iterator

from .common import AsyncTransportClient
from .rpc import deserialize, CALL_FAILED, CALL_SUCCEEDED, serialize, ISerializer, IBlockStream


@dataclass
class AsyncClientConnection:
    transport: AsyncTransportClient
    serializer: ISerializer
    bstream: IBlockStream
    max_retry: int = 3
    retry_timeout: int = 5
    request_in_progress: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        assert self.transport.is_connected, "Transport must be connected"

    async def connect(self) -> 'Proxy':
        return Proxy(self, [], False)

    async def disconnect(self) -> None:
        await self.transport.disconnect()

    async def __aenter__(self) -> 'Proxy':
        return await self.connect()

    async def __aexit__(self, x, y, z) -> bool:
        await self.disconnect()
        return False

    def get_peer_info(self) -> Any:
        raise NotImplementedError()

    async def process_rpc_results(self, data_iter: AsyncIterable[bytes], stream_allowed: bool) -> Any:
        name, args, kwargs = await deserialize(data_iter, stream_allowed, self.serializer, self.bstream)
        assert kwargs == {}
        if name == CALL_FAILED:
            assert len(args) == 2
            exc, tb = args
            raise exc from Exception("RPC server traceback:\n" + tb)
        else:
            assert name == CALL_SUCCEEDED
            assert len(args) == 1
            return args[0]

    def prepare_call(self, path: List[str], timeout: Optional[float], args: Sequence, kwargs: Dict[str, Any]) \
            -> AsyncContextManager[AsyncIterable[bytes]]:

        assert timeout is None, "Timeout not supported yet"
        data_iter = serialize(".".join(path), list(args), kwargs, self.serializer, self.bstream)

        req = self.transport.make_request(data_iter)

        if not self.transport.multiplexed:
            assert not self.request_in_progress, "Can't share connection between requests"

        return req

    async def rpc_call(self,
                       path: List[str],
                       timeout: Optional[float],
                       args: Sequence,
                       kwargs: Dict[str, Any]) -> Any:

        async with self.prepare_call(path, timeout, args, kwargs) as result:
            return await self.process_rpc_results(result, False)

    @contextlib.asynccontextmanager
    async def rpc_streamed_call(self,
                                path: List[str],
                                timeout: Optional[float],
                                args: Sequence,
                                kwargs: Dict[str, Any]) -> Iterator[Any]:
        req = self.prepare_call(path, timeout, args, kwargs)
        self.request_in_progress = True
        try:
            async with req as result_stream:
                yield await self.process_rpc_results(result_stream, True)
        finally:
            self.request_in_progress = False


@dataclass
class Proxy:
    _conn: AsyncClientConnection
    _path: List[str]
    _streamed: bool

    @property
    def streamed(self) -> 'Proxy':
        return self.__class__(self._conn, self._path, True)

    def __getattr__(self, name) -> 'Proxy':
        return self.__class__(self._conn, self._path + [name], self._streamed)

    def __call__(self, *args, _call_timeout: float = None, **kwargs) -> Any:
        if self._streamed:
            return self._conn.rpc_streamed_call(self._path, _call_timeout, args, kwargs)
        else:
            return self._conn.rpc_call(self._path, _call_timeout, args, kwargs)
