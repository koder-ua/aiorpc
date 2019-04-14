import contextlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, AsyncIterator, Sequence, Optional

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

    async def connect(self) -> 'Proxy':
        await self.transport.connect()
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

    async def process_rpc_results(self, data_iter: AsyncIterator[bytes], stream_allowed: bool) -> Any:
        name, args, kwargs = deserialize(data_iter, stream_allowed, self.serializer, self.bstream)
        assert kwargs == {}
        assert len(args) == 1
        if name == CALL_FAILED:
            exc, tb = args[0]
            raise exc from Exception("RPC server traceback:\n" + tb)
        else:
            assert name == CALL_SUCCEEDED
            return args[0]

    async def rpc_call(self,
                       path: List[str],
                       timeout: Optional[float],
                       args: Sequence,
                       kwargs: Dict[str, Any],
                       streamed: bool) -> Any:

        assert timeout is None, "Timeout not supported yet"
        data_iter = serialize(".".join(path), list(args), kwargs, self.serializer, self.bstream)

        req = self.transport.make_request(data_iter)

        @contextlib.asynccontextmanager
        async def call_proxy() -> AsyncIterator:
            if not self.transport.multiplexed:
                assert not self.request_in_progress, "Can't share connection between requests"

            self.request_in_progress = True

            try:
                with req as result_stream:
                    yield await self.process_rpc_results(result_stream, False)
            finally:
                self.request_in_progress = False

        if streamed:
            return call_proxy()
        else:
            async with call_proxy() as result:
                return result


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
        return self._conn.rpc_call(self._path, _call_timeout, args, kwargs, self._streamed)
