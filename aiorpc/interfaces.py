from typing import Callable, AsyncIterable, Dict, Any, AsyncContextManager, List, Protocol, Tuple, NewType, Coroutine, \
    Union

ErrCode = NewType('ErrCode', int)


class CloseRequest(Exception):
    pass


class AsyncTransportClient(Protocol):
    multiplexed: bool

    def __str__(self) -> str:
        ...

    async def get_settings(self) -> Dict[str, Any]:
        ...

    def make_request(self, data: AsyncIterable[bytes]) -> AsyncContextManager[AsyncIterable[bytes]]:
        ...

    async def close(self):
        ...


class AsyncTransportServer(Protocol):
    def __str__(self) -> str:
        ...

    async def serve_forever(self) -> None:
        ...

    async def close(self):
        ...

    async def start_listener(self) -> None:
        ...


class RPCServerFailure(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class AsyncRPCProto(Protocol):
    _request_in_progress: bool
    streamed: Any

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]) -> Any:
        ...

    def __str__(self) -> str:
        ...

    def _get_peer_info(self) -> Any:
        ...

    def __getattr__(self, name: str) -> Any:
        ...


MakeAsyncTransportServer = Callable[[...], AsyncTransportServer]
ConnectAsyncTransport = Callable[[...], AsyncContextManager[AsyncTransportClient]]
RpcRequestProcessCB = Callable[[AsyncIterable[bytes]],
                               Coroutine[Any, Any, Tuple[ErrCode, Union[None, bytes, AsyncIterable[bytes]]]]]
