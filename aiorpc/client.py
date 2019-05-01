import os
import re
import time
import zlib
import logging
import asyncio
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass, field
from typing import (Any, Dict, List, Tuple, Union, AsyncIterator, BinaryIO, NamedTuple, cast, Iterable, Optional,
                    Callable)

from koder_utils import CMDResult, CmdType, IAsyncNode, AnyPath, BaseConnectionPool

from .common import (IReadableAsync, ChunkedFile, ZlibStreamDecompressor, ZlibStreamCompressor, ConnectionFailed,
                     AsyncTransportClient)

from .rpc import BlockType, SimpleBlockStream, JsonSerializer
from .proxy import AsyncClientConnection, Proxy


logger = logging.getLogger("aiorpc")


def to_streamed_content(content: Union[bytes, BinaryIO, IReadableAsync]) -> IReadableAsync:
    if isinstance(content, bytes):
        return ChunkedFile(BytesIO(content))
    elif isinstance(content, IReadableAsync):
        return content
    else:
        return ChunkedFile(content)


class StatRes(NamedTuple):
    mode: int
    ino: int
    dev: int
    nlink: int
    uid: int
    gid: int
    size: int
    atime: int
    mtime: int
    ctime: int


@dataclass
class IAIORPCNode(IAsyncNode):
    conn: AsyncClientConnection
    proxy: Optional[Proxy] = field(default=None, init=False)

    def __str__(self) -> str:
        return f"AgentRPC({self.conn})"

    async def connect(self) -> None:
        self.proxy = await self.conn.connect()

    async def disconnect(self) -> None:
        await self.conn.disconnect()
        self.proxy = None

    async def read(self, path: AnyPath, compress: bool = True) -> bytes:
        return b"".join([chunk async for chunk in self.iter_file(str(path), compress=compress)])

    async def tail_file(self, path: AnyPath, size: int) -> AsyncIterator[bytes]:
        assert self.proxy, "Not connected"
        async with self.proxy.streamed.fs.tail(str(path), size) as block_iter:
            async for chunk in block_iter:
                yield chunk

    async def iter_file(self, path: AnyPath, compress: bool = True) -> AsyncIterator[bytes]:
        assert self.proxy, "Not connected"

        async with self.proxy.streamed.fs.get_file(str(path), compress=compress) as block_iter:
            if compress:
                async for chunk in ZlibStreamDecompressor(block_iter):
                    yield chunk
            else:
                async for chunk in block_iter:
                    yield chunk

    async def write(self, path: AnyPath, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True):
        assert self.proxy, "Not connected"
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        await self.proxy.fs.write_file(str(path), stream, compress=compress)

    async def write_tmp(self, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True) -> Path:
        assert self.proxy, "Not connected"
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        return Path(await self.proxy.fs.write_file(None, stream, compress=compress))

    async def stat(self, path: AnyPath) -> os.stat_result:
        assert self.proxy, "Not connected"
        return cast(os.stat_result, StatRes(*(await self.proxy.fs.stat(str(path)))))

    async def run(self, cmd: CmdType, input_data: Union[bytes, None, BinaryIO] = None,
                  merge_err: bool = True, timeout: float = 60, output_to_devnull: bool = False,
                  term_timeout: float = 1, env: Dict[str, str] = None,
                  compress: bool = True) -> CMDResult:
        assert self.proxy, "Not connected"
        assert isinstance(input_data, bytes) or input_data is None
        code, out, err = await self.proxy.cli.run_cmd(cmd if isinstance(cmd, str) else [str(i) for i in cmd],
                                                     term_timeout=term_timeout,
                                                     timeout=timeout, input_data=input_data, merge_err=merge_err,
                                                     env=env, compress=compress)

        if merge_err:
            assert err is None

        if compress:
            out = zlib.decompress(out)
            err = None if err is None else zlib.decompress(err)

        return CMDResult(cmd, out, err, code)

    async def exists(self, fname: AnyPath) -> bool:
        assert self.proxy, "Not connected"
        return await self.proxy.fs.file_exists(str(fname))

    async def iterdir(self, path: AnyPath) -> Iterable[Path]:
        assert self.proxy, "Not connected"
        return map(Path, await self.proxy.fs.iterdir(str(path)))

    async def collect_historic(self, start: int = 0, size: int = 0) -> AsyncIterator[bytes]:
        assert self.proxy, "Not connected"
        async with self.proxy.streamed.ceph.get_collected_historic_data(start, size) as data_iter:
            async for chunk in data_iter:
                yield chunk

    async def get_sock_count(self, pid: int) -> int:
        assert self.proxy, "Not connected"
        return await self.proxy.fs.count_sockets_for_process(pid)

    async def get_device_for_file(self, fname: str) -> Tuple[str, str]:
        """Find storage device, on which file is located"""

        assert self.proxy, "Not connected"
        dev = (await self.proxy.fs.get_dev_for_file(fname)).decode()
        assert dev.startswith('/dev'), f"{dev!r} is not starts with /dev"
        root_dev = dev = dev.strip()
        rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
        if rr:
            root_dev = rr.group(1)
        return root_dev, dev


async def make_aiorpc_conn(transport: AsyncTransportClient) -> IAIORPCNode:
    await transport.connect()
    params = await transport.get_settings()
    assert params['serializer'] == 'json', f"Serializer {params['serializer']} not supported"
    assert params['bstream'] == 'simple', f"Blocks stream {params['bstream']} not supported"
    base_rpc = AsyncClientConnection(transport, serializer=JsonSerializer(), bstream=SimpleBlockStream())
    return IAIORPCNode(base_rpc)


class ConnectionPool(BaseConnectionPool[IAIORPCNode]):
    def __init__(self,
                 conn_params: Dict[str, Dict[str, Any]],
                 max_conn_per_node: int,
                 max_conn_total: int,
                 transport_cls: Callable[..., AsyncTransportClient]) -> None:
        BaseConnectionPool.__init__(self, max_conn_per_node=max_conn_per_node, max_conn_total=max_conn_total)
        self.conn_params = conn_params
        self.transport_cls = transport_cls

    async def rpc_connect(self, conn_addr: str) -> IAIORPCNode:
        """Connect to nodes and fill Node object with basic node info: ips and hostname"""
        transport: AsyncTransportClient = self.transport_cls(**self.conn_params[conn_addr])
        conn = await make_aiorpc_conn(transport)
        await conn.__aenter__()
        return conn

    async def rpc_disconnect(self, conn: IAIORPCNode) -> None:
        await conn.__aexit__(None, None, None)


async def wait_ready(transport_cls: Callable[..., AsyncTransportClient],
                     conn_params: Dict[str, Any],
                     timeout: float = 30,
                     period: float = 0.1):

    async def do_check():
        async with transport_cls(**conn_params) as conn:  # type: ignore
            await conn.get_settings()

    end_time = time.time() + timeout
    wait_time = timeout

    while wait_time > 0:
        try:
            await asyncio.wait_for(do_check(), wait_time)
            break
        except ConnectionFailed:
            await asyncio.sleep(period)
        wait_time = end_time - time.time()
    else:
        raise ConnectionFailed(f"Can't connect")


async def iter_unreachable(inventory: List[str], pool: ConnectionPool) -> AsyncIterator[str]:
    for hostname in inventory:
        try:
            async with pool.connection(hostname) as conn:
                assert "test" == await conn.proxy.sys.ping("test")
        except Exception as exc:
            print(exc)
            yield hostname


# ----------- EXPERIMENTAL ---------------------------------------------------------------------------------------------


class ReadFileLike:
    pass


class WriteFileLike(IReadableAsync):
    def __init__(self) -> None:
        self._q: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._closed = False

    def close(self) -> None:
        self._q.put(None)

    async def readany(self) -> bytes:
        if self._closed:
            return b''

        data = await self._q.get()
        if data is None:
            assert self._closed
            return b''
        else:
            return data

    async def write(self, data: bytes) -> None:
        await self._q.put(data)


class IAgentRPCNodeWithRemoteFiles(IAIORPCNode):
    async def open(self, path: AnyPath, mode: str = "wb", compress: bool = True) -> Union[ReadFileLike, WriteFileLike]:
        assert self.proxy, "Not connected"
        if mode == "wb":
            flike = WriteFileLike()
            asyncio.create_task(self.write(path, flike, compress=compress))
            return flike
        raise ValueError(f"Unsupported mode {mode}")

