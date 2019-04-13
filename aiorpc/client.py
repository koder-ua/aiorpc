import os
import re
import time
import zlib
import logging
import asyncio
import contextlib
from io import BytesIO
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union, AsyncIterator, BinaryIO, NamedTuple, cast, Iterable


from koder_utils import CMDResult, CmdType, IAsyncNode, AnyPath, BaseConnectionPool

from .interfaces import AsyncRPCProto, ConnectAsyncTransport, ConnectionFailed
from .client_base import BaseAsyncRPC
from .common import IReadableAsync, ChunkedFile, ZlibStreamDecompressor, ZlibStreamCompressor
from .rpc import BlockType, SimpleBlockStream, JsonSerializer


logger = logging.getLogger("aiorpc")


class ConnectionClosed(Exception):
    pass


def to_streamed_content(content: Union[bytes, BinaryIO, IReadableAsync]) -> IReadableAsync:
    if isinstance(content, bytes):
        return ChunkedFile(BytesIO(content))
    elif isinstance(content, IReadableAsync):
        return content
    else:
        return ChunkedFile(content)


def compress_proxy():
    pass


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


class ReadFileLike:
    pass


class WriteFileLike(IReadableAsync):
    def __init__(self) -> None:
        self._q = asyncio.Queue(maxsize=1)
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


@dataclass
class IReadableAsyncFromStream(IReadableAsync):
    chunked_aiter: AsyncIterator[Tuple[BlockType, bytes]]
    expected_type: BlockType = BlockType.binary

    async def readany(self) -> bytes:
        async for tp, data in self.chunked_aiter:
            assert tp == self.expected_type
            return data
        return b''


@dataclass
class IAgentRPCNode(IAsyncNode):
    conn: AsyncRPCProto

    def __str__(self) -> str:
        return f"AgentRPC({self.conn})"

    async def close(self) -> None:
        await self.conn.close()

    async def read(self, path: AnyPath, compress: bool = True) -> bytes:
        return b"".join([chunk async for chunk in self.iter_file(str(path), compress=compress)])

    async def tail_file(self, path: AnyPath, size: int) -> AsyncIterator[bytes]:
        async with self.conn.streamed.fs.tail(str(path), size) as block_iter:
            async for tp, chunk in block_iter:
                assert tp == BlockType.binary
                yield chunk

    async def iter_file(self, path: AnyPath, compress: bool = True) -> AsyncIterator[bytes]:
        async with self.conn.streamed.fs.get_file(str(path), compress=compress) as block_iter:
            if compress:
                async for chunk in ZlibStreamDecompressor(IReadableAsyncFromStream(block_iter)):
                    yield chunk
            else:
                async for tp, chunk in block_iter:
                    assert tp == BlockType.binary
                    yield chunk

    async def write(self, path: AnyPath, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True):
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        await self.conn.fs.write_file(str(path), stream, compress=compress)

    async def write_tmp(self, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True) -> Path:
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        return Path(await self.conn.fs.write_file(None, stream, compress=compress))

    async def stat(self, path: AnyPath) -> os.stat_result:
        return cast(os.stat_result, StatRes(*(await self.conn.fs.stat(str(path)))))

    async def run(self, cmd: CmdType, input_data: Union[bytes, None, BinaryIO] = None,
                  merge_err: bool = True, timeout: float = 60, output_to_devnull: bool = False,
                  term_timeout: float = 1, env: Dict[str, str] = None,
                  compress: bool = True) -> CMDResult:

        assert isinstance(input_data, bytes) or input_data is None
        code, out, err = await self.conn.cli.run_cmd(cmd if isinstance(cmd, str) else [str(i) for i in cmd],
                                                     term_timeout=term_timeout,
                                                     timeout=timeout, input_data=input_data, merge_err=merge_err,
                                                     env=env, compress=compress)

        if merge_err:
            assert err is None

        if compress:
            out = zlib.decompress(out)
            err = None if err is None else zlib.decompress(err)

        return CMDResult(cmd, out, err, code)

    async def disconnect(self) -> None:
        pass

    async def exists(self, fname: AnyPath) -> bool:
        return await self.conn.fs.file_exists(str(fname))

    async def iterdir(self, path: AnyPath) -> Iterable[Path]:
        return map(Path, await self.conn.fs.iterdir(str(path)))

    async def open(self, path: AnyPath, mode: str = "wb", compress: bool = True) -> Union[ReadFileLike, WriteFileLike]:
        if mode == "wb":
            flike = WriteFileLike()
            asyncio.create_task(self.write(path, flike, compress=compress))
            return flike
        raise ValueError(f"Unsupported mode {mode}")

    async def collect_historic(self, start: int = 0, size: int = 0) -> AsyncIterator[bytes]:
        async with self.conn.streamed.ceph.get_collected_historic_data(start, size) as data_iter:
            async for tp, chunk in data_iter:
                assert tp == BlockType.binary
                yield chunk

    async def get_sock_count(self, pid: int) -> int:
        return await self.conn.fs.count_sockets_for_process(pid)


    async def get_device_for_file(self, fname: str) -> Tuple[str, str]:
        """Find storage device, on which file is located"""

        dev = (await self.conn.fs.get_dev_for_file(fname)).decode()
        assert dev.startswith('/dev'), "{!r} is not starts with /dev".format(dev)
        root_dev = dev = dev.strip()
        rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
        if rr:
            root_dev = rr.group(1)
        return root_dev, dev


@contextlib.asynccontextmanager
def connect(conn_factory: ConnectAsyncTransport, conn_params: Dict[str, Any]) -> IAgentRPCNode:
    async with conn_factory(**conn_params) as conn:
        params = await conn.get_settings()
        assert params['serializer'] == 'json', f"Serializer {params['serializer']} not supported"
        assert params['bstream'] == 'simple', f"Blocks stream {params['bstream']} not supported"
        base_rpc = BaseAsyncRPC(conn, _serializer=JsonSerializer(), _bstream=SimpleBlockStream())
        yield IAgentRPCNode(base_rpc)


class ConnectionPool(BaseConnectionPool[IAgentRPCNode]):
    def __init__(self,
                 conn_urls: Dict[str, Dict[str, Any]],
                 max_conn_per_node: int,
                 conn_factory: ConnectAsyncTransport) -> None:
        BaseConnectionPool.__init__(self, max_conn_per_node=max_conn_per_node)
        self.conn_urls = conn_urls
        self.conn_factory = conn_factory

    async def _rpc_connect(self, conn_addr: str) -> IAgentRPCNode:
        """Connect to nodes and fill Node object with basic node info: ips and hostname"""
        return await connect(self.conn_factory, self.conn_urls[conn_addr]).__aenter__()

    async def _rpc_disconnect(self, conn: IAgentRPCNode) -> None:
        await conn.close()


async def wait_ready(conn_factory: ConnectAsyncTransport,
                     conn_params: Any,
                     timeout: float = 30,
                     period: float = 0.1):

    async def do_check():
        async with conn_factory(conn_params) as conn:
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


async def check_nodes(inventory: List[str], pool: ConnectionPool) -> Tuple[List[str], List[str]]:
    failed = []
    good_hosts = []
    for hostname in inventory:
        try:
            async with pool.connection(hostname) as conn:
                if "test" == await conn.conn.sys.ping("test"):
                    good_hosts.append(hostname)
                else:
                    failed.append(hostname)
        except:
            failed.append(hostname)
    return good_hosts, failed

