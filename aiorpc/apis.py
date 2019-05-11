from __future__ import annotations

import abc
import ssl
import zlib
import json
import base64
import struct
import hashlib
import inspect
import subprocess
from enum import Enum
from typing import (AsyncIterable, BinaryIO, NewType, AsyncContextManager, Union, Awaitable, Callable, Any, Optional,
                    TypeVar, Dict, Type, Generic, Coroutine, Tuple, List, NamedTuple, AsyncIterator)
from dataclasses import dataclass, field

import msgpack

from koder_utils import ICloseOnExit


USER_NAME = 'rpc_client'
DEFAULT_FILE_CHUNK = 1 << 20
DEFAULT_COMPRESSOR_CHUNK = 1 << 16
EOD_MARKER = b'\x00'
CUSTOM_TYPE_KEY = '__custom__type_658aaae5-6216-4fe0-8483-d51cf21a6ba5'
NO_PROCESS_KEY = '__custom__type_67dc910b-c892-4a3c-a7d1-df7ebb0b9a02'
STREAM_TYPE = 'binary_stream'
BYTES_TYPE = 'bytes'
EXCEPTION = 'exceptions'

# use string instead of int to unify call and return code
CALL_FAILED = 'fail'
CALL_SUCCEEDED = 'success'


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            FileNotFoundError, UnicodeTranslateError, subprocess.CalledProcessError, subprocess.TimeoutExpired]


exc_map = {exc.__name__: exc for exc in exc_list}


class BlockType(Enum):
    serialized = 1
    binary = 2


class Block(NamedTuple):
    tp: BlockType
    header_size: int
    data_size: int
    hash: bytes
    raw_header: bytes


class IBlockStream(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def check_digest(self, block_data: bytes, expected_digest: bytes):
        ...

    @abc.abstractmethod
    def make_block_header(self, tp: BlockType, data: bytes) -> bytes:
        ...

    @abc.abstractmethod
    def parse_block_header(self, header: bytes) -> Block:
        ...

    @abc.abstractmethod
    def try_parse_block_header(self, buffer: bytes) -> Tuple[Optional[Block], bytes]:
        ...

    @abc.abstractmethod
    async def yield_blocks(self, data_stream: AsyncIterable[bytes]) -> AsyncIterator[Tuple[BlockType, bytes]]:
        ...


def get_key_enc() -> Tuple[str, str]:
    key = "".join((f"{i:02X}" for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join(f"{i:02X}" for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


class IReadableAsync(AsyncIterable[bytes]):
    @abc.abstractmethod
    async def readany(self) -> bytes:
        pass

    def __aiter__(self: T) -> T:
        return self

    async def __anext__(self) -> bytes:
        res = await self.readany()
        if not res:
            raise StopAsyncIteration()
        return res


@dataclass
class ChunkedFile(IReadableAsync):
    fd: BinaryIO
    chunk: int = DEFAULT_FILE_CHUNK
    closed: bool = field(default=False, init=False)
    till_offset: Optional[int] = None
    close_at_the_end: bool = False

    def done(self):
        if self.close_at_the_end:
            self.fd.close()
        self.closed = True

    async def readany(self) -> bytes:
        if self.closed:
            return b""

        if self.till_offset:
            offset = self.fd.tell()
            if offset >= self.till_offset:
                self.done()
                return b""
            max_read = min(offset - self.till_offset, self.chunk)
        else:
            max_read = self.chunk

        data = self.fd.read(max_read)
        if not data:
            self.done()
        return data


@dataclass
class ZlibStreamCompressor(IReadableAsync):
    fd: IReadableAsync
    min_chunk: int = DEFAULT_COMPRESSOR_CHUNK
    compressor: Any = field(default_factory=zlib.compressobj, init=False)
    eof: bool = field(default=False, init=False)

    async def readany(self) -> bytes:
        if self.eof:
            return b''

        curr = b''
        async for chunk in self.fd:
            assert chunk
            curr += self.compressor.compress(chunk)
            if len(curr) >= self.min_chunk:
                return curr

        self.eof = True
        return curr + self.compressor.flush()


@dataclass
class ZlibStreamDecompressor(IReadableAsync):
    fd: IReadableAsync
    min_chunk: int = DEFAULT_COMPRESSOR_CHUNK
    decompressor: Any = field(default_factory=zlib.decompressobj, init=False)
    eof: bool = field(default=False, init=False)

    async def readany(self) -> bytes:
        if self.eof:
            return b''

        curr = b''
        async for chunk in self.fd:
            assert chunk
            curr += self.decompressor.decompress(chunk)
            if len(curr) >= self.min_chunk:
                return curr

        self.eof = True
        return curr + self.decompressor.flush()


ErrCode = NewType('ErrCode', int)


class CloseRequest(Exception):
    pass


class RPCServerFailure(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class ConnectionClosed(Exception):
    pass


class RPCStreamError(Exception):
    pass


class AsyncTransportClient(ICloseOnExit):
    multiplexed: bool
    is_connected: bool

    @abc.abstractmethod
    def __str__(self) -> str:
        pass

    @abc.abstractmethod
    async def get_settings(self) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def make_request(self, data: AsyncIterable[bytes]) -> AsyncContextManager[AsyncIterable[bytes]]:
        pass


class AsyncTransportServer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __str__(self) -> str:
        pass

    @abc.abstractmethod
    async def serve_forever(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    async def start_listener(self) -> None:
        pass


MakeServer = Callable[..., AsyncTransportServer]
ProcessRequest = Callable[[AsyncIterable[bytes]], Awaitable[Tuple[ErrCode, Union[None, bytes, AsyncIterable[bytes]]]]]


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password = encrypt_key(for_check, salt)
    return curr_password == for_check


T = TypeVar('T')
ALL_CONFIG_VARS: Dict[str, Tuple[Type, ConfigVar]] = {}
StartStopFunc = Callable[[Any], Coroutine[Any, Any, None]]


class ConfigVar(Generic[T]):
    def __init__(self, name: str, tp: Type[T]) -> None:
        ALL_CONFIG_VARS[name] = (tp, self)
        self.val: Optional[T] = None

    def set(self, v: T) -> Optional[T]:
        old_v = self.val
        self.val = v
        return old_v

    def __call__(self) -> T:
        assert self.val
        return self.val


def validate_name(name: str):
    assert not name.startswith("_")
    assert name != 'streamed'


exposed = {}
exposed_async = {}


def expose_func(module: str, func: Callable):
    validate_name(module)
    validate_name(func.__name__)
    name = f"{module}.{func.__name__}"

    if inspect.iscoroutinefunction(func):
        exposed_async[name] = func
    else:
        exposed[name] = func

    return func


on_server_startup: List[StartStopFunc] = []


def register_startup(func: StartStopFunc) -> StartStopFunc:
    on_server_startup.append(func)
    return func


on_server_shutdown: List[StartStopFunc] = []


def register_shutdown(func: StartStopFunc) -> StartStopFunc:
    on_server_startup.append(func)
    return func


@dataclass
class RPCClass(Generic[T]):
    pack: Callable[[T], Dict[str, Any]]
    unpack: Callable[[Dict[str, Any]], T]


def default_pack(val: Any) -> Dict[str, Any]:
    return val.__dict__


def default_unpack(tp: Type[T]) -> Callable[[Dict[str, Any]], T]:
    def unpack_closure(attrs: Dict[str, Any]) -> T:
        obj = tp.__new__(tp)
        obj.__dict__.update(attrs)
        return obj
    return unpack_closure


exposed_types: Dict[str, RPCClass] = {}


def expose_type(tp: Type[T],
                pack: Callable[[T], Dict[str, Any]] = None,
                unpack: Callable[[Dict[str, Any]], T] = None) -> Type[T]:
    if pack is None:
        if hasattr(tp, "__to_json__"):
            pack = tp.__json_reduce__  # type: ignore
        else:
            pack = default_pack
    if unpack is None:
        if hasattr(tp, "__from_json__"):
            unpack = tp.__from_json__  # type: ignore
        else:
            unpack = default_unpack(tp)
    exposed_types[f"{tp.__module__}::{tp.__name__}"] = RPCClass(pack, unpack)
    return tp


def configure(**vals):
    for name, val in vals.items():
        tp, var = ALL_CONFIG_VARS[name]
        assert isinstance(val, tp), f"Value for {name} should have type {tp.__name__}, not {type(val).__name__}"
        var.set(val)


# ---- SERIALIZATION ---------------------------------------------------------------------------------------------------

# transforming data into serializable types - basic types, lists, dicts of basic types


def prepare_for_serialization(params: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[IReadableAsync]]:
    streams: List[IReadableAsync] = []
    params = do_prepare_for_serialization(params, streams)
    return params, None if streams == [] else streams[0]


def do_prepare_for_serialization(val: Any, streams: List[IReadableAsync]) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is bytes:
        return {CUSTOM_TYPE_KEY: BYTES_TYPE, 'val': base64.b64encode(val).decode('ascii')}

    if vt in (list, tuple):
        return [do_prepare_for_serialization(i, streams) for i in val]

    if vt is dict:
        assert all(isinstance(key, str) for key in val), "All dict keys mush be strings"
        assert CUSTOM_TYPE_KEY not in val, f"Can't use {CUSTOM_TYPE_KEY!r} as key serializable dict"
        return {key: do_prepare_for_serialization(value, streams) for key, value in val.items()}

    if isinstance(val, IReadableAsync):
        assert len(streams) == 0, "Params can only contains single stream"
        streams.append(val)
        return {CUSTOM_TYPE_KEY: STREAM_TYPE}

    key = f"{vt.__module__}::{vt.__name__}"
    if key in exposed_types:
        return {CUSTOM_TYPE_KEY: key,
                "attrs": do_prepare_for_serialization(exposed_types[key].pack(val), streams)}  # type: ignore

    if isinstance(val, Exception):
        if isinstance(val, subprocess.TimeoutExpired):
            args = (val.cmd, val.timeout, val.stdout, val.stderr)
        elif isinstance(val, subprocess.CalledProcessError):
            args = (val.returncode, val.cmd, val.stdout, val.stderr)
        else:
            args = val.args  # type: ignore
        return {CUSTOM_TYPE_KEY: EXCEPTION,
                "name": val.__class__.__name__,
                "args": do_prepare_for_serialization(args, streams)}

    raise TypeError(f"Can't serialize value of type {vt}")


def after_deserialization(data: Dict[str, Any], stream: Any) -> Tuple[Dict, bool]:
    streams = [stream]
    params = do_after_deserialization(data, streams)
    assert isinstance(params, Dict)
    return params, streams == []


async def yield_bytes_from_binary(iter: AsyncIterator[Tuple[BlockType, bytes]]) -> AsyncIterator[bytes]:
    async for btype, data in iter:
        assert btype == BlockType.binary
        yield data


def do_after_deserialization(val: Any, streams: List) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is list:
        return [do_after_deserialization(i, streams) for i in val]

    if vt is dict:
        class_fqname = val.get(CUSTOM_TYPE_KEY)
        if class_fqname is None:
            assert all(isinstance(key, str) for key in val)
            return {key: do_after_deserialization(value, streams) for key, value in val.items()}
        elif class_fqname == STREAM_TYPE:
            assert streams
            return yield_bytes_from_binary(streams.pop())
        elif class_fqname == BYTES_TYPE:
            return base64.b64decode(val['val'].encode('ascii'))
        elif class_fqname in exposed_types:
            return exposed_types[class_fqname].unpack(val['attrs'])  # type: ignore
        elif class_fqname == EXCEPTION:
            args = do_after_deserialization(val["args"], streams)
            assert isinstance(args, list)
            return exc_map.get(val["name"], Exception)(*args)

    raise TypeError(f"Can't deserialize value of type {vt}")


# ------  SERIALIZERS RawSerializable -> bytes and back ----------------------------------------------------------------


class ISerializer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def pack(self, data: Any) -> bytes:
        ...

    @abc.abstractmethod
    def unpack(self, data: bytes) -> Any:
        ...


class JsonSerializer(ISerializer):
    def pack(self, data: Any) -> bytes:
        return json.dumps(data).encode()

    def unpack(self, data: bytes) -> Any:
        return json.loads(data.decode())


class MsgpackSerializer(ISerializer):
    def pack(self, data: Any) -> bytes:
        return msgpack.packb(data, use_bin_type=True)

    def unpack(self, data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)

# ------- STREAMERS - check sum, stream of blocks, etc -----------------------------------------------------------------


BlockAIter = AsyncIterator[Tuple[BlockType, bytes]]


class SimpleBlockStream(IBlockStream):
    block_header_struct = struct.Struct("!BL")
    hash_digest_size = hashlib.md5().digest_size
    block_header_size = block_header_struct.size + hash_digest_size

    def check_digest(self, block_data: bytes, expected_digest: bytes):
        hashobj = hashlib.md5()
        hashobj.update(block_data)

        if hashobj.digest() != expected_digest:
            raise RPCStreamError("Checksum failed")

    def make_block_header(self, tp: BlockType, data: bytes) -> bytes:
        hashobj = hashlib.md5()
        tp_and_size = self.block_header_struct.pack(tp.value, len(data))
        hashobj.update(tp_and_size)
        hashobj.update(data)
        return hashobj.digest() + tp_and_size

    def parse_block_header(self, header: bytes) -> Block:
        assert len(header) == self.block_header_size
        tp, size = self.block_header_struct.unpack(header[self.hash_digest_size:])
        return Block(BlockType(tp), self.block_header_size, size, header[:self.hash_digest_size],
                     raw_header=header[self.hash_digest_size:])

    def try_parse_block_header(self, buffer: bytes) -> Tuple[Optional[Block], bytes]:
        if len(buffer) < self.block_header_size:
            return None, buffer

        return self.parse_block_header(buffer[:self.block_header_size]), buffer[self.block_header_size:]

    async def yield_blocks(self, data_stream: AsyncIterable[bytes]) -> BlockAIter:  # type: ignore

        buffer = b""
        block: Optional[Block] = None

        async for new_chunk in data_stream:
            buffer += new_chunk

            # while we have enought data to produce new blocks
            while True:
                if block is None:
                    block, buffer = self.try_parse_block_header(buffer)

                # if not enought data - exit
                if block is None or len(buffer) < block.data_size:
                    break

                block_data = buffer[:block.data_size]
                self.check_digest(block.raw_header + block_data, block.hash)
                yield block.tp, block_data

                buffer = buffer[block.data_size:]
                block = None

            # if not enought data and no new data - exit
            if not new_chunk:
                break

        if block is not None:
            raise RPCStreamError("Stream ends before all data transferred")

        if buffer != b'':
            raise RPCStreamError(f"Stream ends before all data transferred: {buffer}")


# full serialization ---------------------------------------------------------------------------------------------------


async def serialize(name: str,
                    args: List,
                    kwargs: Dict[str, Any],
                    serializer: ISerializer,
                    bstream: IBlockStream) -> AsyncIterator[bytes]:
    params = {"args": args, "kwargs": kwargs, "name": name, NO_PROCESS_KEY: ""}
    try:
        serialized_args = serializer.pack(params)
        maybe_stream = None
    except TypeError:
        del params[NO_PROCESS_KEY]
        jargs, maybe_stream = prepare_for_serialization(params)
        serialized_args = serializer.pack(jargs)
        assert NO_PROCESS_KEY.encode() not in serialized_args

    yield bstream.make_block_header(BlockType.serialized, serialized_args) + serialized_args
    if maybe_stream is not None:
        async for data in maybe_stream:
            assert isinstance(data, bytes), f"Stream must yield bytes type, not {type(data)}"
            if not data:
                break
            yield bstream.make_block_header(BlockType.binary, data) + data


async def deserialize(data_stream: AsyncIterable[bytes],
                      allow_streamed: bool,
                      serializer: ISerializer,
                      bstream: IBlockStream) -> Tuple[str, List, Dict]:
    """
    Unpack request from aiohttp.StreamReader or compatible stream
    """
    blocks_iter = bstream.yield_blocks(data_stream).__aiter__()  # type: ignore
    try:
        tp, data = await blocks_iter.__anext__()
    except StopAsyncIteration:
        raise ValueError("No data provided in responce")

    if tp != BlockType.serialized:
        raise RPCStreamError(f"Get block type of {tp.name} instead of json")

    unpacked = serializer.unpack(data)
    if NO_PROCESS_KEY in unpacked:
        params = unpacked
        del unpacked[NO_PROCESS_KEY]
        use_stream = False
    else:
        params, use_stream = after_deserialization(unpacked, blocks_iter)

    name = params.pop('name')
    args = params.pop('args')
    kwargs = params.pop('kwargs')
    assert not params, f"Extra data left {params}"

    if use_stream:
        if not allow_streamed:
            raise ValueError("Streaming not allowed for this call")
    else:
        # check that no data left
        try:
            await blocks_iter.__anext__()
        except StopAsyncIteration:
            pass
        else:
            raise RPCStreamError("Extra data after end of message")

    return name, args, kwargs
