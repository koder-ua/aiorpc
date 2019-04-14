import abc
import json
import base64
import struct
import hashlib
import subprocess
from enum import Enum

from typing import Any, Dict, List, Tuple, Optional, NamedTuple, AsyncIterator, AsyncIterable

from .plugins_api import exposed_types
from .common import IReadableAsync, RPCStreamError


EOD_MARKER = b'\x00'
CUSTOM_TYPE_KEY = '__custom__type_658aaae5-6216-4fe0-8483-d51cf21a6ba5'
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


# ---- SERIALIZATION ---------------------------------------------------------------------------------------------------

# transforming data into serializable types - basic types, lists, dicts of basic types


def prepare_for_serialization(args: List, kwargs: Dict) -> Tuple[Dict[str, Any], Optional[IReadableAsync]]:
    streams: List[IReadableAsync] = []
    p_args = do_prepare_for_serialization(args, streams)
    p_kwargs = do_prepare_for_serialization(kwargs, streams)
    return {'args': p_args, 'kwargs': p_kwargs}, None if streams == [] else streams[0]


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


def after_deserialization(data: Dict[str, Any], stream: Any) -> Tuple[str, List, Dict, bool]:
    args = data.pop('args')
    assert type(args) is list

    name = data.pop('name')
    assert isinstance(name, str)

    kwargs = data.pop('kwargs')
    assert type(kwargs) is dict
    assert all(isinstance(key, str) for key in kwargs)

    streams = [stream]
    args = do_after_deserialization(args, streams)
    assert isinstance(args, list)
    kwargs = do_after_deserialization(kwargs, streams)
    assert isinstance(kwargs, dict)
    assert all(isinstance(key, str) for key in kwargs)
    return name, args, kwargs, streams == []


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
    jargs, maybe_stream = prepare_for_serialization(args, kwargs)
    jargs['name'] = name
    serialized_args = serializer.pack(jargs)
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
        raise RPCStreamError(f"Get block type of {tp.name} instead of serialized")

    unpacked = serializer.unpack(data)
    name, args, kwargs, use_stream = after_deserialization(unpacked, blocks_iter)

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
