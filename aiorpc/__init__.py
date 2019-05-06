import contextlib
import logging
from pathlib import Path
from typing import Dict, AsyncIterator, Any, Optional

logger = logging.getLogger('aiorpc')

from .apis import (ConnectionClosed, get_key_enc, configure, expose_func, expose_type, IReadableAsync, ChunkedFile,
                   ZlibStreamCompressor, ZlibStreamDecompressor, register_startup, register_shutdown, ConfigVar,
                   SimpleBlockStream, JsonSerializer)
from .client import IAIORPCNode, ConnectionPool, iter_unreachable, make_aiorpc_conn
from .plugins import HistoricCollectionConfig, HistoricCollectionStatus
from .aiohttp_transport import AIOHttpTransportClient, start_rpc_server


def get_http_connection_pool(ssl_certs: Dict[str, Path],
                             api_key: str,
                             max_conn_per_node: int,
                             max_conn_total: int = None,
                             **extra) -> ConnectionPool:
    """Can't use aiohttp connection pool, as need to set per-host ssl certificate"""
    params: Dict[str, Dict[str, Any]] = {}
    for node, ssl_cert in ssl_certs.items():
        params[node] = {'ssl_cert': ssl_cert, 'api_key': api_key, 'node': node}
        params[node].update(extra)
    return ConnectionPool(params,
                          transport_cls=AIOHttpTransportClient,
                          max_conn_per_node=max_conn_per_node,
                          max_conn_total=max_conn_total)


@contextlib.asynccontextmanager
async def connect_http(*, node: Optional[str], ssl_cert: Path, api_key: str,
                       url: Optional[str] = None, **extra) -> AsyncIterator[IAIORPCNode]:
    params = extra
    params.update({"url": url, "node": node, "api_key": api_key, "ssl_cert": ssl_cert})
    node = await make_aiorpc_conn(AIOHttpTransportClient(**params))
    await node.__aenter__()  # type: ignore
    try:
        yield node
    finally:
        await node.__aexit__(None, None, None)  # type: ignore
