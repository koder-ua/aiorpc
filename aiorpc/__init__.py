import contextlib
from pathlib import Path
from typing import Dict, AsyncIterator, Any

from .common import ConnectionClosed, get_key_enc
from .client import IAOIRPCNode, ConnectionPool, check_nodes, make_aiorpc_conn
from .plugins import HistoricCollectionConfig, HistoricCollectionStatus
from .server import configure, start_rpc_server
from .aiohttp_transport import AIOHttpTransportClient


def get_http_connection_pool(ssl_certs: Dict[str, Path],
                             api_key: str,
                             max_conn_per_node: int,
                             **extra) -> ConnectionPool:
    params: Dict[str, Dict[str, Any]] = {}
    for host, ssl_cert in ssl_certs.items():
        params[host] = {'ssl_cert': ssl_cert, 'api_key': api_key, 'base_url': host}
        params[host].update(extra)
    return ConnectionPool(params, transport_cls=AIOHttpTransportClient, max_conn_per_node=max_conn_per_node)


@contextlib.asynccontextmanager
async def connect_http(node: str, ssl_cert: Path, api_key: str, **extra) -> AsyncIterator[IAOIRPCNode]:
    params = extra
    params.update({"base_url": node, "api_key": api_key, "ssl_cert": ssl_cert})
    node = await make_aiorpc_conn(AIOHttpTransportClient(**params))
    await node.__aenter__()  # type: ignore
    try:
        yield node
    finally:
        await node.__aexit__(None, None, None)  # type: ignore
