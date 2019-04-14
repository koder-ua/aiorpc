from pathlib import Path
from typing import Dict, AsyncContextManager

from .common import ConnectionClosed
from .client import IAgentRPCNode, ConnectionPool, check_nodes, connect
from .plugins import HistoricCollectionConfig, HistoricCollectionStatus
from .server import configure, start_rpc_server
from .aiohttp_transport import AIOHttpTransportClient, make_client as make_http_client


def get_http_connection_pool(ssl_certs: Dict[str, Path], api_key: str, **extra) -> ConnectionPool:
    params = {host: {'cert': cert, 'api_key': api_key, 'base_url': host} for host, cert in ssl_certs.items()}
    return ConnectionPool(params, conn_factory=make_http_client, **extra)


async def connect_http(node: str, ssl_cert: Path, api_key: str, **extra) -> AsyncContextManager[IAgentRPCNode]:
    return connect(conn_factory=make_http_client, base_url=node, api_key=api_key, ssl_cert=ssl_cert, **extra)

