from pathlib import Path
from typing import Dict

from .client import ConnectionClosed, IAgentRPCNode, ConnectionPool, check_nodes
from .plugins import HistoricCollectionConfig, HistoricCollectionStatus
from .server import configure, start_rpc_server


def get_http_connection_pool(certs: Dict[str, Path], api_key: str, **extra) -> ConnectionPool:
    pass


def connect_http(cert: Path, api_key: str, **extra) -> IAgentRPCNode:
    pass
