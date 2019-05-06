import uuid
import json
import asyncio
import subprocess
import configparser
import logging.config
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from koder_utils import SSH, make_secure, make_cert_and_key, rpc_map, b2ssize, RAttredDict

from . import get_http_connection_pool, get_key_enc, IAIORPCNode, ConnectionPool


SERVICE_FILE_DIR = Path("/lib/systemd/system")
files_folder = 'aiorpc_service_files'
distribution = 'distribution.sh'
root_marker = '.install_root'   # should be the same as INSTALL_ROOT_MARKER in ../unpack.sh
logger = logging.getLogger("aiorpc_svc")
service_name = 'aiorpc.service'


# development hack
INSTALL_PATH = Path(__file__).parent


def find_in_top_tree(name: str, _cache: Dict[str, Path] = {}) -> Path:
    if name not in _cache:
        cpath = INSTALL_PATH
        while not (cpath.parent / name).exists():
            if cpath == cpath.parent:
                raise FileExistsError(f"Can't find {name} folder in tree up from {Path(__file__).parent}")
            cpath = cpath.parent
        _cache[name] = cpath.parent / name
    return _cache[name]


def get_files_folder() -> Path:
    return find_in_top_tree(files_folder)


def get_installation_root() -> Path:
    return find_in_top_tree(root_marker).parent


def get_distribution_file_path() -> Path:
    return find_in_top_tree(distribution)


def get_file(name: str) -> Path:
    return get_files_folder() / name


@dataclass
class AIORPCServiceConfig:
    root: Path
    secrets: Path
    log_config: Path
    server_port: int
    log_level: str
    config: Path
    cmd_timeout: int
    storage: Path
    persistent_log: Optional[Path]
    persistent_log_level: Optional[str]
    listen_ip: str
    service_name: str
    service: Path
    ssl_cert: Path
    ssl_key: Path
    api_key_enc: Path
    historic_ops: Path
    historic_ops_cfg: Path
    api_key: Path
    ssl_cert_templ: Path
    max_conn_per_node: int
    max_conn_total: int
    distribution_file: Path
    raw: configparser.ConfigParser
    rraw: Any


def get_config_target_path() -> Path:
    return get_installation_root() / 'aiorpc_config.cfg'


def get_inventory_path() -> Path:
    return get_installation_root() / 'inventory'


def get_config_default_path() -> Optional[Path]:
    root_cfg = get_config_target_path()
    if root_cfg.exists():
        return root_cfg
    else:
        try:
            return get_file('config.cfg')
        except FileExistsError:
            return None


def get_config(path: Path = None) -> AIORPCServiceConfig:
    cfg = configparser.ConfigParser()

    if not path:
        path = get_config_default_path()

    if path is None or not path.exists():
        raise FileExistsError(f"Can't find config file at {path}")

    cfg.read_file(path.open())

    rcfg = RAttredDict(cfg)

    common = rcfg.common
    server = rcfg.server

    path_formatters: Dict[str, Any] = {'root': get_installation_root() if common.root == 'AUTO' else common.root}

    for name, val in [('secrets', common.secrets), ('storage', server.storage)]:
        path_formatters[name] = val.format(**path_formatters)

    def mkpath(val: str) -> Path:
        return Path(val.format(**path_formatters))

    if getattr(server, "persistent_log", None):
        persistent_log = mkpath(server.persistent_log)
        persistent_log_level = server.persistent_log_level
    else:
        persistent_log = None
        persistent_log_level = None

    return AIORPCServiceConfig(
        root=Path(path_formatters['root']),
        secrets=Path(path_formatters['secrets']),

        log_config=get_file("log_config.json"),
        server_port=int(common.server_port),
        log_level=common.log_level,
        config=path,
        cmd_timeout=int(common.cmd_timeout),

        storage=mkpath(server.storage),
        persistent_log=persistent_log,
        persistent_log_level=persistent_log_level,
        listen_ip=server.listen_ip,
        service_name=service_name,
        service=get_file(f"{service_name}"),
        ssl_cert=mkpath(server.ssl_cert),
        ssl_key=mkpath(server.ssl_key),
        api_key_enc=mkpath(server.api_key_enc),
        historic_ops=mkpath(server.historic_ops),
        historic_ops_cfg=mkpath(server.historic_ops_cfg),
        api_key=mkpath(rcfg.client.api_key),
        ssl_cert_templ=mkpath(rcfg.client.ssl_cert_templ),
        max_conn_total=int(rcfg.client.max_conn_total),
        max_conn_per_node=int(rcfg.client.max_conn_per_node),

        distribution_file=mkpath(rcfg.deploy.distribution_file),

        raw=cfg,
        rraw=rcfg
    )


def get_certificates(cert_name_template: Path) -> Dict[str, Path]:
    certificates: Dict[str, Path] = {}

    certs_folder = cert_name_template.parent
    certs_glob = cert_name_template.name

    if not certs_folder.is_dir():
        raise RuntimeError(f"Can't find cert folder at {certs_folder}")

    before_node, after_node = certs_glob.split("[node]")

    for file in certs_folder.glob(certs_glob.replace('[node]', '*')):
        node_name = file.name[len(before_node): -len(after_node)]
        certificates[node_name] = file

    return certificates


def config_logging(cfg: AIORPCServiceConfig, no_persistent: bool = False):
    log_config = json.load(cfg.log_config.open())

    if not cfg.persistent_log or no_persistent:
        del log_config['handlers']['persistent']
    else:
        log_config['handlers']['persistent']['level'] = cfg.persistent_log_level
        if not cfg.persistent_log.parent.exists():
            cfg.persistent_log.parent.mkdir(parents=True)
        log_config['handlers']['persistent']['filename'] = str(cfg.persistent_log)
        for lcfg in log_config['loggers'].values():
            lcfg['handlers'].append('persistent')

    log_config['handlers']['console']['level'] = cfg.log_level
    logging.config.dictConfig(log_config)


def get_http_conn_pool_from_cfg(cfg: AIORPCServiceConfig = None) -> ConnectionPool:
    if cfg is None:
        cfg = get_config()

    certs = get_certificates(cfg.ssl_cert_templ)
    return get_http_connection_pool(certs, cfg.api_key.open().read(),
                                    max_conn_per_node=cfg.max_conn_per_node,
                                    max_conn_total=cfg.max_conn_total)


# --------------- SSH BASED CONTROLS FUNCTIONS -------------------------------------------------------------------------


async def stop(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Stopping service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "stop", service]) for node in nodes])


async def disable(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Disabling service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "disable", service]) for node in nodes])


async def enable(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Enabling service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "enable", service]) for node in nodes])


async def start(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Starting service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "start", service]) for node in nodes])


async def remove(cfg: AIORPCServiceConfig, nodes: List[SSH]):
    logger.info(f"Removing rpc_agent from nodes {' '.join(node.node for node in nodes)}")

    try:
        await disable(cfg.service_name, nodes)
    except subprocess.SubprocessError:
        pass

    try:
        await stop(cfg.service_name, nodes)
    except subprocess.SubprocessError:
        pass

    async def runner(node: SSH) -> None:
        agent_folder = Path(cfg.root)
        service_target = SERVICE_FILE_DIR / cfg.service_name

        await node.run(["sudo", "rm", "--force", str(service_target)])
        await node.run(["sudo", "systemctl", "daemon-reload"])

        logger.info(f"Removing files from {node.node}")

        for folder in (agent_folder, cfg.storage):
            await node.run(["sudo", "rm", "--preserve-root", "--recursive", "--force", str(folder)])

    for node, val in zip(nodes, await asyncio.gather(*map(runner, nodes), return_exceptions=True)):
        if val is not None:
            assert isinstance(val, Exception)
            logger.error(f"Failed on node {node} with message: {val!s}")

    logger.info(f"Removing local config and inventory")
    if get_config_target_path().exists():
        get_config_target_path().unlink()

    if get_inventory_path().exists():
        get_inventory_path().unlink()


async def deploy(cfg: AIORPCServiceConfig, nodes: List[SSH], max_parallel_uploads: int, inventory: List[str]):
    logger.info(f"Start deploying on nodes: {' '.join(inventory)}")

    if cfg.config != get_config_target_path():
        logger.info(f"Copying config to: {get_config_target_path()}")
        get_config_target_path().open("w").write(cfg.config.open().read())

    upload_semaphore = asyncio.Semaphore(max_parallel_uploads if max_parallel_uploads else len(nodes))

    if max_parallel_uploads:
        logger.debug(f"Max uploads is set to {max_parallel_uploads}")

    cfg.secrets.mkdir(mode=0o770, parents=True, exist_ok=True)

    make_secure(cfg.api_key, cfg.api_key_enc)
    api_key, api_enc_key = get_key_enc()

    with cfg.api_key.open('w') as fd:
        fd.write(api_key)

    with cfg.api_key_enc.open('w') as fd:
        fd.write(api_enc_key)

    logger.debug(f"Api keys generated")

    async def runner(node: SSH):
        logger.debug(f"Start deploying node {node.node}")

        await node.run(["sudo", "mkdir", "--parents", cfg.root])
        await node.run(["sudo", "mkdir", "--parents", cfg.storage])

        temp_distr_file = f"/tmp/distribution_{uuid.uuid1()!s}.{cfg.distribution_file.name.split('.')[1]}"

        logger.debug(f"Copying {b2ssize(cfg.distribution_file.stat().st_size)}B of archive to {node.node}")
        async with upload_semaphore:
            await node.copy(cfg.distribution_file, temp_distr_file)

        logger.debug(f"Installing distribution and making dirs on {node.node}")
        # await node.run(["sudo", "tar", "--xz", "--extract", "--directory=" +
        #   str(cfg.root), "--file", temp_distr_file])
        await node.run(["sudo", "bash", temp_distr_file, "--install", str(cfg.root)])
        await node.run(["sudo", "chown", "--recursive", "root.root", cfg.root])
        await node.run(["sudo", "chmod", "--recursive", "o-w", cfg.root])
        await node.run(["sudo", "mkdir", "--parents", cfg.secrets])

        logger.debug(f"Generating certs for {node.node}")
        ssl_cert_file = Path(str(cfg.ssl_cert_templ).replace("[node]", node.node))
        ssl_key_file = cfg.secrets / f'key.{node.node}.tempo'
        make_secure(ssl_cert_file, ssl_key_file)

        await make_cert_and_key(ssl_key_file, ssl_cert_file,
                                f"/C=NN/ST=Some/L=Some/O=aiorpc/OU=aiorpc/CN={node.node}")

        logger.debug(f"Copying certs and keys to {node.node}")
        await node.run(["sudo", "tee", cfg.ssl_cert], input_data=ssl_cert_file.open("rb").read())
        await node.run(["sudo", "tee", cfg.ssl_key], input_data=ssl_key_file.open("rb").read())
        await node.run(["sudo", "tee", cfg.api_key_enc], input_data=api_enc_key.encode("utf8"))
        ssl_key_file.unlink()
        await node.run(["rm", temp_distr_file])

        logger.debug(f"Copying service file to {node.node}")
        service_content = cfg.service.open().read()
        service_content = service_content.replace("{INSTALL}", str(cfg.root))
        service_content = service_content.replace("{CONFIG_PATH}", str(cfg.config))

        await node.run(["sudo", "tee", f"/lib/systemd/system/{cfg.service_name}"],
                       input_data=service_content.encode())
        await node.run(["sudo", "systemctl", "daemon-reload"])
        logger.debug(f"Done with {node.node}")

    await asyncio.gather(*map(runner, nodes))
    await enable(cfg.service_name, nodes)
    await start(cfg.service_name, nodes)

    with get_inventory_path().open("w") as fd:
        fd.write("\n".join(inventory) + "\n")


# --------------- RPC BASED CONTROLS FUNCTIONS -------------------------------------------------------------------------


async def check_node(conn: IAIORPCNode, hostname: str) -> bool:
    return await conn.proxy.sys.ping("test") == 'test'


async def status(cfg: AIORPCServiceConfig, nodes: List[str]) -> None:
    ssl_certs = get_certificates(cfg.ssl_cert_templ)
    pool_am = get_http_connection_pool(ssl_certs, cfg.api_key.open().read(), cfg.max_conn_per_node,
                                       port=cfg.server_port)
    async with pool_am as pool:
        max_node_name_len = max(map(len, nodes))
        async for node_name, res in rpc_map(pool, check_node, nodes):
            if isinstance(res, Exception):
                logger.error(f"{node_name} - error: {res!s}")
            else:
                logger.info("{0:>{1}} {2:>8}".format(node_name, max_node_name_len, "RUN" if res else "NOT RUN"))
