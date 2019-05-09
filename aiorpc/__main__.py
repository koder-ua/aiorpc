import sys
import asyncio
import getpass
import argparse
from pathlib import Path
from typing import Any, List

from koder_utils import SSH, read_inventory

from . import get_key_enc, start_rpc_server, configure
from .service import (get_installation_root, get_config_default_path, get_config, config_logging, status,
                      get_inventory_path, deploy, start, stop, remove)


def parse_args(argv: List[str]) -> Any:
    try:
        inst_root = get_installation_root()
        cfg_def_path = get_config_default_path()
    except RuntimeError:
        inst_root = None
        cfg_def_path = None

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest='subparser_name')

    deploy_parser = subparsers.add_parser('install', help='Deploy agent on nodes from inventory')
    deploy_parser.add_argument("--max-parallel-uploads", default=0, type=int,
                               help="Max parallel archive uploads to target nodes (default: %(default)s)")
    if inst_root:
        deploy_parser.add_argument("--target", metavar='TARGET_FOLDER', default=inst_root,
                                   help="Path to deploy agent to on target nodes (default: %(default)s)")
    else:
        deploy_parser.add_argument("--target", metavar='TARGET_FOLDER', required=True,
                                   help="Path to deploy agent to on target nodes (default: %(default)s)")

    deploy_parser.add_argument("--inventory", metavar='INVENTORY_FILE', required=True, type=Path,
                               help="Path to file with list of ssh ip/names of ceph nodes")

    stop_parser = subparsers.add_parser('stop', help='Stop daemons')
    start_parser = subparsers.add_parser('start', help='Start daemons')
    remove_parser = subparsers.add_parser('uninstall', help='Remove service')

    for sbp in (deploy_parser, start_parser, stop_parser, remove_parser):
        sbp.add_argument("--ssh-user", metavar='SSH_USER', default=getpass.getuser(),
                         help="SSH user, (default: %(default)s)")

    status_parser = subparsers.add_parser('status', help='Show daemons statuses')
    server = subparsers.add_parser('server', help='Run web server')
    subparsers.add_parser('gen_key', help='Generate new key')

    for sbp in (deploy_parser, start_parser, stop_parser, status_parser, remove_parser, server):
        if cfg_def_path:
            sbp.add_argument("--config", metavar='CONFIG_FILE', default=cfg_def_path,
                             type=Path, help="Config file path (default: %(default)s)")
        else:
            sbp.add_argument("--config", metavar='CONFIG_FILE', required=True,
                             help="Config file path (default: %(default)s)")
        sbp.add_argument("--install-root", default=None, help="Dev hack, dont use it")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)

    # DEV HACK
    if opts.install_root:
        from . import service
        service.INSTALL_PATH = opts.install_root

    cfg = get_config(opts.config)
    config_logging(cfg, no_persistent=True)


    if opts.subparser_name == 'status':
        inventory = read_inventory(get_inventory_path())
        asyncio.run(status(cfg, inventory))
    elif opts.subparser_name in {'install', 'start', 'stop', 'uninstall'}:
        if opts.subparser_name == 'install':
            inventory = read_inventory(opts.inventory)
        else:
            inventory = read_inventory(get_inventory_path())

        nodes = [SSH(name_or_ip, ssh_user=opts.ssh_user) for name_or_ip in inventory]

        if opts.subparser_name == 'install':
            asyncio.run(deploy(cfg, nodes, max_parallel_uploads=opts.max_parallel_uploads, inventory=inventory))
        elif opts.subparser_name == 'start':
            asyncio.run(start(cfg.service_name, nodes))
        elif opts.subparser_name == 'stop':
            asyncio.run(stop(cfg.service_name, nodes))
        elif opts.subparser_name == 'uninstall':
            asyncio.run(remove(cfg, nodes))
        else:
            assert False, f"Unknown command {opts.subparser_name}"
    elif opts.subparser_name == 'server':
        configure(historic_ops=cfg.historic_ops, historic_ops_cfg=cfg.historic_ops_cfg)
        start_rpc_server(ip=cfg.listen_ip,
                         ssl_cert=cfg.ssl_cert,
                         ssl_key=cfg.ssl_key,
                         api_key_enc=cfg.api_key_enc.open().read(),
                         port=cfg.server_port)
    elif opts.subparser_name == 'gen_key':
        key, enc_key = get_key_enc()
        print(f"Key={key}\nenc_key={enc_key}")
    else:
        assert False, f"Unknown command {opts.subparser_name}"
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
