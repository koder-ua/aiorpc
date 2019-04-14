import asyncio
import contextlib
import functools
import json
import os
import subprocess
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Iterable, Dict, Tuple, List, Any, Iterator

import pytest
from aiohttp.web_runner import AppRunner, TCPSite

from aiorpc import connect_http, rpc, get_http_connection_pool, ConnectionPool
from aiorpc.server import handle_rpc
from aiorpc.aiohttp_transport import AIOHttpTransportServer


server_addr = 'localhost'
PORT = 55668

TEST_FOLDER = Path(__file__).parent
SSL_CERT = Path(TEST_FOLDER / 'ssl.crt')
SSL_KEY = Path(TEST_FOLDER / 'ssl.key')
API_KEY_ENC = Path(TEST_FOLDER / 'api_key.enc').open().read()
API_KEY = Path(TEST_FOLDER / 'api.key').open().read()
LARGE_FILE_PATH = "/home/koder/Downloads/ops.tar.gz"


@contextlib.asynccontextmanager
async def start_server() -> Iterable[None]:
    handler = functools.partial(handle_rpc, serializer=rpc.JsonSerializer(), bstream=rpc.SimpleBlockStream())
    server = AIOHttpTransportServer(on_server_startup=[],
                                    on_server_shutdown=[],
                                    process_request=handler,
                                    ip=server_addr,
                                    port=PORT,
                                    ssl_cert=SSL_CERT,
                                    ssl_key=SSL_KEY,
                                    api_key_enc=API_KEY_ENC,
                                    settings={"serializer": "json", "bstream": "simple"})

    app = server.make_app()
    runner = AppRunner(app)
    await runner.setup()
    site = TCPSite(runner, server_addr, PORT, ssl_context=server.make_ssl_context())
    await site.start()
    try:
        yield
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_connect():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            pass


@pytest.mark.asyncio
async def test_connect():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            assert await conn.proxy.sys.ping("pong") == "pong"


@pytest.fixture
async def conn_pool_32():
    return get_http_connection_pool(ssl_certs={server_addr: SSL_CERT}, api_key=API_KEY, max_conn_per_node=32,
                                    port=PORT)


@pytest.fixture
async def conn_pool_2():
    return get_http_connection_pool(ssl_certs={server_addr: SSL_CERT}, api_key=API_KEY, max_conn_per_node=2,
                                    port=PORT)


@pytest.mark.asyncio
async def test_read():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            expected_data = SSL_CERT.open("rb").read()

            data = await conn.read(SSL_CERT, compress=False)
            assert data == expected_data

            data = await conn.read(SSL_CERT, compress=True)
            assert data == expected_data

            data = await conn.read(SSL_CERT)
            assert data == expected_data

            with SSL_CERT.open('rb') as fd:
                async for block in conn.iter_file(SSL_CERT, compress=True):
                    assert fd.read(len(block)) == block
                assert fd.read() == b''

            with SSL_CERT.open('rb') as fd:
                async for block in conn.iter_file(SSL_CERT, compress=False):
                    assert fd.read(len(block)) == block
                assert fd.read() == b''

            with SSL_CERT.open('rb') as fd:
                async for block in conn.iter_file(SSL_CERT):
                    assert fd.read(len(block)) == block
                assert fd.read() == b''


@pytest.mark.asyncio
async def test_read_large():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            with open(LARGE_FILE_PATH, 'rb') as fd:
                async for chunk in conn.iter_file(LARGE_FILE_PATH, compress=False):
                    assert fd.read(len(chunk)) == chunk

                assert fd.read() == b''


@pytest.mark.asyncio
async def test_write():
    data = b'-' * 100_000
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            with tempfile.NamedTemporaryFile() as fl:
                await conn.write(fl.name, data, compress=False)
                assert data == fl.file.read()

            assert not Path(fl.name).exists()

            with tempfile.NamedTemporaryFile() as fl:
                await conn.write(fl.name, data, compress=True)
                assert data == fl.file.read()

            assert not Path(fl.name).exists()

            with tempfile.NamedTemporaryFile() as fl:
                await conn.write(fl.name, data)
                assert data == fl.file.read()

            assert not Path(fl.name).exists()

            with tempfile.NamedTemporaryFile() as src:
                src.file.write(data)
                src.file.seek(0, os.SEEK_SET)
                with tempfile.NamedTemporaryFile() as dst:
                    await conn.write(dst.name, src.file)
                    assert data == dst.file.read()

            assert not Path(dst.name).exists()
            assert not Path(src.name).exists()

            with tempfile.NamedTemporaryFile() as dst:
                await conn.write(dst.name, BytesIO(data))
                assert data == dst.file.read()

            assert not Path(dst.name).exists()


@pytest.mark.asyncio
async def test_write_temp():
    data = b'-' * 100_000
    tmpdirlist = os.listdir('/tmp')

    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            fpath = await conn.write_tmp(data, compress=False)

    assert fpath.open('rb').read() == data
    assert str(fpath.parent) == '/tmp'
    assert fpath.name not in tmpdirlist
    fpath.unlink()


@pytest.mark.asyncio
async def test_write_large():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            with open(LARGE_FILE_PATH, 'rb') as src:
                with tempfile.NamedTemporaryFile() as dst:
                    await conn.write(dst.name, src, compress=False)

                    chunk = 1 << 20
                    src.seek(0, os.SEEK_SET)

                    while True:
                        data = src.read(chunk)
                        assert data == dst.file.read(chunk)
                        if not data:
                            break


@pytest.mark.asyncio
async def test_fs_utils():
    exists = '/'
    not_exists = '/this_folder_does_not_exists'
    assert Path(exists).exists()
    assert not Path(not_exists).exists()

    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            assert await conn.exists(exists)
            assert not (await conn.exists(not_exists))

            assert await conn.exists(Path(exists))
            assert not (await conn.exists(Path(not_exists)))

            assert list(await conn.stat(exists)) == list(os.stat(exists))
            assert sorted(list(await conn.iterdir(exists))) == sorted(list(Path(exists).iterdir()))

            assert list(await conn.stat(exists)) == list(os.stat(exists))
            assert sorted(list(await conn.iterdir(exists))) == sorted(list(Path(exists).iterdir()))


@pytest.mark.asyncio
async def test_lsdir_parallel(conn_pool_32: ConnectionPool):
    folders = ['/usr/bin', '/usr/lib', '/usr/local/lib', '/bin', '/sbin', '/run', '/var/lib', '/var/run',
               '/etc', '/boot', '/lib']

    async with start_server():
        async with conn_pool_32:
            async def loader(path: str, loops: int = 100):
                expected = sorted(list(Path(path).iterdir()))
                async with conn_pool_32.connection(server_addr) as conn:
                    for _ in range(loops):
                        assert sorted(list(await conn.iterdir(path))) == expected

            await asyncio.gather(*map(loader, folders))


@pytest.mark.asyncio
async def test_lsdir_parallel_max2(conn_pool_2: ConnectionPool):
    folders = ['/usr/bin', '/usr/lib', '/usr/local/lib', '/bin', '/bin']

    curr_count = 0
    max_count = 0

    async with start_server():
        async with conn_pool_2:
            async def loader(path: str, loops: int = 10):
                expected = sorted(list(Path(path).iterdir()))
                async with conn_pool_2.connection(server_addr) as conn:
                    nonlocal max_count
                    nonlocal curr_count
                    curr_count += 1
                    max_count = max(max_count, curr_count)
                    for _ in range(loops):
                        assert sorted(list(await conn.iterdir(path))) == expected
                    curr_count -= 1

            await asyncio.gather(*map(loader, folders))

        assert max_count == 2


@pytest.mark.asyncio
async def test_run():
    expected_res = subprocess.run('ls -1 /', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert expected_res.returncode == 0

    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            for cmd in ['ls -1 /', ['ls', '-1', '/'], ['ls', '-1', Path('/')]]:
                ps_result = await conn.run(cmd)
                assert ps_result.returncode == 0
                ps_result.check_returncode()
                assert ps_result.stderr_b is None
                assert ps_result.stdout_b.decode() == ps_result.stdout
                assert ps_result.args == cmd
                assert expected_res.stdout.decode() == ps_result.stdout

            ps_result = await conn.run("ls -1 /", compress=False)
            assert ps_result.returncode == 0
            ps_result.check_returncode()
            assert ps_result.stderr_b is None
            assert ps_result.stdout_b.decode() == ps_result.stdout
            assert ps_result.args == "ls -1 /"
            assert expected_res.stdout.decode() == ps_result.stdout


@pytest.mark.asyncio
async def test_run_issues():
    not_existed_exe = 'this-cmd-does-not-exists-1241414515415'
    fails_with_code_1 = 'exit 1'
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            with pytest.raises(FileNotFoundError):
                await conn.run([not_existed_exe])

            ps_result = await conn.run(not_existed_exe)
            assert ps_result.returncode != 0

            ps_result = await conn.run(fails_with_code_1)
            assert ps_result.returncode == 1


@pytest.mark.asyncio
async def test_run_input():
    expected_res = subprocess.run('ls -1 /', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert expected_res.returncode == 0
    cmd = 'ls -1 /'

    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            for proc in ["bash", ["bash"]]:
                ps_result = await conn.run(proc, input_data=cmd.encode())
                assert ps_result.returncode == 0
                assert ps_result.stderr_b is None
                assert ps_result.args == proc
                assert expected_res.stdout.decode() == ps_result.stdout


@pytest.mark.asyncio
async def test_run_timeout():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            for proc in [["sleep", "3600"], "sleep 3600"]:
                coro = conn.run(proc, timeout=0.5, term_timeout=0.1)
                done, not_done = await asyncio.wait([coro], timeout=2)
                assert done
                task, = done
                with pytest.raises(subprocess.TimeoutExpired):
                    await task


@pytest.mark.asyncio
async def test_large_transfer(conn_pool_32: ConnectionPool):
    with tempfile.NamedTemporaryFile() as dst:
        dt = ("-" * 150 + "\n").encode() * 100
        for _ in range(100):
            dst.file.write(dt)
        dst.file.flush()

    async def read_coro():
        async with conn_pool_32.connection(server_addr) as rpc_node:
            for _ in range(10):
                get_dt = await rpc_node.run(f"tail -n 10000 {dst.name}", timeout=15, term_timeout=1)
                assert get_dt.returncode == 0
                assert len(get_dt.stdout_b) == 100 * len(dt)

    async with start_server():
        coros = [read_coro() for _ in range(10)]
        await asyncio.wait(coros)


@pytest.mark.asyncio
async def test_python27():
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            for proc in [["python2.7", "-c", "import sys; print sys.version_info"],
                         "python2.7 -c 'import sys; print sys.version_info'"]:

                ps_result = await conn.run(proc)
                assert ps_result.returncode == 0
                assert ps_result.stderr_b is None
                assert ps_result.args == proc
                assert "sys.version_info(major=2, minor=7, " in ps_result.stdout


@pytest.mark.asyncio
async def test_run_environ():
    var_name = "TEST_VAR"
    var_val = "TEST_VALUE"
    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            env = await conn.proxy.cli.environ()

            assert var_name not in env
            assert 'PATH' in env

            for proc in [["python2.7", "-c", "import os; print os.environ['TEST_VAR']"]]:
                ps_result = await conn.run(proc, merge_err=False)
                assert ps_result.returncode != 0
                assert "KeyError: 'TEST_VAR'" in ps_result.stderr_b.decode()

            new_env = env.copy()
            new_env[var_name] = var_val
            for proc in [["python2.7", "-c", "import os; print os.environ['TEST_VAR']"], "echo $TEST_VAR"]:
                ps_result = await conn.run(proc, env=new_env, merge_err=False)
                assert ps_result.returncode == 0
                assert var_val == ps_result.stdout.strip()


def get_mounts() -> Dict[str, Tuple[str, str]]:
    lsblk = json.loads(subprocess.check_output("lsblk --json", shell=True))

    def iter_mounts(curr: List[Dict[str, Any]], parent_device: str = None) -> Iterator[Tuple[str, str, str]]:
        for dev_info in curr:
            name = parent_device if parent_device else dev_info["name"]
            if dev_info.get("mountpoint") is not None:
                yield name, dev_info["name"], dev_info["mountpoint"]

            if 'children' in dev_info:
                yield from iter_mounts(dev_info['children'], name)

    return {mp: ('/dev/' + dev,  '/dev/' + partition) for dev, partition, mp in iter_mounts(lsblk["blockdevices"])}


def find_mount_point(path: Path) -> Path:
    path = path.resolve()
    while not os.path.ismount(path):
        assert str(path) != '/'
        path = path.parent
    return path


@pytest.mark.asyncio
async def test_other_fs():
    mounts = get_mounts()

    async with start_server():
        async with connect_http(server_addr, ssl_cert=SSL_CERT, api_key=API_KEY, port=PORT) as conn:
            with tempfile.NamedTemporaryFile() as fl:
                expected_device, expected_partition = mounts[str(find_mount_point(Path(fl.name)))]
                device, partition = await conn.proxy.fs.get_dev_and_partition(fl.name)
                assert Path(device).is_block_device()
                assert Path(partition).is_block_device()
                assert expected_partition == partition
                assert expected_device == device

                assert len(await conn.proxy.fs.find_pids_for_cmd('zsh', find_binary=True)) > 0
                assert (await conn.proxy.fs.count_sockets_for_process(os.getpid())) > 0

                for dev_name in (await conn.proxy.fs.get_block_devs_info()):
                    assert Path(f"/dev/{dev_name}").is_block_device()
