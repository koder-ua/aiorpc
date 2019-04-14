import os
import json
import zlib
import errno
import shutil
import os.path
import tempfile
import functools
import subprocess
import distutils.spawn
from pathlib import Path
import stat as stat_module
from typing import List, Optional, Dict, Any, Tuple, Iterator, Union

from koder_utils import run_stdout

from ..plugins_api import expose_func
from ..common import IReadableAsync, ChunkedFile, ZlibStreamCompressor


expose = functools.partial(expose_func, "fs")


MAX_FILE_SIZE = 8 * (1 << 20)


@expose
def expanduser(path: str) -> str:
    return os.path.expanduser(path)


@expose
def iterdir(path: str) -> List[str]:
    return list(map(str, Path(path).iterdir()))


@expose
def get_file(path: str, compress: bool = True) -> IReadableAsync:
    fd = ChunkedFile(open(path, "rb"))
    return ZlibStreamCompressor(fd) if compress else fd


@expose
def get_file_no_stream(path: str, compress: bool = True) -> Union[IReadableAsync, bytes]:
    if os.stat(path).st_size > MAX_FILE_SIZE:
        raise ValueError("File to large for single-shot stransfer")
    fc = open(path, "rb").read()
    return zlib.compress(fc) if compress else fc


@expose
async def write_file(path: Optional[str], content: IReadableAsync, compress: bool = False) -> str:
    if path is None:
        fd, path = tempfile.mkstemp()
        os.close(fd)

    unzipobj = zlib.decompressobj() if compress else None
    with open(path, "wb") as fobj:
        async for data in content:
            if compress:
                assert unzipobj is not None  # make mypy happy
                data = unzipobj.decompress(data)
            fobj.write(data)

        if compress:
            assert unzipobj is not None  # make mypy happy
            fobj.write(unzipobj.flush())

    return path


@expose
def file_exists(path: str) -> bool:
    return os.path.exists(path)


@expose
def rmtree(path: str):
    shutil.rmtree(path)


@expose
def makedirs(path: str):
    os.makedirs(path)


@expose
def unlink(path: str):
    os.unlink(path)


@expose
async def which(name: str) -> Optional[str]:
    try:
        return await run_stdout(["which", name])
    except subprocess.CalledProcessError:
        return None


def fall_down(node: Dict, root: str, res_dict: Dict[str, str]):
    if 'mountpoint' in node and node['mountpoint']:
        res_dict[node['mountpoint']] = root

    for ch_node in node.get('children', []):
        fall_down(ch_node, root, res_dict)


async def get_mountpoint_to_dev_mapping() -> Dict[str, str]:
    lsblk = json.loads(await run_stdout(["lsblk", '-a', '--json']))
    res: Dict[str, str] = {}
    for node in lsblk['blockdevices']:
        fall_down(node, node['name'], res)
    return res


def follow_symlink(fname: str) -> str:
    while os.path.islink(fname):
        dev_link_next = os.readlink(fname)
        dev_link_next = os.path.join(os.path.dirname(fname), dev_link_next)
        fname = os.path.abspath(dev_link_next)
    return fname


async def get_mounts() -> Dict[str, Tuple[str, str]]:
    lsblk = json.loads(await run_stdout("lsblk --json"))

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


@expose
async def get_dev_and_partition(fname: str) -> Tuple[str, str]:
    mounts = await get_mounts()
    return mounts[str(find_mount_point(Path(fname)))]


@expose
def binarys_exists(names: List[str]) -> List[bool]:
    return [(distutils.spawn.find_executable(name) is not None) for name in names]


@expose
def find_pids_for_cmd(bname: str, find_binary: bool = True) -> List[int]:
    bin_path = distutils.spawn.find_executable(bname) if find_binary else bname

    if not bin_path:
        raise NameError(f"Can't found binary path for {bname!r}")

    # follow all symlinks & other
    bin_path = str(Path(bin_path).resolve())

    res = []
    for name in os.listdir('/proc'):
        if name.isdigit() and os.path.isdir(f'/proc/{name}'):
            exe = f'/proc/{name}/exe'
            if os.path.exists(exe) and os.path.islink(exe) and bin_path == os.readlink(exe):
                res.append(int(name))

    return res


@expose
def get_block_devs_info(filter_virtual: bool = True) -> Dict[str, Tuple[bool, str]]:
    res = {}
    for name in os.listdir("/sys/block"):
        rot_fl = f"/sys/block/{name}/queue/rotational"
        sched_fl = f"/sys/block/{name}/queue/scheduler"
        if os.path.isfile(rot_fl) and os.path.isfile(sched_fl):
            if filter_virtual and follow_symlink(f'/sys/block/{name}').startswith('/sys/devices/virtual'):
                continue
            res[name] = (
                open(rot_fl).read().strip() == 1,
                open(sched_fl).read().strip()
            )
    return res


@expose
def stat(path: str) -> List[int]:
    return list(os.stat(path))  # type: ignore


@expose
def stat_all(paths: List[str]) -> List[List[int]]:
    return list(map(stat, paths))


@expose
def count_sockets_for_process(pid: int) -> int:
    count = 0
    for fd in os.listdir(f'/proc/{pid}/fd'):
        try:
            if stat_module.S_ISSOCK(os.stat(f'/proc/{pid}/fd/{fd}').st_mode):
                count += 1
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    return count


@expose
def tail(path: str, bytes: int) -> IReadableAsync:
    fd = open(path, 'rb')
    fd.seek(bytes, os.SEEK_END)
    return ChunkedFile(fd)
