import os
import abc
import json

import time
import asyncio
import os.path
import functools
import subprocess
import collections
from pathlib import Path
from dataclasses import dataclass, field
from typing import (Iterator, List, Dict, Tuple, Any, Callable, Set, Iterable, Optional, AsyncIterator, Awaitable,
                    BinaryIO, cast)

from koder_utils import LocalHost, b2ssize
from cephlib import (RecId, CephCLI, CephOp, ParseResult, RecordFile, CephHealth, iter_log_messages, iter_ceph_logs_fd,
                     CephRelease, OpRec, IPacker, get_historic_packer, get_ceph_version)

from .. import (logger, expose_func, expose_type, IReadableAsync, ChunkedFile, register_startup, register_shutdown,
                ConfigVar)


expose = functools.partial(expose_func, "ceph")


historic_ops_file = ConfigVar[Path]('historic_ops', Path)
historic_ops_cfg_file = ConfigVar[Path]('historic_ops_cfg', Path)


class NoPoolFound(Exception):
    pass


FileRec = Tuple[RecId, Any]
BinaryFileRec = Tuple[RecId, bytes]
BinInfoFunc = Callable[[], Awaitable[Iterable[FileRec]]]

GiB = 1 << 30
MiB = 1 << 20
DEFAULT_MAX_REC_FILE_SIZE = GiB
DEFAULT_MIN_DEVICE_FREE = 50 * GiB
DEFAULT_SIZE = 20
DEFAULT_DURATION = 600


@expose_type
@dataclass
class HistoricCollectionConfig:
    osd_ids: List[int]
    size: int
    duration: int
    ceph_extra_args: List[str]
    collection_end_time: float
    min_duration: Optional[int] = 50
    dump_unparsed_headers: bool = False
    pg_dump_timeout: Optional[int] = None
    extra_cmd: List[str] = field(default_factory=list)
    extra_dump_timeout: Optional[int] = None
    max_record_file: int = DEFAULT_MAX_REC_FILE_SIZE
    min_device_free: int = DEFAULT_MIN_DEVICE_FREE
    packer_name: str = 'compact'
    cmd_timeout: float = 50
    stats_keep_cycles: int = 10

    def __str__(self) -> str:
        attrs = "\n     ".join(f"{name}: {getattr(self, name)!r}" for name in self.__dataclass_fields__)  # type: ignore
        return f"{self.__class__.__name__}:\n    {attrs}"


@expose_type
@dataclass
class HistoricCollectionStatus:
    cfg: Optional[HistoricCollectionConfig]
    path: str
    file_size: int
    disk_free_space: int


def almost_sorted_ceph_log_messages(sort_buffer_size: int) -> Iterator[List[Tuple[float, CephHealth]]]:
    all_messages: List[Tuple[float, CephHealth]] = []
    for fd in iter_ceph_logs_fd():
        for message in iter_log_messages(fd):
            all_messages.append(message)
            if len(all_messages) > sort_buffer_size:
                all_messages.sort()
                yield all_messages[:sort_buffer_size // 2]
                del all_messages[:sort_buffer_size // 2]
    yield all_messages


@expose
def find_issues_in_ceph_log(max_lines: int = 100000, max_issues: int = 100) -> str:
    errs_warns = []
    for idx, ln in enumerate(open("/var/log/ceph/ceph.log")):
        if idx == max_lines:
            break
        if 'cluster [ERR]' in ln or "cluster [WRN]" in ln:
            errs_warns.append(ln)
            if len(errs_warns) == max_issues:
                break
    return "".join(errs_warns[-max_lines:])


#  Don't using namedtuples/classes to simplify serialization
@expose
def analyze_ceph_logs_for_issues(sort_buffer_size: int = 10000) \
        -> Tuple[Dict[str, int], List[Tuple[bool, float, float]]]:

    error_per_type: Dict[CephHealth, int] = collections.Counter()
    status_ranges: List[Tuple[bool, float, float]] = []
    currently_healthy = False
    region_started_at: float = 0.0

    utc = None
    for all_messages in almost_sorted_ceph_log_messages(sort_buffer_size):
        for utc, mess_id in all_messages:
            if region_started_at < 1.0:
                region_started_at = utc
                currently_healthy = mess_id == CephHealth.HEALTH_OK
                continue

            if mess_id != CephHealth.HEALTH_OK:
                error_per_type[mess_id] += 1
                if currently_healthy:
                    status_ranges.append((True, region_started_at, utc))
                    region_started_at = utc
                    currently_healthy = False
            elif not currently_healthy:
                status_ranges.append((False, region_started_at, utc))
                region_started_at = utc
                currently_healthy = True

    if utc and utc != region_started_at:
        status_ranges.append((currently_healthy, region_started_at, utc))

    return {key.name: val for key, val in error_per_type.items()}, status_ranges


class Recorder(metaclass=abc.ABCMeta):
    def __init__(self, cli: CephCLI, cfg: HistoricCollectionConfig,
                 record_file: Optional[RecordFile], packer: Optional[IPacker]) -> None:
        assert record_file
        assert packer

        self.cli = cli
        self.cfg = cfg
        self.record_file = record_file
        self.packer = packer

    async def start(self) -> None:
        pass

    @abc.abstractmethod
    async def cycle(self) -> None:
        pass

    async def close(self) -> None:
        await self.cycle()


class DumpHistoric(Recorder):
    def __init__(self, cli: CephCLI, cfg: HistoricCollectionConfig,
                 record_file: Optional[RecordFile], packer: Optional[IPacker]) -> None:
        Recorder.__init__(self, cli, cfg, record_file, packer)
        self.osd_ids = self.cfg.osd_ids.copy()
        self.not_inited_osd: Set[int] = set(self.cfg.osd_ids)
        self.pools_map: Dict[int, Tuple[str, int]] = {}
        self.pools_map_no_name: Dict[int, int] = {}
        self.last_time_ops: Set[str] = set()

    async def reload_pools(self) -> Optional[FileRec]:
        pools = await self.cli.get_pools()

        new_pools_map: Dict[int, Tuple[str, int]] = {}
        for idx, (pool_id, pool_name) in enumerate(sorted(pools.items())):
            new_pools_map[pool_id] = pool_name, idx

        if new_pools_map != self.pools_map:
            self.pools_map = new_pools_map
            self.pools_map_no_name = {num: idx for num, (_, idx) in new_pools_map.items()}
            return RecId.pools, self.pools_map
        return None

    async def dump_historic(self) -> AsyncIterator[FileRec]:
        ctime = int(time.time())
        curr_not_inited = self.not_inited_osd
        self.not_inited_osd = set()
        for osd_id in curr_not_inited:
            if not await self.cli.set_history_size_duration(osd_id, self.cfg.size, self.cfg.duration):
                self.not_inited_osd.add(osd_id)

        new_rec = await self.reload_pools()
        if new_rec:
            # pools updated - skip this cycle, as different ops may came from pools before and after update
            yield new_rec
        else:
            prev_ops = self.last_time_ops
            self.last_time_ops = set()
            for osd_id in set(self.osd_ids).difference(self.not_inited_osd):
                try:
                    parsed = await self.cli.get_historic(osd_id)
                except (subprocess.CalledProcessError, OSError):
                    self.not_inited_osd.add(osd_id)
                    continue

                if self.cfg.size != parsed['size'] or self.cfg.duration != parsed['duration']:
                    self.not_inited_osd.add(osd_id)
                    continue

                ops: List[CephOp] = []

                for op in self.parse_historic_records(parsed['ops']):
                    if op.tp is not None and op.description not in prev_ops:
                        assert op.pack_pool_id is None
                        op.pack_pool_id = self.pools_map_no_name[op.pool_id]
                        ops.append(op)

                self.last_time_ops.update(op.description for op in ops)
                yield (RecId.ops, (osd_id, ctime, ops))

    def parse_historic_records(self, ops: List[OpRec]) -> Iterator[CephOp]:
        for raw_op in ops:
            if self.cfg.min_duration and int(raw_op.get('duration') * 1000) < self.cfg.min_duration:
                continue
            try:
                parse_res, ceph_op = CephOp.parse_op(raw_op)
                if ceph_op:
                    yield ceph_op
                elif parse_res == ParseResult.unknown:
                    pass
            except Exception:
                pass

    async def cycle(self) -> None:
        total_size = 0
        async for rec_id, data in self.dump_historic():
            if self.record_file:
                rec = self.packer.pack_record(rec_id, data)
                if rec:
                    total_size += len(rec[1])
                    self.record_file.write_record(*rec, flush=False)

        if self.record_file:
            self.record_file.flush()

        logger.debug(f"Dump osd provides {b2ssize(total_size)}B")

    async def close(self) -> None:
        await self.cycle()
        for osd_id in self.cfg.osd_ids:
            await self.cli.set_history_size_duration(osd_id, DEFAULT_SIZE, DEFAULT_DURATION)


class DumpPGDump(Recorder):
    async def cycle(self) -> None:
        data = (await self.cli.run_json_raw("pg dump")).strip()
        if data.startswith("dumped all"):
            data = data.replace("dumped all", "", 1).lstrip()
        rec = self.packer.pack_record(RecId.pgdump, data)
        if rec:
            self.record_file.write_record(*rec)
        logger.debug(f"Pg dump provides {b2ssize(len(rec[1]))}B")


class InfoDumper(Recorder):
    async def cycle(self) -> None:
        logger.debug(f"Run cluster info: {self.cfg.extra_cmd}")
        output = {'time': int(time.time())}

        for cmd in self.cfg.extra_cmd:
            try:
                output[cmd] = await self.cli.run_no_ceph(cmd)
            except subprocess.SubprocessError as exc:
                logger.error("Cmd failed: %s", exc)

        if len(output) > 1:
            rec = self.packer.pack_record(RecId.cluster_info, output)
            if rec:
                self.record_file.write_record(*rec)
            logger.debug(f"Cluster info provides {b2ssize(len(rec[1]))}B")


class CephHistoricDumper:
    def __init__(self, release: CephRelease,
                 record_file_path: Path,
                 collection_config: HistoricCollectionConfig) -> None:
        self.release = release
        self.record_file_path = record_file_path
        self.cfg = collection_config
        self.historic: Optional[DumpHistoric] = None

        self.cli = CephCLI(node=None, extra_params=self.cfg.ceph_extra_args, timeout=collection_config.cmd_timeout,
                           release=self.release)

        self.packer: IPacker = get_historic_packer(self.cfg.packer_name)
        if not self.record_file_path.exists():
            self.record_file_path.parent.mkdir(parents=True, exist_ok=True)
            with self.record_file_path.open("wb"):
                pass
        self.record_fd = self.record_file_path.open("r+b")
        self.record_file = RecordFile(self.record_fd)
        if self.record_file.prepare_for_append(truncate_invalid=True):
            logger.error(f"Records file broken at offset {self.record_file.tell()}, truncated to last valid record")

        self.exit_evt = asyncio.Event()
        self.active_loops_tasks: Set[Awaitable] = set()

    def start(self) -> None:
        assert not self.active_loops_tasks
        self.historic = DumpHistoric(self.cli, self.cfg, self.record_file, self.packer)

        recorders = [(self.cfg.duration, self.historic)]

        info_dumper = InfoDumper(self.cli, self.cfg, self.record_file, self.packer)
        pg_dumper = DumpPGDump(self.cli, self.cfg, self.record_file, self.packer)
        recorders.extend([(self.cfg.extra_dump_timeout, info_dumper), (self.cfg.pg_dump_timeout, pg_dumper)])

        self.active_loops_tasks = {asyncio.create_task(self.loop(timeout, recorder)) for timeout, recorder in recorders}

    def get_free_space(self) -> int:
        vstat = os.statvfs(str(self.record_file_path))
        return vstat.f_bfree * vstat.f_bsize

    def check_recording_allowed(self) -> bool:
        assert self.cfg

        disk_free = self.get_free_space()
        if disk_free <= self.cfg.min_device_free:
            logger.warning("Stop recording due to disk free space %s less then minimal %s",
                           b2ssize(disk_free), b2ssize(self.cfg.min_device_free))
            return False

        if self.record_file.tell() >= self.cfg.max_record_file:
            logger.warning("Stop recording due to record file too large - %s, while %s is a limit",
                           b2ssize(self.record_file.tell()), b2ssize(self.cfg.max_record_file))
            return False

        if time.time() >= self.cfg.collection_end_time:
            logger.warning("Stop recording due record time expired")
            return False

        return True

    async def stop(self, timeout=60) -> bool:
        self.exit_evt.set()
        _, self.active_loops_tasks = await asyncio.wait(self.active_loops_tasks, timeout=timeout)  # type: ignore

        if not self.active_loops_tasks:
            self.record_file.close()
            self.record_fd.close()

        return not self.active_loops_tasks

    async def loop(self, timeout: Optional[float], recorder: Recorder) -> None:

        if timeout is None:
            return

        exit_requested = False

        try:
            next_run: float = time.time()

            await recorder.start()

            while True:
                sleep_for = next_run - time.time()

                if sleep_for > 0:
                    try:
                        await asyncio.wait_for(self.exit_evt.wait(), timeout=sleep_for)
                        exit_requested = True
                    except asyncio.TimeoutError:
                        pass

                if exit_requested:
                    logger.debug(f"Stopping loop for {recorder.__class__.__name__}")
                    await recorder.close()
                    break

                if not self.check_recording_allowed():
                    break

                await recorder.cycle()
                next_run = time.time() + timeout
        except asyncio.CancelledError:
            logger.warning(f"Loop for {recorder.__class__.__name__} canceled")
            raise
        except Exception:
            logger.exception(f"In loop {recorder.__class__.__name__}")
            raise
        finally:
            logger.info(f"Exit loop {recorder.__class__.__name__}")


dumper: Optional[CephHistoricDumper] = None


@expose
async def start_historic_collection(historic_config: HistoricCollectionConfig, save: bool = True) -> None:
    global dumper
    assert dumper is None, "Collection already running"

    version = await get_ceph_version(LocalHost(), extra_args=historic_config.ceph_extra_args)
    historic_ops = historic_ops_file()
    if not historic_ops.parent.exists():
        historic_ops.parent.mkdir(parents=True)

    hc = str(historic_config).replace('\n', '\n    ')
    logger.info(f"Start historic collection with config:\n    {hc}")
    dumper = CephHistoricDumper(version.release, historic_ops, historic_config)
    dumper.start()
    cfg_path = historic_ops_cfg_file()

    if save:
        with cfg_path.open("w") as fd:
            logger.info(f"Storing historic config to {cfg_path}")
            fd.write(json.dumps(historic_config.__dict__))


@expose
async def stop_historic_collection(not_err: bool = False) -> None:
    global dumper
    if not dumper:
        if not_err:
            return
        assert False, "Not running"

    cfg_path = historic_ops_cfg_file()
    if cfg_path.exists:
        cfg_path.unlink()

    assert await dumper.stop(), "Not all loops finised successfully"
    dumper = None


@expose
async def remove_historic_data() -> None:
    assert not dumper, "Collection running. Stop first"
    historic_ops_file().unlink()


@expose
def get_historic_collection_status() -> HistoricCollectionStatus:
    historic_ops = historic_ops_file()
    record_cfg = None if not dumper else dumper.cfg

    try:
        vstat = os.statvfs(str(historic_ops))
        free = vstat.f_bfree * vstat.f_bsize
    except OSError:
        free = 0

    size = 0
    try:
        if historic_ops.exists():
            size = historic_ops.stat().st_size
    except OSError:
        pass

    return HistoricCollectionStatus(record_cfg,
                                    str(historic_ops),
                                    disk_free_space=free,
                                    file_size=size)


@expose
def get_collected_historic_data(offset: int, size: int = None) -> IReadableAsync:
    historic_ops = historic_ops_file()
    assert historic_ops.exists(), f"File {historic_ops} with ops not found"
    rfd = cast(BinaryIO, historic_ops.open("rb"))

    if offset:
        rfd.seek(offset)

    return ChunkedFile(rfd,
                       close_at_the_end=True,
                       till_offset=offset + size if size is not None else None)


@register_startup
async def restore_collection(_: Any):
    cfg_path = historic_ops_cfg_file()

    if cfg_path.exists():
        try:
            historic_config_dct = json.load(cfg_path.open())
            historic_config = HistoricCollectionConfig(**historic_config_dct)
        except:
            logger.exception(f"Can't load historic config from {cfg_path}")
            return

        await start_historic_collection(historic_config, save=False)


@register_shutdown
async def stop_collection(_: Any):
    await stop_historic_collection(not_err=True)


@expose
async def configure_historic(osd_ids: List[int],
                             size: int,
                             duration: float,
                             ceph_extra_args: List[str],
                             cmd_timeout: float,
                             release_i: int) -> Tuple[List[int], Dict[int, Tuple[int, float]]]:
    cli = CephCLI(node=None, extra_params=ceph_extra_args, timeout=cmd_timeout, release=CephRelease(release_i))
    prev_settings: Dict[int, Tuple[int, float]] = {}
    failed: List[int] = []
    for osd_id in osd_ids:
        sd = await cli.get_history_size_duration(osd_id)
        if sd:
            prev_settings[osd_id] = sd
            if not await cli.set_history_size_duration(osd_id, size, duration):
                failed.append(osd_id)

    return failed, prev_settings


previous_ops: Set[str] = set()


@expose
async def get_historic(osd_ids: List[int],
                       size: int,
                       duration: float,
                       ceph_extra_args: List[str],
                       cmd_timeout: float,
                       release_i: int,
                       min_duration: int = 0,
                       packer_name: str = 'compact') -> bytes:
    cli = CephCLI(node=None, extra_params=ceph_extra_args, timeout=cmd_timeout, release=CephRelease(release_i))
    all_ops: Dict[int, List[CephOp]] = {}
    curr_ops: Set[str] = set()

    for osd_id in osd_ids:
        try:
            raw_ops = await cli.get_historic(osd_id)
        except (subprocess.CalledProcessError, OSError):
            continue

        if raw_ops['size'] != size or raw_ops['duration'] != duration:
            raise RuntimeError(
                f"Historic ops setting changed for osd {osd_id}. Expect: duration={duration}, size={size}" +
                f". Get: duration={raw_ops['duration']}, size={raw_ops['size']}")

        for raw_op in raw_ops['ops']:
            if min_duration > int(raw_op.get('duration') * 1000):
                continue
            try:
                _, op = CephOp.parse_op(raw_op)
                if not op:
                    continue
            except Exception:
                continue

            if op.tp is not None and op.description not in previous_ops:
                op.pack_pool_id = op.pool_id
                all_ops.setdefault(osd_id, []).append(op)
                curr_ops.add(op.description)

    previous_ops.clear()
    previous_ops.update(curr_ops)

    return b"".join(pack_historic(packer_name, osd_id, ops) for osd_id, ops in all_ops.items())


def pack_historic(packer: str, osd_id: int, ops: Iterable[CephOp]) -> bytes:
    packer = get_historic_packer(packer)
    rec_tp, data = packer.pack_record(RecId.ops, (osd_id, int(time.time()), ops))
    assert rec_tp == RecId.ops
    return data


def unpack_historic_simple(data: bytes, packer: str = 'compact') -> Iterator[Dict[str, Any]]:
    return get_historic_packer(packer).unpack(RecId.ops, data)
