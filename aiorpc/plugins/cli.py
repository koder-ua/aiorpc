import os
import zlib
import time
import signal
import asyncio
import functools
import subprocess
from typing import List, Optional, Tuple, Dict

from koder_utils import start_proc, run_proc_timeout, CmdType, CMDResult

from ..plugins_api import expose_func
from ..common import logger


expose = functools.partial(expose_func, "cli")


all_procs: List[asyncio.subprocess.Process] = []
last_killall_requested: int = 0
last_killall_sig: Optional[int] = None


# TODO: make this streaming data from process to caller
# TODO: how to pass exit code back in this case?
@expose
async def run_cmd(cmd: CmdType,
                  timeout: int = None,
                  input_data: bytes = None,
                  compress: bool = True,
                  merge_err: bool = False,
                  output_to_devnull: bool = False,
                  term_timeout: int = 1,
                  env: Dict[str, str] = None,
                  **kwargs) -> Tuple[int, bytes, bytes]:

    start_time = time.time()

    proc, input_data = await start_proc(cmd, input_data, merge_err, output_to_devnull,
                                        env=env, preexec_fn=os.setsid, **kwargs)
    try:
        group = os.getpgid(proc.pid)
    except ProcessLookupError:
        group = None

    # there a race between creating of process and killing all processes, fix it
    if start_time < last_killall_requested:
        assert last_killall_sig is not None
        proc.send_signal(last_killall_sig)

    all_procs.append(proc)

    try:
        res = await run_proc_timeout(cmd, proc, timeout=timeout,
                                     input_data=input_data,
                                     term_timeout=term_timeout,
                                     term_group=group)
    except subprocess.CalledProcessError as exc:
        res = CMDResult(exc.cmd, stdout_b=exc.stdout, stderr_b=exc.stderr, returncode=exc.returncode)

    if compress:
        out = zlib.compress(res.stdout_b)
        err = None if res.stderr_b is None else zlib.compress(res.stderr_b)
    else:
        out = res.stdout_b
        err = res.stderr_b

    return proc.returncode, out, err


@expose
def killall(signal_num: int = signal.SIGKILL):
    logger.info("Signal %s is requested for all procs", signal)

    if signal_num in (signal.SIGKILL, signal.SIGTERM):
        global last_killall_requested
        global last_killall_sig
        last_killall_requested = time.time()
        last_killall_sig = signal_num

    for proc in all_procs:
        try:
            proc.send_signal(signal_num)
        except:
            pass


@expose
def environ() -> Dict[str, str]:
    return os.environ
