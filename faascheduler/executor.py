import os
import signal
import socket
import sys
import time
from typing import Any, Optional

import dask
from dask.local import identity
from distributed.protocol.pickle import loads

from faascheduler.cache import PersistantCache
from faascheduler.parameters import Command
from faascheduler.scheduler.sql import SqlScheduler
from faascheduler.stats import stats
from faascheduler.task_queue import FSQueue


def setup_exit_gracefully(cache: PersistantCache) -> None:
    def exit_gracefully(signum: int, frame: Any) -> None:
        statsname = f"{socket.gethostname()}-{os.getpid()}"
        cache[statsname] = stats
        sys.exit(0)

    signal.signal(signal.SIGTERM, exit_gracefully)


class Executor:
    def __init__(
        self,
        work_queue: FSQueue | None,
        done_queue: FSQueue | None,
        shared_cache: PersistantCache,
        local_cache: PersistantCache | None,
        sql_path: str | None,
    ) -> None:
        self.__work_queue = work_queue
        self.__done_queue = done_queue
        self.__shared_cache = shared_cache
        self.__sqlscheduler = None
        if sql_path is not None:
            self.__sqlscheduler = SqlScheduler(sql_path)
        self.__local_cache = local_cache if local_cache is not None else shared_cache
        self.__hostname = socket.gethostname()

    def execute(self, task: Any, start: Optional[float], req_id: Optional[str], sync: bool = False) -> Any:
        if start is None:
            start = time.time()
        args = loads(task)
        command, *task_args = args

        match Command(command):
            case Command.exec:
                return self.__run(task_args, start, req_id, sync)
            case Command.store:
                self.__store(task_args, sync)
            case Command.graph:
                self.__receive_dag(task_args, req_id)
            case Command.graph_node:
                self.__run_dag_node(task_args, req_id)
            case Command.echo:
                return "OK"

    def __run_dag_node(self, args: tuple, req_id: str | None) -> None:
        if self.__sqlscheduler is None:
            raise ValueError("No sql scheduler path available")
        self.__sqlscheduler.run_task_while_available(
            self.__work_queue, self.__done_queue, self.__local_cache, self.__shared_cache, req_id, self.__hostname, args
        )

    def __receive_dag(self, args: tuple, req_id: str | None) -> None:
        if self.__sqlscheduler is None:
            raise ValueError("No sql scheduler path available")
        dsk, results = args
        self.__sqlscheduler.receive_dag(dsk, results, self.__local_cache, self.__done_queue)
        self.__sqlscheduler.run_task_while_available(
            self.__work_queue, self.__done_queue, self.__local_cache, self.__shared_cache, req_id, self.__hostname, None
        )

    def __store(self, args: tuple, sync: bool) -> None:
        key, value = args
        self.__local_cache[key] = value
        if not sync:
            self.__done_queue.put((Command.store.value, key))

    def __run(self, args: tuple, start: float, req_id: Optional[str], sync: bool = False) -> tuple | None:
        key, task, deps, get_id, pack_exception, use_remote_cache = args
        s3_load_init = time.time()
        data = {dep: self.__local_cache[dep] for dep in deps}
        s3_load_end = time.time()
        key, res_info, failed = dask.local.execute_task(key, (task, data), identity, identity, get_id, pack_exception)

        err_info = None
        if failed:
            s3_write_init = None
            s3_write_end = None
            err_info = res_info
        else:
            res, _ = res_info
            s3_write_init = time.time()
            self.__local_cache[key] = res
            if use_remote_cache and id(self.__local_cache) != id(self.__shared_cache):
                self.__shared_cache[key] = res
            s3_write_end = time.time()
        end = time.time()
        res = (
            key,
            failed,
            err_info,
            (start, end, s3_load_init, s3_load_end, s3_write_init, s3_write_end, req_id, self.__hostname),
        )
        if not sync:
            self.__done_queue.put(res)
            return None
        else:
            return res
