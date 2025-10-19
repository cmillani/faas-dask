import multiprocessing as mp
from pathlib import Path
from types import TracebackType

from dask import config

from faascheduler.cache import DataCache, PersistantCache
from faascheduler.cache.disk import LocalDiskCache
from faascheduler.main import faas_scheduler
from faascheduler.scheduler import Scheduler
from faascheduler.task_queue.multiprocessing import MultiprocessingQueue

from .worker import create_process_worker


class MultiprocessingCluster:
    def __init__(
        self,
        scheduler: Scheduler,
        local_cache: DataCache | None = None,
        shared_cache: DataCache | None = None,
    ) -> None:
        self.__scheduler = scheduler
        self.__optimizations = []

        if local_cache is None:
            local_cache_path_str = "./fsch_cache"
            local_cache_path = Path(local_cache_path_str)
            if not local_cache_path.exists():
                local_cache_path.mkdir()
            if not local_cache_path.is_dir():
                raise Exception()  # TODO: Exception name
            local_cache = LocalDiskCache(local_cache_path)

        if shared_cache is None:
            shared_cache_path_str = "./fsch_cache_shr"
            shared_cache_path = Path(shared_cache_path_str)
            if not shared_cache_path.exists():
                shared_cache_path.mkdir()
            if not shared_cache_path.is_dir():
                raise Exception()  # TODO: Exception name
            shared_cache = LocalDiskCache(shared_cache_path)

        self.__local_cache = PersistantCache(data_cache=local_cache)
        self.__shared_cache = PersistantCache(data_cache=shared_cache)
        self.__done_queue = MultiprocessingQueue()
        self.__work_queue = MultiprocessingQueue()

        self.__processes: list[mp.Process] = []
        for _ in range(mp.cpu_count()):
            self.__processes.append(
                create_process_worker(self.__work_queue, self.__done_queue, self.__shared_cache, self.__local_cache)
            )

    def __enter__(self) -> None:
        sch = faas_scheduler(self.__shared_cache, self.__work_queue, self.__done_queue, self.__optimizations, self.__scheduler)
        config.set({"scheduler": sch})
        for process in self.__processes:
            process.start()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> None:
        if __exc_type is not None:
            print(__exc_type, __exc_value, __traceback)
        del config.config["scheduler"]
        for process in self.__processes:
            process.terminate()
