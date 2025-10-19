import multiprocessing as mp
import time
from uuid import uuid4

from distributed.protocol.pickle import dumps

from faascheduler.cache import PersistantCache
from faascheduler.executor import Executor, setup_exit_gracefully
from faascheduler.task_queue import FSQueue


def worker(
    work_queue: FSQueue, done_queue: FSQueue, shared_cache: PersistantCache, local_cache: PersistantCache
) -> None:
    setup_exit_gracefully(shared_cache)
    executor = Executor(work_queue, done_queue, shared_cache, local_cache, "local.db")
    while True:
        raw_task = work_queue.get()
        task = dumps(raw_task)
        start = time.time()
        executor.execute(task, start=start, req_id=str(uuid4()))


def create_process_worker(
    work_queue: FSQueue, done_queue: FSQueue, shared_cache: PersistantCache, local_cache: PersistantCache
) -> mp.Process:
    process = mp.Process(target=worker, args=(work_queue, done_queue, shared_cache, local_cache))
    return process
