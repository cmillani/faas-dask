from functools import partial
from typing import Any, Callable, Mapping

import dask

from faascheduler.cache import PersistantCache
from faascheduler.parameters import SchedulerStrategy
from faascheduler.scheduler import DictionaryScheduler, Scheduler, DagOptimization
from faascheduler.task_queue import FSQueue


def faas_scheduler(
    cache: PersistantCache, work_queue: FSQueue, done_queue: FSQueue, optimizations: list[DagOptimization], scheduler: Scheduler = DictionaryScheduler()
) -> Callable:
    return partial(schedule, cache=cache, work_queue=work_queue, done_queue=done_queue, optimizations=optimizations, scheduler=scheduler)


def schedule(
    dsk: Mapping,
    result: Any,
    cache: PersistantCache,
    work_queue: FSQueue,
    done_queue: FSQueue,
    optimizations: list[DagOptimization],
    scheduler: Scheduler,
    get_id: Callable = dask.local.default_get_id,
    pack_exception: Callable = dask.local.default_pack_exception,
    raise_exception: Callable = dask.local.reraise,
    strategy: SchedulerStrategy = SchedulerStrategy.local,
    enable_cache: bool = True,
) -> Any:
    match strategy:
        case SchedulerStrategy.local:
            return scheduler.run(
                dsk, result, cache, work_queue, done_queue, optimizations, get_id, pack_exception, raise_exception, enable_cache
            )
        case SchedulerStrategy.remote:
            raise NotImplementedError()
