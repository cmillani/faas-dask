import time
from typing import Any, Callable, Mapping

import dask

from faascheduler.cache import PersistantCache
from faascheduler.parameters import Command
from faascheduler.scheduler import Scheduler, DagOptimization
from faascheduler.stats import stats
from faascheduler.task_queue import FSQueue


class DictionaryScheduler(Scheduler):
    def __fire_tasks(
        self,
        state: Mapping,
        dsk: Mapping,
        results: set,
        work_queue: FSQueue,
        get_id: Callable = dask.local.default_get_id,
        pack_exception: Callable = dask.local.default_pack_exception,
    ) -> None:
        """Fire off a task to the thread pool"""
        # Determine number of tasks to submit
        nready = len(state["ready"])

        # Prep all ready tasks for submission
        args = []
        for _ in range(nready):
            # Get the next task to compute (most recently added)
            key = state["ready"].pop()
            # Notify task is running
            state["running"].add(key)

            # Prep args to send
            deps = dask.core.get_dependencies(dsk, key)

            args.append(
                (
                    Command.exec.value,
                    key,
                    dsk[key],
                    deps,
                    get_id,
                    pack_exception,
                    key in results,
                )
            )

        # Submit
        for arg in args:
            stats[str(arg[0])]["sqs_buffer_sent"] = time.time()
            work_queue.put(arg)

    def __seed_remote_cache(
        self,
        original_cache: Mapping,
        persistant_cache: Mapping,
        work_queue: FSQueue,
        done_queue: FSQueue,
    ) -> None:
        # We start state using an empty cache or else state is inconsistant
        # Right after we set the correct cache and handle computed tasks
        # TODO: It may be better to change code bellow to simply change ready tasks to
        # finished, but we need to make sure that it is enough to keep consistency. The current
        # code should keep everything in order.
        # TODO: This whole work should go to the lambda side once we have sqlite there
        store_command_count = 0
        for key, value in original_cache.items():
            if key not in persistant_cache:
                persistant_cache[key] = value
                store_command_count += 1
                # TODO: Should store be simply other tasks? It may work better with native dask tasks, and
                # could work well with task-chaining
                command = (Command.store.value, key, value)
                work_queue.put(command)

        for _ in range(store_command_count):
            done_queue.get()  # Simply wait until all needed cache data is stored

    def __check_cache(self, dsk: Mapping, state: Mapping, cache: Mapping, enable_cache: bool, keyorder: Any) -> None:
        if enable_cache:
            true_ready: list = []
            while len(state["ready"]):
                task = state["ready"].pop(0)
                if task in cache:
                    state["running"].add(task)
                    dask.local.finish_task(dsk, task, state, set(), keyorder.get, delete=False)
                else:
                    true_ready.append(task)
            state["ready"].extend(true_ready)

    def run(
        self,
        dsk: Mapping,
        result: Any,
        cache: PersistantCache,
        work_queue: FSQueue,
        done_queue: FSQueue,
        optimizations: list[DagOptimization],
        get_id: Callable = dask.local.default_get_id,
        pack_exception: Callable = dask.local.default_pack_exception,
        raise_exception: Callable = dask.local.reraise,
        enable_cache: bool = True,
    ) -> Any:
        state, dsk, results, keyorder = self._prepare(dsk, result, optimizations)
        self.__seed_remote_cache(state["cache"], cache, work_queue, done_queue)
        state["cache"] = cache

        if state["waiting"] and not state["ready"]:
            raise ValueError("Found no accessible jobs in dask")
        self.__check_cache(dsk, state, cache, enable_cache, keyorder)

        # Main loop, wait on tasks to finish, insert new ones
        while state["waiting"] or state["ready"] or state["running"]:
            self.__fire_tasks(state, dsk, results, work_queue, get_id, pack_exception)
            key, failed, err_info, add_data = done_queue.get()
            stats[str(key)]["tsk_started"] = add_data[0]
            stats[str(key)]["tsk_finished"] = add_data[1]
            stats[str(key)]["tsk_s3_load_init"] = add_data[2]
            stats[str(key)]["tsk_s3_load_end"] = add_data[3]
            stats[str(key)]["tsk_s3_write_init"] = add_data[4]
            stats[str(key)]["tsk_s3_write_end"] = add_data[5]

            stats[str(key)]["req_id"] = add_data[6]
            stats[str(key)]["hostname"] = add_data[7]

            stats[str(key)]["sqs_buffer_read"] = time.time()
            stats[str(key)]["sch_start_processed"] = stats[str(key)]["sqs_buffer_read"]

            if failed:
                exc, tb = err_info
                raise_exception(exc, tb)

            # Workers will retry failed functions, this is needed to guard against timeouts and sporadic issues, and may
            # cause the same message to be received twice. We need to guard so that this behaviour does not break the
            # scheduler
            # TODO: if the task is in any state other than "running" or "finished" we may cause a deadlock
            # If we receive a message indicating a message in other state is done we should handle it as if it was running
            if key in state["running"]:
                dask.local.finish_task(dsk, key, state, results, keyorder.get, delete=False)
            elif key not in dsk:
                # We still need to make sure that this message is intended to this scheduler
                # Avoiding raising error bacause of unhandlede consumer concurrency: currently only one active
                # scheduler is supported
                pass
            stats[str(key)]["sch_done_processed"] = time.time()

        return dask.local.nested_get(result, cache)
