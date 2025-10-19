import time
from typing import Any, Callable, Mapping

import dask
import pysqlite3 as sqlite3
from dask.local import default_get_id as get_id
from dask.local import default_pack_exception as pack_exception
from dask.local import identity
from distributed.protocol.pickle import dumps, loads

from faascheduler.cache import PersistantCache
from faascheduler.parameters import Command
from faascheduler.scheduler import Scheduler, DagOptimization
from faascheduler.stats import stats
from faascheduler.task_queue import FSQueue


class SqlScheduler(Scheduler):
    def __init__(self, db: str = "local.db") -> None:
        self.__con = sqlite3.connect(db, isolation_level=None)
        self.__cur = self.__con.cursor()
        # Note: Optimizations are not fully implemented for SQL scheduler
        self.__optimizations: list[DagOptimization] = []

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
        self.__optimizations = optimizations
        work_queue.put((Command.graph, dsk, result))
        res_set = self.__list_results(result)
        res_received = 0
        while len(res_set) > res_received:
            done_queue.get()
            res_received += 1

        return dask.local.nested_get(result, cache)

    def receive_dag(self, dsk, result, persistant_cache: PersistantCache, done_queue: FSQueue):
        self.__ensure_tables()
        state, dsk, _, _ = self._prepare(dsk, result, self.__optimizations)
        self.__seed_cache(state["cache"], persistant_cache)
        results_list = self.__list_results(result)
        self.__sync_dask(dsk, results_list)

        for key in self.__get_done_in_keys(list(results_list)):
            done_queue.put({key: persistant_cache[key]})

    def run_task_while_available(
        self,
        work_queue: FSQueue,
        done_queue: FSQueue,
        local_cache: PersistantCache,
        shared_cache: PersistantCache,
        req_id,
        hostname,
        key_n_task=None,
    ) -> None:
        last_task = None
        if key_n_task is not None:
            key, task = key_n_task
            stats[str(key)]["sqs_buffer_read"] = time.time()
            last_task = key
            self.__execute(key, task, local_cache, shared_cache, done_queue, req_id, hostname)
        while True:
            tasks = self.__get_task_to_send()
            if last_task is not None:
                stats[str(last_task)]["sch_done_processed"] = time.time()
            if len(tasks) == 0:
                break

            for key, task in tasks[1:]:
                stats[str(key)]["sqs_buffer_sent"] = time.time()
                work_queue.put((Command.graph_node, key, task))

            key, task = tasks[0]
            last_task = key
            self.__execute(key, task, local_cache, shared_cache, done_queue, req_id, hostname)

    def __list_results(self, results) -> set:
        if isinstance(results, list):
            base = set()
            for item in results:
                base = base.union(self.__list_results(item))
            return base
        else:
            return set([memoryview(dumps(results))])

    def __execute(
        self,
        key: Any,
        task: Any,
        local_cache: PersistantCache,
        shared_cache: PersistantCache,
        done_queue: FSQueue,
        req_id: str | None,
        hostname: str,
    ) -> None:
        keystr = str(key)
        stats[keystr]["req_id"] = req_id
        stats[keystr]["hostname"] = hostname
        deps = self.__get_dependencies(key)
        stats[keystr]["tsk_s3_load_init"] = time.time()
        data = {dep: local_cache[dep] for dep in deps}
        stats[keystr]["tsk_s3_load_end"] = time.time()
        stats[keystr]["tsk_started"] = time.time()
        key, res_info, failed = dask.local.execute_task(key, (task, data), identity, identity, get_id, pack_exception)
        stats[keystr]["tsk_finished"] = time.time()

        if failed:
            err_info = res_info  # TODO: What to do?
        else:
            res, _ = res_info
            stats[keystr]["tsk_s3_write_init"] = time.time()
            local_cache[key] = res
            stats[keystr]["tsk_s3_write_end"] = time.time()

        stats[str(key)]["sch_start_processed"] = time.time()
        is_result = self.__complete_task(key)
        if is_result:
            shared_cache[key] = res
            done_queue.put(key)

    ### SQL
    def __get_done_in_keys(self, keys: list[Any]) -> list[Any]:
        in_qtty = ",".join("?" * len(keys))
        self.__cur.execute(f"SELECT key FROM tasks WHERE key IN ({in_qtty}) AND state = 2", keys)
        data = self.__cur.fetchall()
        return [loads(key) for key, *__ in data]

    def __get_dependencies(self, key: Any) -> list[Any]:
        encoded_key = memoryview(dumps(key))
        self.__cur.execute("SELECT dependency_key FROM dependencies WHERE task_key = (?)", [encoded_key])
        data = self.__cur.fetchall()
        return [loads(dependency_key) for dependency_key, *__ in data]

    def __seed_cache(self, original_cache: dict, persistant_cache: PersistantCache) -> None:
        keys_to_done: list[memoryview] = []
        for key, value in original_cache.items():
            if key not in persistant_cache:
                persistant_cache[key] = value
                keys_to_done.append(memoryview(dumps(key)))
        in_qtty = ",".join("?" * len(keys_to_done))
        self.__cur.execute(f"UPDATE tasks SET state = 2 WHERE key IN ({in_qtty})", keys_to_done)

    def __sync_dask(self, dsk: Mapping, results: set):
        tasks: list[tuple[str, str]] = []
        deps: list[tuple[str, str]] = []
        for key in dsk.keys():
            key_memorydump = memoryview(dumps(key))
            is_result = key_memorydump in results
            tasks.append((key_memorydump, memoryview(dumps(dsk[key])), 0, 0, is_result))
            key_deps = dask.core.get_dependencies(dsk, key)
            for dep in key_deps:
                deps.append((key_memorydump, memoryview(dumps(dep))))

        self.__cur.executemany(
            "INSERT OR IGNORE INTO tasks (key, task, state, qtd_dependencies, is_result) VALUES (?, ?, ?, ?, ?)", tasks
        )
        self.__cur.executemany("INSERT OR IGNORE INTO dependencies (task_key, dependency_key) VALUES (?, ?)", deps)
        self.__global_sync_deps()  # TODO: assure other connections will either block or read stale before commit
        # self.__con.commit()

    def __get_task_to_send(self) -> list[Any]:
        # We use update returting to assure atomic operation
        self.__cur.execute("UPDATE tasks SET state = 1 WHERE state = 0 AND qtd_dependencies = 0 RETURNING key, task")
        keys = self.__cur.fetchall()
        # self.__con.commit()
        return [(loads(key), loads(task)) for key, task in keys]

    def __complete_task(self, key: Any) -> bool:
        encoded_key = memoryview(dumps(key))
        self.__cur.execute("UPDATE tasks SET state = 2 where key = (?) RETURNING is_result", [encoded_key])
        is_result = self.__cur.fetchone()[0]
        self.__cur.execute(
            """
            UPDATE tasks as t
                SET qtd_dependencies = qtd_dependencies - 1 
                FROM dependencies d
                WHERE d.dependency_key = (?) and t.key = d.task_key
            """,
            [encoded_key],
        )
        # self.__con.commit()

        return is_result

    def __global_sync_deps(self) -> None:
        # SQLITE 3.33 has update FROM, which may perform better
        self.__cur.execute(
            """
            with pending_deps as (
                select count(*) as quantity, task_key 
                    from dependencies d
                    join tasks dep_t on dep_t.key = d.dependency_key
                    join tasks tsk on tsk.key = d.task_key
                    where dep_t.state = 0 and tsk.state = 0
                    group by task_key
            )
            UPDATE tasks
                SET qtd_dependencies = COALESCE((select quantity from pending_deps pd where pd.task_key = tasks.key), 0)
                WHERE tasks.state = 0
            """
        )

    def __ensure_tables(self):
        self.__cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key BLOB,
                task BLOB,
                state INTEGER,
                qtd_dependencies INTEGER,
                is_result INTEGER,

                CONSTRAINT unique_key UNIQUE (key)
            );
            """
        )

        self.__cur.execute("CREATE INDEX IF NOT EXISTS tsk_key ON tasks (key, task)")
        self.__cur.execute("CREATE INDEX IF NOT EXISTS tsk_key ON tasks (state, key)")

        self.__cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_key BLOB,
                dependency_key BLOB,

                FOREIGN KEY (task_key) references task (key),
                FOREIGN KEY (dependency_key) references task (key),
                CONSTRAINT unique_dep UNIQUE (task_key, dependency_key)
            )
            """
        )

        self.__cur.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics (
                task_key BLOB
                task_start
                task_end
                sd_load_init
                s3_load_end
                sd_write_init
                s3_write_end
                req_id
                hostname
                sqs_buffer_read
                sqs_buffer_sent
                sch_start_processed
                sch_done_processed
            )
            """
        )
