from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping

import dask
from dask.core import (
    get_dependencies,
    reverse_dict,
)
from dask.optimization import cull, fuse, inline, inline_functions

from faascheduler.cache import PersistantCache
from faascheduler.task_queue import FSQueue

from enum import Enum

class DagOptimization(Enum):
    constant_inline: str = "constant_inline"
    dependency_fusion: str = "dependency_fusion"


class Scheduler(ABC):
    def _get_funcs_to_inline(self, dsk) -> list:
        dependencies = {k: get_dependencies(dsk, k) for k in dsk}
        dependents = reverse_dict(dependencies)

        funcs = set()
        for k in dsk:
            if len(dependencies[k]) == len(dependents[k]) == 1:
                funcs.add(dsk[k][0])

        return list(funcs)

    def _prepare(self, dsk: Mapping, result: Any, optimizations: list[DagOptimization]) -> tuple[Mapping, Mapping, set, Any]:
        if isinstance(result, list):
            result_flat = set(dask.core.flatten(result))
        else:
            result_flat = {result}
        results = set(result_flat)

        dsk = dict(dsk)
        # if start_state_from_dask fails, we will have something
        # to pass to the final block.
        state = {}

        if DagOptimization.constant_inline in optimizations:
            dsk = inline(dsk, inline_constants=True)
        if DagOptimization.dependency_fusion in optimizations:
            to_fuse = self._get_funcs_to_inline(dsk)
            dsk = inline_functions(dsk, results, to_fuse)
            dsk, deps = fuse(dsk)
        dsk, deps = cull(dsk, result)

        keyorder = dask.order.order(dsk)
        state = dask.local.start_state_from_dask(dsk, cache={}, sortkey=keyorder.get)

        return (state, dsk, results, keyorder)

    @abstractmethod
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
    ) -> Any: ...
