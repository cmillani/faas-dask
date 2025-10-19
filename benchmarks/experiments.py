import json
import time
from functools import wraps
from pathlib import Path
from random import randint
from typing import Any, Callable, Generator

import dask.array as da
import numpy as np
import numpy.typing as npt
import pandas as pd
from main import (
    ClusterMixin,
    DaskEc2Benchmark,
    DaskFargateBenchmark,
    Ec2ClusterSize,
    FargateClusterSize,
    TempTestBucket,
    TestData,
    bump_repetition,
    reset_basestats,
)
from scipy.stats import expon

# NOTE: %pip install dask-cloudprovider[aws] s3fs

def retries(max_retry: int = 1) -> Callable:
    def retries_wrapper(func: Callable) -> Callable:
        @wraps(func)
        def retriable_func(*args, **kwargs) -> Any:
            exps: list[Exception] = []
            while len(exps) < max_retry:
                try:
                    return func(*args, **kwargs)
                except Exception as exp:
                    print("Failed, will retry!", exp)
                    exps.append(exp)
                    time.sleep(30)
            raise exps[-1] from Exception(exps)

        return retriable_func

    return retries_wrapper


def poisson_generator(mean_delay: int) -> Generator[float, None, None]:
    number_gen = np.random.default_rng(42)  # seed to keep experiments reproducible
    while True:
        yield expon.rvs(scale=mean_delay, random_state=number_gen)


def repeat_generator(repeatable: list[int] | int):
    to_repeat = [repeatable] if isinstance(repeatable, int) else repeatable
    current = 0
    while True:
        yield to_repeat[current % len(to_repeat)]
        current += 1


@retries()
def __handle_cluster(
    manager: ClusterMixin,
    input_path: str,
    repeat_count: int,
    sleep_seconds_between: Generator[float, None, None],
    func: Callable[[str, Generator[float, None, None]], tuple[list[da.Array], list[float]]],
):
    results: list[Any] = []
    times: list[int] = []
    print("-OK")
    print("Starting manager", end="", flush=True)
    reset_basestats()
    with manager:
        print("-OK")
        for i in range(repeat_count):
            print(f"Starting {i+1}/{repeat_count}", end="", flush=True)
            t0 = time.time()
            result = func(input_path, sleep_seconds_between)
            t1 = time.time()
            bump_repetition()
            print("-OK")
            results.append(result)
            times.append(t1 - t0)
            sleep_dalay = next(sleep_seconds_between)
            time.sleep(sleep_dalay)
    return (results, times)


def run_multiple(
    manager: ClusterMixin,
    data: TestData,
    repeat_count: int,
    sleep_seconds_between: Generator[float, None, None] | None,
    func: Callable[[str], tuple[list[da.Array], list[float]]],
):
    tags = manager.tags
    region = "us-east-1"
    origin_bucket = "faascheduler-input"
    bucket_name = f"fsch-input-data-{randint(0,99999999)}"
    input_path = f"s3://{bucket_name}/{data.value}"
    print("Creating data bucket", end="", flush=True)
    with TempTestBucket(bucket_name, region, tags, origin_bucket, data.value):
        manager.bucket_name = bucket_name
        results, times = __handle_cluster(manager, input_path, repeat_count, sleep_seconds_between, func)
    return results, times


def save_outputs(
    results: tuple[list[tuple[npt.NDArray, int]], list[int], list[float]], experiment_id: str, cluster: ClusterMixin
):
    # Create folder structure
    results_path = Path.cwd().joinpath("fsch_bench_results")
    if not results_path.exists():
        results_path.mkdir()
    if not results_path.is_dir():
        raise ValueError('Path "fsch_bench_results" exists and is not a folder')

    experiment_path = results_path.joinpath(f"{cluster.cluster_id}_{experiment_id}")
    experiment_path.mkdir()  # Path should never exist, as cluster_id is random

    # Saves metadata
    with open(experiment_path.joinpath("metadata.json"), "w") as jsonfile:
        metadata = cluster.tags.copy()
        metadata["workers_boot_time"] = cluster.workers_boot_time
        metadata["cluster_boot_time"] = cluster.cluster_boot_time
        metadata["experiment"] = experiment_id
        metadata["cluster_size"] = cluster.cluster_size_data
        metadata["cluster_name"] = cluster.cluster_name
        json.dump(metadata, jsonfile)

    with open(experiment_path.joinpath("stats.json"), "w") as jsonfile:
        json.dump(cluster.stats, jsonfile)

    outputs, times = results

    # Save cycle time
    pd.DataFrame({"time": times}).to_csv(experiment_path.joinpath("times.csv"))

    # Save raw output and output time
    execution_to_time = {}
    for index_output, (array_list, arr_time, sleep_time) in enumerate(outputs):
        for index_array, array in enumerate(array_list):
            arr = da.array(array)
            arr.to_zarr(experiment_path.joinpath(f"{index_output}_{index_array}_{experiment_id}"))
        execution_to_time[index_output] = arr_time
        execution_to_time[str(index_output) + "_sleep"] = sleep_time

    pd.DataFrame(execution_to_time).to_csv(experiment_path.joinpath("execution_to_times.csv"))
