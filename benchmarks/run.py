import argparse
import functools
import operator
import time
from itertools import product
from pathlib import Path
from typing import Callable, Generator, cast

import dask.array as da
import numpy as np
from experiments import (
    poisson_generator,
    repeat_generator,
    run_multiple,
    save_outputs,
)
from main import (
    DaskEc2Benchmark,
    DaskFargateBenchmark,
    DaskLambdaBenchmark,
    Ec2ClusterSize,
    FargateClusterSize,
    LambdaClusterSize,
    TestData,
    bump_experiment,
)
from scipy import ndimage as ndi
from scipy import signal

from faascheduler.aws.main import PersistenceType, QueueType, SchedulerType
from faascheduler.scheduler import DagOptimization

import boto3
from botocore.exceptions import ClientError

def thumbnail(original: da.Array, scale: int) -> da.Array:
    """
    Original should be 2x2 array,

    More detail here https://stackoverflow.com/a/65118276/5771078
    """
    original_size = original.chunksize
    return cast(da.Array, original[::scale, ::scale, ::scale].rechunk(original_size))


def _semblance_block(chunk: da.Array, kernel: da.Array) -> da.Array:
    def operator(x: da.Array) -> da.Array:
        np.seterr(all="ignore")

        region = x.reshape(-1, x.shape[-1])
        ntraces, _ = region.shape

        s1 = np.sum(region, axis=0) ** 2
        s2 = np.sum(region**2, axis=0)

        sembl = s1.sum(axis=-1) / s2.sum(axis=-1)
        sembl /= ntraces

        return cast(da.Array, sembl)

    return cast(da.Array, ndi.generic_filter(input=chunk, function=lambda x: operator(x.reshape(kernel)), size=kernel))


def semblance(array: da.Array) -> da.Array:
    _kernel = (3, 3, 3)
    # Generate Dask Array as necessary and perform algorithm
    hw = tuple(np.array(_kernel) // 2)
    array_da = da.overlap.overlap(array, depth=hw, boundary="reflect")

    result = array_da.map_blocks(
        _semblance_block, kernel=_kernel, dtype=array_da.dtype, meta=np.array((), dtype=array_da.dtype)
    )

    axes = {0: hw[0], 1: hw[1], 2: hw[2]}

    result = da.overlap.trim_internal(result, axes=axes, boundary="reflect")

    result[da.isnan(result)] = 0

    return cast(da.Array, result)


def phase_rotation(arr: da.Array) -> da.Array:
    _rotation = 90
    phi = np.deg2rad(_rotation, dtype=arr.dtype)

    analytical_trace = arr.map_blocks(signal.hilbert, dtype=arr.dtype, meta=np.array((), dtype=arr.dtype))

    result = analytical_trace.real * da.cos(phi) - analytical_trace.imag * da.sin(phi)

    result[da.isnan(result)] = 0

    return cast(da.Array, result)


def explore_base(
    path: str, method: Callable[[da.Array], da.Array], sleep_seconds_between: Generator[float, None, None]
) -> tuple[list[da.Array, list[float]]]:
    arr = da.from_zarr(path)
    results: list[da.Array] = []
    times: list[float] = []
    sleeps: list[float] = []  # before first execution, no sleep
    for view in create_lazy_views(arr):
        print("\tCompute", end="", flush=True)
        t0 = time.time()
        results.append(method(view)[:, :, 0].compute())
        t1 = time.time()
        bump_experiment()
        to_sleep = next(sleep_seconds_between)
        sleeps.append(to_sleep)
        time.sleep(to_sleep)
        print(f"-OK-{t1 - t0}")
        times.append(t1 - t0)
    return results, times, sleeps


def explore_raw(path: str, sleep_seconds_between: Generator[float, None, None]) -> tuple[list[da.Array], list[float]]:
    return explore_base(path, lambda x: x, sleep_seconds_between)


def explore_phase(path: str, sleep_seconds_between: Generator[float, None, None]) -> tuple[list[da.Array], list[float]]:
    return explore_base(path, phase_rotation, sleep_seconds_between)


def explore_semblance(
    path: str, sleep_seconds_between: Generator[float, None, None]
) -> tuple[list[da.Array], list[float]]:
    return explore_base(path, semblance, sleep_seconds_between)


def save_dag(path: str, method: Callable[[da.Array], da.Array]) -> None:
    filename = Path(path).name
    arr = da.from_zarr(path)
    views = create_lazy_views(arr)
    default_kwargs = {"optimize_graph": True, "format": "png"}
    hlg_kwargs = {"format": "png"}

    method(views[0])[:, :, 0].visualize(filename=f"{filename}_{method.__name__}_first", **default_kwargs)
    method(views[0])[:, :, 0].dask.visualize(filename=f"{filename}_{method.__name__}_first_hlg", **hlg_kwargs)
    method(views[-1])[:, :, 0].visualize(filename=f"{filename}_{method.__name__}_last", **default_kwargs)
    method(views[-1])[:, :, 0].dask.visualize(filename=f"{filename}_{method.__name__}_last_hlg", **hlg_kwargs)


def create_lazy_views(arr: da.Array) -> list[da.Array]:
    results: list[da.Array] = []
    steps = 256
    direction_count = 5  # total 26 images
    for i in range(0, (direction_count * steps), steps):
        for j in range(0, (direction_count * steps), steps):
            results.append(thumbnail(arr[i : i + 1024, j : j + 1024, :], 3))
    results.append(thumbnail(arr[:, :, :], 3))
    return results


repeat_count = 1
persistence_type = PersistenceType.s3
scheduler_type = SchedulerType.dictionary
dag_optimizations: list[DagOptimization] = []
threads = 5
queue_types = []
lambda_path = None


def __run_lambda(
    memory_sizes, queue_types, lambda_batch_sizes, scenarios, input_data_list, sleeps, experiment_base_name
):
    path = Path(lambda_path)
    if not path.exists() or path.is_dir():
        raise ValueError("Invalid lambda zip path")

    print(f"Creating lambda infrastructure using package '{path.absolute()}'")
    lambda_sizes: list[LambdaClusterSize] = []

    for queue_type, lambda_batch_size, memory_size in product(queue_types, lambda_batch_sizes, memory_sizes):
        lambda_sizes += [
            LambdaClusterSize(
                path, memory_size, lambda_batch_size, 20, scheduler_type, persistence_type, queue_type, threads, dag_optimizations
            )
        ]

    product_params = [scenarios, lambda_sizes, input_data_list, sleeps]
    experiment_parameters = product(*product_params)
    exp_count = functools.reduce(operator.mul, map(len, product_params), 1)
    for idx, (scenario, lambda_size, input_data, sleep) in enumerate(experiment_parameters):
        opt_suffix = ""
        if len(dag_optimizations) > 0:
            opt_suffix = "_opt_" + "_".join(sorted([item.value for item in dag_optimizations]))
        experiment_name = (
            experiment_base_name
            + "_"
            + scheduler_type.value
            + "_"
            + persistence_type.value
            + "_"
            + input_data.value.replace("/", "_")
            + scenario.__name__
            + opt_suffix
        )
        print(f">> {idx}/{exp_count} - Experiment {experiment_name} ")
        cluster = DaskLambdaBenchmark(lambda_size, experiment_name)
        results = run_multiple(
            cluster,
            input_data,
            repeat_count,
            sleep,
            scenario,
        )
        save_outputs(results, experiment_name, cluster)


def __run_fargate(scenarios, input_data_list, sleeps, worker_counts, memory_and_cpus, experiment_base_name) -> None:
    fargate_sizes: list[FargateClusterSize] = []
    for worker_count, (memory, cpu) in product(worker_counts, memory_and_cpus):
        fargate_sizes.append(FargateClusterSize(1024, 2048, cpu, memory, worker_count))

    product_params = [scenarios, fargate_sizes, input_data_list, sleeps]
    experiment_parameters = product(*product_params)
    exp_count = functools.reduce(operator.mul, map(len, product_params), 1)

    for idx, (scenario, fargate_size, input_data, sleep) in enumerate(experiment_parameters):
        experiment_name = experiment_base_name + "_" + input_data.value.replace("/", "_") + scenario.__name__
        print(f">> {idx}/{exp_count} - Experiment {experiment_name} ")
        cluster = DaskFargateBenchmark(fargate_size, experiment_name)
        results = run_multiple(
            cluster,
            input_data,
            repeat_count,
            sleep,
            scenario,
        )
        save_outputs(results, experiment_name, cluster)


def __run_ec2(scenarios, input_data_list, sleeps, worker_counts, worker_names, experiment_base_name) -> None:
    vs_sizes: list[FargateClusterSize] = []
    for worker_count, worker_name in product(worker_counts, worker_names):
        vs_sizes.append(Ec2ClusterSize("m5.large", worker_name, worker_count))

    product_params = [scenarios, vs_sizes, input_data_list, sleeps]
    experiment_parameters = product(*product_params)
    exp_count = functools.reduce(operator.mul, map(len, product_params), 1)

    for idx, (scenario, vs_size, input_data, sleep) in enumerate(experiment_parameters):
        experiment_name = experiment_base_name + "_" + input_data.value.replace("/", "_") + scenario.__name__
        print(f">> {idx}/{exp_count} - Experiment {experiment_name} ")
        cluster = DaskEc2Benchmark(vs_size, experiment_name)
        results = run_multiple(
            cluster,
            input_data,
            repeat_count,
            sleep,
            scenario,
        )
        save_outputs(results, experiment_name, cluster)


def run_compare_chunk_batch_size():
    memory_size = 1024
    lambda_batch_sizes = [1, 3, 5]
    sleep = [repeat_generator(0)]

    __run_lambda(
        [memory_size],
        queue_types,
        lambda_batch_sizes,
        [explore_phase],
        TestData.crosslines(),
        sleep,
        "chunk_batch_size",
    )


def run_cooldown():
    memory_size = 1024
    lambda_batch_sizes = [1]
    sleeps = [repeat_generator(0), repeat_generator(60), repeat_generator(120), repeat_generator(300)]

    __run_lambda(
        [memory_size],
        queue_types,
        lambda_batch_sizes,
        [explore_phase],
        [TestData.crossline1024],
        sleeps,
        "cooldowns",
    )


def run_compare_memory():
    memory_sizes = [512, 1024, 2048]
    lambda_batch_sizes = [1]
    sleeps = [repeat_generator(0)]

    __run_lambda(
        memory_sizes,
        queue_types,
        lambda_batch_sizes,
        [explore_phase],
        [TestData.crossline1024],
        sleeps,
        "memory",
    )


def run_compare_vm():
    print("Starting experiments...")
    #### GLOBAL PARAMETERS
    mean_delay_sec = 10
    vs_cloud_expname = "vs_cloud"
    inputs = [TestData.crossline256, TestData.crossline1024]
    sleeps = [poisson_generator(mean_delay_sec)]
    scenarios = [explore_phase, explore_semblance]

    #### EC2
    print(">>\n>>\n>>Starting EC2...")
    vm_worker_counts = [2, 4]
    worker_sizes = ["m5.large", "m5.xlarge"]
    __run_ec2(scenarios, inputs, sleeps, vm_worker_counts, worker_sizes, vs_cloud_expname)

    #### FARGATE
    print(">>\n>>\n>>Starting Fargate...")
    memory_and_cpus = [(2048, 1024), (16384, 4096)]
    fg_worker_counts = [2, 4]
    __run_fargate(scenarios, inputs, sleeps, fg_worker_counts, memory_and_cpus, vs_cloud_expname)

    #### LAMBDA
    print(">>\n>>\n>>Starting Lambda...")
    memory_sizes = [1024, 2048]
    lambda_batch_sizes = [1]
    __run_lambda(memory_sizes, queue_types, lambda_batch_sizes, scenarios, inputs, sleeps, vs_cloud_expname)


def run_sanity():
    __run_lambda([1024], queue_types, [1], [explore_raw], [TestData.crossline512], [repeat_generator(0)], "sanity")

def run_generate_data():
    # First step, make sure bucket exists
    origin_bucket = "faascheduler-input"
    s3_client = boto3.client("s3")
    
    try:
        # Try to get bucket data, if non existant will return 404 error
        s3_client.head_bucket(Bucket=origin_bucket)
        print("Bucket exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print("Creating bucket")
            s3_client.create_bucket(ACL="private", Bucket=origin_bucket)
        else:
            print("Failure checking bucket existance")
            raise e
    
    # If bucked is available (code above did not fail), upload data
    print("Uploading data")    
    crossline_sizes = {TestData.crossline1024: 1024, TestData.crossline512: 512, TestData.crossline256: 256}
    state = da.random.RandomState(42)
    for test_data, chunk_size  in crossline_sizes.items():
        target_url=f"s3://{origin_bucket}/{test_data.value}"
        print(f"Uploading {target_url}")
        state.random(size=(2048, 2048, 9),
                     chunks=(chunk_size, chunk_size, chunk_size)).to_zarr(target_url, overwrite=True)


experiments_mapping = {
    "cooldown": run_cooldown,
    "sizes": run_compare_chunk_batch_size,
    "memory": run_compare_memory,
    "vm": run_compare_vm,
    "sanity": run_sanity,
    "gen_data": run_generate_data,
}


#### Decide if just print data or run experiments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="FaasDask Benchmarks", description="Runs benchmarks for our paper :)", epilog="More information soon..."
    )
    parser.add_argument("experiments", nargs="+", choices=experiments_mapping.keys())
    parser.add_argument("-r", "--repeat", type=int, default=1)
    parser.add_argument(
        "-p",
        "--persistence-type",
        type=str,
        default="s3",
        choices=[type.value for type in PersistenceType],
    )
    parser.add_argument(
        "-s",
        "--scheduler-type",
        type=str,
        default="dictionary",
        choices=[type.value for type in SchedulerType],
    )
    parser.add_argument(
        "-q", "--queue-type", type=str, nargs="*", default=["threadpool"], choices=[type.value for type in QueueType]
    )
    parser.add_argument("-l", "--lambda-path", type=str, required=True)
    parser.add_argument("-t", "--thread-count", type=int, default=5)
    parser.add_argument("-o", "--optimizations", type=str, nargs="*", default=[], choices=[type.value for type in DagOptimization])
    args = parser.parse_args()

    repeat_count = args.repeat
    persistence_type = PersistenceType(args.persistence_type)
    scheduler_type = SchedulerType(args.scheduler_type)
    dag_optimizations = [DagOptimization(type) for type in args.optimizations]
    queue_types = [QueueType(type) for type in args.queue_type]
    threads = args.thread_count
    lambda_path = args.lambda_path

    if scheduler_type == SchedulerType.sql:
        print('ERROR:: SQL scheduler is not supported. Use -s dictionary')
        exit(1)
    if persistence_type == PersistenceType.efs and (QueueType.threadpool in queue_types or QueueType.sync in queue_types):
        print('ERROR:: Only queuetype "invokesync" is supported with EFS. Use -q invokesync')
        exit(1)

    for experiment in args.experiments:
        experiments_mapping[experiment]()
