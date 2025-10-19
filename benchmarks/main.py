from __future__ import annotations

import concurrent
import json
import time
from dataclasses import dataclass, fields
from enum import Enum
from pathlib import Path
from random import randint
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, ContextManager, Dict, Mapping, Protocol, cast

import boto3
from dask.distributed import Client

from faascheduler.aws.main import PersistenceType, QueueType, SchedulerType
from faascheduler.stats import base_stats, stats

if TYPE_CHECKING:
    from mypy_boto3_s3.literals import RegionName

from faascheduler.aws import AwsLambdaCluster, create_bucket, empty_and_delete_bucket
from faascheduler.scheduler import DagOptimization

class TestData(Enum):
    crossline256 = "f3/f3_cross_256"
    crossline512 = "f3/f3_cross_512"
    crossline1024 = "f3/f3_cross_1024"

    @staticmethod
    def crosslines() -> list[TestData]:
        return [TestData.crossline256, TestData.crossline512, TestData.crossline1024]


class TempTestBucket:
    def __init__(
        self, bucket_name: str, region: "RegionName", tags: Mapping[str, str], origin_bucket: str, origin_prefix: str
    ) -> None:
        self.__bucket_name = bucket_name
        self.__region = region
        self.__tags = tags
        self.__origin_bucket = origin_bucket
        self.__origin_prefix = origin_prefix
        self.__s3client = boto3.client("s3")

    def __enter__(self) -> None:
        create_bucket(self.__s3client, self.__bucket_name, self.__region, self.__tags)

        def copy_data(origin, name, destination):
            self.__s3client.copy_object(
                Bucket=destination,
                CopySource=f"/{origin}/{name}",
                Key=name,
            )

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=150) as executor:
            # Start the load operations and mark each future with its URL
            futures = [
                executor.submit(copy_data, self.__origin_bucket, obj.key, self.__bucket_name)
                for obj in boto3.resource("s3").Bucket(self.__origin_bucket).objects.filter(Prefix=self.__origin_prefix)
            ]
            concurrent.futures.wait(futures)

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        empty_and_delete_bucket(self.__s3client, self.__bucket_name)


class AwsMixin:
    region = "us-east-1"
    docker_image = "cmillani/dask-scipy:3.10-slim"

    def get_tags(self, cluster_type: str, experiment_id: str, cluster_id: str):
        return {
            "faascheduler:clusterType": cluster_type,
            "faascheduler:experimentId": experiment_id,
            "faascheduler:clusterId": cluster_id,
        }


@dataclass
class Ec2ClusterSize:
    scheduler_vm: str
    worker_vm: str
    worker_count: int

    def description(self):
        return f"ec2_{self.worker_count}x{self.worker_vm}"


class DataclassType(Protocol):
    # as already noted in comments, checking for this attribute is currently
    # the most reliable way to ascertain that something is a dataclass
    __dataclass_fields__: ClassVar[Dict[str, Any]]


def asstrdict(obj: DataclassType) -> dict:
    return {field.name: str(getattr(obj, field.name)) for field in fields(obj)}


class ClusterMixin(ContextManager):
    cluster_boot_time: int | None = None
    workers_boot_time: int | None = None
    cluster_id: str
    tags: dict[str, str]
    iam_client = boto3.client("iam")
    sts_client = boto3.client("sts")
    bucket_name: str | None = None
    cluster_size_data: dict
    stats: dict = {}
    cluster_name: str

    def create_policy(self, name: str, bucket_name: str, tags: Mapping[str, str]) -> str:
        policy_dict = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Effect": "Allow",
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"],
                },
                {
                    "Action": ["s3:ListBucket"],
                    "Effect": "Allow",
                    "Resource": [f"arn:aws:s3:::{bucket_name}"],
                },
            ],
        }
        policy_json = json.dumps(policy_dict)
        policy = self.iam_client.create_policy(
            PolicyName=name,
            PolicyDocument=policy_json,
            Description="Policy to allow access to S3 to run Faascheduler benchers",
            Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
        )

        return policy["Policy"]["Arn"]

    def get_policy_arn(self, name: str) -> str:
        account_id = self.sts_client.get_caller_identity()["Account"]
        return f"arn:aws:iam::{account_id}:policy/{name}"

    def delete_policy(self, arn: str) -> None:
        attached_roles = self.iam_client.list_entities_for_policy(PolicyArn=arn, EntityFilter="Role")["PolicyRoles"]
        for role in attached_roles:
            self.iam_client.detach_role_policy(RoleName=role["RoleName"], PolicyArn=arn)
        self.iam_client.delete_policy(PolicyArn=arn)


class DaskEc2Benchmark(AwsMixin, ClusterMixin):
    def __init__(self, cluster_size: Ec2ClusterSize, experiment_id: str) -> None:
        self.__cluster_size = cluster_size
        self.cluster_size_data = asstrdict(cluster_size)
        self.experiment_id = experiment_id
        self.__profile_data = None
        self.instance_profile = {
            "Arn": "arn:aws:iam::245983579475:instance-profile/faascheduler-bench",
        }
        self.cluster_id = str(randint(0, 99999999))
        self.tags = self.get_tags(self.__cluster_size.description(), experiment_id, self.cluster_id)

    def __setup_profile(self, name: str, bucket_name: str, tags: Mapping[str, str]) -> dict[str, str]:
        policy_arn = self.create_policy(name, bucket_name, self.tags)

        keyvalue_tags = [{"Key": key, "Value": value} for key, value in tags.items()]
        profile = self.iam_client.create_instance_profile(InstanceProfileName=name, Tags=keyvalue_tags)
        profile_arn = profile["InstanceProfile"]["Arn"]

        role_arn = self.__create_role(name, tags)
        self.iam_client.attach_role_policy(RoleName=name, PolicyArn=policy_arn)

        self.iam_client.add_role_to_instance_profile(InstanceProfileName=name, RoleName=name)

        return {"Name": name, "PolicyARN": policy_arn, "ProfileARN": profile_arn, "RoleARN": role_arn}

    def __create_role(self, name: str, tags: Mapping[str, str]) -> str:
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": ["ec2.amazonaws.com"]}, "Action": ["sts:AssumeRole"]}
            ],
        }
        json_assume_role_policy = json.dumps(assume_role_policy)

        key_value_tags = [{"Key": key, "Value": value} for key, value in tags.items()]

        role = self.iam_client.create_role(
            AssumeRolePolicyDocument=json_assume_role_policy, RoleName=name, Tags=key_value_tags
        )

        return cast(str, role["Role"]["Arn"])

    def __cleanup_profile(self, profile_data):
        self.iam_client.remove_role_from_instance_profile(
            InstanceProfileName=profile_data["Name"], RoleName=profile_data["Name"]
        )
        self.delete_policy(profile_data["PolicyARN"])
        self.iam_client.delete_role(RoleName=profile_data["Name"])
        self.iam_client.delete_instance_profile(InstanceProfileName=profile_data["Name"])

    def __enter__(self):
        from dask_cloudprovider.aws import EC2Cluster

        if bucket_name := self.bucket_name:
            identity_name = f"fsch-ec2-{self.cluster_id}"
            self.__profile_data = self.__setup_profile(identity_name, bucket_name, self.tags)
            self.instance_profile = {"Arn": self.__profile_data["ProfileARN"]}
            # Without this cluster boot would fail without any clue. After debugging (and pausing here) everything
            # worker, so sleep was added to ensure instance profile is ready
            time.sleep(30)

        t0 = time.time()
        self.cluster = EC2Cluster(
            region=self.region,
            availability_zone=self.region + "a",
            vpc="vpc-dd94f4a6",
            subnet_id="subnet-bbe700dc",
            security=False,
            docker_image=self.docker_image,
            bootstrap=False,
            ami="ami-0c3e6018b0227e5ab",
            iam_instance_profile=self.instance_profile,
            volume_tags=self.tags,
            instance_tags=self.tags,
            scheduler_instance_type=self.__cluster_size.scheduler_vm,
            worker_instance_type=self.__cluster_size.worker_vm,
        )

        t1 = time.time()
        self.cluster.scale_up(self.__cluster_size.worker_count)
        self.cluster.wait_for_workers(self.__cluster_size.worker_count)
        t2 = time.time()
        # Initializes
        Client(self.cluster)

        self.cluster_boot_time = t1 - t0
        self.workers_boot_time = t2 - t1

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        self.cluster_name = self.cluster.name
        self.cluster.close()
        if profile_data := self.__profile_data:
            self.__cleanup_profile(profile_data)


@dataclass
class FargateClusterSize:
    scheduler_cpu: int
    scheduler_mem: int
    worker_cpu: int
    worker_mem: int
    worker_count: int

    def description(self):
        return f"fargate_{self.worker_count}x{self.worker_cpu}mcorex{self.worker_mem}mb"


class DaskFargateBenchmark(AwsMixin, ClusterMixin):
    def __init__(self, cluster_size: FargateClusterSize, experiment_id: str) -> None:
        self.__cluster_size = cluster_size
        self.cluster_size_data = asstrdict(cluster_size)
        self.experiment_id = experiment_id
        self.docker_image = "245983579475.dkr.ecr.us-east-1.amazonaws.com/dask-scipy:3.10-slim"
        self.policies = [
            "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess",
        ]
        self.bucket_name: str | None = None
        self.cluster_id = str(randint(0, 99999999))
        self.tags = self.get_tags(self.__cluster_size.description(), experiment_id, self.cluster_id)
        self.options = {
            "region_name": self.region,
            "image": self.docker_image,
            "task_role_policies": self.policies,
            "scheduler_cpu": self.__cluster_size.scheduler_cpu,
            "scheduler_mem": self.__cluster_size.scheduler_mem,
            "worker_cpu": self.__cluster_size.worker_cpu,
            "worker_mem": self.__cluster_size.worker_mem,
            "tags": self.tags,
        }
        self.__is_retry = False

    def __enter__(self):
        from dask_cloudprovider.aws import FargateCluster

        if bucket_name := self.bucket_name:  # bucketname is set after init, so this cannot be there
            policy_name = f"fsch-fargate-{self.cluster_id}"
            if not self.__is_retry:
                self.__policy_arn = self.create_policy(policy_name, bucket_name, self.tags)
            else:
                self.__policy_arn = self.get_policy_arn(policy_name)
            self.options["task_role_policies"].append(self.__policy_arn)

        self.__is_retry = True  # If Client fails
        t0 = time.time()
        self.cluster = FargateCluster(**self.options)
        t1 = time.time()
        self.cluster.scale_up(self.__cluster_size.worker_count)
        self.cluster.wait_for_workers(self.__cluster_size.worker_count)
        t2 = time.time()
        # Initializes

        Client(self.cluster)

        self.cluster_boot_time = t1 - t0
        self.workers_boot_time = t2 - t1

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        self.cluster_name = self.cluster.name
        self.cluster.close()
        if policy_arn := self.__policy_arn:
            self.delete_policy(policy_arn)


@dataclass
class LambdaClusterSize:
    lambda_path: Path
    memory_mb: int
    batch_size: int
    timeout_sec: int
    scheduler_type: SchedulerType
    persistence_type: PersistenceType
    queue_type: QueueType
    threads: int
    optimizations: list[DagOptimization]

    def description(self):
        return f"lambda_mem{self.memory_mb}"  # TODO


class LocalClusterBenchmark(ClusterMixin):
    def __enter__(self) -> None:
        self.client = Client()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> TYPE_CHECKING | None:
        self.client.close()


class DaskLambdaBenchmark(AwsMixin, ClusterMixin):
    def __init__(self, size: LambdaClusterSize, experiment_id: str) -> None:
        self.__cluster_size = size
        self.cluster_size_data = asstrdict(size)
        self.cluster_id = str(randint(0, 99999999))
        self.experiment_id = experiment_id
        self.tags = self.get_tags(self.__cluster_size.description(), experiment_id, self.cluster_id)
        self.workers_boot_time = 0

    def __enter__(self) -> None:
        stats.clear()
        self.bucket_name = self.bucket_name or "faascheduler-input"
        self.cluster = AwsLambdaCluster(
            self.__cluster_size.lambda_path,
            self.bucket_name,
            self.__cluster_size.scheduler_type,
            self.__cluster_size.persistence_type,
            self.__cluster_size.queue_type,
            self.__cluster_size.optimizations,
            self.region,
            self.tags,
            self.__cluster_size.memory_mb,
            self.__cluster_size.timeout_sec,
            self.__cluster_size.batch_size,
            self.__cluster_size.threads,
        )
        t0 = time.time()
        self.cluster.start()
        t1 = time.time()
        self.cluster_boot_time = t1 - t0

    @property
    def stats(self):
        return stats

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        self.cluster_name = self.cluster.cluster_name
        print("Closing Cluster")
        self.cluster.close()
        print("Cluster Closed!!")


class DaskLocalhostBenchmark(ClusterMixin):
    def __enter__(self) -> None:
        self.client = Client()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> TYPE_CHECKING | None:
        self.client.close()


def reset_basestats():
    base_stats.clear()
    base_stats["experiment"] = 0
    base_stats["repetition"] = 0


def bump_experiment():
    base_stats["experiment"] += 1


def bump_repetition():
    base_stats["experiment"] = 0
    base_stats["repetition"] += 1
