import json
import random
import string
import time
from enum import Enum
from pathlib import Path
from random import randint
from types import TracebackType
from typing import TYPE_CHECKING, Mapping, Sequence, cast

import boto3
from dask import config

from faascheduler.cache import PersistantCache
from faascheduler.cache.aws import AwsS3Cache
from faascheduler.main import faas_scheduler
from faascheduler.scheduler import DictionaryScheduler, Scheduler, SqlScheduler, DagOptimization
from faascheduler.task_queue import FSQueue
from faascheduler.task_queue.aws import AwsBoto3Sync, AwsSqsQueue, AwsSqsSyncQueue

if TYPE_CHECKING:
    from mypy_boto3_ec2.type_defs import TagTypeDef as TagTypeDefEC2
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.literals import RegionName
    from mypy_boto3_s3.type_defs import TaggingTypeDef, TagTypeDef


def create_bucket(s3client: "S3Client", name: str, region: "RegionName", tags: Mapping[str, str]) -> None:
    key_value_tags: Sequence["TagTypeDef"] = [{"Key": key, "Value": value} for key, value in tags.items()]
    bucket_tags: "TaggingTypeDef" = {"TagSet": key_value_tags}
    # https://github.com/boto/boto3/issues/125
    if region == "us-east-1":
        s3client.create_bucket(ACL="private", Bucket=name)
    else:
        s3client.create_bucket(
            ACL="private",
            Bucket=name,
            # Type is ignored since not all regions are accepted here
            CreateBucketConfiguration={"LocationConstraint": region},  # type:ignore
        )
    s3client.put_bucket_tagging(Bucket=name, Tagging=bucket_tags)


def empty_and_delete_bucket(s3client: "S3Client", bucket_name: str) -> None:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    s3client.delete_bucket(Bucket=bucket_name)


class PersistenceType(Enum):
    s3: str = "s3"
    efs: str = "efs"


class SchedulerType(Enum):
    dictionary: str = "dictionary"
    sql: str = "sql"

    def get_sch(self) -> Scheduler:
        match self:
            case SchedulerType.dictionary:
                return DictionaryScheduler()
            case SchedulerType.sql:
                return SqlScheduler()


class QueueType(Enum):
    sync = "sync"
    threadpool = "threadpool"
    invokesync = "invokesync"

    def get_work_done_pair(self, region: str, work_name: str, done_name: str, threads: int) -> tuple[FSQueue, FSQueue]:
        match self:
            case QueueType.sync:
                return (AwsSqsSyncQueue(work_name, region), AwsSqsSyncQueue(done_name, region))
            case QueueType.threadpool:
                return (
                    AwsSqsQueue(work_name, region, write_enable=True, read_enable=False),
                    AwsSqsQueue(done_name, region, write_enable=False, read_enable=True),
                )
            case QueueType.invokesync:
                queue = AwsBoto3Sync(region, threads)
                return (queue, queue)


class AwsLambdaCluster:
    def __init__(
        self,
        lambda_zip: Path,
        data_bucket_name: str = None,
        scheduler_type: SchedulerType = SchedulerType.dictionary,
        persistence_type: PersistenceType = PersistenceType.s3,
        queue_type: QueueType = QueueType.threadpool,
        optimizations: list[DagOptimization] = [],
        region: "RegionName" = "us-east-1",
        tags: Mapping[str, str] | None = None,
        memory_mb: int = 512,
        timeout_sec: int = 30,
        batch_size: int = 1,
        threads: int = 5,
    ) -> None:
        if tags is None:
            tags = {}
        tags = dict(tags)

        cluster_name = f"faascheduler-{randint(0,99999999)}"
        self.cluster_name = cluster_name
        tags["Dask Cluster"] = cluster_name

        self.__queue_type = queue_type
        self.__threads = threads
        self.__scheduler_type = scheduler_type
        self.__optimizations = optimizations
        self.__persistence_type = persistence_type
        self.__bucket_name = f"fsch-data-{cluster_name}"
        self.__efs_access_point: str | None = None
        self.__work_queue_name = f"fsch-work-{cluster_name}"
        self.__done_queue_name = f"fsch-done-{cluster_name}"
        self.__lambda_name = f"fsch-lambda-{cluster_name}"
        self.__code_bucket_name = f"{self.__lambda_name}-code"
        self.__role_name = f"fsch-role-{cluster_name}"
        self.__role_policy_name = f"{self.__role_name}-policy"

        self.__region = region
        self.__tags = tags
        self.__memory_mb = memory_mb
        self.__timeout_sec = timeout_sec
        self.__lambda_zip = lambda_zip
        self.__data_bucket_name = data_bucket_name
        self.__batch_size = batch_size

        self.__s3_client = boto3.client("s3")
        self.__sqs_client = boto3.client("sqs", region_name=region)
        self.__lambda_client = boto3.client("lambda", region_name=region)
        self.__iam_client = boto3.client("iam")
        self.__efs_client = boto3.client("efs", region_name=region)
        self.__ec2_client = boto3.client("ec2", region_name=region)

    def start(self) -> None:
        self.__security_group_id = None
        self.__efs_access_point = None
        create_bucket(self.__s3_client, self.__bucket_name, self.__region, self.__tags)
        match self.__persistence_type:
            case PersistenceType.s3:
                pass  # S3 bucket should always be present
            case PersistenceType.efs:
                self.__security_group_id = self.__create_security_group(self.cluster_name, self.__tags)
                self.__efs_access_point = self.__create_efs(self.__security_group_id, self.__tags)

        self.__work_queue_url = self.__create_queue(self.__work_queue_name, self.__tags, self.__timeout_sec)
        self.__done_queue_url = self.__create_queue(self.__done_queue_name, self.__tags, self.__timeout_sec)

        role_buckets = []
        if self.__data_bucket_name is not None:
            role_buckets.append(self.__data_bucket_name)
        role_buckets.append(self.__bucket_name)
        self.__role_arn = self.__create_role(
            self.__role_name,
            self.__tags,
            buckets=role_buckets,
            queues_urls=[self.__work_queue_url, self.__done_queue_url],
        )
        time.sleep(5)

        lambda_envs = {
            "S3_CACHE_BUCKET": self.__bucket_name,
            "SQS_REGION": self.__region,
            "SQS_DONEQUEUE_NAME": self.__done_queue_name,
            "SQS_WORKQUEUE_NAME": self.__work_queue_name,
            "CACHE_TYPE": self.__persistence_type.value,
            "CACHE_PATH": "/mnt/app/blob",
            "SQLITE_PATH": "/mnt/app/sql",
        }

        self.__create_lambda(
            self.__lambda_name,
            self.__region,
            self.__tags,
            self.__memory_mb,
            self.__timeout_sec,
            lambda_envs,
            self.__lambda_zip,
            self.__role_arn,
            self.__batch_size,
            self.__work_queue_url,
            self.__efs_access_point,
        )

        cache = PersistantCache(AwsS3Cache(self.__bucket_name))
        config.set({"lambda_name": self.__lambda_name})
        self.__work_queue, self.__done_queue = self.__queue_type.get_work_done_pair(
            self.__region, self.__work_queue_name, self.__done_queue_name, self.__threads
        )
        sch = faas_scheduler(cache, self.__work_queue, self.__done_queue, self.__optimizations, self.__scheduler_type.get_sch())

        config.set({"scheduler": sch})

    def close(self) -> None:
        # Stop Threads
        for queue_listener in [self.__work_queue, self.__done_queue]:
            if isinstance(queue_listener, AwsSqsQueue):
                cast(AwsSqsQueue, queue_listener).stop()

        # Lambda
        try:
            self.__lambda_client.delete_function(FunctionName=self.__lambda_name)
        except Exception:
            print("Failed deleting Lambda")

        # S3 (Data & Code)
        try:
            empty_and_delete_bucket(self.__s3_client, self.__code_bucket_name)
            empty_and_delete_bucket(self.__s3_client, self.__bucket_name)
        except Exception:
            print("Failed deleting S3")

        # IAM Role
        try:
            self.__iam_client.detach_role_policy(
                RoleName=self.__role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            )
            self.__iam_client.delete_role_policy(RoleName=self.__role_name, PolicyName=self.__role_policy_name)
            self.__iam_client.delete_role(RoleName=self.__role_name)
        except Exception:
            print("Failed deleting roles")

        # Queues
        try:
            if done_queue_url := self.__done_queue_url:
                self.__sqs_client.delete_queue(QueueUrl=done_queue_url)
            if work_queue_url := self.__work_queue_url:
                self.__sqs_client.delete_queue(QueueUrl=work_queue_url)
        except Exception:
            print("Failed deleting queues")

        # Efs
        if security_group := self.__security_group_id:
            pass  # TODO: Delete all efs data

        del config.config["lambda_name"]
        del config.config["scheduler"]

    def __enter__(self) -> None:
        self.start()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> None:
        if __exc_type is not None:
            print(__exc_type, __exc_value, __traceback)
        self.close()

    def __create_queue(self, name: str, tags: Mapping[str, str], timeout_sec: int) -> str:
        """Creates queue, returning URL"""
        return self.__sqs_client.create_queue(
            QueueName=name, tags=tags, Attributes={"VisibilityTimeout": str(timeout_sec)}
        )["QueueUrl"]

    def __create_efs(self, security_group: str, tags: Mapping[str, str]) -> str:
        """
        Returns ARN of access point
        """
        key_value_tags: Sequence["TagTypeDef"] = [{"Key": key, "Value": value} for key, value in tags.items()]
        creation_token = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(64))
        efs_response = self.__efs_client.create_file_system(
            CreationToken=creation_token,
            Encrypted=False,
            ThroughputMode="elastic",
            AvailabilityZoneName="us-east-1a",
            Backup=False,
            Tags=key_value_tags,
        )

        filesystem_id = efs_response["FileSystemId"]
        filesystem_status = ""
        while filesystem_status != "available":
            response = self.__efs_client.describe_file_systems(
                FileSystemId=filesystem_id,
            )
            filesystem_status = response["FileSystems"][0]["LifeCycleState"]
            print(f"{filesystem_id} {filesystem_status} .... waiting")
            time.sleep(5)

        access_point_response = self.__efs_client.create_access_point(
            ClientToken=creation_token,
            Tags=key_value_tags,
            FileSystemId=filesystem_id,
            PosixUser={
                "Uid": 1000,
                "Gid": 1000,
            },
            RootDirectory={
                "Path": "/app",
                "CreationInfo": {"OwnerUid": 1000, "OwnerGid": 1000, "Permissions": "0777"},
            },
        )

        mount_target_response = self.__efs_client.create_mount_target(
            FileSystemId=filesystem_id,
            SubnetId=self.__get_subnet_id(),
            SecurityGroups=[
                security_group,
            ],
        )

        mount_target_id = mount_target_response["MountTargetId"]
        mount_target_status = ""
        while mount_target_status != "available":
            response = self.__efs_client.describe_mount_targets(
                MountTargetId=mount_target_id,
            )
            mount_target_status = response["MountTargets"][0]["LifeCycleState"]
            print(f"{mount_target_id} {mount_target_status} .... waiting")
            time.sleep(5)

        return cast(str, access_point_response["AccessPointArn"])

    def __get_subnet_cidr_block(self) -> str:
        subnets = self.__get_subnet()
        return subnets["Subnets"][0]["CidrBlock"]

    def __get_subnet_id(self) -> str:
        subnets = self.__get_subnet()
        return subnets["Subnets"][0]["SubnetId"]

    def __get_subnet(self) -> dict:
        return self.__ec2_client.describe_subnets(
            Filters=[
                {
                    "Name": "availabilityZone",
                    "Values": [
                        "us-east-1a",
                    ],
                }
            ]
        )

    def __get_lambda_vpc(self, security_group_id: str) -> dict:
        return {
            "SubnetIds": [
                self.__get_subnet_id(),
            ],
            "SecurityGroupIds": [
                security_group_id,
            ],
        }

    def __create_role(
        self, name: str, tags: Mapping[str, str], buckets: Sequence[str], queues_urls: Sequence[str]
    ) -> str:
        """
        Creates role with permission to given queues and buckets, and allows assumeRole for lambda.
        Returns ARN
        """
        queue_arns = [
            self.__sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            for url in queues_urls
        ]
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*" for bucket_name in buckets],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}" for bucket_name in buckets],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:SendMessage",
                    ],
                    "Resource": queue_arns,
                },
                {
                    "Effect": "Allow",
                    "Resource": "*",
                    "Action": [
                        "ec2:DescribeInstances",
                        "ec2:CreateNetworkInterface",
                        "ec2:AttachNetworkInterface",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DeleteNetworkInterface",
                    ],
                },
            ],
        }
        json_policy = json.dumps(policy)
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": ["lambda.amazonaws.com"]}, "Action": ["sts:AssumeRole"]}
            ],
        }
        json_assume_role_policy = json.dumps(assume_role_policy)

        key_value_tags: Sequence["TagTypeDef"] = [{"Key": key, "Value": value} for key, value in tags.items()]
        # Firstly, create role that is able to be used by lambda
        role = self.__iam_client.create_role(
            AssumeRolePolicyDocument=json_assume_role_policy, RoleName=name, Tags=key_value_tags
        )
        # Attach basic lambda permissions
        self.__iam_client.attach_role_policy(
            RoleName=name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )

        self.__iam_client.attach_role_policy(
            RoleName=name, PolicyArn="arn:aws:iam::aws:policy/AmazonElasticFileSystemClientFullAccess"
        )

        self.__iam_client.attach_role_policy(
            RoleName=name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
        )

        # Define as inline policy permissions needed to R/W S3 and SQS
        self.__iam_client.put_role_policy(RoleName=name, PolicyName=self.__role_policy_name, PolicyDocument=json_policy)
        time.sleep(5)

        return cast(str, role["Role"]["Arn"])

    def __create_security_group(self, name: str, tags: Mapping[str, str]) -> str:
        key_value_tags: Sequence["TagTypeDefEC2"] = [{"Key": key, "Value": value} for key, value in tags.items()]
        group = self.__ec2_client.create_security_group(
            Description="Faascheduler SG",
            GroupName=name,
            TagSpecifications=[
                {
                    "ResourceType": "security-group",
                    "Tags": key_value_tags,
                },
            ],
        )
        group_id = group["GroupId"]

        self.__ec2_client.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=[
                {
                    "FromPort": 2049,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {
                            "CidrIp": self.__get_subnet_cidr_block(),
                            "Description": "EFS NFS mount",
                        },
                    ],
                    "ToPort": 2049,
                },
            ],
        )

        return group_id

    def __create_lambda(
        self,
        name: str,
        region: "RegionName",
        tags: Mapping[str, str],
        memory_mb: int,
        timeout_sec: int,
        envs: Mapping[str, str],
        zip: Path,
        role: str,
        batch_size: int,
        queue_url: str,
        efs_access_point: str | None,
    ) -> None:
        create_bucket(self.__s3_client, self.__code_bucket_name, region, tags)
        zip_s3_key = "lambda_function_payload.zip"
        with open(zip, mode="rb") as lambda_zip:
            self.__s3_client.upload_fileobj(lambda_zip, self.__code_bucket_name, zip_s3_key)

        additional_configs: dict = {}
        if efs_access_point is not None:
            additional_configs["FileSystemConfigs"] = [{"Arn": efs_access_point, "LocalMountPath": "/mnt/app"}]
            additional_configs["VpcConfig"] = self.__get_lambda_vpc(self.__security_group_id)
        aws_function = self.__lambda_client.create_function(
            FunctionName=name,
            Runtime="python3.10",
            Role=role,
            Handler="main.on_task",
            Code={
                "S3Bucket": self.__code_bucket_name,
                "S3Key": zip_s3_key,
            },
            Description="Faascheduler worker lambda function, created dynamically with boto3",
            Timeout=timeout_sec,
            MemorySize=memory_mb,
            Publish=True,
            Environment={"Variables": envs},
            Tags=tags,
            **additional_configs,  # type: ignore
        )
        waiter = self.__lambda_client.get_waiter("function_active")
        print("Waiting function")
        waiter.wait(FunctionName=aws_function["FunctionArn"])
        print("Function active")

        queue_arn = self.__sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        self.__lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=name,
            Enabled=True,
            BatchSize=batch_size,
            MaximumBatchingWindowInSeconds=0,
        )
