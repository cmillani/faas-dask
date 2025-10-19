from functools import cached_property
from typing import Iterator, Set, cast

import boto3
from botocore.client import ClientError
from botocore.response import StreamingBody

from faascheduler.cache import DataCache


class AwsS3Cache(DataCache):
    def __init__(self, bucket: str) -> None:
        self.__client = boto3.resource("s3")
        self.__client_low = boto3.resource("s3").meta.client
        self.__bucket_name = bucket
        self.__cached_keys = set()
        try:
            self.__client_low.head_bucket(Bucket=self.__bucket_name)
        except ClientError as exc:
            raise ValueError(f"Bucket {self.__bucket_name}: non-existant or not enough permissions") from exc

    def set(self, key: str, data: bytes) -> None:
        self.__client_low.put_object(Bucket=self.__bucket_name, Key=key, Body=data)
        # Remote cache is only obtained once, we keep it up to date this way
        self.__cached_keys.add(key)

    def get(self, key: str) -> bytes:
        # TODO: is session left open?
        response = self.__client_low.get_object(Bucket=self.__bucket_name, Key=key)
        return cast(StreamingBody, response["Body"]).read()

    def contains(self, key: str) -> bool:
        return key in self.__all_keys

    def iter(self) -> Iterator[str]:
        return self.__all_keys

    def len(self) -> int:
        return len(self.__all_keys)

    def clear(self) -> None:
        self.__client.Bucket(self.__bucket_name).objects.all().delete()

    @cached_property
    def __all_keys(self) -> Set[str]:
        # Note: once cache is set nothing will be reused, a better implementation would consider both other
        # schedulers inserting on the cache (not contemplated currently, since we only suport one scheduler)
        self.__cached_keys = set([obj.key for obj in self.__client.Bucket(self.__bucket_name).objects.all()])
        return self.__cached_keys
