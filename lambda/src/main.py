import os
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from pathlib import Path
from typing import Any

from distributed.protocol.pickle import dumps

from faascheduler.cache import PersistantCache
from faascheduler.cache.aws import AwsS3Cache
from faascheduler.cache.disk import LocalDiskCache
from faascheduler.executor import Executor, setup_exit_gracefully
from faascheduler.task_queue.aws import AwsSqsSyncQueue

cache_bucket = os.environ.get("S3_CACHE_BUCKET", "faascheduler-data")
print("FaaS-Dask:: Connecting to S3")
s3cache = AwsS3Cache(cache_bucket)
shared_cache = PersistantCache(data_cache=s3cache)

local_cache = None
sqlite_path = None
sql_db_path = None
work_queue = None
done_queue = None
if os.environ.get("CACHE_TYPE", "s3") == "efs":
    cache_path_str = os.environ["CACHE_PATH"]
    cache_path = Path(cache_path_str)
    sqlite_path_str = os.environ["SQLITE_PATH"]
    sqlite_path = Path(sqlite_path_str)
    for path in [sqlite_path, cache_path]:
        if not path.exists():
            path.mkdir()
    disk_cache = LocalDiskCache(cache_path)
    local_cache = PersistantCache(data_cache=disk_cache)
    # SQLite experiment demonstrated that it locks easily, so 
    # we disabled it. Keeping for historical reason.
    #sql_db_path = str(sqlite_path.joinpath("sql.db"))
    print("FaaS-Dask:: Skipping SQS (not supported when using EFS)")
else:
    print("FaaS-Dask:: Connecting to SQS")
    region = os.environ.get("SQS_REGION", "us-east-1")
    done_queue_name = os.environ.get("SQS_DONEQUEUE_NAME", "faascheduler-donequeue")
    work_queue_name = os.environ.get("SQS_WORKQUEUE_NAME", "faascheduler-workqueue")
    done_queue = AwsSqsSyncQueue(done_queue_name, region)
    work_queue = AwsSqsSyncQueue(work_queue_name, region)

executor = Executor(work_queue, done_queue, shared_cache, local_cache, sql_db_path)
setup_exit_gracefully(shared_cache)
print("FaaS-Dask:: Initialized successfully")


def on_task(event: dict, context: Any) -> dict | None:
    start = time.time()
    if "SyncBody" in event:  # Sync invoke
        task = urlsafe_b64decode(event["SyncBody"].encode("ascii"))
        response_data = executor.execute(task, start=start, req_id=context.aws_request_id, sync=True)
        result = urlsafe_b64encode(dumps(response_data)).decode("ascii")
        return {"SyncResult": result}
    else:  # SQS invocation
        for message in event["Records"]:
            task = urlsafe_b64decode(message["body"].encode("ascii"))
            executor.execute(task, start=start, req_id=context.aws_request_id)
        return None
