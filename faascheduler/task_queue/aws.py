import json
import threading
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from itertools import islice
from queue import Empty, Queue
from typing import Any

import boto3
from dask import config
from distributed.protocol.pickle import dumps, loads

from faascheduler.stats import stats
from faascheduler.task_queue import FSQueue


class TaskQueue:
    """
    Thread safe queue with batch operations
    """

    def __init__(self, data_event: threading.Event) -> None:
        self.lock = threading.Lock()
        self.data_event = data_event
        self.items: list[Any] = []

    def add(self, item: Any) -> None:
        with self.lock:
            self.data_event.set()
            self.items.append(item)

    def add_many(self, items: list[Any]) -> None:
        with self.lock:
            self.data_event.set()
            self.items.extend(items)

    def get_many(self, max_count: int) -> Any:
        with self.lock:
            if max_count > len(self.items):
                max_count = len(self.items)
                self.data_event.clear()
            items, self.items = self.items[:max_count], self.items[max_count:]
        return items

    def get_all(self) -> Any:
        with self.lock:
            items, self.items = self.items, []
            self.data_event.clear()
        return items


class AwsSqsSyncQueue(FSQueue):
    def __init__(self, queue: str, region: str) -> None:
        self.__client = boto3.resource("sqs", region_name=region)
        self.__queue = self.__client.get_queue_by_name(QueueName=queue)

    def put(self, data: Any) -> None:
        self.__queue.send_message(MessageBody=urlsafe_b64encode(dumps(data)).decode("ascii"))

    def get(self) -> Any:
        while not (
            messages := self.__queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=20, WaitTimeSeconds=20)
        ):
            pass  # Keep trying to long pool messages
        # Success! Will receive exactly one message (given MaxNumberOfMessages) so it should be safe to pop and decode
        message = messages.pop()
        self.__queue.delete_messages(
            Entries=[
                {
                    "Id": message.message_id,
                    "ReceiptHandle": message.receipt_handle,
                }
            ]
        )
        return loads(urlsafe_b64decode(message.body.encode("ascii")))

    def purge(self) -> None:
        self.__queue.purge()


class AwsSqsQueue(FSQueue):
    def __init__(self, queue: str, region: str, read_enable: bool, write_enable: bool) -> None:
        self.read_enable = read_enable
        self.write_enable = write_enable
        self.__client = boto3.client("sqs", region_name=region)
        self.__sqs_resource = boto3.resource("sqs", region_name=region)
        self.__queue = self.__sqs_resource.get_queue_by_name(QueueName=queue)
        self.__cache: list[Any] = []
        self.__get_data_event = threading.Event()
        self.__writa_data_event = threading.Event()
        self.__send_queue: TaskQueue = TaskQueue(self.__writa_data_event)
        self.__read_queue: TaskQueue = TaskQueue(self.__get_data_event)
        self.__read_pool = 7
        self.__write_pool = 5
        self.__should_stop = threading.Event()
        self.__start_threads()

    def __start_threads(self) -> None:
        if self.write_enable:
            for _ in range(self.__write_pool):
                send_thread = threading.Thread(
                    target=self.__send_thread, args=[self.__writa_data_event, self.__should_stop]
                )
                send_thread.daemon = True
                send_thread.start()
        if self.read_enable:
            for _ in range(self.__read_pool):
                read_thread = threading.Thread(target=self.__read_thread, args=[self.__should_stop])
                read_thread.daemon = True
                read_thread.start()

    def __send_thread(self, writa_data_event: threading.Event, should_stop: threading.Event) -> None:
        batch_size = 10
        while not should_stop.is_set():
            messages = self.__send_queue.get_many(batch_size)
            if len(messages) == 0:
                # No new message, event is set when new messages are inserted into queue, so we wait
                writa_data_event.wait()
                # Start again, to get new data
                continue
            msg_it = iter(messages)
            while batch := tuple(islice(msg_it, batch_size)):
                for message in batch:
                    stats[str(message[0])]["sqs_api_sent"] = time.time()

                batch_messages = [
                    {"Id": str(i), "MessageBody": urlsafe_b64encode(dumps(message)).decode("ascii")}
                    for i, message in enumerate(batch)
                ]

            self.__recursive_batch_sender(batch_messages)

    def __recursive_batch_sender(self, entries: list[dict]) -> None:
        """
        Tries to break batches into smaller ones to respect batch size limit

        If a single message is still bigger than limit, error is raised
        """
        try:
            self.__queue.send_messages(Entries=entries)  # type: ignore
        except self.__client.exceptions.BatchRequestTooLong as e:
            if len(entries) == 1:
                raise e
            else:
                cut = len(entries) // 2
                self.__recursive_batch_sender(entries[:cut])
                self.__recursive_batch_sender(entries[cut:])

    def __read_thread(self, should_stop: threading.Event) -> None:
        while not should_stop.is_set():
            # Long polling, blocks up to WaitTimeSeconds or unting there are MaxNumberOfMessages to return
            if messages := self.__queue.receive_messages(
                MaxNumberOfMessages=10, VisibilityTimeout=20, WaitTimeSeconds=5
            ):
                parsed_messages = [loads(urlsafe_b64decode(message.body.encode("ascii"))) for message in messages]
                for message in parsed_messages:
                    stats[str(message[0])]["sqs_api_read"] = time.time()
                # Mark all as OK
                self.__queue.delete_messages(
                    Entries=[
                        {
                            "Id": message.message_id,
                            "ReceiptHandle": message.receipt_handle,
                        }
                        for message in messages
                    ]
                )
                self.__read_queue.add_many(parsed_messages)

    def put(self, data: Any) -> None:
        self.__send_queue.add(data)

    def get(self) -> Any:
        if len(self.__cache) == 0:
            self.__get_data_event.wait()
            self.__cache = self.__read_queue.get_all()

        return self.__cache.pop()

    def purge(self) -> None:
        self.__queue.purge()

    def stop(self) -> None:
        self.__should_stop.set()  # Marks threads for exit
        self.__writa_data_event.set()  # Unlocks all write threads so they can finish


class AwsBoto3Sync(FSQueue):
    def __init__(self, region: str, thread_pool_size=5):
        self.__send_queue: Queue = Queue()
        self.__read_queue: Queue = Queue()
        self.__should_stop = threading.Event()
        self.__lambda_name: str = config.get("lambda_name")
        self.__region: str = region
        self.__start_threads(thread_pool_size)

        if self.__lambda_name is None:
            raise ValueError(
                'Dask config "lambda_name" must be set. Use faascheduler.aws.AwsLambdaCluster to start the cluster.'
            )

    def put(self, data: Any) -> None:
        self.__send_queue.put(data)

    def get(self) -> Any:
        return self.__read_queue.get()

    def stop(self) -> None:
        self.__should_stop.set()  # Marks threads for exit

    def __send_thread(self, should_stop: threading.Event) -> None:
        client = boto3.client("lambda", region_name=self.__region)
        while not should_stop.is_set():
            try:
                message = self.__send_queue.get(timeout=1)  # Timeout so that we can kill thread when not needed
                payload = json.dumps({"SyncBody": urlsafe_b64encode(dumps(message)).decode("ascii")}).encode()
                lambda_response = client.invoke(
                    FunctionName=self.__lambda_name,
                    Payload=payload,
                )
                response_data = lambda_response["Payload"]
                raw_response = json.loads(response_data.read())

                result = raw_response["SyncResult"]

                self.__read_queue.put(loads(urlsafe_b64decode(result.encode("ascii"))))
            except Empty:
                pass

    def __start_threads(self, thread_pool_size) -> None:
        for _ in range(thread_pool_size):
            send_thread = threading.Thread(target=self.__send_thread, args=[self.__should_stop])
            send_thread.daemon = True
            send_thread.start()
