from multiprocessing import Queue
from typing import Any

from . import FSQueue


class MultiprocessingQueue(FSQueue):
    def __init__(self) -> None:
        self.__queue: Queue = Queue()

    def put(self, data: Any) -> None:
        self.__queue.put(data)

    def get(self) -> Any:
        return self.__queue.get()
