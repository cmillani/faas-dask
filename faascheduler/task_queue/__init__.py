from abc import ABC, abstractmethod
from typing import Any


class FSQueue(ABC):
    @abstractmethod
    def put(self, data: Any) -> None:
        pass

    @abstractmethod
    def get(self) -> Any:
        pass
