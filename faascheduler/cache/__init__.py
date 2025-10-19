from abc import ABC, abstractmethod
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any, Iterable, Iterator, Mapping, cast

from distributed.protocol.pickle import dumps, loads


class DataCache(ABC):
    @abstractmethod
    def iter(self) -> Iterable[str]:
        pass

    @abstractmethod
    def len(self) -> int:
        pass

    @abstractmethod
    def contains(self, key: str) -> bool:
        pass

    @abstractmethod
    def set(self, key: str, data: bytes) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> bytes:
        pass


class PersistantCache(Mapping):
    def __init__(self, data_cache: DataCache) -> None:
        self.__data_cache: DataCache = data_cache

    def __key_to_filename(self, key: Any) -> str:
        key_data = dumps(key)
        # b64 encode is not safe since name can be greater than max file name
        return urlsafe_b64encode(key_data).decode("ascii")

    def __filename_to_key(self, filename: str) -> bytes:
        return urlsafe_b64decode(filename.encode("ascii"))

    def __setitem__(self, key: Any, obj: Any) -> None:
        obj_data = dumps(obj)
        self.__data_cache.set(self.__key_to_filename(key), obj_data)

    def __getitem__(self, key: Any) -> Any:
        return loads(self.__data_cache.get(self.__key_to_filename(key)))

    def __delitem__(self, key: Any) -> None:
        # Purposely not implemented - on pure functions should not be a problem
        pass

    def __iter__(self) -> Iterator[Any]:
        for file in self.__data_cache.iter():
            key: str = loads(self.__filename_to_key(file))
            yield key

    def __len__(self) -> int:
        return cast(int, self.__data_cache.len())

    def __contains__(self, key: Any) -> bool:
        return cast(bool, self.__data_cache.contains(self.__key_to_filename(key)))
