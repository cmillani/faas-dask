from pathlib import Path
from typing import Iterable

from faascheduler.cache import DataCache


class LocalDiskCache(DataCache):
    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path or Path.cwd()

    def set(self, key: str, data: bytes) -> None:
        with open(self.base_path.joinpath(key), "wb") as file:
            file.write(data)

    def get(self, key: str) -> bytes:
        with open(self.base_path.joinpath(key), "rb") as file:
            data = file.read()
            return data

    def contains(self, key: str) -> bool:
        return self.base_path.joinpath(key).exists()

    def iter(self) -> Iterable[str]:
        return (file.name for file in self.base_path.iterdir())

    def len(self) -> int:
        return len(list(self.base_path.iterdir()))
