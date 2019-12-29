from abc import ABC, abstractmethod
from typing import Any, Dict


class Sink(ABC):
    @abstractmethod
    def open(self, ext: str) -> None:
        pass

    @abstractmethod
    def is_opened(self) -> bool:
        pass

    @abstractmethod
    def write(self, data: bytes) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
