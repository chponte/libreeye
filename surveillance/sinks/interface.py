from abc import ABC, abstractmethod
from typing import Any, Dict


class Sink(ABC):
    @abstractmethod
    def open(self, video_info: Dict[str, Any]):
        pass

    @abstractmethod
    def is_opened(self) -> bool:
        pass

    @abstractmethod
    def write(self, data: bytes):
        pass

    @abstractmethod
    def close(self):
        pass
