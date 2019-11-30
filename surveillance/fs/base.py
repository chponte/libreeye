from abc import ABC, abstractmethod


class ItemStorage(ABC):
    @abstractmethod
    def get_days(self) -> int:
        pass

    @abstractmethod
    def walk(self):
        pass


class Item(ABC):
    @abstractmethod
    def get_path(self) -> str:
        pass

    @abstractmethod
    def getmtime(self) -> float:
        pass

    @abstractmethod
    def remove(self) -> None:
        pass