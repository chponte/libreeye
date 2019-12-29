from libreeye.fs.base import ItemStorage, Item
import logging
import os

_logger = logging.getLogger(__name__)


class LocalStorage(ItemStorage):
    def __init__(self, path, days):
        self._path = path
        self._days = days

    def get_days(self):
        return self._days

    def walk(self):
        for root, dirs, files in os.walk(self._path):
            yield (
                LocalStorage(root, self._path),
                [LocalStorage(os.path.join(root, d), self._days) for d in dirs],
                [LocalItem(os.path.join(root, f)) for f in files]
            )


class LocalItem(Item):
    def __init__(self, path):
        self._path = path

    def get_path(self):
        return self._path

    def getmtime(self):
        return os.path.getmtime(self._path)

    def remove(self):
        _logger.info(f'Removing file {self._path}')
        os.remove(self._path)
