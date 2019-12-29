import logging
import os
from libreeye.sinks.interface import Sink
import time
from typing import Dict

_logger = logging.getLogger(__name__)


class FileSink(Sink):
    def __init__(self, path):
        super().__init__()
        self._path = path
        os.makedirs(self._path, mode=0o755, exist_ok=True)
        self._file = None
        self._filename = None
        self._ext = None
        self._byte_count = 0

    def open(self, ext: str) -> None:
        if self._file is not None:
            return
        self._ext = ext
        self._filename = os.path.join(self._path, f'{time.strftime("%d_%m_%y_%H_%M", time.localtime())}.{self._ext}')
        _logger.debug('Opening file %s', self._filename)
        self._file = open(self._filename, mode='wb')
        self._byte_count = 0

    def is_opened(self) -> bool:
        return self._file is not None

    def write(self, data: bytes) -> None:
        #_logger.debug('Writing bytes %d-%d into file', self._byte_count, self._byte_count + len(data) - 1)
        self._byte_count += len(data)
        self._file.write(data)

    def close(self) -> None:
        _logger.debug('Closing file %s', self._filename)
        self._file.close()
        self._file = None
        self._filename = None
