import logging
import os
from surveillance.sinks.interface import Sink
import time
from typing import Any, Dict, Union

_logger = logging.getLogger(__name__)


class FileSink(Sink):
    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        self._path = conf['path']
        self._file = None
        self._ext = None
        self._byte_count = 0

    def open(self, ext: str) -> None:
        if self._file is not None:
            return
        self._ext = ext
        filename = os.path.join(self._path, f'{time.asctime(time.localtime())}.{self._ext}')
        _logger.debug('FileSink: opening file %s', filename)
        self._file = open(filename, mode='wb')
        self._byte_count = 0

    def is_opened(self) -> bool:
        return self._file is not None

    def write(self, data: bytes) -> None:
        _logger.debug('Writing bytes %d-%d into file', self._byte_count, self._byte_count + len(data) - 1)
        self._byte_count += len(data)
        self._file.write(data)

    def close(self) -> None:
        _logger.debug('FileSink: close() called')
        self._file.close()
        self._file = None
