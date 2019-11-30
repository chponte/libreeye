import errno
import logging
import time
from surveillance.camera import Camera
from surveillance.sinks.interface import Sink
from typing import Dict, List, Union

_logger = logging.getLogger(__name__)


class CameraRecorder:
    class _NoSinkError(Exception):
        pass

    def __init__(self, conf: Dict[str, str]):
        # Configuration attributes
        self._length = int(conf['segment_length']) * 60
        self._timeout = int(conf['timeout'])
        # Internal attributes
        self._sinks: List['Sink'] = []
        self._sinks_avail: List['Sink'] = []
        self._interrupt = False
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._camera = Camera(conf)

    def add_sinks(self, sinks: List['Sink']):
        self._sinks.extend(sinks)

    def _open_sinks(self):
        for sink in self._sinks:
            try:
                _logger.debug('Opening sink %s', sink.__class__.__name__)
                sink.open('mkv')
                self._sinks_avail.append(sink)
            except OSError as err:
                _logger.warning(str(err))
        if len(self._sinks_avail) == 0:
            raise self._NoSinkError()

    def _write_to_sinks(self, b):
        for sink in self._sinks_avail:
            try:
                _logger.debug(f'Writting to sink {type(sink).__name__}')
                sink.write(b)
                _logger.debug(f'Write to sink {type(sink).__name__} complete')
            except OSError as err:
                _logger.error(
                    'Error occurred with sink %s (current segment may be lost): %s', sink.__class__.__name__, str(err)
                )
                self._sinks_avail.remove(sink)
                if len(self._sinks_avail) == 0:
                    raise self._NoSinkError()

    def _close_sinks(self):
        for sink in self._sinks_avail:
            try:
                sink.close()
            except OSError as err:
                _logger.error('Error occurred while closing sink %s: %s', sink.__class__.__name__, str(err))
        self._sinks_avail.clear()

    def _segment_loop(self):
        last_time = 0
        t = (time.time() + self._gm_offset) % self._length
        while not self._interrupt and t > last_time:
            last_time = t
            buffer = self._camera.read(self._timeout)
            _logger.debug(f'segment iteration, {len(buffer)} bytes')
            self._write_to_sinks(buffer)
            t = (time.time() + self._gm_offset) % self._length
        _logger.debug('Segment ended')

    def start(self):
        while not self._interrupt:
            try:
                if not self._camera.started:
                    self._camera.start()
                self._open_sinks()
                self._segment_loop()
                if self._interrupt:
                    self._write_to_sinks(self._camera.stop())
                else:
                    self._write_to_sinks(self._camera.reset())
                self._close_sinks()
            except self._NoSinkError:
                _logger.error('No sinks available, restarting process')
                self._camera.stop()
                exit(errno.ECONNRESET)
            except OSError as err:
                _logger.error(str(err))
                self._camera.discard()
                self._close_sinks()
                exit(err.errno)

    def stop(self):
        _logger.debug('Stop called')
        self._interrupt = True
