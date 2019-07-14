import ffmpeg
import logging
import numpy as np
import os
import subprocess
from surveillance.sinks.interface import Sink
import time
from typing import Any, Dict, Union

_logger = logging.getLogger(__name__)


class FileSink(Sink):
    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        self._path = conf['path']
        self._width = 0
        self._height = 0
        self._frame_rate = 0
        self._subprocess: Union['None', 'subprocess.Popen'] = None
        self._length = int(conf['segment_length']) * 60
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._last_write = 0
        self._frame_num = 0

    def _open_stream(self):
        filename = os.path.join(self._path, f'{time.asctime(time.localtime())}.mp4')
        _logger.debug('FileSink: opening file %s', filename)
        self._subprocess = (
            ffmpeg
            .input(
                'pipe:',
                format='rawvideo',
                pix_fmt='rgb24',
                s=f'{self._width}x{self._height}',
                # r=self._frame_rate,
                v='warning'
            )
            .filter(
                'drawtext',
                text="%{localtime:%d/%m/%y %H\:%M\:%S}",
                expansion='normal',
                font='LiberationMono',
                fontsize=21,
                fontcolor='white',
                shadowx=1,
                shadowy=1
            )
            .output(filename, vcodec='libx264')  # pix_fmt='yuv420p'
            .run_async(pipe_stdin=True)
        )
        self._frame_num = 0
        self._last_write = (time.time() + self._gm_offset) % self._length

    def _close_stream(self):
        try:
            self._subprocess.communicate(timeout=10)
            _logger.debug('FileSink: subprocess exited gracefully')
        except subprocess.TimeoutExpired:
            _logger.debug('FileSink: subprocess timeout expired, killing subprocess')
            self._subprocess.terminate()
        self._subprocess = None

    def open(self, video_info: Dict[str, Any]):
        # If sink is already opened, do nothing and return
        if self._subprocess is not None:
            return
        self._width = int(video_info['width'])
        self._height = int(video_info['height'])
        f, s = video_info['avg_frame_rate'].split('/')
        self._frame_rate = int(f) // int(s)
        self._open_stream()

    def is_opened(self) -> bool:
        return self._subprocess is not None

    def write(self, frames: 'np.ndarray'):
        if (time.time() + self._gm_offset) % self._length < self._last_write:
            _logger.debug('FileSink: segment ended, switching files')
            self._close_stream()
            self._open_stream()
        _logger.debug('Writing frames %d-%d into file', self._frame_num, self._frame_num + frames.shape[0] - 1)
        self._frame_num += frames.shape[0]
        self._subprocess.stdin.write(frames.astype(np.uint8).tobytes())
        self._last_write = (time.time() + self._gm_offset) % self._length

    def close(self):
        _logger.debug('FileSink: close() called')
        self._close_stream()


