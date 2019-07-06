from abc import ABC, abstractmethod
import ffmpeg
import logging
import numpy as np
import os
import subprocess
import time
from typing import Any, Dict, Union


class Sink(ABC):
    @abstractmethod
    def open(self, video_info: Dict[str, Any]):
        pass

    @abstractmethod
    def write(self, data: bytes):
        pass

    @abstractmethod
    def close(self):
        pass


class FileSink(Sink):
    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        self._path = conf['path']
        self._length = int(conf['segment_length']) * 60
        self._width = 0
        self._height = 0
        self._frame_rate = 0
        self._subprocess: Union['None', 'subprocess.Popen'] = None
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._last_write = 0

    def _open_stream(self):
        logger = logging.getLogger(__name__)
        filename = os.path.join(self._path, f'{time.asctime(time.localtime())}.mp4')
        logger.debug('FileSink: opening file %s', filename)
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
        self._last_write = (time.time() + self._gm_offset) % self._length

    def _close_stream(self):
        logger = logging.getLogger(__name__)
        try:
            self._subprocess.communicate(timeout=10)
            logger.debug('FileSink: subprocess exited gracefully')
        except subprocess.TimeoutExpired:
            logger.debug('FileSink: subprocess timeout expired, killing subprocess')
            self._subprocess.terminate()

    def open(self, video_info: Dict[str, Any]):
        if self._subprocess is not None:
            # TODO: throw error when opening an already opened stream
            return
        self._width = int(video_info['width'])
        self._height = int(video_info['height'])
        f, s = video_info['avg_frame_rate'].split('/')
        self._frame_rate = int(f) // int(s)
        self._open_stream()

    def write(self, frames: 'np.ndarray'):
        if (time.time() + self._gm_offset) % self._length < self._last_write:
            logger = logging.getLogger(__name__)
            logger.debug('FileSink: segment ended, switching files')
            self._close_stream()
            self._open_stream()
        self._subprocess.stdin.write(frames.astype(np.uint8).tobytes())
        self._last_write = (time.time() + self._gm_offset) % self._length

    def close(self):
        logger = logging.getLogger(__name__)
        logger.debug('FileSink: terminating ffmpeg subprocess')
        self._close_stream()
