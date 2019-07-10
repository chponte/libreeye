import ffmpeg
import logging
import numpy as np
import subprocess
from surveillance.sink import Sink
from typing import List, Union


class CameraRecorder:
    def __init__(self, url: str):
        self._url = url
        self._sinks: List['Sink'] = []
        self._subprocess: Union['None', 'subprocess.Popen'] = None
        self._framebuffer_size = 5
        self._interrupt = False

    def add_sinks(self, sinks: List['Sink']):
        self._sinks.extend(sinks)

    def start(self):
        logger = logging.getLogger(__name__)
        logger.debug('camera %d: url %s', id(self), self._url)
        probe = ffmpeg.probe(self._url)
        video_info = probe['streams'][0]
        width, height = int(video_info['width']), int(video_info['height'])
        # TODO: this expression results in ZeroDivisionError sometimes
        # frame_rate = int(eval(video_info['avg_frame_rate']))
        logger.debug('camera %d: %dx%d', id(self), width, height)

        for sink in self._sinks:
            sink.open(video_info)

        self._subprocess = (
            ffmpeg
            .input(self._url, rtsp_transport='tcp', v='warning')
            .output('pipe:', format='rawvideo', pix_fmt='rgb24')
            .run_async(pipe_stdin=True, pipe_stdout=True)
        )

        while not self._interrupt:
            logger.debug('camera %d: frame block iteration', id(self))
            size = self._framebuffer_size * width * height * 3
            in_bytes = self._subprocess.stdout.read(size)
            if not in_bytes:
                # TODO: Handle premature termination
                break
            frames = np.frombuffer(in_bytes, np.uint8).reshape([self._framebuffer_size, height, width, 3])
            for sink in self._sinks:
                logger.debug('camera %d: call to %s write', id(self), type(sink))
                sink.write(frames)

        logger.debug('camera %d: stopping ffmpeg subprocess', id(self))
        try:
            out_bytes, err_bytes = self._subprocess.communicate(b'q', timeout=10)
            logger.debug('camera %d: subprocess exited gracefully', id(self))
        except subprocess.TimeoutExpired:
            logger.debug('camera %d: subprocess wait timeout expired, killing subprocess', id(self))
            self._subprocess.kill()

        for sink in self._sinks:
            # TODO: write last out_bytes to file before closing
            sink.close()

    def stop(self):
        logger = logging.getLogger(__name__)
        logger.debug('camera %d: stop called', id(self))
        self._interrupt = True
