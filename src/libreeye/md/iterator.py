import logging
import numpy as np
import os
import sys

import ffmpeg

_logger = logging.getLogger(__name__)


class FrameIterator:
    def __init__(self, input_format, input_width, input_height,
                 input_framerate, scale):
        super().__init__()
        self._input_format = input_format
        self._input_width = input_width
        self._scaled_width = round(input_width * scale)
        self._input_height = input_height
        self._scaled_height = round(input_height * scale)
        self._input_framerate = input_framerate

    def __iter__(self):
        self._ffmpeg_open()
        _logger.debug('entering frame iterator loop')
        frame_bytes = self._ffmpeg.stdout.read(
            self._scaled_height * self._scaled_width
        )
        while len(frame_bytes) > 0:
            frame = (
                np.frombuffer(frame_bytes, np.uint8)
                .reshape([self._scaled_height, self._scaled_width])
            )
            yield frame
            frame_bytes = self._ffmpeg.stdout.read(
                self._scaled_height * self._scaled_width
            )
        _logger.debug('frame iterator loop exited')
        self._ffmpeg = None

    def _ffmpeg_open(self):
        # Open video with ffmpeg
        pipeline = ffmpeg.input('pipe:', f='mjpeg', v='warning')
        # Apply framestep filter to reduce fps down to 1
        pipeline = pipeline.filter('framestep', step=self._input_framerate)
        # Apply scale filter if necessary
        if (self._input_width > self._scaled_width or
                self._input_height > self._scaled_height):
            pipeline = pipeline.filter(
                'scale',
                width=self._scaled_width,
                height=self._scaled_height
            )
        # Create ffmpeg process
        self._ffmpeg = (
            pipeline.output('pipe:', f='rawvideo', pix_fmt='gray8')
            .run_async(pipe_stdin=True, pipe_stdout=True)
        )

    def write(self, frame) -> None:
        if self._ffmpeg is not None:
            self._ffmpeg.stdin.write(frame)

    def close(self):
        if self._ffmpeg is not None:
            self._ffmpeg.stdin.close()
