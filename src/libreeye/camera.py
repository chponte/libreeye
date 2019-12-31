# This file is part of Libreeye.
# Copyright (C) 2019 by Christian Ponte
#
# Libreeye is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Libreeye is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Libreeye. If not, see <http://www.gnu.org/licenses/>.

import errno
import ffmpeg
import logging
import queue
import subprocess
import threading
import time
from typing import List

_logger = logging.getLogger(__name__)
_stdout_thread_counter = 0
_stderr_thread_counter = 0


class Camera:
    def __init__(self, url: str, resolution=None, ffmpeg_input_options=None):
        # Configuration parameters
        self._url = url
        self._resolution = resolution
        self._ffmpeg_input_options = ffmpeg_input_options
        # Class attributes
        self._started = False
        self._ffmpeg: 'subprocess.Popen' = None
        self._video_queue: 'queue.Queue' = None
        self._stdout_handler_t: 'threading.Thread' = None
        self._stderr_handler_t: 'threading.Thread' = None

    @staticmethod
    def _stdout_handler(stream, q: 'queue.Queue', started: List[bool] = None):
        _logger.debug('Thread %s handler started', threading.current_thread().getName())
        size = 2 ** 20  # 1MB buffer size
        try:
            if started is not None:
                stream.peek(1)
                started[0] = True
            while True:
                block = stream.read(size)
                if stream.closed or len(block) == 0:
                    break
                q.put(block)
        except OSError as err:
            _logger.warning(str(err))
        _logger.debug('Thread %s handler ended', threading.current_thread().getName())

    @staticmethod
    def _stderr_handler(stream):
        _logger.debug('Thread %s handler started', threading.current_thread().getName())
        message = ''
        try:
            while True:
                c = stream.read(1)
                if stream.closed or len(c) == 0:
                    break
                c = c.decode()
                if c == '\n':
                    _logger.warning('ffmpeg: %s', message)
                    message = ''
                else:
                    message += c
        except OSError as err:
            _logger.warning(str(err))
        _logger.debug('Thread %s handler ended', threading.current_thread().getName())

    def _create_ffmpeg(self) -> 'subprocess.Popen':
        # If there are any input options, provide them to the constructor
        if self._ffmpeg_input_options is None:
            ffmpeg_pipe = ffmpeg.input(self._url, v='warning')
        else:
            ffmpeg_pipe = ffmpeg.input(
                self._url, v='warning', **self._ffmpeg_input_options
            )
        # If a resolution is specified, rescale the image
        if self._resolution:
            ffmpeg_pipe = ffmpeg_pipe.filter(
                'scale',
                width=self._resolution[0],
                height=self._resolution[1]
            )
        # Draw the time watermark
        ffmpeg_pipe = ffmpeg_pipe.filter(
            'drawtext',
            text='%{localtime:%d/%m/%y %H\:%M\:%S}',  # pylint: disable=anomalous-backslash-in-string
            expansion='normal',
            font='LiberationMono',
            fontsize=21,
            fontcolor='white',
            shadowx=1,
            shadowy=1
        )
        # Create an output pipe from which bytes can be read
        ffmpeg_pipe = ffmpeg_pipe.output(
            'pipe:',
            format='matroska',
            vcodec='libx264',
            preset='fast',
            threads=1
        )
        _logger.debug('Created new ffmpeg pipe')
        return ffmpeg_pipe.run_async(
            pipe_stdin=True,
            pipe_stdout=True,
            pipe_stderr=True
        )

    @staticmethod
    def _prepare_ffmpeg_subproc(ffmpeg_subproc) -> tuple:
        global _stdout_thread_counter
        global _stderr_thread_counter

        video_queue=queue.Queue()
        stdout_started=[False]
        stdout_handler_t=threading.Thread(
            target = Camera._stdout_handler,
            name = f'stdout-handler-{_stdout_thread_counter}',
            kwargs = {'stream': ffmpeg_subproc.stdout, 'q': video_queue, 'started': stdout_started}
        )
        _stdout_thread_counter += 1
        stdout_handler_t.start()
        # Wait for the process to connect to the camera and start providing frames
        timeout=time.time() + 30
        _logger.debug('Waiting for the fist byte from stdout stream')
        while ffmpeg_subproc.poll() is None and not stdout_started[0] and time.time() < timeout:
            time.sleep(.25)
        # If subprocess has ended prematurely
        if ffmpeg_subproc.poll() is not None:
            _logger.warning('FFmpeg process terminated prematurely')
            errors=ffmpeg_subproc.stderr.read()
            if errors is not None:
                for line in errors.decode().split('\n'):
                    _logger.warning('ffmpeg: %s', line)
            return None
        # If subprocess is running but no byte could be read
        if not stdout_started[0]:
            ffmpeg_subproc.kill()
            _logger.warning('Reached timeout while waiting for camera images')
            errors=ffmpeg_subproc.stderr.read()
            if errors is not None:
                for line in errors.decode().split('\n'):
                    _logger.warning('ffmpeg: %s', line)
            return None
        _logger.debug('First byte retrieved')
        stderr_handler_t=threading.Thread(
            target = Camera._stderr_handler, name = f'stderr-handler-{_stderr_thread_counter}', args = (ffmpeg_subproc.stderr,))
        _stderr_thread_counter += 1
        stderr_handler_t.start()
        return (video_queue, stdout_handler_t, stderr_handler_t)

    @staticmethod
    def _stop_ffmpeg_subproc(ffmpeg_subproc, timeout):
        _logger.debug('writting \'q\' to ffmpeg subprocess')
        ffmpeg_subproc.stdin.write(b'q')
        ffmpeg_subproc.stdin.close()
        try:
            ffmpeg_subproc.wait(timeout)
            _logger.debug('ffmpeg subprocess terminated')
        except subprocess.TimeoutExpired:
            _logger.debug('wait on ffmpeg timed out, sending kill signal')
            ffmpeg_subproc.kill()

    def has_started(self) -> bool:
        return self._started

    def start(self):
        _logger.debug('start called')
        if self._started:
            _logger.debug('camera is already running')
            raise RuntimeError('camera is already running')
        # Prepare FFmpeg subprocess
        self._ffmpeg=self._create_ffmpeg()
        objs=self._prepare_ffmpeg_subproc(self._ffmpeg)
        # Check for errors
        if objs is None:
            raise ConnectionError(errno.ECONNREFUSED, 'Could not read data from camera')
        # Initialize object attributes
        self._started=True
        self._video_queue=objs[0]
        self._stdout_handler_t=objs[1]
        self._stderr_handler_t=objs[2]

    def stop(self) -> bytes:
        _logger.debug('stop called')
        if not self._started:
            _logger.debug('cannot stop a non-started camera')
            raise RuntimeError('stop called on a non-started camera')
        self._started=False
        # Stop ffmpeg subprocess
        self._stop_ffmpeg_subproc(self._ffmpeg, 30)
        self._ffmpeg=None
        # Check if handler threads have ended
        self._stdout_handler_t.join(timeout = 1)
        if self._stdout_handler_t.isAlive():
            _logger.warning(
                f'stdout handler thread {self._stdout_handler_t.name} did not finish before deleting its reference')
        self._stdout_handler_t=None
        self._stderr_handler_t.join(timeout = 1)
        if self._stderr_handler_t.isAlive():
            _logger.warning(
                f'stderr handler thread {self._stderr_handler_t.name} did not finish before deleting its reference')
        self._stderr_handler_t=None
        # Read last frames
        block=bytes()
        while not self._video_queue.empty():
            block += self._video_queue.get(block = False)
        self._video_queue=None
        _logger.debug('stop finished')
        return block

    def reset(self) -> bytes:
        _logger.debug('reset called')
        if not self._started:
            _logger.debug('cannot reset a non-started camera')
            raise RuntimeError('reset called on a non-started camera')
        # Start and prepare a new ffmpeg process before ending the current one
        ffmpeg_subproc=self._create_ffmpeg()
        objs=self._prepare_ffmpeg_subproc(ffmpeg_subproc)
        # Check for errors
        if objs is None:
            raise ConnectionError(errno.ECONNREFUSED, 'Could not read data from camera')
        # Stop previous ffmpeg process
        self._stop_ffmpeg_subproc(self._ffmpeg, 30)
        # Replace old object attributes
        self._ffmpeg=ffmpeg_subproc
        old_video_queue=self._video_queue
        self._video_queue=objs[0]
        # Check if handler threads have ended
        self._stdout_handler_t.join(timeout = 1)
        if self._stdout_handler_t.isAlive():
            _logger.warning(
                f'stdout handler thread {self._stdout_handler_t.name} did not finish before deleting its reference'
            )
        self._stdout_handler_t=objs[1]
        self._stderr_handler_t.join(timeout = 1)
        if self._stderr_handler_t.isAlive():
            _logger.warning(
                f'stderr handler thread {self._stderr_handler_t.name} did not finish before deleting its reference'
            )
        self._stderr_handler_t=objs[2]
        # Read last frames from previous ffmpeg subprocess
        block=bytes()
        while not old_video_queue.empty():
            block += old_video_queue.get(block = False)
        _logger.debug('reset finished')
        return block

    def discard(self):
        _logger.debug('discard called')
        if not self._started:
            _logger.debug('nothing to discard')
            return
        self._started=False
        if self._ffmpeg is not None:
            self._ffmpeg.kill()
            self._ffmpeg=None
        self._stdout_handler_t=None
        self._stderr_handler_t=None
        self._video_queue=None

    def read(self, timeout: int = None) -> bytes:
        _logger.debug('read called')
        if not self._started:
            raise RuntimeError('cannot read from a non-started camera')
        try:
            return self._video_queue.get(block = True, timeout = timeout)
        except queue.Empty:
            raise TimeoutError(errno.ETIMEDOUT, 'Reached timeout while waiting for camera images')
