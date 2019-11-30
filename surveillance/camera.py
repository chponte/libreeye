import errno
import ffmpeg
import logging
import queue
import subprocess
import threading
import time
from typing import Dict, List

_logger = logging.getLogger(__name__)


class Camera:
    def __init__(self, config: Dict[str, str]):
        # Configuration parameters
        self._url = config['camera']
        self._protocol = config['protocol']
        self._vp9_speed = {'low': 8, 'mid': 0, 'high': -8}[config['vp9_encoder_quality']]
        # Class attributes
        self.started = False
        self._ffmpeg: 'subprocess.Popen' = None
        self._video_queue: 'queue.Queue' = None
        self._read_thread: 'threading.Thread' = None
        self._error_thread: 'threading.Thread' = None

    @staticmethod
    def _stdout_handler(stream, q: 'queue.Queue', started: List[bool] = None):
        _logger.debug('Thread %s handler started (stdout)', threading.current_thread().getName())
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
        _logger.debug('Thread %s handler started (stderr)', threading.current_thread().getName())
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

    def _ffmpeg_init(self) -> 'subprocess.Popen':
        return (
            ffmpeg
            .input(self._url, r=5, rtsp_transport=self._protocol, v='warning')
            .filter(
                'scale',
                width=1280,
                height=720
            )
            .filter(
                'drawtext',
                text='%{localtime:%d/%m/%y %H\:%M\:%S}',  # pylint: disable=anomalous-backslash-in-string
                expansion='normal',
                font='LiberationMono',
                fontsize=21,
                fontcolor='white',
                shadowx=1,
                shadowy=1
            )
            .output('pipe:', format='matroska', vcodec='libx264', preset='fast', threads=1)
            .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
        )

    def start(self):
        _logger.debug('start called')
        # Open FFmpeg subprocess
        self._ffmpeg = self._ffmpeg_init()
        self._video_queue = queue.Queue()
        self._read_thread = threading.Thread(
            target=self._stdout_handler,
            kwargs={'stream': self._ffmpeg.stdout, 'q': self._video_queue}
        )
        self._read_thread.start()
        self._error_thread = threading.Thread(target=self._stderr_handler, args=(self._ffmpeg.stderr,))
        self._error_thread.start()
        self.started = True

    def stop(self) -> bytes:
        _logger.debug('stop called')
        self.started = False
        _logger.debug('writting \'q\' to ffmpeg subprocess')
        if not self._ffmpeg.stdin.closed:
            self._ffmpeg.stdin.write(b'q')
            self._ffmpeg.stdin.close()
        _logger.debug('waiting for threads to finish')
        self._read_thread.join(timeout=10)
        if self._read_thread.isAlive():
            _logger.debug('Join on read thread timed out')
        self._read_thread = None
        # If the read thread ended, it is safe to assume that the error thread ended aswell
        self._error_thread = None
        self._ffmpeg.kill()
        self._ffmpeg = None
        block = bytes()
        while not self._video_queue.empty():
            block += self._video_queue.get(block=False)
        self._video_queue = None
        _logger.debug('stop finished')
        return block

    def reset(self) -> bytes:
        _logger.debug('reset called')
        self.started = False
        # Start new ffmpeg process before ending current one
        new_ffmpeg = self._ffmpeg_init()
        new_queue = queue.Queue()
        stdout_started = [False]
        new_stdout_thread = threading.Thread(
            target=self._stdout_handler,
            kwargs={'stream': new_ffmpeg.stdout, 'q': new_queue, 'started': stdout_started}
        )
        new_stdout_thread.start()
        # Wait for the process to connect to the camera and start providing frames
        timeout = time.time() + 30
        while not stdout_started[0] and time.time() < timeout:
            time.sleep(.25)
        if not stdout_started[0]:
            new_ffmpeg.kill()
            self.stop()
            raise TimeoutError(errno.ETIMEDOUT, 'Reached timeout while waiting for camera images')
        new_stderr_thread = threading.Thread(target=self._stderr_handler, args=(new_ffmpeg.stderr,))
        new_stderr_thread.start()
        _logger.debug('new video process allocated')
        # End previous ffmpeg process and replace object attributes with new ffmpeg instance
        block = self.stop()
        self._ffmpeg = new_ffmpeg
        self._video_queue = new_queue
        self._read_thread = new_stdout_thread
        self._error_thread = new_stderr_thread
        self.started = True
        _logger.debug('reset finished')
        # Return remaining bytes from last ffmpeg process
        return block

    def discard(self):
        _logger.debug('discard called')
        self.started = False
        self._ffmpeg.kill()
        self._ffmpeg = None
        self._read_thread = None
        self._video_queue = None

    def read(self, timeout: int = None) -> bytes:
        _logger.debug('read called')
        try:
            return self._video_queue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutError(errno.ETIMEDOUT, 'Reached timeout while waiting for camera images')
