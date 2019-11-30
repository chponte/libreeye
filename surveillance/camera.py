import errno
import ffmpeg
import logging
import queue
import subprocess
import threading
from typing import Dict

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
    def _stdout_handler(stream, q: 'queue.Queue'):
        _logger.debug('Thread %s handler started (stdout)', threading.current_thread().getName())
        try:
            size = 256 * 2 ** 10 # 256kb buffer size
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
                'drawtext',
                text='%{localtime:%d/%m/%y %H\:%M\:%S}',
                expansion='normal',
                font='LiberationMono',
                fontsize=21,
                fontcolor='white',
                shadowx=1,
                shadowy=1
            )
            .output('pipe:', format='matroska', vcodec='libvpx-vp9', deadline='realtime', speed=self._vp9_speed)
            .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
        )

    def start(self):
        # Open FFmpeg subprocess
        self._ffmpeg = self._ffmpeg_init()
        self._video_queue = queue.Queue()
        self._read_thread = threading.Thread(target=self._stdout_handler, args=(self._ffmpeg.stdout, self._video_queue))
        self._read_thread.start()
        self._error_thread = threading.Thread(target=self._stderr_handler, args=(self._ffmpeg.stderr,))
        self._error_thread.start()
        self.started = True

    def stop(self) -> bytes:
        self.started = False
        if not self._ffmpeg.stdin.closed:
            self._ffmpeg.stdin.write(b'q')
            self._ffmpeg.stdin.close()
        self._read_thread.join()
        self._read_thread = None
        self._error_thread.join()
        self._error_thread = None
        self._ffmpeg.kill()
        self._ffmpeg = None
        block = bytes()
        while not self._video_queue.empty():
            block += self._video_queue.get(block=False)
        self._video_queue = None
        return block

    def reset(self) -> bytes:
        self.started = False
        new_ffmpeg = self._ffmpeg_init()
        new_queue = queue.Queue()
        new_stdout_thread = threading.Thread(target=self._stdout_handler, args=(new_ffmpeg.stdout, new_queue))
        new_stderr_thread = threading.Thread(target=self._stderr_handler, args=(new_ffmpeg.stderr,))
        new_ffmpeg.stdout.peek(1)
        block = self.stop()
        self._ffmpeg = new_ffmpeg
        self._video_queue = new_queue
        self._read_thread = new_stdout_thread
        self._read_thread.start()
        self._error_thread = new_stderr_thread
        self._error_thread.start()
        self.started = True
        return block

    def discard(self):
        self.started = False
        self._ffmpeg.kill()
        self._ffmpeg = None
        self._read_thread = None
        self._video_queue = None

    def read(self, timeout: int = None) -> bytes:
        try:
            return self._video_queue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutError(errno.ETIMEDOUT, 'Reached timeout while waiting for camera images')
