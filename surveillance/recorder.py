import errno
import ffmpeg
import io
import logging
import queue
import re
import subprocess
from threading import Thread
import time
from surveillance.sinks.interface import Sink
from typing import Dict, List, Union
import warnings

_logger = logging.getLogger(__name__)


class CameraRecorder:
    class _OutReaderThread(Thread):
        def __init__(self, stream: 'io.BufferedReader', block_size: int, sinks: List['Sink'],
                     error_queue: 'queue.Queue'):
            super().__init__()
            self._stream = stream
            self._block_size = block_size
            self._sinks = sinks
            self._error_queue = error_queue

        def run(self) -> None:
            try:
                while True:
                    block = self._stream.read(self._block_size)
                    if self._stream.closed or len(block) == 0:
                        break
                    for sink in self._sinks:
                        if sink.is_opened():
                            sink.write(block)
            except OSError as err:
                self._error_queue.put(err)

    class _ErrReaderThread(Thread):
        def __init__(self, stream: 'io.BufferedReader', q: 'queue.Queue'):
            super().__init__()
            self._stream = stream
            self._queue = q

        @staticmethod
        def _parse_error_msg(msg: str) -> Exception:
            _logger.debug(msg)
            # Check for TCP error
            #   > [tcp @ 0x563bcc796c80] Connection to tcp://192.168.0.123:554?timeout=5000000 failed: No route to host
            m = re.match(r'^\[tcp @ [a-z0-9]+\] ([^\n]+)$', msg)
            if m is not None:
                return ConnectionRefusedError(errno.EHOSTUNREACH, m[1])
            # Check for RSTP connection reset
            #   > [rtsp @ 0x5654b71fb580] CSeq 11 expected, 0 received.
            m = re.match(r'^\[rtsp @ [a-z0-9]+\] CSeq (\d+) expected, (\d+) received\.$', msg)
            if m is not None:
                return ConnectionResetError(errno.ECONNRESET, f'CSeq {m[1]} expected, {m[2]} received.')
            # Check for RTSP probe codec error
            #   > [rtsp @ 0x56404d2932c0] Could not find codec parameters for stream 0 (Video: h264, none): unspecified
            #   size
            m = re.match(r'^\[rtsp @ [a-z0-9]+\] (Could not find codec parameters for stream \d+ \(Video: [^\n]*, '
                         r'none\): unspecified size)$', msg)
            if m is not None:
                return ConnectionAbortedError(errno.ECONNABORTED, m[1])
            # If no other check applies, raise a general warning
            return RuntimeWarning(msg)

        def run(self) -> None:
            line = ''
            while True:
                c = self._stream.read(1).decode()
                # Exit thread if no character was retrieved
                if self._stream.closed or len(c) == 0:
                    break
                # If the character is not a break line, append the character into the line and continue looping
                if c != '\n':
                    line += c
                    continue
                # If the character is a break line, put the line onto the queue and clear the buffer
                err = self._parse_error_msg(line)
                self._queue.put(err)
                line = ''

    def __init__(self, conf: Dict[str, str]):
        # Configuration attributes
        self._url = conf['camera']
        self._timeout = int(conf['timeout'])
        self._length = int(conf['segment_length']) * 60
        self._buffer_size = int(conf['buffer_size']) * 2 ** 20
        self._reconnect_attempts = int(conf['reconnect_attempts'])
        self._reconnect_delay = int(conf['reconnect_base_delay'])
        # Internal attributes
        self._sinks: List['Sink'] = []
        self._interrupt = False
        self._ffmpeg_stream: Union['None', 'subprocess.Popen'] = None
        self._stdout_reader_thread: Union['None', 'Thread'] = None
        self._stderr_reader_thread: Union['None', 'Thread'] = None
        self._error_queue: Union['None', 'queue.Queue'] = None
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))

    def _open_ffmpeg_stream(self):
        # Check if FFmpeg stream is already opened
        if self._ffmpeg_stream is not None:
            return
        # Open FFmpeg subprocess
        self._ffmpeg_stream = (
            ffmpeg
            .input(self._url, rtsp_transport='tcp', stimeout=self._timeout * 10 ** 3, v='warning')
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
            .output('pipe:', format='matroska', vcodec='libvpx-vp9')
            .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
        )
        # Create error reader thread
        self._error_queue = queue.Queue()
        self._stderr_reader_thread = self._ErrReaderThread(self._ffmpeg_stream.stderr, self._error_queue)
        self._stderr_reader_thread.start()
        # Wait for the first byte to be returned or the subprocess to die (and thus the error thread to queue the error)
        self._ffmpeg_stream.stdout.peek(1)
        while not self._error_queue.empty():
            error = self._error_queue.get()
            # If the error is a warning, do not raise any exception
            if isinstance(error, Warning):
                _logger.warning(str(error))
                continue
            # Otherwise remove invalid object references and raise the error
            self._ffmpeg_stream.terminate()
            self._ffmpeg_stream = None
            self._stderr_reader_thread = None
            raise error from None
        # If no errors have occurred, create the output reader thread as well
        self._stdout_reader_thread = self._OutReaderThread(
            self._ffmpeg_stream.stdout, self._buffer_size, self._sinks, self._error_queue
        )
        self._stdout_reader_thread.start()

    def _close_ffmpeg_stream(self):
        # End FFmpeg subprocess and remove object reference
        if self._ffmpeg_stream is not None:
            self._ffmpeg_stream.stdin.write(b'q')
            self._ffmpeg_stream.stdin.close()
            self._ffmpeg_stream = None

    def _join_reader_threads(self, timeout: int) -> None:
        # Read last thread results and remove object references
        self._stdout_reader_thread.join(timeout=timeout)
        if self._stdout_reader_thread.is_alive():
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached when joining stdout reader thread')
        self._stdout_reader_thread = None
        self._stderr_reader_thread.join(timeout=timeout)
        if self._stderr_reader_thread.is_alive():
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached when joining stderr reader thread')
        self._stderr_reader_thread = None

    def add_sinks(self, sinks: List['Sink']):
        self._sinks.extend(sinks)

    def run(self):
        self._interrupt = False
        attempts = self._reconnect_attempts
        error_wait_time = self._reconnect_delay
        while not self._interrupt:
            # Prepare sinks
            for sink in self._sinks:
                try:
                    sink.open('mkv')
                except OSError as err:
                    warnings.warn(str(err), RuntimeWarning)
            # If no sinks are available, terminate execution
            if all([not sink.is_opened() for sink in self._sinks]):
                attempts -= 1
                if attempts == 0:
                    raise RuntimeError()
                time.sleep(error_wait_time)
                error_wait_time *= 2
                continue
            # Open FFmpeg process and launch reader threads
            try:
                self._open_ffmpeg_stream()
            except OSError as err:
                attempts -= 1
                if attempts == 0:
                    raise RuntimeError()
                warnings.warn(str(err))
                time.sleep(error_wait_time)
                error_wait_time *= 2
                continue
            # If this point is reached then at least one Sink is available and the FFmpeg subprocess could be opened,
            # so error count and sleep time can be reset
            attempts = self._reconnect_attempts
            error_wait_time = self._reconnect_delay
            # Consume video stream until segment is finished
            try:
                last_time = 0
                while not self._interrupt:
                    t = (time.time() + self._gm_offset) % self._length
                    # Check for errors
                    while not self._error_queue.empty():
                        error = self._error_queue.get()
                        # If the error is a warning, do not raise any exception
                        if isinstance(error, Warning):
                            _logger.warning(str(error))
                            continue
                        # Otherwise raise the error
                        raise error from None
                    # If segment has ended, exit loop
                    if t < last_time:
                        break
                    last_time = t
                    time.sleep(1)
            except OSError as err:
                # If an error occurs during the writing loop then issue a warning, try to close all sinks and restart
                # the main loop to reattempt the recording
                warnings.warn(str(err))
            # Exit FFmpeg process
            try:
                self._close_ffmpeg_stream()
                self._join_reader_threads(30)
            except TimeoutError as err:
                # If the FFmpeg process is not responding, attempt to close the sinks and issue a warning before
                # restarting the main loop
                warnings.warn(str(err))
            # Close sinks
            for sink in self._sinks:
                if sink.is_opened():
                    try:
                        sink.close()
                    except OSError as err:
                        _logger.error('Error occurred while closing sink: %s', str(err))

    def stop(self):
        _logger.debug('stop called')
        self._close_ffmpeg_stream()
        self._interrupt = True
