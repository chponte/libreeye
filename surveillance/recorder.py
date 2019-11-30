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
        self._protocol = conf['protocol']
        self._timeout = int(conf['timeout'])
        self._length = int(conf['segment_length']) * 60
        self._buffer_size = int(conf['buffer_size']) * 2 ** 20
        self._reconnect_attempts = int(conf['reconnect_attempts'])
        self._reconnect_delay = int(conf['reconnect_base_delay'])
        self._vp9_speed = {'low': 8, 'mid': 0, 'high': -8}[conf['vp9_encoder_quality']]
        # Internal attributes
        self._sinks: List['Sink'] = []
        self._interrupt = False
        self._ffmpeg_stream: Union['None', 'subprocess.Popen'] = None
        self._stdout_reader_thread: Union['None', 'Thread'] = None
        self._stderr_reader_thread: Union['None', 'Thread'] = None
        self._error_queue: Union['None', 'queue.Queue'] = None
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('Camera object (%d) created with attributes:', id(self))
            for att in dir(self):
                _logger.debug('\t%s: %s', att, getattr(self, att))

    def _open_ffmpeg_stream(self):
        # Check if FFmpeg stream is already opened
        if self._ffmpeg_stream is not None:
            return
        _logger.debug('Opening ffmpeg stream')
        # Open FFmpeg subprocess
        self._ffmpeg_stream = (
            ffmpeg
            .input(self._url, rtsp_transport=self._protocol, stimeout=self._timeout * 10 ** 6, v='warning')
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
            .output('pipe:', format='matroska', vcodec='libvpx-vp9', deadline='realtime', speed=self._vp9_speed)
            .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
        )
        # Create error reader thread
        _logger.debug('Creating error reading thread')
        self._error_queue = queue.Queue()
        self._stderr_reader_thread = self._ErrReaderThread(self._ffmpeg_stream.stderr, self._error_queue)
        self._stderr_reader_thread.start()
        # Wait for the first byte to be returned or the subprocess to die (and thus the error thread to queue the error)
        _logger.debug('Peeking for first data byte')
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
        _logger.debug('Creating video reading thread')
        self._stdout_reader_thread = self._OutReaderThread(
            self._ffmpeg_stream.stdout, self._buffer_size, self._sinks, self._error_queue
        )
        self._stdout_reader_thread.start()

    def _close_ffmpeg_stream(self):
        # End FFmpeg subprocess and remove object reference
        if self._ffmpeg_stream is not None:
            _logger.debug('Quitting FFmpeg stream')
            self._ffmpeg_stream.stdin.write(b'q')
            self._ffmpeg_stream.stdin.close()
            self._ffmpeg_stream = None

    def _join_reader_threads(self, timeout: int) -> None:
        # Read last thread results and remove object references
        _logger.debug('Joining video reader thread (timeout=%d)', timeout)
        self._stdout_reader_thread.join(timeout=timeout)
        if self._stdout_reader_thread.is_alive():
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached while waiting for video reader thread to end')
        self._stdout_reader_thread = None
        _logger.debug('Joining error reader thread (timeout=%d)', timeout)
        self._stderr_reader_thread.join(timeout=timeout)
        if self._stderr_reader_thread.is_alive():
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached while waiting for error reader thread to end')
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
                    _logger.debug('Opening sink %s', sink.__class__.__name__)
                    sink.open('mkv')
                except OSError as err:
                    _logger.warning(str(err))
            # If no sinks are available, terminate execution
            if all([not sink.is_opened() for sink in self._sinks]):
                attempts -= 1
                if attempts == 0:
                    _logger.debug('No sinks available and no retries left')
                    raise OSError(errno.EIO, 'No sinks could be opened')
                _logger.warning('No sinks available, retrying %d times after %d seconds', attempts, error_wait_time)
                time.sleep(error_wait_time)
                error_wait_time *= 2
                continue
            # Open FFmpeg process and launch reader threads
            try:
                self._open_ffmpeg_stream()
            except OSError as err:
                attempts -= 1
                if attempts == 0:
                    raise err
                _logger.warning(
                    'An error occurred while opening the FFmpeg stream (%s), retrying %d times after %d seconds',
                    str(err), attempts, error_wait_time
                )
                time.sleep(error_wait_time)
                error_wait_time *= 2
                continue
            # If this point is reached then at least one Sink is available and the FFmpeg subprocess could be opened,
            # so error count and sleep time can be reset
            attempts = self._reconnect_attempts
            error_wait_time = self._reconnect_delay
            # Consume video stream until segment is finished
            _logger.debug('Entering main loop')
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
                _logger.warning(
                    'An error occurred while reading the video stream (%s), exiting main loop and restarting the '
                    'recording', str(err)
                )
            # Exit FFmpeg process
            try:
                self._close_ffmpeg_stream()
                self._join_reader_threads(30)
            except TimeoutError as err:
                # If the FFmpeg process is not responding, attempt to close the sinks and issue a warning before
                # restarting the main loop
                _logger.warning(str(err))
            # Close sinks
            for sink in self._sinks:
                if sink.is_opened():
                    try:
                        _logger.debug('Closing sink %s', sink.__class__.__name__)
                        sink.close()
                    except OSError as err:
                        _logger.error('Error occurred while closing sink: %s', str(err))

    def stop(self):
        _logger.debug('Stop called')
        self._interrupt = True
