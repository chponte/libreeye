import errno
import ffmpeg
import io
import logging
import numpy as np
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
    class _BlockReaderThread(Thread):
        def __init__(self, stream: 'io.BufferedReader', block_size: int, q: 'queue.Queue'):
            super().__init__()
            self._stream = stream
            self._block_size = block_size
            self._queue = q

        def run(self) -> None:
            while True:
                b = self._stream.read(self._block_size)
                if self._stream.closed or len(b) == 0:
                    break
                self._queue.put(b)

    class _LineReaderThread(Thread):
        def __init__(self, stream: 'io.BufferedReader', q: 'queue.Queue'):
            super().__init__()
            self._stream = stream
            self._queue = q

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
                self._queue.put(line)
                line = ''

    def __init__(self, conf: Dict[str, str]):
        # Configuration attributes
        self._url = conf['camera']
        self._timeout = int(conf['timeout'])
        self._reconnect_attempts = int(conf['reconnect_attempts'])
        self._reconnect_delay = int(conf['reconnect_base_delay'])
        self._buffer_size = int(conf['buffer_size'])
        # Internal attributes
        self._sinks: List['Sink'] = []
        self._interrupt = False
        self._ffmpeg_stream: Union['None', 'subprocess.Popen'] = None
        self._ffmpeg_block_iterator = None
        self._stdout_reader_thread: Union['None', 'Thread'] = None
        self._stdout_queue: Union['None', 'queue.Queue'] = None
        self._stderr_reader_thread: Union['None', 'Thread'] = None
        self._stderr_queue: Union['None', 'queue.Queue'] = None

    @staticmethod
    def _parse_error_msg(msg: str) -> None:
        _logger.debug(msg)
        # Check for TCP error
        #   > [tcp @ 0x563bcc796c80] Connection to tcp://192.168.0.123:554?timeout=5000000 failed: No route to host
        m = re.match(r'^\[tcp @ [a-z0-9]+\] ([^\n]+)$', msg)
        if m is not None:
            raise ConnectionRefusedError(errno.EHOSTUNREACH, m[1])
        # Check for RSTP connection reset
        #   > [rtsp @ 0x5654b71fb580] CSeq 11 expected, 0 received.
        m = re.match(r'^\[rtsp @ [a-z0-9]+\] CSeq (\d+) expected, (\d+) received\.$', msg)
        if m is not None:
            raise ConnectionResetError(errno.ECONNRESET, f'CSeq {m[1]} expected, {m[2]} received.')
        # If no other check applies, raise a general warning
        warnings.warn(msg, RuntimeWarning)

    def _probe_camera(self):
        # Probe camera
        try:
            probe = ffmpeg.probe(self._url)
        except ffmpeg.Error as err:
            for line in err.stderr.decode().strip().split('\n'):
                self._parse_error_msg(line)
            return
        # format = probe['format']
        # _logger.debug('probe format: %s', format)
        # Check number of streams
        if len(probe['streams']) != 1:
            raise NotImplementedError(
                errno.EBADMSG, f'ffprobe returned an unsupported number of streams ({len(probe["streams"])})'
            )
        video_info = probe['streams'][0]
        _logger.debug(video_info)
        if video_info['width'] == 0 or video_info['height'] == 0:
            raise ValueError(errno.EBADMSG, 'ffprobe returned multiple empty fields')
        if re.match(r'^\d+/0$', video_info['avg_frame_rate']):
            raise ValueError(
                errno.EBADMSG, f'ffprobe returned an invalid avg_frame_rate ({video_info["avg_frame_rate"]})'
            )
        return video_info

    def _open_ffmpeg_stream(self, block_size: int):
        # Check if FFmpeg stream is already opened
        if self._ffmpeg_stream is not None:
            return

        self._ffmpeg_stream = (
            ffmpeg
            .input(self._url, rtsp_transport='tcp', v='warning')
            .output('pipe:', format='rawvideo', pix_fmt='rgb24')
            .run_async(pipe_stdin=True, pipe_stdout=True, pipe_stderr=True)
        )

        self._stdout_queue = queue.Queue()
        self._stdout_reader_thread = self._BlockReaderThread(self._ffmpeg_stream.stdout, block_size, self._stdout_queue)
        self._stdout_reader_thread.start()
        self._stderr_queue = queue.Queue()
        self._stderr_reader_thread = self._LineReaderThread(self._ffmpeg_stream.stderr, self._stderr_queue)
        self._stderr_reader_thread.start()
        self._ffmpeg_block_iterator = self._block_loop_iterator()

    def _close_ffmpeg_stream(self):
        # End FFmpeg subprocess and remove object reference
        if self._ffmpeg_stream is not None:
            self._ffmpeg_stream.stdin.write(b'q')
            self._ffmpeg_stream.stdin.close()
            self._ffmpeg_stream = None
        # Read last thread results and remove object references
        last_blocks = []
        self._stdout_reader_thread.join(timeout=5)
        if not self._stdout_reader_thread.is_alive():
            while not self._stdout_queue.empty():
                last_blocks.append(self._stdout_queue.get())
        else:
            _logger.debug('stdout thread alive, ignoring last blocks')
        self._stdout_reader_thread = None
        self._stdout_queue = None
        self._stderr_reader_thread.join(timeout=5)
        if not self._stderr_reader_thread.is_alive():
            while not self._stderr_queue.empty():
                self._parse_error_msg(self._stderr_queue.get())
        else:
            _logger.debug('stderr thread alive, ignoring last blocks')
        self._stderr_reader_thread = None
        self._stderr_queue = None
        self._ffmpeg_block_iterator = None
        return last_blocks

    def _block_loop_iterator(self):
        last_block_epoch = time.time()
        while not self._interrupt:
            try:
                # Read block from stdout queue
                block = self._stdout_queue.get(block=True, timeout=1)
                last_block_epoch = time.time()
                yield block
            except queue.Empty:
                # If get() timeout is reached, check for errors
                while not self._stderr_queue.empty():
                    self._parse_error_msg(self._stderr_queue.get())
                # If stderr queue is empty, check cumulative timeout
                if time.time() - last_block_epoch > self._timeout:
                    self._close_ffmpeg_stream()
                    raise TimeoutError(errno.ETIMEDOUT, 'FFmpeg read timeout reached')

    def add_sinks(self, sinks: List['Sink']):
        self._sinks.extend(sinks)

    def start(self):
        self._interrupt = False
        attempts = self._reconnect_attempts
        error_wait_time = self._reconnect_delay
        video_info = None
        while not self._interrupt:
            if video_info is None:
                # Probe the camera to obtain its video format
                try:
                    video_info = self._probe_camera()
                except ValueError as err:
                    _logger.warning(str(err))
                    continue
                except OSError:
                    attempts -= 1
                    if attempts == 0:
                        raise
                    time.sleep(error_wait_time)
                    error_wait_time *= 2
                    continue
            width, height = int(video_info['width']), int(video_info['height'])
            frame_size = width * height * 3
            # Prepare sinks
            sinks_with_errors = False
            for sink in self._sinks:
                try:
                    sink.open(video_info)
                except OSError as err:
                    sinks_with_errors = True
                    warnings.warn(str(err), RuntimeWarning)
            # If no sinks are available, terminate execution
            if all([not sink.is_opened() for sink in self._sinks]):
                attempts -= 1
                if attempts == 0:
                    raise RuntimeError()
                time.sleep(error_wait_time)
                error_wait_time *= 2
                continue
            # If this point is reached, at least one Sink is available for writing frames, so error count and timeout
            # should be restarted
            attempts = self._reconnect_attempts
            error_wait_time = self._reconnect_delay
            # Create FFmpeg reader process and handler threads
            self._open_ffmpeg_stream(frame_size * self._buffer_size)
            # Iterate differently depending if some sinks could not be opened
            try:
                # If some sink is unavailable, iterate 1000 blocks, try to reopen it and reenter main loop
                if sinks_with_errors:
                    i = 1000
                    while i > 0:
                        byte_block = next(self._ffmpeg_block_iterator)
                        frames = (
                            np
                            .frombuffer(byte_block, np.uint8)
                            .reshape([len(byte_block) // frame_size, height, width, 3])
                        )
                        for sink in self._sinks:
                            if sink.is_opened():
                                sink.write(frames)
                # Else iterate until an exception occurs or stop() is called
                else:
                    while True:
                        byte_block = next(self._ffmpeg_block_iterator)
                        frames = (
                            np
                            .frombuffer(byte_block, np.uint8)
                            .reshape([len(byte_block) // frame_size, height, width, 3])
                        )
                        for sink in self._sinks:
                            sink.write(frames)
            except StopIteration:
                pass
            except OSError as err:
                # If an exception occurs, check again all components to see which raised the exception
                video_info = None
                warnings.warn(str(err), RuntimeWarning)
                continue
            # Close FFmpeg stream and write last frames
            try:
                last_blocks = self._close_ffmpeg_stream()
                for byte_block in last_blocks:
                    _logger.debug('Read %d bytes', len(byte_block))
                    frames = (
                        np
                        .frombuffer(byte_block, np.uint8)
                        .reshape([len(byte_block) // frame_size, height, width, 3])
                    )
                    for sink in self._sinks:
                        sink.write(frames)
            except OSError as err:
                # If an error happens while writing last frames, ignore those last frames and try to end normally
                warnings.warn(str(err), RuntimeWarning)
            # Close sinks
            for sink in self._sinks:
                try:
                    sink.close()
                except OSError as err:
                    _logger.error('Error occurred while closing sink: %s', str(err))
                    pass
        _logger.debug('loop finished')

    def stop(self):
        _logger.debug('stop called')
        self._interrupt = True
