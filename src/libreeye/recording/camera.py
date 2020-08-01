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

from typing import List
import errno
import fcntl
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time

import ffmpeg

from libreeye.md.iterator import FrameIterator
from libreeye.md.algorithms.basic import MotionDetection

_logger = logging.getLogger(__name__)


class Camera:
    def __init__(self, name, config, storage_list):
        self._name = name
        self._config = config
        self._storage_list = storage_list
        self._active = False
        self._process = None

    def _configure_logger(self):
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        fh = logging.FileHandler(self._config.logfile(), 'a')
        fh.setFormatter(logging.Formatter(
            fmt='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
            datefmt='%d/%m %H:%M:%S'
        ))
        root_logger.addHandler(fh)
        sys.stdout = logging.root.handlers[0].stream
        sys.stderr = logging.root.handlers[0].stream

    def _create_ffmpeg(self) -> subprocess.Popen:
        _logger.debug('_create_ffmpeg called')
        ffmpeg_pipe = ffmpeg.input(
            self._config.url(), v='warning', **self._config.ffmpeg_options())
        # If a resolution is specified, rescale the image
        if self._config.resolution():
            ffmpeg_pipe = ffmpeg_pipe.filter(
                'scale',
                width=self._config.resolution()[0],
                height=self._config.resolution()[1]
            )
        # Draw the time watermark
        # ffmpeg_pipe = ffmpeg_pipe.filter(
        #     'drawtext',
        #     text='%{localtime:%d/%m/%y %H\:%M\:%S}',  # pylint: disable=anomalous-backslash-in-string
        #     expansion='normal',
        #     font='LiberationMono',
        #     fontsize=21,
        #     fontcolor='white',
        #     shadowx=1,
        #     shadowy=1
        # )
        # Create an output pipe from which bytes can be read
        ffmpeg_pipe = ffmpeg_pipe.output(
            'pipe:',
            format='rawvideo',
            vcodec='copy',
            threads=1
        )
        p = ffmpeg_pipe.run_async(
            pipe_stdin=True,
            pipe_stdout=True
        )
        flag = fcntl.fcntl(p.stdout.fileno(), fcntl.F_GETFD)
        fcntl.fcntl(p.stdout.fileno(), fcntl.F_SETFL, flag | os.O_NONBLOCK)
        return p

    def _stop_ffmpeg(self, process):
        _logger.debug('writting \'q\' to ffmpeg subprocess')
        process.communicate(b'q')
        _logger.debug('ffmpeg subprocess terminated')

    def _ffmpeg_probe(self):
        _logger.debug('_ffmpeg_probe called')
        try:
            p = ffmpeg.probe(self._config.url(),
                             **self._config.ffmpeg_options())
            if len(p['streams']) != 1:
                raise RuntimeError(
                    'Unexpected number of streams while ffprobing camera'
                )
            _logger.debug(p['streams'][0])
            return p['streams'][0]
        except ffmpeg.Error as e:
            _logger.error(e.stderr.decode())
            raise RuntimeError('Error while ffprobing camera') from e

    def _create_motion_thread(self, probe):
        config = self._config.motion()
        num, denom = [int(n) for n in probe['r_frame_rate'].split('/')]
        frame_iter = FrameIterator(
            probe['codec_name'],
            probe['width'],
            probe['height'],
            num // denom,
            config.resolution_scale()
        )
        motion = MotionDetection(config, frame_iter, config.logfile())
        thread = threading.Thread(target=motion.run)
        thread.start()
        return frame_iter, thread

    def _interrupt(self, signum, _):
        self._active = False

    def start(self):
        self._process = multiprocessing.Process(target=self.run)
        self._process.start()

    def run(self):
        self._configure_logger()
        _logger.debug('camera process started')
        # Finish setup
        self._active = True
        signal.signal(signal.SIGTERM, self._interrupt)
        # Probe camera
        probe = self._ffmpeg_probe()
        # Open writers
        writers = [s.create_writer(self._name, probe)
                   for s in self._storage_list]
        # Start motion detection thread if enabled
        frame_iter = None
        motion_thread = None
        if self._config.motion() is not None:
            frame_iter, motion_thread = self._create_motion_thread(probe)
        # Loop until finished
        while self._active:
            # Start FFmpeg input stream
            process = self._create_ffmpeg()
            # Read loop
            while self._active and process.poll() is None:
                frame = process.stdout.read()
                if frame is not None:
                    for w in writers:
                        w.write(frame)
                    if frame_iter is not None:
                        frame_iter.write(frame)
                time.sleep(0.001)
            # Stop FFmpeg input stream
            if process.poll() is None:
                self._stop_ffmpeg(process)
        # Close writers
        for w in writers:
            w.close()
        # Wait for motion thread to finish
        if motion_thread is not None:
            frame_iter.close()
            motion_thread.join()
        _logger.debug('run finished')

    def stop(self):
        self._process.terminate()
        self._process.join()
        exitcode = self._process.exitcode
        self._process = None
        return exitcode
