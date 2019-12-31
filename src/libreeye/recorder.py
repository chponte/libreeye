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

from libreeye.camera import Camera
from libreeye.sinks.aws_bucket import AWSBucketSink
from libreeye.sinks.interface import Sink
from libreeye.sinks.file import FileSink
from typing import Dict, List, Union
import argparse
import errno
import logging
import signal
import sys
import time

_logger = logging.getLogger(__name__)


class Recorder:
    class _NoSinkError(Exception):
        pass

    def __init__(self, url: str, segment_length: int,
                 timeout: int, resolution=None, ffmpeg_input_options=None):
        # Configuration attributes
        self._length = segment_length
        self._timeout = timeout
        # Internal attributes
        self._sinks: List['Sink'] = []
        self._sinks_avail: List['Sink'] = []
        self._interrupt = False
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._camera = Camera(url, resolution, ffmpeg_input_options)

    def add_sinks(self, sinks: List['Sink']):
        self._sinks.extend(sinks)

    def _open_sinks(self):
        for sink in self._sinks:
            try:
                _logger.debug('Opening sink %s', sink.__class__.__name__)
                sink.open('mkv')
                self._sinks_avail.append(sink)
            except OSError as err:
                _logger.warning(str(err))
        if len(self._sinks_avail) == 0:
            raise self._NoSinkError()

    def _write_to_sinks(self, b):
        for sink in self._sinks_avail:
            try:
                sink.write(b)
            except OSError as err:
                _logger.error(
                    'Error occurred with sink %s (current segment may be lost): %s', sink.__class__.__name__, str(err)
                )
                self._sinks_avail.remove(sink)
                if len(self._sinks_avail) == 0:
                    raise self._NoSinkError()

    def _close_sinks(self):
        for sink in self._sinks_avail:
            try:
                sink.close()
            except OSError as err:
                _logger.error('Error occurred while closing sink %s: %s', sink.__class__.__name__, str(err))
        self._sinks_avail.clear()

    def _segment_loop(self):
        _logger.debug('Segment started')
        last_time = 0
        t = (time.time() + self._gm_offset) % self._length
        while not self._interrupt and t > last_time:
            last_time = t
            buffer = self._camera.read(self._timeout)
            _logger.debug('segment iteration, %i bytes', len(buffer))
            self._write_to_sinks(buffer)
            t = (time.time() + self._gm_offset) % self._length
        _logger.debug('Segment ended')

    def run(self):
        while not self._interrupt:
            try:
                if not self._camera.has_started():
                    self._camera.start()
                self._open_sinks()
                self._segment_loop()
                if self._interrupt:
                    self._write_to_sinks(self._camera.stop())
                else:
                    self._write_to_sinks(self._camera.reset())
                self._close_sinks()
            except self._NoSinkError:
                _logger.error('No sinks available, restarting process')
                self._camera.stop()
                exit(errno.ECONNRESET)
            except OSError as err:
                _logger.error(str(err))
                self._camera.discard()
                self._close_sinks()
                exit(err.errno)

    def stop(self):
        _logger.debug('Stop called')
        self._interrupt = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # Camera class arguments
    parser.add_argument('--camera-url', type=str, required=True)
    parser.add_argument('--camera-length', type=int, required=True)
    parser.add_argument('--camera-timeout', type=int, required=True)
    parser.add_argument('--camera-resolution-w', type=int)
    parser.add_argument('--camera-resolution-h', type=int)
    parser.add_argument('--ffmpeg-input-options', type=str)
    # FileSink class arguments
    parser.add_argument('--file-path', type=str, required=True)
    # AWSBucketSink class arguments
    parser.add_argument('--aws-bucket', type=str, required=True)
    parser.add_argument('--aws-folder', type=str, required=True)
    parser.add_argument('--aws-timeout', type=int, required=True)
    return parser.parse_args()


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
        datefmt='%d/%m %H:%M:%S',
        stream=sys.stderr
    )
    # Parse arguments
    args = parse_args()
    _logger.debug('Arguments: %s', args)
    if args.camera_resolution_w is None or args.camera_resolution_h is None:
        resolution = None
    else:
        resolution = (args.camera_resolution_w, args.camera_resolution_h)
    if args.ffmpeg_input_options is None:
        ffmpeg_input_options = None
    else:
        ffmpeg_input_options = {
            name.lstrip('-'): val
            for [name, val] in
            [pair.split(' ') for pair in args.ffmpeg_input_options.split(',')]
        }
    # Create recorder
    recorder = Recorder(
        url=args.camera_url,
        segment_length=args.camera_length,
        timeout=args.camera_timeout,
        resolution=resolution,
        ffmpeg_input_options=ffmpeg_input_options
    )
    # Configure signal handler to exit execution
    def handle_sigterm(signum, _):
        _logger.info('received signal %d, interrupting execution...', signum)
        recorder.stop()
    signal.signal(signal.SIGTERM, handle_sigterm)
    _logger.debug("Set %s function as signal %d handler", handle_sigterm.__name__, signal.SIGTERM)
    # Create sinks
    recorder.add_sinks([
        FileSink(args.file_path),
        AWSBucketSink(args.aws_bucket, args.aws_folder, args.aws_timeout)
    ])
    try:
        _logger.debug("Running camera recorder function")
        recorder.run()
    except OSError as err:
        _logger.error(err.args[1])
        exit(err.errno)
    _logger.debug('Program terminated with no errors')
    exit(0)
