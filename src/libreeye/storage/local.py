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


from datetime import datetime, timedelta
from typing import Dict
import logging
import os
import time

import ffmpeg

from libreeye.storage.base import Storage, Item, Writer
from libreeye.utils.config import LocalStorageConfig


_logger = logging.getLogger(__name__)


class LocalStorage(Storage):
    def __init__(self, config: LocalStorageConfig):
        self._path = config.root()
        self._segment_length = config.segment_length()
        self._days = config.expiration()

    def list_expired(self):
        due_date = (datetime.today() - timedelta(days=self._days)).timestamp()
        for root, _, files in os.walk(self._path):
            for f in files:
                fullpath = os.path.join(root, f)
                if os.path.getmtime(fullpath) <= due_date:
                    yield LocalItem(fullpath)

    def create_writer(self, name, probe):
        return LocalWriter(
            os.path.join(self._path, name),
            self._segment_length,
            probe['codec_name']
        )


class LocalItem(Item):
    def __init__(self, path):
        self._path = path

    def get_path(self):
        return self._path

    def remove(self):
        _logger.info(f'Removing file {self._path}')
        os.remove(self._path)


class LocalWriter(Writer):
    def __init__(self, path, segment_length, ffmpeg_format):
        super().__init__()
        self._path = path
        os.makedirs(self._path, mode=0o755, exist_ok=True)
        self._segment_length = segment_length
        self._ffmpeg_format = ffmpeg_format
        self._ffmpeg = None
        self._segment_start = 0

    def _ffmpeg_open(self):
        _logger.debug('_ffmpeg_open called')
        filename = os.path.join(
            self._path,
            f'{time.strftime("%d_%m_%y_%H_%M", time.localtime())}.mkv'
        )
        self._ffmpeg = (
            ffmpeg
            .input('pipe:', f=self._ffmpeg_format, v='warning')
            .output(filename, vcodec='copy')
            .overwrite_output()
            .run_async(pipe_stdin=True)
        )

    def _ffmpeg_close(self):
        _logger.debug('_ffmpeg_close called')
        self._ffmpeg.stdin.close()
        self._ffmpeg.wait()
        self._ffmpeg = None

    def write(self, frame) -> None:
        if self._ffmpeg is None:
            self._ffmpeg_open()
            self._segment_start = time.time()
        if time.time() - self._segment_start > self._segment_length:
            self._ffmpeg_close()
            self._ffmpeg_open()
            self._segment_start = time.time()
        self._ffmpeg.stdin.write(frame)

    def close(self):
        if self._ffmpeg is not None:
            self._ffmpeg_close()
