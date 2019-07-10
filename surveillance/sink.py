from abc import ABC, abstractmethod
import boto3
import ffmpeg
import logging
import numpy as np
import os
import subprocess
from threading import Thread
import time
from typing import Any, Dict, Union


class Sink(ABC):
    @abstractmethod
    def open(self, video_info: Dict[str, Any]):
        pass

    @abstractmethod
    def write(self, data: bytes):
        pass

    @abstractmethod
    def close(self):
        pass


class FileSink(Sink):
    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        self._path = conf['path']
        self._width = 0
        self._height = 0
        self._frame_rate = 0
        self._subprocess: Union['None', 'subprocess.Popen'] = None
        self._length = int(conf['segment_length']) * 60
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._last_write = 0

    def _open_stream(self):
        logger = logging.getLogger(__name__)
        filename = os.path.join(self._path, f'{time.asctime(time.localtime())}.mp4')
        logger.debug('FileSink: opening file %s', filename)
        self._subprocess = (
            ffmpeg
            .input(
                'pipe:',
                format='rawvideo',
                pix_fmt='rgb24',
                s=f'{self._width}x{self._height}',
                # r=self._frame_rate,
                v='warning'
            )
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
            .output(filename, vcodec='libx264')  # pix_fmt='yuv420p'
            .run_async(pipe_stdin=True)
        )
        self._last_write = (time.time() + self._gm_offset) % self._length

    def _close_stream(self):
        logger = logging.getLogger(__name__)
        try:
            self._subprocess.communicate(timeout=10)
            logger.debug('FileSink: subprocess exited gracefully')
        except subprocess.TimeoutExpired:
            logger.debug('FileSink: subprocess timeout expired, killing subprocess')
            self._subprocess.terminate()

    def open(self, video_info: Dict[str, Any]):
        if self._subprocess is not None:
            # TODO: throw error when opening an already opened stream
            return
        self._width = int(video_info['width'])
        self._height = int(video_info['height'])
        f, s = video_info['avg_frame_rate'].split('/')
        self._frame_rate = int(f) // int(s)
        self._open_stream()

    def write(self, frames: 'np.ndarray'):
        if (time.time() + self._gm_offset) % self._length < self._last_write:
            logger = logging.getLogger(__name__)
            logger.debug('FileSink: segment ended, switching files')
            self._close_stream()
            self._open_stream()
        self._subprocess.stdin.write(frames.astype(np.uint8).tobytes())
        self._last_write = (time.time() + self._gm_offset) % self._length

    def close(self):
        logger = logging.getLogger(__name__)
        logger.debug('FileSink: close() called')
        self._close_stream()


class AWSBucketSink(Sink):
    class UploadThread(Thread):
        def __init__(self, s3, bucket, key, ffmpeg_suprocess, packet_size):
            super().__init__()
            self._s3 = s3
            self._bucket = bucket
            self._key = key
            self._ffmpeg_subprocess: 'subprocess.Popen' = ffmpeg_suprocess
            self._packet_size = packet_size
            self._create_multiupload()

        def _create_multiupload(self):
            mpu = self._s3.create_multipart_upload(Bucket=self._bucket, Key=self._key)
            self._mpu_id = mpu['UploadId']
            self._parts = []
            self._num = 1

        def _upload_part(self, buffer: bytes):
            # Upload part
            part = self._s3.upload_part(
                Body=buffer, Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, PartNumber=self._num
            )
            self._parts.append({'PartNumber': self._num, 'ETag': part['ETag']})
            self._num += 1

        def _complete_upload(self):
            self._s3.complete_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, MultipartUpload={'Parts': self._parts}
            )

        def run(self) -> None:
            logger = logging.getLogger(__name__)
            while True:
                buffer = self._ffmpeg_subprocess.stdout.read(self._packet_size)
                if not buffer:
                    logger.debug('AWSBucketSink: exit thread loop')
                    break
                logger.debug('AWSBucketSink: uploading %d bytes', len(buffer))
                self._upload_part(buffer)
            self._ffmpeg_subprocess.terminate()
            logger.debug('AWSBucketSink: subprocess terminated')
            self._complete_upload()
            logger.debug('AWSBucketSink: upload completed')

    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        # AWS attributes
        self._aws_id = conf['aws_access_key_id']
        self._aws_secret = conf['aws_secret_access_key']
        self._bucket = conf['bucket']
        self._key_prefix = conf['path']
        self._part_size = round(float(conf['part_min_size']) * 2 ** 20)
        self._s3 = None
        # FFmpeg subprocess
        self._ffmpeg_subprocess: Union['None', 'subprocess.Popen'] = None
        self._width: int = 0
        self._height: int = 0
        self._frame_rate: int = 0
        # Video segmentation attributes
        self._length = int(conf['segment_length']) * 60
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._last_write = 0

    def _open_stream(self):
        logger = logging.getLogger(__name__)
        logger.debug('AWSBucketSink: opening piped stream')
        self._ffmpeg_subprocess = (
            ffmpeg
            .input(
                'pipe:',
                format='rawvideo',
                pix_fmt='rgb24',
                s=f'{self._width}x{self._height}',
                # r=self._frame_rate,
                v='warning'
            )
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
            .output('pipe:', format='h264')  # pix_fmt='yuv420p'
            .run_async(pipe_stdin=True, pipe_stdout=True)
        )
        self._last_write = (time.time() + self._gm_offset) % self._length

    def _close_stream(self):
        logger = logging.getLogger(__name__)
        self._ffmpeg_subprocess.stdin.close()
        self._ffmpeg_subprocess = None

    def open(self, video_info: Dict[str, Any]):
        logger = logging.getLogger(__name__)
        if self._ffmpeg_subprocess is not None:
            # TODO: throw error when opening an already opened stream
            return
        # Open S3 client using provided credentials
        session = boto3.Session(
            aws_access_key_id=self._aws_id,
            aws_secret_access_key=self._aws_secret
        )
        self._s3 = session.client('s3')
        logger.debug(self._s3)
        # Open FFmpeg subprocess
        self._width = int(video_info['width'])
        self._height = int(video_info['height'])
        f, s = video_info['avg_frame_rate'].split('/')
        self._frame_rate = int(f) // int(s)

    def write(self, frames: 'np.ndarray'):
        logger = logging.getLogger(__name__)
        # Check if FFmpeg stream is started
        if self._ffmpeg_subprocess is None:
            self._open_stream()
            key = os.path.join(self._key_prefix, f'{time.asctime(time.localtime())}.mp4')
            t = AWSBucketSink.UploadThread(self._s3, self._bucket, key, self._ffmpeg_subprocess, self._part_size)
            t.start()
        # Check if segment has ended
        if (time.time() + self._gm_offset) % self._length < self._last_write:
            logger.debug('AWSBucketSinkSink: segment ended, switching files')
            self._close_stream()
            self._open_stream()
            key = os.path.join(self._key_prefix, f'{time.asctime(time.localtime())}.mp4')
            t = AWSBucketSink.UploadThread(self._s3, self._bucket, key, self._ffmpeg_subprocess, self._part_size)
            t.start()
        self._ffmpeg_subprocess.stdin.write(frames.astype(np.uint8).tobytes())
        self._last_write = (time.time() + self._gm_offset) % self._length

    def close(self):
        logger = logging.getLogger(__name__)
        logger.debug('AWSBucketSink: close() called')
        self._close_stream()
