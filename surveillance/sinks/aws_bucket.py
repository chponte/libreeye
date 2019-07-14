import boto3
import botocore
import errno
import ffmpeg
import logging
import numpy as np
import os
import subprocess
from surveillance.sinks.interface import Sink
from threading import Thread
import time
from typing import Any, Dict, Union

_logger = logging.getLogger(__name__)


class AWSBucketSink(Sink):
    class UploadThread(Thread):
        def __init__(self, sink: 'Sink', s3: 'botocore.client.S3', bucket: str, key: str,
                     ffmpeg_suprocess: 'subprocess.Popen', packet_size: int):
            super().__init__()
            self._sink: 'Sink' = sink
            self._s3 = s3
            self._bucket = bucket
            self._key = key
            self._ffmpeg_subprocess: 'subprocess.Popen' = ffmpeg_suprocess
            self._packet_size = packet_size

        def _create_multiupload(self):
            try:
                mpu = self._s3.create_multipart_upload(Bucket=self._bucket, Key=self._key)
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self._sink._thread_exception = ConnectionError(errno.ENETUNREACH, str(err))
                exit(errno.ENETUNREACH)
            self._mpu_id = mpu['UploadId']
            self._parts = []
            self._num = 1

        def _upload_part(self, buffer: bytes):
            # Upload part
            try:
                part = self._s3.upload_part(
                    Body=buffer, Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, PartNumber=self._num
                )
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self._sink._thread_exception = ConnectionError(errno.ETIMEDOUT, str(err))
                exit(errno.ETIMEDOUT)
            self._parts.append({'PartNumber': self._num, 'ETag': part['ETag']})
            self._num += 1

        def _complete_upload(self):
            try:
                self._s3.complete_multipart_upload(
                    Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, MultipartUpload={'Parts': self._parts}
                )
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self._sink._thread_exception = ConnectionError(errno.ETIMEDOUT, str(err))
                exit(errno.ETIMEDOUT)

        def run(self) -> None:
            self._create_multiupload()
            while True:
                buffer = self._ffmpeg_subprocess.stdout.read(self._packet_size)
                if not buffer:
                    _logger.debug('AWSBucketSink: exit thread loop')
                    break
                _logger.debug('AWSBucketSink: uploading %d bytes', len(buffer))
                self._upload_part(buffer)
            self._ffmpeg_subprocess.terminate()
            _logger.debug('AWSBucketSink: subprocess terminated')
            self._complete_upload()
            _logger.debug('AWSBucketSink: upload completed')

    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        # AWS attributes
        self._aws_id = conf['aws_access_key_id']
        self._aws_secret = conf['aws_secret_access_key']
        self._bucket = conf['bucket']
        self._key_prefix = conf['path']
        self._timeout = int(conf['timeout'])
        # The minimum part size, as stated in the documentation, is 5MB (except for the last one)
        # https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
        self._part_min_size = 5 * 2 ** 20
        self._part_size = round(float(conf['part_min_size']) * 2 ** 20)
        self._s3 = None
        # FFmpeg subprocess
        self._ffmpeg_subprocess: Union['None', 'subprocess.Popen'] = None
        self._width: int = 0
        self._height: int = 0
        self._frame_rate: int = 0
        # Uploader thread
        self._thread: Union['None', Thread] = None
        self._thread_exception: Union['None', 'Exception'] = None
        # Video segmentation attributes
        self._length = int(conf['segment_length']) * 60
        self._gm_offset = time.mktime(time.localtime(0)) - time.mktime(time.gmtime(0))
        self._last_write = 0

    def _open_stream(self):
        _logger.debug('AWSBucketSink: opening piped stream')
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

    def _create_upload_thread(self, key: str) -> None:
        self._thread = AWSBucketSink.UploadThread(
            self, self._s3, self._bucket, key, self._ffmpeg_subprocess, max(self._part_min_size, self._part_size)
        )
        self._thread_exception = None
        self._thread.start()

    def _close_stream(self):
        # Close FFmpeg subprocess
        self._ffmpeg_subprocess.stdin.close()
        # Wait for upload thread to complete or to return an exception
        while self._thread.is_alive():
            if self._thread_exception is not None:
                break
            self._thread.join(timeout=1.5)
        # Clear old references
        self._ffmpeg_subprocess = None
        self._thread = None
        # Raise exception if needed
        if self._thread_exception is not None:
            e = self._thread_exception
            self._thread_exception = None
            raise e

    def open(self, video_info: Dict[str, Any]):
        # If sink is already opened, do nothing and return
        if self._ffmpeg_subprocess is not None:
            return
        # Open S3 client using provided credentials
        session = boto3.Session(
            aws_access_key_id=self._aws_id,
            aws_secret_access_key=self._aws_secret
        )
        self._s3 = session.client(
            service_name='s3',
            config=botocore.config.Config(
                connect_timeout=self._timeout,
                read_timeout=self._timeout,
                retries={'max_attempts': 0}
            )
        )
        # Check whether the bucket exists or not
        try:
            bucket_list = [b['Name'] for b in self._s3.list_buckets()['Buckets']]
            if self._bucket not in bucket_list:
                raise FileNotFoundError(errno.ENOENT, f'There is no bucket named {self._bucket}.')
        except botocore.exceptions.ClientError as err:
            if err.response['ResponseMetadata']['HTTPStatusCode'] == 403:
                raise PermissionError(errno.EPERM, str(err)) from None
            raise
        except botocore.exceptions.EndpointConnectionError as err:
            raise ConnectionError(errno.ENETUNREACH, str(err)) from None
        _logger.debug(self._s3)
        # Open FFmpeg subprocess
        self._width = int(video_info['width'])
        self._height = int(video_info['height'])
        f, s = video_info['avg_frame_rate'].split('/')
        self._frame_rate = int(f) // int(s)
        self._open_stream()
        self._create_upload_thread(os.path.join(self._key_prefix, f'{time.asctime(time.localtime())}.mp4'))

    def is_opened(self) -> bool:
        return self._ffmpeg_subprocess is not None

    def write(self, frames: 'np.ndarray'):
        # Check if errors have occurred in the upload thread
        if self._thread_exception is not None:
            self._close_stream()
        # Check if segment has ended
        t = (time.time() + self._gm_offset) % self._length
        if t < self._last_write:
            _logger.debug('AWSBucketSinkSink: segment ended, switching files')
            # Close current stream and open a new one
            self._close_stream()
            self._open_stream()
            self._create_upload_thread(os.path.join(self._key_prefix, f'{time.asctime(time.localtime())}.mp4'))
            # Call write again to initialize the process, create the upload thread and write frames
            return self.write(frames)
        self._ffmpeg_subprocess.stdin.write(frames.astype(np.uint8).tobytes())
        self._last_write = t

    def close(self):
        _logger.debug('AWSBucketSink: close() called')
        self._close_stream()
