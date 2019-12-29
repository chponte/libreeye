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

import boto3
import botocore
import errno
import logging
import os
import queue
from libreeye.sinks.interface import Sink
import threading
import time
from typing import Dict, Union, List

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)


class AWSBucketSink(Sink):
    @staticmethod
    def _create_multiupload(s3, bucket, key, errors):
        try:
            mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
            _logger.debug('Created multipart upload with id %s', mpu['UploadId'])
            return mpu['UploadId']
        except botocore.exceptions.BotoCoreError as err:
            _logger.debug('Error occurred: %s', str(err))
            errors.append(ConnectionError(errno.ECONNRESET, str(err)))
            exit(errno.ECONNRESET)

    @staticmethod
    def _upload_part(s3, bucket, key, mpu, parts, buffer: bytes, errors):
            try:
                num = parts[-1]['PartNumber'] + 1 if len(parts) > 0 else 1
                _logger.debug('Uploading part %d of length %d', num, len(buffer))
                part = s3.upload_part(
                    Body=buffer, Bucket=bucket, Key=key, UploadId=mpu, PartNumber=num
                )
                parts.append({'PartNumber': num, 'ETag': part['ETag']})
                _logger.debug('Obtained ETag %s', parts[-1])
            except botocore.exceptions.BotoCoreError as err:
                _logger.debug('Error occurred: %s', str(err))
                errors.append(ConnectionError(errno.ECONNRESET, str(err)))
                exit(errno.ECONNRESET)

    @staticmethod
    def _complete_upload(s3, bucket, key, mpu, parts, errors):
            try:
                response = s3.complete_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=mpu, MultipartUpload={'Parts': parts}
                )
                _logger.debug('Completed multipart upload for key \'%s\'', response['Key'])
            except botocore.exceptions.BotoCoreError as err:
                _logger.debug('Error occurred: %s', str(err))
                errors.append(ConnectionError(errno.ECONNRESET, str(err)))
                exit(errno.ECONNRESET)

    @staticmethod
    def _upload_thread_handler(s3, bucket, key: str, que: 'queue.Queue', stop: 'List[bool]', errors: 'List[Exception]'):
        _logger.debug('Upload thread %s started', threading.current_thread().getName())

        mpu = AWSBucketSink._create_multiupload(s3, bucket, key, errors)
        parts = []
        # Minimum part size is 5MB
        part_min_size = 5 * 2 ** 20
        buffer = bytes()
        _logger.debug('Entering loop')
        while len(stop) == 0 or not que.empty():
            try:
                block = que.get(block=True, timeout=1)
                buffer += block
                _logger.debug('Obtained block of size %d, making a total buffer size of %d', len(block), len(buffer))
                if len(buffer) > part_min_size:
                    AWSBucketSink._upload_part(s3, bucket, key, mpu, parts, buffer, errors)
                    buffer = bytes()
            except queue.Empty:
                pass
        _logger.debug('Loop finished')
        if len(buffer) > 0:
            AWSBucketSink._upload_part(s3, bucket, key, mpu, parts, buffer, errors)
        AWSBucketSink._complete_upload(s3, bucket, key, mpu, parts, errors)
        _logger.debug('Upload thread %s ended', threading.current_thread().getName())

    def __init__(self, bucket, folder, timeout):
        super().__init__()
        # AWS attributes
        # self._aws_id = conf['aws_access_key_id']
        # self._aws_secret = conf['aws_secret_access_key']
        self._bucket = bucket
        self._key_prefix = folder
        self._timeout = timeout
        self._s3 = None
        # Uploader thread
        self._thread: 'threading.Thread' = None
        self._queue: 'queue.Queue' = None
        self._errors: 'List[Exception]' = []
        self._stop: 'List[bool]' = []

    def open(self, ext: str) -> None:
        # Check if sink is already opened
        if self._thread is not None:
            return
        
        try:
            # Open S3 client using provided credentials
            session = boto3.Session(
                # aws_access_key_id=self._aws_id,
                # aws_secret_access_key=self._aws_secret
            )
            self._s3 = session.client(
                service_name='s3',
                config=botocore.client.Config(
                    connect_timeout=self._timeout,
                    read_timeout=self._timeout,
                    retries={'max_attempts': 0}
                )
            )
            # Check whether the bucket exists or not
            if not any([b['Name'] == self._bucket for b in self._s3.list_buckets()['Buckets']]):
                raise FileNotFoundError(errno.ENOENT, f'There is no bucket named {self._bucket}.')
        except botocore.exceptions.ClientError as err:
            if err.response['ResponseMetadata']['HTTPStatusCode'] == 403:
                raise PermissionError(errno.EPERM, str(err)) from err
            raise ConnectionError(errno.ECONNRESET, str(err)) from err
        except botocore.exceptions.BotoCoreError as err:
            raise ConnectionError(errno.ECONNRESET, str(err)) from err
        # Create upload thread
        self._queue = queue.Queue()
        self._errors.clear()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._upload_thread_handler, 
            kwargs={
                's3': self._s3,
                'bucket': self._bucket,
                'key': os.path.join(self._key_prefix, f'{time.strftime("%d_%m_%y_%H_%M", time.localtime())}.{ext}'), 
                'que': self._queue, 
                'stop': self._stop,
                'errors': self._errors
            }
        )
        self._thread.start()

    def is_opened(self) -> bool:
        return self._thread is not None

    def write(self, byte_block: bytes):
        # Check if errors have occurred in the upload thread
        if len(self._errors) > 0:
            raise self._errors[0]
        # Put the byte block into the upload thread queue
        self._queue.put(byte_block, block=False)

    def close(self):
        _logger.debug('Closing bucket \'%s\'', self._bucket)
        self._stop.append(True)
        self._thread.join(timeout=self._timeout)
        if self._thread.is_alive():
            self._thread = None
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached while waiting for multipart upload thread to finalize')
        self._thread = None
        for error in self._errors:
            raise error
