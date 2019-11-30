import boto3
import botocore
import errno
import logging
import os
import queue
from surveillance.sinks.interface import Sink
from threading import Thread
import time
from typing import Dict, Union

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
class AWSBucketSink(Sink):
    # noinspection PyUnresolvedReferences
    class _UploadThread(Thread):
        def __init__(self, sink: 'Sink', s3: 'botocore.client.S3', bucket: str, key: str,
                     block_queue: 'queue.Queue'):
            super().__init__()
            self._sink: 'Sink' = sink
            self._s3 = s3
            self._bucket = bucket
            self._key = key
            # The minimum part size, as stated in the documentation, is 5MB (except for the last one)
            # https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
            self._part_min_size = 5 * 2 ** 20
            self.block_queue: 'queue.Queue' = block_queue
            self.error: Union[None, 'Exception'] = None
            self.stop: bool = False

        def _create_multiupload(self):
            try:
                mpu = self._s3.create_multipart_upload(Bucket=self._bucket, Key=self._key)
                self._mpu_id = mpu['UploadId']
                self._parts = []
                self._num = 1
                _logger.debug('Created multipart upload with id %s', self._mpu_id)
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self.error = ConnectionError(errno.ENETUNREACH, str(err))
                exit(errno.ENETUNREACH)

        def _upload_part(self, buffer: bytes):
            _logger.debug('Uploading part %d of length %d', self._num, len(buffer))
            try:
                part = self._s3.upload_part(
                    Body=buffer, Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, PartNumber=self._num
                )
                self._parts.append({'PartNumber': self._num, 'ETag': part['ETag']})
                self._num += 1
                _logger.debug('Obtained ETag %s', self._parts[-1])
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self.error = ConnectionError(errno.ETIMEDOUT, str(err))
                exit(errno.ETIMEDOUT)

        def _complete_upload(self):
            try:
                response = self._s3.complete_multipart_upload(
                    Bucket=self._bucket, Key=self._key, UploadId=self._mpu_id, MultipartUpload={'Parts': self._parts}
                )
                _logger.debug('Completed multipart upload for key \'%s\'', response['Key'])
            except (
                    botocore.exceptions.ReadTimeoutError,
                    botocore.exceptions.EndpointConnectionError
            ) as err:
                self.error = ConnectionError(errno.ETIMEDOUT, str(err))
                exit(errno.ETIMEDOUT)

        def run(self) -> None:
            self._create_multiupload()
            buffer = bytes()
            while not self.stop or not self.block_queue.empty():
                try:
                    block = self.block_queue.get(block=True, timeout=1)
                    buffer += block
                    if len(buffer) > self._part_min_size:
                        self._upload_part(buffer)
                        buffer = bytes()
                except queue.Empty:
                    pass
            if len(buffer) > 0:
                self._upload_part(buffer)
            self._complete_upload()

    def __init__(self, conf: Dict[str, str]):
        super().__init__()
        # AWS attributes
        self._aws_id = conf['aws_access_key_id']
        self._aws_secret = conf['aws_secret_access_key']
        self._bucket = conf['bucket']
        self._key_prefix = conf['path']
        self._timeout = int(conf['timeout'])
        self._s3 = None
        # Uploader thread
        self._thread: Union['None', 'AWSBucketSink._UploadThread'] = None
        self._thread_queue: Union[None, 'queue.Queue'] = None
        self._thread_exception: Union['None', 'Exception'] = None
        # Video segmentation attributes
        self._ext = None

    def open(self, ext: str) -> None:
        # Check if sink is already opened
        if self._thread is not None:
            return
        # Store file extension
        self._ext = ext
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
        # Create upload thread
        key = os.path.join(self._key_prefix, f'{time.strftime("%d-%m-%y %H:%M", time.localtime())}.{self._ext}')
        _logger.debug('Opening bucket \'%s\' key \'%s\'', self._bucket, key)
        self._thread_queue = queue.Queue()
        self._thread = self._UploadThread(self, self._s3, self._bucket, key, self._thread_queue)
        self._thread_exception = None
        self._thread.start()

    def is_opened(self) -> bool:
        return self._thread is not None

    def write(self, byte_block: bytes):
        # Check if errors have occurred in the upload thread
        if self._thread.error is not None:
            raise self._thread_exception
        # Put the byte block into the upload thread queue
        self._thread.block_queue.put(byte_block)

    def close(self):
        _logger.debug('Closing bucket \'%s\'', self._bucket)
        self._thread.stop = True
        self._thread.join(timeout=self._timeout)
        if self._thread.is_alive():
            raise TimeoutError(errno.ETIMEDOUT, 'Timeout reached while waiting for multipart upload thread to finalize')
        err = self._thread.error
        self._thread = None
        if err is not None:
            raise err
