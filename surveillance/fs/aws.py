from surveillance.fs.base import ItemStorage, Item
import boto3
import botocore
import logging
import os

_logger = logging.getLogger(__name__)


class AWSStorage(ItemStorage):
    def __init__(self, config):
        self._config = config
        # The '' argument is to add trailing slash to the path
        self._s3 = None
        self._prefix = os.path.join(config['path'], '')
        self._objs = None

    def _retrieve_bucket_objects(self):
        if self._s3 is None:
            session = boto3.Session(
                aws_access_key_id=self._config['aws_access_key_id'],
                aws_secret_access_key=self._config['aws_secret_access_key']
            )
            self._s3 = session.client(
                service_name='s3',
                config=botocore.client.Config(
                    connect_timeout=self._config['timeout'],
                    read_timeout=self._config['timeout'],
                    retries={'max_attempts': 0}
                )
            )
        responses = [self._s3.list_objects_v2(Bucket=self._config['bucket'], Prefix=self._config['path'])]
        while responses[-1]['IsTruncated']:
            responses.append(self._s3.list_objects_v2(
                Bucket=self._config['bucket'],
                Prefix=self._config['path'],
                ContinuationToken=responses[-1]['NextContinuationToken']
            ))
        self._objs = [c for r in responses for c in r['Contents']]

    def get_days(self):
        return self._config['days']

    def get_prefix(self):
        return self._prefix

    def walk(self):
        if self._objs is None:
            self._retrieve_bucket_objects()

        # Explore objects, separate files from directories and group the latter by name
        abs_dirs = {}
        files = []
        for o in self._objs:
            key_no_prefix = o['Key'][len(self._prefix):]
            # If the whole key is the prefix, ignore the object
            if len(key_no_prefix) == 0:
                continue
            # If object is immediately in this dir, it is a file
            elif '/' not in key_no_prefix:
                files.append(AWSItem(self._config, self._s3, o))
            # Else it is a directory, so group them by dir name
            else:
                d, _ = key_no_prefix.split('/', 1)
                if d not in abs_dirs:
                    abs_dirs[d] = []
                abs_dirs[d].append(o)
        # Translate directories into AWSDir objects
        dirs = []
        for d in abs_dirs:
            buff = AWSStorage(self._config)
            # The '' argument is to add trailing slash to the path
            buff._prefix = os.path.join(self._prefix, d, '')
            buff._objs = abs_dirs[d]
            dirs.append(buff)
        # Yield this dir subdirectories and files
        yield self, dirs, files
        # Recursively yield the subdirectories
        for d in dirs:
            for a, b, c in d.walk():
                yield a, b, c


class AWSItem(Item):
    def __init__(self, config, s3, aws_obj):
        self._config = config
        self._s3 = s3
        self._key = aws_obj['Key']
        self._lastmodified = aws_obj['LastModified']

    def get_path(self):
        return self._key

    def getmtime(self):
        return self._lastmodified.timestamp()

    def remove(self):
        _logger.info(f'Removing AWS object {self._key}')
        self._s3.delete_object(Bucket=self._config['bucket'], Key=self._key)
