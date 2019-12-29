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

from libreeye.fs.base import ItemStorage, Item
import boto3
import botocore
import logging
import os

# Disable logs from boto3 and botocore
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)
_timeout = 60


class AWSStorage(ItemStorage):
    def __init__(self, bucket, prefix, days):
        self._s3 = None
        self._bucket = bucket
        self._prefix = os.path.join(prefix, '')  # Add trailing slash to prefix
        self._days = days
        self._objs = None

    def _retrieve_bucket_objects(self):
        if self._s3 is None:
            session = boto3.Session()
            self._s3 = session.client(
                service_name='s3',
                config=botocore.client.Config(
                    connect_timeout=_timeout,
                    read_timeout=_timeout,
                    retries={'max_attempts': 0}
                )
            )
        responses = [self._s3.list_objects_v2(
            Bucket=self._bucket, Prefix=self._prefix
        )]
        while responses[-1]['IsTruncated']:
            responses.append(self._s3.list_objects_v2(
                Bucket=self._bucket,
                Prefix=self._prefix,
                ContinuationToken=responses[-1]['NextContinuationToken']
            ))
        self._objs = [c for r in responses for c in r['Contents']]

    def get_days(self):
        return self._days

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
                files.append(AWSItem(self._s3, self._bucket, o))
            # Else it is a directory, so group them by dir name
            else:
                d, _ = key_no_prefix.split('/', 1)
                if d not in abs_dirs:
                    abs_dirs[d] = []
                abs_dirs[d].append(o)
        # Translate directories into AWSDir objects
        dirs = []
        for d in abs_dirs:
            new_prefix = buff._prefix = os.path.join(self._prefix, d, '')
            buff = AWSStorage(self._bucket, new_prefix, self._days)
            buff._objs = abs_dirs[d]
            dirs.append(buff)
        # Yield this dir subdirectories and files
        yield self, dirs, files
        # Recursively yield the subdirectories
        for d in dirs:
            for a, b, c in d.walk():
                yield a, b, c


class AWSItem(Item):
    def __init__(self, s3, bucket, aws_obj):
        self._s3 = s3
        self._bucket = bucket
        self._key = aws_obj['Key']
        self._lastmodified = aws_obj['LastModified']

    def get_path(self):
        return self._key

    def getmtime(self):
        return self._lastmodified.timestamp()

    def remove(self):
        _logger.info(f'Removing AWS object {self._key}')
        self._s3.delete_object(Bucket=self._bucket, Key=self._key)
