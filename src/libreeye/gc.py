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
from libreeye.fs.local import LocalStorage
from libreeye.fs.aws import AWSStorage
from typing import Tuple
import argparse
import logging
import os
import sys


_logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--local',
        action='append',
        nargs=2,
        metavar=('PATH', 'EXPIRATION')
    )
    parser.add_argument(
        '--aws',
        action='append',
        nargs=3,
        metavar=('BUCKET', 'FOLDER', 'EXPIRATION')
    )
    return parser


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
        datefmt='%d/%m %H:%M:%S',
        stream=sys.stderr
    )
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()
    # Run garbage collector for each camera configuration file
    fs_list = []
    if args.local is not None:
        fs_list += [LocalStorage(path, int(exp)) for [path, exp] in args.local]
    if args.aws is not None:
        fs_list += [AWSStorage(bucket, folder, int(exp))
                    for [bucket, folder, exp] in args.aws]
    _logger.info(f'Running garbage collector')
    for fs in fs_list:
        due_date = (datetime.today() - timedelta(days=fs.get_days())).timestamp()
        for root, _, files in fs.walk():
            for f in files:
                if f.getmtime() <= due_date:
                    f.remove()
    _logger.info(f'Garbage collector ended')
