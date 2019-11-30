#!/usr/bin/env python3

from datetime import datetime, timedelta
from surveillance.fs.local import ItemStorage, LocalStorage
from surveillance.fs.aws import AWSStorage
from sys import argv
import logging
import os
import yaml

_logger = logging.getLogger(__name__)


def configure_logging():
    # Configure file log
    level = logging.INFO
    logging.basicConfig(
        level=level,
        format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
        datefmt='%d/%m %H:%M:%S',
        filename='/var/log/surveillance/garbagecollector.log',
        filemode='a'
    )
    console = logging.StreamHandler()
    console.setLevel(level)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(filename)-s:%(lineno)d %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)


def iterate_fs(config_root: str, base_path: str):
    for f in os.listdir(config_root):
        # Only read yml files, skip the rest
        if not f.endswith('yml'):
            continue
        with open(os.path.join(config_root, f), 'r') as fd:
            try:
                c = yaml.safe_load(fd)
                yield LocalStorage(base_path, c['storage']['local'])
                if 'aws' in c['storage']:
                    yield AWSStorage(c['storage']['aws'])
            except yaml.YAMLError as err:
                print(err)
                pass


if __name__ == '__main__':
    # Program arguments
    config_root = argv[1]
    base_path = argv[2]
    # Configure logging first
    configure_logging()
    # Run garbage collector for each camera configuration file
    _logger.info(f'Running garbage collector')
    for fs in iterate_fs(config_root, base_path):
        due_date = (datetime.today() - timedelta(days=fs.get_days())).timestamp()
        for root, _, files in fs.walk():
            for f in files:
                if f.getmtime() <= due_date:
                    f.remove()
    _logger.info(f'Garbage collector ended')