#!/usr/bin/env python

import errno
import logging
import signal
from surveillance.recorder import CameraRecorder
from surveillance.sinks.aws_bucket import Sink, AWSBucketSink
from surveillance.sinks.file import FileSink
import sys
from typing import Dict, List, Union
import os
import yaml

_logger = logging.getLogger(__name__)


def configure_logging(conf: Dict[str, str]):
    # Configure file log
    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }
    logging.basicConfig(
        level=levels[conf['level'].lower()],
        format='[%(relativeCreated)012d:%(levelname).1s] %(filename)s:%(lineno)d %(message)s',
        filename=os.path.join('/var/log/surveillance', conf['file']),
        filemode='a'
    )
    # Configure console logs
    if 'console' in conf:
        console = logging.StreamHandler()
        console.setLevel(levels[conf['console'].lower()])
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(filename)-s:%(lineno)d %(message)s')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger().addHandler(console)


def configure_sinks(conf: Dict[str, dict]) -> List['Sink']:
    sinks: List['Sink'] = []
    if 'local' in conf:
        _logger.debug('Configuring local sink')
        sinks.append(FileSink(conf['local']))
    if 'aws' in conf:
        _logger.debug('Configuring AWS sink')
        sinks.append(AWSBucketSink(conf['aws']))
    return sinks


def main():
    recorder: Union['None', 'CameraRecorder'] = None

    # Configure signal handler to exit execution
    def handle_sigterm(signum, _):
        _logger.info('received signal %d, interrupting execution...', signum)
        recorder.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    _logger.debug("Set %s function as signal %d handler", handle_sigterm.__name__, signal.SIGTERM)

    # Read configuration file
    config_file = os.path.abspath(sys.argv[1])
    if not os.path.isfile(config_file):
        _logger.error("Configuration file %s does not exist", config_file)
        exit(errno.ENOENT)
    _logger.debug("Reading configuration file %s", os.path.abspath(sys.argv[1]))
    with open(config_file, 'rt') as c:
        try:
            config = yaml.safe_load(c)
        except yaml.YAMLError as err:
            _logger.error(str(err))
            exit(errno.EINVAL)
    # Configure logger
    if 'log' in config:
        configure_logging(config['log'])
    recorder = CameraRecorder(config['recorder'])
    # Configure sinks
    recorder.add_sinks(configure_sinks(config['storage']))
    try:
        _logger.debug("Running camera recorder function")
        recorder.run()
    except OSError as err:
        _logger.error(err.args[1])
        exit(err.errno)
    _logger.debug('Program terminated with no errors')
    exit(0)


if __name__ == '__main__':
    main()
