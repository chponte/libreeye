import logging
import signal
from surveillance.recorder import CameraRecorder
from surveillance.sinks.aws_bucket import Sink, AWSBucketSink
from surveillance.sinks.file import FileSink
import sys
from typing import Dict, List, Union
import yaml


def configure_logging(conf: Dict[str, str]):
    root_logger = logging.getLogger()
    if 'console' in conf and bool(conf['console']):
        root_logger.addHandler(logging.StreamHandler(sys.stdout))
    if 'file' in conf:
        root_logger.addHandler(logging.StreamHandler(open(conf['file'], 'wa')))
    if 'level' in conf and conf['level'].lower() == 'debug':
        root_logger.setLevel(logging.DEBUG)
    if 'level' in conf and conf['level'].lower() == 'info':
        root_logger.setLevel(logging.INFO)
    if 'level' not in conf:
        root_logger.setLevel(logging.ERROR)


def configure_sinks(conf: Dict[str, dict]) -> List['Sink']:
    sinks: List['Sink'] = []
    if 'local' in conf:
        sinks.append(FileSink(conf['local']))
    if 'aws' in conf:
        sinks.append(AWSBucketSink(conf['aws']))
    return sinks


def main():
    logger = logging.getLogger(__name__)
    recorder: Union['None', 'CameraRecorder'] = None

    def handle_sigterm(signum, _):
        logger.info('received signal %d, interrupting execution...', signum)
        recorder.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    # Read configuration file
    with open('config/config.yml', 'r') as c:
        try:
            config = yaml.safe_load(c)
        except yaml.YAMLError as exc:
            print(exc)
            exit(1)
        c.close()
    # Configure logger
    if 'log' in config:
        configure_logging(config['log'])
    recorder = CameraRecorder(config['recorder'])
    # Configure sinks
    recorder.add_sinks(configure_sinks(config['storage']))
    try:
        recorder.start()
    except OSError as err:
        logger.error(err.args[1])
        exit(err.errno)
    logger.debug('program terminated with no errors')
    exit(0)


if __name__ == '__main__':
    main()
