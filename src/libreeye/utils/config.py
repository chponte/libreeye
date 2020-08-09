import configparser
import logging
import shlex
import os

_logger = logging.getLogger(__name__)


class Config:
    def __init__(self, root):
        config = configparser.ConfigParser()
        config.read(os.path.join(root, 'libreeye.conf'))
        # daemon options
        self._daemon = config['daemon']
        # configurations for cameras
        self._cameras = {}
        for r, _, files in os.walk(os.path.join(root, 'cameras.d')):
            for f in files:
                # Skip if file extension is not .conf
                name, ext = os.path.splitext(f)
                if ext == '.conf':
                    self._cameras[name] = CameraConfig(os.path.join(r, f))
        self._storage = StorageConfig(os.path.join(root, 'storage.conf'))

    def cameras(self):
        return self._cameras

    def storage(self):
        return self._storage

    def daemon_logfile(self):
        return self._daemon.get('Log')


class CameraConfig:
    def __init__(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        # Section: input
        if not config.has_section('general'):
            raise KeyError(f'\"general\" section missing in {path}')
        self._general = config['general']
        # Section: input
        if not config.has_section('input'):
            raise KeyError(f'\"input\" section missing in {path}')
        self._input = CameraInputConfig(config['input'])
        # Section: output
        if not config.has_section('output'):
            raise KeyError(f'\"output\" section missing in {path}')
        self._output = CameraOutputConfig(config['output'])
        # Section: motion
        self._motion = (
            CameraMotionConfig(config['motion'])
            if config.has_section('motion')
            else None
        )

    def input(self):
        return self._input

    def output(self):
        return self._output

    def motion(self):
        return self._motion

    def logfile(self):
        return self._general.get('Log')


class CameraInputConfig:
    def __init__(self, config):
        self._input = config

    def url(self):
        return self._input.get('Url').strip('\'\"')

    def timeout(self):
        return self._input.getint('Timeout', 30)

    def resolution(self):
        return (
            tuple([int(v) for v in self._input.get('Resolution').split(',')])
            if 'Resolution' in self._input
            else None
        )

    def ffmpeg_options(self):
        if 'FFmpegOptions' not in self._input:
            options = {}
        else:
            split = shlex.split(self._input.get('FFmpegOptions'))
            options = dict(zip([s[1:] for s in split[0::2]], split[1::2]))
        _logger.debug(options)
        return options


class CameraOutputConfig:
    def __init__(self, config):
        self._output = config

    def local_ffmpeg_options(self):
        if 'LocalFFmpegOptions' not in self._output:
            options = {}
        else:
            split = shlex.split(self._output.get('LocalFFmpegOptions'))
            options = dict(zip([s[1:] for s in split[0::2]], split[1::2]))
        _logger.debug(options)
        return options

    def youtube_ffmpeg_options(self):
        if 'YoutubeFFmpegOptions' not in self._output:
            options = {}
        else:
            split = shlex.split(self._output.get('YoutubeFFmpegOptions'))
            options = dict(zip([s[1:] for s in split[0::2]], split[1::2]))
        _logger.debug(options)
        return options


class CameraMotionConfig:
    def __init__(self, config):
        self._motion = config

    def resolution_scale(self):
        return self._motion.getfloat('ResolutionScale')

    def threshold(self):
        return self._motion.getfloat('Threshold')

    def min_area(self):
        return self._motion.getfloat('MinArea')

    def cooldown(self):
        return self._motion.getint('Cooldown')

    def logfile(self):
        return self._motion.get('Log')


class StorageConfig:
    def __init__(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        self._local = LocalStorageConfig(config['local'])
        self._youtube = YoutubeStorageConfig(config['youtube'])

    def local(self):
        return self._local

    def youtube(self):
        return self._youtube


class LocalStorageConfig:
    def __init__(self, config):
        self._local = config

    def root(self):
        return self._local.get('Path')

    def segment_length(self):
        return self._local.getint('SegmentLength')

    def expiration(self):
        return self._local.getint('Expiration', 30)


class YoutubeStorageConfig:
    def __init__(self, config):
        self._youtube = config

    def secrets_file(self):
        return self._youtube.get('SecretsFile')

    def segment_length(self):
        return self._youtube.getint('SegmentLength')

    def expiration(self):
        return self._youtube.getint('Expiration', 30)
