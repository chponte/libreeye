import configparser
import logging
import shlex
import os

_logger = logging.getLogger(__name__)


class Config:
    def __init__(self, root):
        config = configparser.ConfigParser()
        config.read(os.path.join(root, 'libreeye.conf'))
        # garbage collector options
        self._gc = config['garbage-collector']
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

    def daemon_watchdog(self):
        return {
            'On': True,
            'Off': False
        }[self._daemon.get('Watchdog')]

    def daemon_logfile(self):
        return self._daemon.get('Log')

    def gc_frequency(self):
        return {
            'hourly': 3600,
            'daily': 86400,
            'weekly': 604800
        }[self._gc.get('Frequency')]

    def gc_logfile(self):
        self._gc.get('Log')


class CameraConfig:
    def __init__(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        # Section: general
        if not config.has_section('general'):
            raise KeyError(f'\"general\" section missing in {path}')
        self._general = config['general']
        # Section: motion
        self._motion = (
            CameraMotionConfig(config['motion'])
            if config.has_section('motion')
            else None
        )

    def motion(self):
        return self._motion

    def url(self):
        return self._general.get('Url').strip('\'\"')

    def logfile(self):
        return self._general.get('Log')

    def timeout(self):
        return self._general.getint('Timeout', 30)

    def resolution(self):
        return (
            tuple([int(v) for v in self._general.get('Resolution').split(',')])
            if 'Resolution' in self._general
            else None
        )

    def ffmpeg_options(self):
        if 'FFmpegOptions' not in self._general:
            options = {}
        else:
            split = shlex.split(self._general.get('FFmpegOptions'))
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
        # Build local Namespace
        self._local = LocalStorageConfig(config['local'])

    def local(self):
        return self._local


class LocalStorageConfig:
    def __init__(self, config):
        self._local = config

    def root(self):
        return self._local.get('Path')

    def segment_length(self):
        return self._local.getint('SegmentLength')

    def expiration(self):
        return self._local.getint('Expiration', 30)
