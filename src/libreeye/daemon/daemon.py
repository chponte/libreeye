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

from argparse import Namespace
from daemon import DaemonContext
from daemon.pidfile import PIDLockFile
from libreeye.daemon import definitions, socket_actions
from typing import List, Union
import configparser
import docker
import errno
import json
import logging
import os
import pkg_resources
import sched
import signal
import socketserver
import sys
import threading
import time

logging.getLogger('docker').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)
_WATCHDOG_DELAY = 60


class _ThreadingUnixRequestHandler(socketserver.BaseRequestHandler):
    def _start_camera(self, cid: int):
        Daemon().start_camera(cid)

    def _list_cameras(self) -> dict:
        cameras = Daemon().list_cameras()
        socket_actions.write_msg(self.request, json.dumps(cameras))

    def _stop_camera(self, cid: int) -> int:
        exitcode = Daemon().stop_camera(cid)
        socket_actions.write_msg(self.request, json.dumps({
            'exitcode': exitcode
        }))

    def _run_gc(self) -> int:
        exitcode = Daemon().run_garbage_collector()
        socket_actions.write_msg(self.request, json.dumps({
            'exitcode': exitcode
        }))

    def handle(self):
        msg = json.loads(socket_actions.read_msg(self.request))
        _logger.debug('received message %s on thread %s', msg,
                      threading.current_thread().name)
        if msg['object'] == 'camera':
            if msg['action'] == 'start':
                self._start_camera(int(msg['id']))
            if msg['action'] == 'ls':
                self._list_cameras()
            if msg['action'] == 'stop':
                self._stop_camera(int(msg['id']))
        if msg['object'] == 'gc':
            if msg['action'] == 'run':
                self._run_gc()


class _ThreadingUnixServer(socketserver.ThreadingMixIn,
                           socketserver.UnixStreamServer):
    def server_bind(self):
        if os.path.exists(self.server_address):
            os.remove(self.server_address)
        socketserver.UnixStreamServer.server_bind(self)
        os.chmod(self.server_address, 0o660)
        os.chown(self.server_address, 0, 0)


class Daemon():
    # Singleton
    def __new__(cls, *_, **__):
        try:
            return cls._instance
        except AttributeError:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, context: DaemonContext):
        # Singleton
        if self._initialized:  # pylint: disable=access-member-before-definition
            return
        self._initialized = True
        # Set daemon as active until stopped
        self._active = True
        # Read daemon config
        self._conf = Daemon._read_main_config(
            '/etc/libreeye/libreeye.conf'
        )
        # Configure logging
        Daemon._create_file_if_missing(self._conf.log)
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
            datefmt='%d/%m %H:%M:%S',
            filename=self._conf.log
        )
        # Configure DaemonContext
        context.signal_map = {signal.SIGTERM: self.terminate}
        context.stdout = logging.root.handlers[0].stream
        context.stderr = logging.root.handlers[0].stream
        # Create camera objects from config
        self._cameras = Daemon._read_cameras_config(
            '/etc/libreeye/cameras.d/'
        )
        # Read storage config
        self._storage = Daemon._read_storage_config(
            '/etc/libreeye/storage.conf'
        )
        # Create sched
        self._sched = sched.scheduler(time.time, time.sleep)
        # Schedule watchdog
        if self._conf.watchdog:  # pylint: disable=no-member
            self._sched.enter(_WATCHDOG_DELAY, 0, self._sched_docker_watchdog)
        # Schedule garbage collector
        self._sched.enter(
            self._conf.gc.frequency,  # pylint: disable=no-member
            1,
            self._sched_gc
        )

    @staticmethod
    def _read_main_config(path: str) -> Namespace:
        config = configparser.ConfigParser()
        config.read(path)
        # Build gc Namespace
        gc_section = config['garbage-collector']
        freq = {
            'hourly': 3600,
            'daily': 86400,
            'weekly': 604800
        }[gc_section.get('Frequency')]
        log = gc_section.get('Log')
        gc = Namespace(
            frequency=freq,
            log=log,
            container_name='libreeye.gc'
        )
        # Build daemon Namespace
        daemon_section = config['daemon']
        watchdog = {
            'On': True,
            'Off': False
        }[daemon_section.get('Watchdog')]
        log = daemon_section.get('Log')
        return Namespace(
            watchdog=watchdog,
            log=log,
            gc=gc
        )

    @staticmethod
    def _read_cameras_config(path: str) -> List[Namespace]:
        configs = list()
        for f in os.listdir(path):
            config = configparser.ConfigParser()
            config.read(os.path.join(path, f))
            # Section: general
            if not config.has_section('general'):
                raise KeyError(f'\"general\" section missing in {f}')
            general = Namespace(name=os.path.splitext(f)[0])
            general.url = config['general'].get('Url')
            general.segment = config['general'].getint('SegmentLength')
            general.log = config['general'].get('Log')
            general.timeout = config['general'].getint('Timeout', 30)
            if 'Resolution' in config['general']:
                general.resolution = tuple(
                    [int(v) for v in
                     config['general'].get('Resolution').split(',')]
                )
            else:
                general.resolution = None
            if 'InputOptions' in config['general']:
                general.ioptions = config['general'].get('InputOptions')
            else:
                general.ioptions = None
            # Section: motion
            if config.has_section('motion'):
                motion = Namespace(
                    scale=config['motion'].getfloat('ResolutionScale'),
                    threshold=config['motion'].getfloat('Threshold'),
                    min_area=config['motion'].getfloat('MinArea'),
                    cooldown=config['motion'].getint('Cooldown'),
                    log=config['motion'].get('Log')
                )
            else:
                motion = None
            # Internal state
            state = Namespace(
                active=False,
                camera_cname=f'libreeye.recorder-{len(configs)}',
                motion_cname=f'libreeye.md-{len(configs)}'
            )
            # Add camera to config list
            configs.append(Namespace(
                general=general,
                motion=motion,
                state=state
            ))
        return configs

    @staticmethod
    def _read_storage_config(path: str) -> Namespace:
        config = configparser.ConfigParser()
        config.read(path)
        local_section = config['local']
        # Build local Namespace
        local_path = local_section.get('Path')
        local_exp = local_section.getint('Expiration', 30)
        local = Namespace(
            path=local_path,
            expiration=local_exp
        )
        aws_section = config['aws']
        # Build aws Namespace
        aws_bucket = aws_section.get('Bucket')
        aws_exp = aws_section.getint('Expiration', 30)
        aws_timeout = aws_section.getint('Timeout', 60)
        aws = Namespace(
            bucket=aws_bucket,
            expiration=aws_exp,
            timeout=aws_timeout
        )
        return Namespace(
            local=local,
            aws=aws
        )

    @staticmethod
    def _create_dir_if_missing(dir_path: str):
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path, mode=0o755)

    @staticmethod
    def _create_file_if_missing(file_path: str):
        if not os.path.isfile(file_path):
            Daemon._create_dir_if_missing(os.path.dirname(file_path))
            open(file_path, 'w').close()

    @staticmethod
    def _docker_get_image() -> docker.models.images.Image:
        _logger.debug('_docker_get_image called')
        docker_image_name = (
            f'chponte/libreeye:'
            f'{pkg_resources.get_distribution("libreeye").version}'
        )
        client = docker.from_env()
        try:
            return client.images.get(docker_image_name)
        except docker.errors.ImageNotFound:
            _logger.debug('docker image not found')
            # Remove all images from previous versions
            for image in client.images.list('chponte/libreeye:'):
                _logger.debug('remove docker image %s', image.tags[0])
                client.images.remove(image.id)
            # Download image for current version
            image = client.images.pull(docker_image_name)
            _logger.debug('docker image %s pulled', image.tags[0])
            return image

    @staticmethod
    def _docker_is_container_running(name: str) -> bool:
        client = docker.from_env()
        return len(client.containers.list(filters={
            'ancestor': Daemon._docker_get_image().tags[0],
            'name': name
        })) > 0

    def _sched_docker_watchdog(self):
        _logger.debug('_sched_docker_watchdog called')
        # Schedule next run
        self._sched.enter(_WATCHDOG_DELAY, 0, self._sched_docker_watchdog)
        # Check if active cameras are running
        for i, c in enumerate(self._cameras):
            if not c.state.active:
                continue
            # Check recorder container
            if not Daemon._docker_is_container_running(c.state.camera_cname):
                _logger.warning('active camera %i is not running, starting it '
                                'again', i)
                self._docker_start_camera_container(c)
            # Check motion detection container
            if not (c.motion is None or
                    Daemon._docker_is_container_running(c.state.motion_cname)):
                _logger.warning('motion detection for camera %i is not running,'
                                ' starting it again', i)
                self._docker_start_motion_container(c)

    def _sched_gc(self):
        _logger.debug('_sched_gc called')
        # Schedule next run
        self._sched.enter(
            self._conf.gc.frequency,  # pylint: disable=no-member
            1,
            self._sched_gc
        )
        # Run garbage collector
        self.run_garbage_collector(wait=False)

    def _docker_start_camera_container(self, camera):
        _logger.debug('_docker_start_camera_container called on camera %s',
                      camera.general.name)
        client = docker.from_env()
        media_mount_path = os.path.join(self._storage.local.path, camera.general.name)  # pylint: disable=no-member
        recorder_args = list()
        recorder_args.append(f'--camera-url {camera.general.url}')
        recorder_args.append(f'--camera-length {camera.general.segment}')
        recorder_args.append(f'--camera-timeout {camera.general.timeout}')
        if camera.general.resolution is not None:
            recorder_args.append(
                f'--camera-resolution-w {camera.general.resolution[0]}'
            )
            recorder_args.append(
                f'--camera-resolution-h {camera.general.resolution[1]}'
            )
        if camera.general.ioptions is not None:
            recorder_args.append(
                f'--ffmpeg-input-options \'{camera.general.ioptions}\''
            )
        recorder_args.append(f'--file-path {media_mount_path}')
        recorder_args.append(f'--aws-bucket {self._storage.aws.bucket}')  # pylint: disable=no-member
        recorder_args.append(f'--aws-folder {camera.general.name}')
        recorder_args.append(f'--aws-timeout {self._storage.aws.timeout}')  # pylint: disable=no-member
        client.containers.run(
            Daemon._docker_get_image().id,
            '/bin/sh -c "exec python -m libreeye.recorder'
            f' {" ".join(recorder_args)}'
            f' >>{camera.general.log} 2>&1"',
            name=camera.state.camera_cname,
            detach=True,
            mounts=[
                # Time locale configuration from host
                docker.types.Mount(
                    target='/etc/localtime',
                    source='/etc/localtime',
                    type='bind',
                    read_only=True
                ),
                # AWS Credentials file
                docker.types.Mount(
                    target='/root/.aws/credentials',
                    source='/etc/libreeye/aws_credentials',
                    type='bind',
                    read_only=True
                ),
                # Video writting directory
                docker.types.Mount(
                    target=media_mount_path,
                    source=media_mount_path,
                    type='bind'
                ),
                # Process log file
                docker.types.Mount(
                    target=camera.general.log,
                    source=camera.general.log,
                    type='bind'
                )
            ],
            nano_cpus=1*10**9,  # TODO: Configuration parameter
            stdout=True,
            stderr=True,
            stop_signal='SIGTERM',
            auto_remove=True
        )

    def _docker_start_motion_container(self, camera):
        _logger.debug('_docker_start_motion_container called on camera %s',
                      camera.general.name)
        client = docker.from_env()
        media_mount_path = os.path.join(self._storage.local.path, camera.general.name)  # pylint: disable=no-member
        client.containers.run(
            Daemon._docker_get_image().id,
            '/bin/sh -c "exec python -m libreeye.md'
            f' --scale {camera.motion.scale}'
            f' --threshold {camera.motion.threshold}'
            f' --min-area {camera.motion.min_area}'
            f' --cooldown {camera.motion.cooldown}'
            f' {media_mount_path}'
            f' >>{camera.motion.log} 2>&1"',
            name=camera.state.motion_cname,
            mounts=[
                # Time locale configuration from host
                docker.types.Mount(
                    target='/etc/localtime',
                    source='/etc/localtime',
                    type='bind',
                    read_only=True
                ),
                # Video directory
                docker.types.Mount(
                    target=media_mount_path,
                    source=media_mount_path,
                    type='bind'
                ),
                # Process log file
                docker.types.Mount(
                    target=camera.motion.log,
                    source=camera.motion.log,
                    type='bind'
                )
            ],
            detach=True,
            nano_cpus=1*10**9,  # TODO: Configuration parameter
            stdout=True,
            stderr=True,
            stop_signal='SIGTERM',
            auto_remove=True
        )

    def start_camera(self, cid: int):
        _logger.debug('start_camera called on id %i', cid)
        camera = self._cameras[cid]
        # Change camera state
        camera.state.active = True
        # Check if the recorder for the camera is already running
        if Daemon._docker_is_container_running(camera.state.camera_cname):
            raise RuntimeError()
        # Check if media path already exists
        Daemon._create_dir_if_missing(
            os.path.join(self._storage.local.path, camera.general.name)  # pylint: disable=no-member
        )
        # Check if log file already exists
        Daemon._create_file_if_missing(camera.general.log)
        # Start container
        self._docker_start_camera_container(camera)
        # Check if motion detection is enabled for this camera
        if camera.motion is None:
            return
        if Daemon._docker_is_container_running(camera.state.motion_cname):
            return
        # Check if log file already exists
        Daemon._create_file_if_missing(camera.motion.log)
        # Start motion detection container
        self._docker_start_motion_container(camera)

    def _docker_stop_container(self, name: str) -> int:
        client = docker.from_env()
        try:
            [container] = client.containers.list(filters={
                'ancestor': Daemon._docker_get_image().id,
                'name': name
            })
        except ValueError:
            # Container not found
            raise NameError()
        container.kill('SIGTERM')
        # TODO: Add timeout, check for 404 if kill is too fast
        r = container.wait()
        return r['StatusCode']

    def stop_camera(self, cid: int) -> int:
        _logger.debug('stop_camera called on id %i', cid)
        self._cameras[cid].state.active = False
        exitcode = self._docker_stop_container(
            self._cameras[cid].state.camera_cname
        )
        if self._cameras[cid].motion is None:
            return exitcode
        self._docker_stop_container(
            self._cameras[cid].state.motion_cname
        )
        return exitcode

    def list_cameras(self):
        _logger.debug('list_cameras called')
        # Add docker information
        cameras = dict()
        client = docker.from_env()
        for i, c in enumerate(self._cameras):
            cameras[i] = dict()
            cameras[i]['name'] = c.general.name,
            cameras[i]['active'] = c.state.active
            cameras[i]['motion'] = c.motion is not None
            # Read container status if camera is active
            if c.state.active:
                try:
                    [container] = client.containers.list(filters={
                        'ancestor': Daemon._docker_get_image().id,
                        'name': c.state.camera_cname
                    })
                    cameras[i]['status'] = container.status
                except ValueError:
                    # If container not found, assume it exited
                    cameras[i]['status'] = 'exited'
        return cameras

    def run_garbage_collector(self, wait=True) -> Union[None, int]:
        _logger.debug('run_garbage_collector called')
        # Check if garbage collector is already running
        client = docker.from_env()
        if Daemon._docker_is_container_running(self._conf.gc.container_name):  # pylint: disable=no-member
            raise RuntimeError()
        # Check if log file already exists
        Daemon._create_if_missing(self._conf.gc.log)  # pylint: disable=no-member
        # Run container in attached mode
        local_args = list()
        aws_args = list()
        for c in self._cameras:
            local_path = os.path.join(self._storage.local.path, c.general.name)  # pylint: disable=no-member
            local_exp = self._storage.local.expiration  # pylint: disable=no-member
            local_args.append(f'--local {local_path} {local_exp}')
            aws_bucket = self._storage.aws.bucket  # pylint: disable=no-member
            aws_exp = self._storage.aws.expiration  # pylint: disable=no-member
            aws_args.append(f'--aws {aws_bucket} {c.general.name} {aws_exp}')
        local_args = ' '.join(local_args)
        aws_args = ' '.join(aws_args)
        container = client.containers.run(
            Daemon._docker_get_image().id,
            '/bin/sh -c "exec python -m libreeye.gc'  # pylint: disable=no-member
            f' {local_args}'
            f' {aws_args}'
            f' >>{self._conf.gc.log} 2>&1"',
            name=self._conf.gc.container_name,  # pylint: disable=no-member
            detach=True,
            mounts=[
                # Time locale configuration from host
                docker.types.Mount(
                    target='/etc/localtime',
                    source='/etc/localtime',
                    type='bind',
                    read_only=True
                ),
                # AWS Credentials file
                docker.types.Mount(
                    target='/root/.aws/credentials',
                    source='/etc/libreeye/aws_credentials',
                    type='bind',
                    read_only=True
                ),
                # Video writting directory
                docker.types.Mount(
                    target=self._storage.local.path,  # pylint: disable=no-member
                    source=self._storage.local.path,  # pylint: disable=no-member
                    type='bind'
                ),
                # Process log file
                docker.types.Mount(
                    target=self._conf.gc.log,  # pylint: disable=no-member
                    source=self._conf.gc.log,  # pylint: disable=no-member
                    type='bind'
                )
            ],
            nano_cpus=int(0.25*10**9),  # TODO: Configuration parameter
            stdout=True,
            stderr=True,
            stop_signal='SIGTERM',
            auto_remove=True
        )
        if wait:
            r = container.wait()
            return r['StatusCode']
        return None

    def run(self):
        _logger.debug('run called')
        # Start all cameras
        for i in range(len(self._cameras)):
            self.start_camera(i)
        # Listen for requests through the socket
        server = _ThreadingUnixServer(
            definitions.socket_path,
            _ThreadingUnixRequestHandler
        )
        with server:
            # Start a thread with the message server -- that thread will then
            # start one more thread for each request
            server_thread = threading.Thread(target=server.serve_forever)
            # Exit the server thread when the main thread terminates
            server_thread.daemon = True
            server_thread.start()
            # Run scheduled events indefinitely
            while self._active:
                self._sched.run(blocking=False)
                time.sleep(2.5)
            # Terminate the message server
            server.shutdown()
        # Stop all cameras
        for i, c in enumerate(self._cameras):
            if c.state.active:
                self.stop_camera(i)
        _logger.debug('daemon end')

    def terminate(self, *_):
        _logger.debug('terminate called')
        self._active = False


def main():
    # Check user
    if os.getuid() != 0:
        print('daemon must be run as root!', file=sys.stderr)
        sys.exit(errno.EPERM)
    # Create DaemonContext
    context = DaemonContext(
        uid=0,
        gid=0,
        pidfile=PIDLockFile(definitions.pidfile)
    )
    # Create daemon, complete DaemonContext configuration
    daemon = Daemon(context=context)
    # Check lock
    if context.pidfile.is_locked():
        try:
            os.kill(context.pidfile.read_pid(), 0)
        except OSError:
            context.pidfile.break_lock()
        else:
            print('daemon is already running!', file=sys.stderr)
            sys.exit(1)
    # Start daemon
    with context:
        daemon.run()


if __name__ == '__main__':
    main()
