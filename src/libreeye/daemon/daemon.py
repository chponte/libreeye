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
import copy
import docker
import errno
import json
import logging
import os
import pkg_resources
import sched
import signal
import socket
import socketserver
import sys
import threading
import time

logging.getLogger('docker').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)
_WATCHDOG_DELAY = 60
_DOCKER_IMAGE_NAME = f'libreeye' \
    f':{pkg_resources.get_distribution("libreeye").version}'


class _ThreadingUnixRequestHandler(socketserver.BaseRequestHandler):
    def _start_camera(self, id: int):
        Daemon().start_camera(id)

    def _list_cameras(self) -> dict:
        cameras = Daemon().list_cameras()
        socket_actions.write_msg(self.request, json.dumps(cameras))

    def _stop_camera(self, id: int) -> int:
        exitcode = Daemon().stop_camera(id)
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
    _instance = None  # Singleton

    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:  # pylint: disable=access-member-before-definition
            return
        self._initialized = True
        self._active = True
        # Read daemon config
        self._conf = Daemon._read_main_config(
            '/etc/libreeye/libreeye.conf'
        )
        # Create camera objects from config
        self._cameras = [Namespace(
            conf=c,
            state=Namespace(
                active=False
            )
        ) for c in Daemon._read_cameras_config(
            '/etc/libreeye/cameras.conf'
        )]
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
            log=log
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
        config = configparser.ConfigParser()
        config.read(path)
        for section in config.sections():
            # Read fields
            if config[section].get('url') is None:
                raise NotImplementedError()
            url = config[section].get('url')
            protocol = config[section].get('protocol', 'udp')
            timeout = config[section].getint('Timeout', 30)
            segment = config[section].getint('SegmentLength', 3600)
            log = config[section].get(
                'Log', f'/var/log/libreeye/cameras/{section}.log'
            )
            # Create namespace
            camera = Namespace(
                name=section,
                url=url,
                protocol=protocol,
                timeout=timeout,
                segment=segment,
                log=log
            )
            configs.append(camera)
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
    def _is_container_running(name: str) -> bool:
        client = docker.from_env()
        return len(client.containers.list(filters={
            'ancestor': _DOCKER_IMAGE_NAME,
            'name': name
        })) > 0

    def _sched_docker_watchdog(self):
        _logger.debug(f'_sched_docker_watchdog called')
        # Schedule next run
        self._sched.enter(_WATCHDOG_DELAY, 0, self._sched_docker_watchdog)
        # Check if active cameras are running
        for i, c in enumerate(self._cameras):
            if not c.state.active:
                continue
            docker_container_name = f'libreeye.recorder-{i}'
            if Daemon._is_container_running(docker_container_name):
                continue
            _logger.warning('active camera %i is not running, starting it '
                            'again', i)
            self.start_camera(i)

    def _sched_gc(self):
        _logger.debug(f'_sched_gc called')
        # Schedule next run
        self._sched.enter(
            self._conf.gc.frequency,  # pylint: disable=no-member
            1,
            self._sched_gc
        )
        # Run garbage collector
        self.run_garbage_collector(wait=False)

    def start_camera(self, id: int):
        _logger.debug(f'start_camera called on id %i', id)
        camera_name = self._cameras[id].conf.name
        # Change camera state
        self._cameras[id].state.active = True
        # Check if the recorder for the camera is already running
        client = docker.from_env()
        docker_container_name = f'libreeye.recorder-{id}'
        if Daemon._is_container_running(docker_container_name):
            raise RuntimeError()
        # Check if log file already exists
        if not os.path.isfile(self._cameras[id].conf.log):
            open(self._cameras[id].conf.log, 'w').close()
        # Run container in detached mode
        docker_video_path = '/mnt/video'
        docker_log_file = '/var/log/container.log'
        client.containers.run(
            _DOCKER_IMAGE_NAME,
            '/bin/sh -c "exec python -m libreeye.recorder'  # pylint: disable=no-member
            f' --camera-url {self._cameras[id].conf.url}'
            f' --camera-proto {self._cameras[id].conf.protocol}'
            f' --camera-timeout {self._cameras[id].conf.timeout}'
            f' --camera-length {self._cameras[id].conf.segment}'
            f' --file-path {os.path.join(docker_video_path, camera_name)}'
            f' --aws-bucket {self._storage.aws.bucket}'
            f' --aws-folder {camera_name}'
            f' --aws-timeout {self._storage.aws.timeout}'
            f' >>{docker_log_file} 2>&1"',
            name=docker_container_name,
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
                    target=docker_video_path,
                    source=self._storage.local.path,  # pylint: disable=no-member
                    type='bind'
                ),
                # Process log file
                docker.types.Mount(
                    target=docker_log_file,
                    source=self._cameras[id].conf.log,
                    type='bind'
                )
            ],
            stdout=True,
            stderr=True,
            stop_signal='SIGTERM',
            auto_remove=True
        )

    def stop_camera(self, id: int) -> int:
        _logger.debug('stop_camera called on id %i', id)
        self._cameras[id].state.active = False
        docker_container_name = f'libreeye.recorder-{id}'
        client = docker.from_env()
        try:
            [container] = client.containers.list(filters={
                'ancestor': _DOCKER_IMAGE_NAME,
                'name': docker_container_name
            })
        except ValueError:
            # Container not found
            raise NameError()
        container.kill('SIGTERM')
        # TODO: Add timeout, check for 404 if kill is too fast
        r = container.wait()
        return r['StatusCode']

    def list_cameras(self):
        _logger.debug('list_cameras called')
        # Add docker information
        cameras = dict()
        client = docker.from_env()
        for i, c in enumerate(self._cameras):
            cameras[i] = dict()
            cameras[i]['name'] = c.conf.name,
            cameras[i]['active'] = c.state.active
            # Read container status if camera is active
            if c.state.active:
                docker_container_name = f'libreeye.recorder-{i}'
                try:
                    [container] = client.containers.list(filters={
                        'ancestor': _DOCKER_IMAGE_NAME,
                        'name': docker_container_name
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
        docker_container_name = 'libreeye.gc'
        if Daemon._is_container_running(docker_container_name):
            raise RuntimeError()
        # Check if log file already exists
        if not os.path.isfile(self._conf.gc.log):  # pylint: disable=no-member
            open(self._conf.gc.log, 'w').close()  # pylint: disable=no-member
        # Run container in attached mode
        docker_video_path = '/mnt/video'
        docker_log_file = '/var/log/container.log'
        local_args = list()
        aws_args = list()
        for c in self._cameras:
            local_path = os.path.join(docker_video_path, c.conf.name)
            local_exp = \
                self._storage.local.expiration  # pylint: disable=no-member
            local_args.append(f'--local {local_path} {local_exp}')
            aws_bucket = self._storage.aws.bucket  # pylint: disable=no-member
            aws_exp = self._storage.aws.expiration  # pylint: disable=no-member
            aws_args.append(f'--aws {aws_bucket} {c.conf.name} {aws_exp}')
        local_args = ' '.join(local_args)
        aws_args = ' '.join(aws_args)
        container = client.containers.run(
            _DOCKER_IMAGE_NAME,
            '/bin/sh -c "exec python -m libreeye.gc'
            f' {local_args}'
            f' {aws_args}'
            f' >>{docker_log_file} 2>&1"',
            name=docker_container_name,
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
                    target=docker_video_path,
                    source=self._storage.local.path,  # pylint: disable=no-member
                    type='bind'
                ),
                # Process log file
                docker.types.Mount(
                    target=docker_log_file,
                    source=self._conf.gc.log,  # pylint: disable=no-member
                    type='bind'
                )
            ],
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
        for i in range(len(self._cameras)):
            self.stop_camera(i)

    def terminate(self, *args):
        _logger.debug('terminate called')
        self._active = False


def main():
    # Configure logging
    conf = configparser.ConfigParser()
    conf.read('/etc/libreeye/libreeye.conf')
    log_file = conf['daemon'].get('Log', '/var/log/libreeye/libreeyed.log')
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
        datefmt='%d/%m %H:%M:%S',
        filename=log_file
    )
    # Check user
    if os.getuid() != 0:
        print('daemon must be run as root!', file=sys.stderr)
        exit(errno.EPERM)
    d = Daemon()
    context = DaemonContext(
        uid=0,
        gid=0,
        pidfile=PIDLockFile(definitions.pidfile),
        signal_map={
            signal.SIGTERM: d.terminate
        },
        stdout=logging.root.handlers[0].stream,
        stderr=logging.root.handlers[0].stream
    )
    # Check lock
    if context.pidfile.is_locked():
        try:
            os.kill(context.pidfile.read_pid(), 0)
        except OSError:
            context.pidfile.break_lock()
        else:
            print('daemon is already running!', file=sys.stderr)
            exit(1)
    # Start daemon
    with context:
        d.run()


if __name__ == '__main__':
    main()
