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

from typing import List, Union
import configparser
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

from daemon import DaemonContext
from daemon.pidfile import PIDLockFile

from libreeye.daemon import definitions, socket_actions
from libreeye.recording.camera import Camera
from libreeye.storage.local import LocalStorage
from libreeye.storage.youtube import YoutubeStorage
from libreeye.utils.config import Config

logging.getLogger('urllib3').setLevel(logging.WARNING)
_logger = logging.getLogger(__name__)


class _ThreadingUnixRequestHandler(socketserver.BaseRequestHandler):
    def _start_camera(self, c):
        Daemon().start_camera(c)

    def _list_cameras(self) -> dict:
        cameras = Daemon().list_cameras()
        socket_actions.write_msg(self.request, json.dumps(cameras))

    def _stop_camera(self, c) -> int:
        exitcode = Daemon().stop_camera(c)
        socket_actions.write_msg(self.request, json.dumps({
            'exitcode': exitcode
        }))

    def handle(self):
        msg = json.loads(socket_actions.read_msg(self.request))
        _logger.debug('received message %s on thread %s', msg,
                      threading.current_thread().name)
        if msg['object'] == 'camera':
            if msg['action'] == 'start':
                self._start_camera(msg['id'])
            if msg['action'] == 'ls':
                self._list_cameras()
            if msg['action'] == 'stop':
                self._stop_camera(msg['id'])


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

    def __init__(self, context: DaemonContext = None):
        # Singleton
        if getattr(self, '_initialized', False):
            return
        if context is None:
            raise TypeError('__init__() requires a DaemonContext when called'
                            ' for the first time')
        self._initialized = True
        # Set daemon as active until stopped
        self._active = True
        # Read daemon config
        self._conf = Config('/etc/libreeye/')
        # Configure logging
        os.makedirs(os.path.dirname(
            self._conf.daemon_logfile()), exist_ok=True)
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
            datefmt='%d/%m %H:%M:%S',
            filename=self._conf.daemon_logfile()
        )
        # Configure DaemonContext
        context.signal_map = {signal.SIGTERM: self.terminate}
        context.stdout = logging.root.handlers[0].stream
        context.stderr = logging.root.handlers[0].stream
        # Create sched
        self._sched = sched.scheduler(time.time, time.sleep)
        # Process dict
        self._cameras = {
            name: {'running': False} for name in self._conf.cameras()
        }
        # Storage list
        self._storage = [
            LocalStorage(self._conf.storage().local()),
            YoutubeStorage(self._conf.storage().youtube())
        ]

    def _clean_storage_and_schedule(self):
        def thread_body():
            # Clean expired files from all storage systems
            for s in self._storage:
                for i in s.list_expired():
                    i.remove()
            # Schedule next run
            self._sched.enter(86400, 1, self._clean_storage_and_schedule)
        _logger.debug('_clean_storage_and_schedule called')
        threading.Thread(target=thread_body).start()

    def start_camera(self, name):
        _logger.debug('start_camera called on %s', name)
        state = self._cameras[name]
        # Check if the recorder for the camera is already running
        if state['running']:
            return
        # Start camera process
        conf = self._conf.cameras()[name]
        camera = Camera(name, conf, self._storage)
        camera.start()
        state['camera'] = camera
        state['running'] = True

    def stop_camera(self, name: str) -> int:
        _logger.debug('stop_camera called on camera %s', name)
        state = self._cameras[name]
        # Check if running
        if not state['running']:
            _logger.debug('camera was not running')
            return
        # Stop process
        exitcode = state['camera'].stop()
        _logger.debug('camera process ended')
        # Delete dict entry
        self._cameras[name] = {'running': False}
        return exitcode

    def _stop_all_cameras(self):
        _logger.debug('_stop_all called')
        # Signal all containers first
        for name in self._cameras:
            self.stop_camera(name)

    def list_cameras(self):
        _logger.debug('list_cameras called')
        info = dict()
        for name in self._cameras:
            state = self._cameras[name]
            info[name] = dict()
            info[name]['active'] = state['running']
            # info[name]['motion'] = 'motion_process' in state
        return info

    def run(self):
        _logger.debug('run called')
        # Start all cameras
        for c in self._conf.cameras():
            self.start_camera(c)
        # Listen for requests through the socket
        server = _ThreadingUnixServer(
            definitions.socket_path,
            _ThreadingUnixRequestHandler
        )
        # Clean and schedule for the first time
        self._clean_storage_and_schedule()
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
        # Stop all running containers
        self._stop_all_cameras()
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
