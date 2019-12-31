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

import argparse
import errno
import json
import os
import pkg_resources
import shutil
import socket
import subprocess
import sys
from libreeye.daemon import definitions, socket_actions


def system_actions(args):
    # Check user
    if os.getuid() != 0:
        print('system actions have to be run as root',
              file=sys.stderr)
        exit(errno.EPERM)
    # Create configuration files
    if args.action == 'create-config':
        src_config_path = pkg_resources.resource_filename(
            'libreeye', 'package_data/etc/libreeye'
        )
        dst_config_path = '/etc/libreeye'
        # Copy general files
        if not os.path.isdir(dst_config_path):
            os.mkdir(dst_config_path, mode=0o755)
        for f in os.listdir(src_config_path):
            src_f = os.path.join(src_config_path, f)
            # Copy nothing but files
            if not os.path.isfile(src_f):
                continue
            dst_f = os.path.join(dst_config_path, f)
            # If file already exists skip it
            if os.path.isfile(dst_f):
                print(
                    f'Warning: file {dst_f} already exists, keeping old one',
                    file=sys.stderr
                )
                continue
            shutil.copy(os.path.join(src_config_path, f), dst_f)
        # If cameras directory is empty, create a sample file
        src_config_path = os.path.join(src_config_path, 'cameras.d')
        dst_config_path = os.path.join(dst_config_path, 'cameras.d')
        if not os.path.isdir(dst_config_path):
            os.mkdir(dst_config_path, mode=0o755)
        if len(os.listdir(dst_config_path)) == 0:
            shutil.copy(
                os.path.join(src_config_path, 'sample.conf'),
                os.path.join(dst_config_path, 'sample.conf')
            )
    # Enable systemd service
    if args.action == 'systemd-enable':
        service_path = pkg_resources.resource_filename(
            'libreeye', 'package_data/etc/systemd/libreeye.service'
        )
        subprocess.run(f'systemctl enable {service_path}', shell=True,
                       check=True)


def camera_actions(args):
    if args.action == 'ls':
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(definitions.socket_path)
        socket_actions.write_msg(sock, json.dumps(
            {'object': 'camera', 'action': 'ls'}))
        answer = json.loads(socket_actions.read_msg(sock))
        sock.close()
        print(answer)
    if args.action == 'start' or args.action == 'stop':
        if args.id is None:
            print(f'{args.action} requires a camera id')
            exit(1)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(definitions.socket_path)
        socket_actions.write_msg(sock, json.dumps(
            {'object': 'camera', 'action': args.action, 'id': args.id}))
        if args.action == 'stop':
            answer = json.loads(socket_actions.read_msg(sock))
            print(f'Camera terminated with code {answer["exitcode"]}')
        sock.close()


def gc_actions(args):
    if args.action == 'run':
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(definitions.socket_path)
        socket_actions.write_msg(sock, json.dumps(
            {'object': 'gc', 'action': args.action}))
        answer = json.loads(socket_actions.read_msg(sock))
        print(f'Garbage collector terminated with code {answer["exitcode"]}')
        sock.close()


def configure_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='libreeye daemon control utility')
    subparsers = parser.add_subparsers(title='objects')
    # System
    system_subparser = subparsers.add_parser(name='system')
    system_subparser.add_argument('action', choices=['create-config',
                                                     'systemd-enable'])
    system_subparser.set_defaults(func=system_actions)
    # Camera
    camera_subparser = subparsers.add_parser(name='camera')
    camera_subparser.add_argument('action', choices=['ls', 'start', 'stop'])
    camera_subparser.add_argument('id', type=int, nargs='?', default=None)
    camera_subparser.set_defaults(func=camera_actions)
    # Garbage collector
    gc_subparser = subparsers.add_parser(name='gc')
    gc_subparser.add_argument('action', choices=['run'])
    gc_subparser.set_defaults(func=gc_actions)
    return parser


def main():
    parser = configure_argparse()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
