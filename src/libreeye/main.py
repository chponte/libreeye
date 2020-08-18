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
import socket
import sys

from libreeye.daemon import definitions, socket_actions
from libreeye.storage.youtube import YoutubeStorage
from libreeye.utils.config import Config


def camera_actions(args):
    if args.action == 'ls':
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(definitions.socket_path)
            socket_actions.write_msg(sock, json.dumps(
                {'object': 'camera', 'action': 'ls'}))
            answer = json.loads(socket_actions.read_msg(sock))
            sock.close()
            print(answer)
        except FileNotFoundError as _:
            print('Could not connect to the daemon', file=sys.stderr)
            exit(errno.ESRCH)
    if args.action == 'start' or args.action == 'stop':
        if args.id is None:
            print(f'{args.action} requires a camera id')
            exit(errno.EINVAL)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(definitions.socket_path)
            socket_actions.write_msg(sock, json.dumps(
                {'object': 'camera', 'action': args.action, 'id': args.id}))
            if args.action == 'stop':
                answer = json.loads(socket_actions.read_msg(sock))
                print(f'Camera terminated with code {answer["exitcode"]}')
            sock.close()
        except FileNotFoundError as _:
            print('Could not connect to the daemon', file=sys.stderr)
            exit(errno.ESRCH)


def storage_actions(args):
    # Create configuration files
    if args.type == 'youtube':
        if args.action == 'auth-user':
            config = Config('/etc/libreeye')
            ys = YoutubeStorage(config.storage().youtube())
            ys.oauth_login()


def configure_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='libreeye daemon control utility')
    subparsers = parser.add_subparsers(title='objects')
    # Camera
    camera_subparser = subparsers.add_parser(name='camera')
    camera_subparser.add_argument('action', choices=['ls', 'start', 'stop'])
    camera_subparser.add_argument('id', type=str, nargs='?', default=None)
    camera_subparser.set_defaults(func=camera_actions)
    # Storage
    storage_subparser = subparsers.add_parser(name='storage')
    storage_subparser.add_argument('type', choices=['youtube'])
    storage_subparser.add_argument(
        'action', choices=['auth-user'])
    storage_subparser.set_defaults(func=storage_actions)
    return parser


def main():
    parser = configure_argparse()
    args = parser.parse_args()
    args.func(args)
