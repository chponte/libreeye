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

# Messages are delimited by the character \0

def write_msg(socket, message):
    socket.sendall(f'{message}\0'.encode('ascii'))


def read_msg(socket) -> str:
    answer = b''
    while len(answer) == 0 or answer[-1] != ord('\0'):
        answer += socket.recv(1024)
    return answer.decode('ascii')[:-1]
