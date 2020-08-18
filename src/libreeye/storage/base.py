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

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class Writer(ABC):
    @abstractmethod
    def write(self, frame) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class Item(ABC):
    @abstractmethod
    def get_path(self) -> str:
        pass

    @abstractmethod
    def remove(self) -> None:
        pass


class Storage(ABC):
    @abstractmethod
    def list_expired(self):
        pass

    @abstractmethod
    def create_writer(self, name, camera_config, probe, error_queue) -> Writer:
        pass