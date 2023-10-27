#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import atexit
import logging

from graphscope.config import Config
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import message_pb2


class BaseServiceServicer(coordinator_service_pb2_grpc.CoordinatorServiceServicer):
    """Base class of coordinator service"""

    def __init__(self, config: Config):
        self._config = config
        atexit.register(self.cleanup)

    def __del__(self):
        self.cleanup()

    def Connect(self, request, context):
        return message_pb2.ConnectResponse(solution=self._config.solution)

    @property
    def launcher_type(self):
        return self._config.launcher_type

    def cleanup(self):
        pass
