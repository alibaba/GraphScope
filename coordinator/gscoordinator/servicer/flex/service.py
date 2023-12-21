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

"""Service under FLEX Architecture"""

import atexit

# import itertools
import logging
import os
import threading

# from google.protobuf.json_format import MessageToDict
from graphscope.config import Config
from graphscope.gsctl.utils import dict_to_proto_message
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import flex_pb2
from graphscope.proto import message_pb2

# from gscoordinator.scheduler import schedule
# from gscoordinator.utils import WORKSPACE
# from gscoordinator.utils import delegate_command_to_pod
# from gscoordinator.utils import run_kube_cp_command

from gscoordinator.servicer.flex.interactive import *

__all__ = ["FlexServiceServicer", "init_flex_service_servicer"]

logger = logging.getLogger("graphscope")


class FlexServiceServicer(coordinator_service_pb2_grpc.CoordinatorServiceServicer):
    """Service under flex architecture."""

    services_initializer = {
        "interactive": init_interactive_service,
    }

    def __init__(self, config: Config):
        self._config = config
        # We use the solution encompasses the various applications and use cases of
        # the product across different industries and business scenarios,
        # e.g. interactive, analytical
        self._solution = self._config.solution.lower()

        atexit.register(self.cleanup)

        # lock to protect the service
        self._lock = threading.RLock()

        # initialize specific service client
        self._service_client = self._initialize_service_client()

    def __del__(self):
        self.cleanup()

    def _initialize_service_client(self):
        initializer = self.services_initializer.get(self._solution)
        if initializer is None:
            raise RuntimeError("Failed to launch {0} service".format(self._solution))
        return initializer(self._config)

    def cleanup(self):
        pass

    def Connect(self, request, context):
        return message_pb2.ConnectResponse(solution=self._solution)

    def ListGraph(self, request, context):
        try:
            graphs = self._service_client.list_graph()
        except Exception as e:
            logger.warning("Failed to list graph: %s", str(e))
            return flex_pb2.ListGraphResponse(
                code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
            )
        else:
            return flex_pb2.ListGraphResponse(
                code=error_codes_pb2.OK,
                graphs=[
                    dict_to_proto_message(g.to_dict(), flex_pb2.GraphProto())
                    for g in graphs
                ],
            )


def init_flex_service_servicer(config: Config):
    return FlexServiceServicer(config)
