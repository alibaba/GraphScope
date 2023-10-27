#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited. All Rights Reserved.
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
import time
from typing import List

import click
import grpc
from graphscope.client.utils import GS_GRPC_MAX_MESSAGE_LENGTH
from graphscope.client.utils import handle_grpc_error
from graphscope.gsctl.config import GS_CONFIG_DEFAULT_LOCATION
from graphscope.gsctl.config import get_current_context
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import interactive_pb2
from graphscope.proto import message_pb2
from graphscope.version import __version__


class GRPCClient(object):
    def __init__(self, endpoint):
        # create the grpc stub
        options = [
            ("grpc.max_send_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_metadata_size", GS_GRPC_MAX_MESSAGE_LENGTH),
        ]
        self._channel = grpc.insecure_channel(endpoint, options=options)
        self._stub = coordinator_service_pb2_grpc.CoordinatorServiceStub(self._channel)

        atexit.register(self.close)

    @handle_grpc_error
    def _connect_impl(self, timeout_seconds):
        begin_time = time.time()

        request = message_pb2.ConnectRequest(version=__version__)
        while True:
            try:
                response = self._stub.Connect(request)
                break
            except Exception as e:
                msg = f"code: {e.code().name}, details: {e.details()}"
                click.secho(
                    f"Failed to connect coordinator: {e}, try after second...",
                    fg="yellow",
                )
                if time.time() - begin_time >= timeout_seconds:
                    raise ConnectionError(f"Connect coordinator timeout, {msg}")
                time.sleep(1)

        return response.solution

    def connect(self, timeout_seconds=10):
        return self._connect_impl(timeout_seconds)

    def close(self):
        try:
            self._channel.close()
        except:  # noqa: E722
            pass

    @handle_grpc_error
    def create_interactive_graph(self, graph_def: interactive_pb2.GraphProto):
        request = interactive_pb2.CreateInteractiveGraphRequest(graph_def=graph_def)
        return self._stub.CreateInteractiveGraph(request)

    @handle_grpc_error
    def remove_interactive_graph(self, graph: str):
        request = interactive_pb2.RemoveInteractiveGraphRequest(graph_name=graph)
        return self._stub.RemoveInteractiveGraph(request)

    @handle_grpc_error
    def list_interactive_graph(self):
        request = interactive_pb2.ListInteractiveGraphRequest()
        return self._stub.ListInteractiveGraph(request)

    @handle_grpc_error
    def import_interactive_graph(self, schema_mapping: interactive_pb2.SchemaMapping):
        request = interactive_pb2.ImportInteractiveGraphRequest(
            schema_mapping=schema_mapping
        )
        return self._stub.ImportInteractiveGraph(request)

    @handle_grpc_error
    def list_interactive_job(self):
        request = interactive_pb2.ListInteractiveJobRequest()
        return self._stub.ListInteractiveJob(request)

    @handle_grpc_error
    def create_interactive_procedure(self, procedure: interactive_pb2.Procedure):
        request = interactive_pb2.CreateInteractiveProcedureRequest(
            procedure_def=procedure
        )
        return self._stub.CreateInteractiveProcedure(request)

    @handle_grpc_error
    def list_interactive_procedure(self, graph: str):
        request = interactive_pb2.ListInteractiveProcedureRequest(graph_name=graph)
        return self._stub.ListInteractiveProcedure(request)

    @handle_grpc_error
    def update_interactive_procedure(self, procedures: List[interactive_pb2.Procedure]):
        request = interactive_pb2.UpdateInteractiveProcedureRequest(
            procedures=procedures
        )
        return self._stub.UpdateInteractiveProcedure(request)

    @handle_grpc_error
    def remove_interactive_procedure(self, graph: str, procedure: str):
        request = interactive_pb2.RemoveInteractiveProcedureRequest(
            graph_name=graph, procedure_name=procedure
        )
        return self._stub.RemoveInteractiveProcedure(request)

    @handle_grpc_error
    def get_interactive_service_status(self):
        request = interactive_pb2.GetInteractiveServiceStatusRequest()
        return self._stub.GetInteractiveServiceStatus(request)

    @handle_grpc_error
    def start_interactive_service(self, service: interactive_pb2.Service):
        request = interactive_pb2.StartInteractiveServiceRequest(service_def=service)
        return self._stub.StartInteractiveService(request)

    @handle_grpc_error
    def stop_interactive_service(self):
        request = interactive_pb2.StopInteractiveServiceRequest()
        return self._stub.StopInteractiveService(request)

    @handle_grpc_error
    def restart_interactive_service(self):
        request = interactive_pb2.RestartInteractiveServiceRequest()
        return self._stub.RestartInteractiveService(request)

    @handle_grpc_error
    def get_node_status(self):
        request = interactive_pb2.GetNodeStatusRequest()
        return self._stub.GetNodeStatus(request)


def get_grpc_client(coordinator_endpoint=None):
    if coordinator_endpoint is not None:
        return GRPCClient(coordinator_endpoint)

    # use the latest context in config file
    current_context = get_current_context()
    if current_context is None:
        raise RuntimeError(
            "No available context found in {0}, please connect to a launched coordinator first.".format(
                GS_CONFIG_DEFAULT_LOCATION
            )
        )
    return GRPCClient(current_context.coordinator_endpoint)
