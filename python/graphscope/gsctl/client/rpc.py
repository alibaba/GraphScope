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
from graphscope.proto import flex_pb2
from graphscope.proto import message_pb2
from graphscope.version import __version__


class GRPCClient(object):
    def __init__(self, endpoint):
        self._endpoint = endpoint
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
                    f"Couldn't connect to current server: {self._endpoint}: i/o timeout",
                    fg="yellow",
                )
                if time.time() - begin_time >= timeout_seconds:
                    click.secho(f"Unable to connect to server: {msg}", fg="yellow")
                    return None
                time.sleep(8)
        return response.solution

    @property
    def coordinator_endpoint(self):
        return self._endpoint

    def connect(self, timeout_seconds=24):
        return self._connect_impl(timeout_seconds)

    def connection_available(self):
        try:
            request = message_pb2.ConnectRequest(version=__version__)
            response = self._stub.Connect(request)
            return response.solution
        except:  # noqa: E722
            return None

    def close(self):
        try:
            self._channel.close()
        except:  # noqa: E722
            pass

    @handle_grpc_error
    def create_graph(self, graph_def: flex_pb2.GraphProto):
        request = flex_pb2.CreateGraphRequest(graph_def=graph_def)
        return self._stub.CreateGraph(request)

    @handle_grpc_error
    def delete_graph(self, graph_name: str):
        request = flex_pb2.DeleteGraphRequest(graph_name=graph_name)
        return self._stub.DeleteGraph(request)

    @handle_grpc_error
    def list_graph(self):
        request = flex_pb2.ListGraphRequest()
        return self._stub.ListGraph(request)

    @handle_grpc_error
    def create_job(
        self,
        type: str,
        schedule: flex_pb2.Schedule,
        description: flex_pb2.JobDescription,
    ):
        request = flex_pb2.CreateJobRequest(
            type=type, schedule=schedule, description=description
        )
        return self._stub.CreateJob(request)

    @handle_grpc_error
    def list_job(self):
        request = flex_pb2.ListJobRequest()
        return self._stub.ListJob(request)

    @handle_grpc_error
    def cancel_job(self, jobid: str, delete_scheduler: bool):
        request = flex_pb2.CancelJobRequest(jobid=jobid, delete_scheduler=delete_scheduler)
        return self._stub.CancelJob(request)

    @handle_grpc_error
    def create_procedure(self, procedure_def: flex_pb2.Procedure):
        request = flex_pb2.CreateProcedureRequest(procedure_def=procedure_def)
        return self._stub.CreateProcedure(request)

    @handle_grpc_error
    def list_procedure(self):
        request = flex_pb2.ListProcedureRequest()
        return self._stub.ListProcedure(request)

    @handle_grpc_error
    def update_interactive_procedure(self, procedures: List[flex_pb2.Procedure]):
        request = flex_pb2.UpdateInteractiveProcedureRequest(procedures=procedures)
        return self._stub.UpdateInteractiveProcedure(request)

    @handle_grpc_error
    def remove_interactive_procedure(self, graph: str, procedure: str):
        request = flex_pb2.RemoveInteractiveProcedureRequest(
            graph_name=graph, procedure_name=procedure
        )
        return self._stub.RemoveInteractiveProcedure(request)

    @handle_grpc_error
    def get_interactive_service_status(self):
        request = flex_pb2.GetInteractiveServiceStatusRequest()
        return self._stub.GetInteractiveServiceStatus(request)

    @handle_grpc_error
    def start_interactive_service(self, service: flex_pb2.Service):
        request = flex_pb2.StartInteractiveServiceRequest(service_def=service)
        return self._stub.StartInteractiveService(request)

    @handle_grpc_error
    def stop_interactive_service(self):
        request = flex_pb2.StopInteractiveServiceRequest()
        return self._stub.StopInteractiveService(request)

    @handle_grpc_error
    def restart_interactive_service(self):
        request = flex_pb2.RestartInteractiveServiceRequest()
        return self._stub.RestartInteractiveService(request)

    @handle_grpc_error
    def get_node_status(self):
        request = flex_pb2.GetNodeStatusRequest()
        return self._stub.GetNodeStatus(request)


def get_grpc_client(coordinator_endpoint=None):
    if coordinator_endpoint is not None:
        return GRPCClient(coordinator_endpoint)

    # use the latest context in config file
    current_context = get_current_context()
    if current_context is None:
        command = "gsctl connect --coordinator-endpoint <endpoint>"
        click.secho(
            "No available context found, you may want to connect to a coordinator by: {0}".format(
                command
            ),
            fg="blue",
        )
        return None

    return GRPCClient(current_context.coordinator_endpoint)
