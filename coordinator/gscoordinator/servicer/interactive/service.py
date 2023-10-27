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

"""Interactive Service under FLEX Architecture"""

import itertools
import logging
import os
import threading

import interactive_client
from google.protobuf.json_format import MessageToDict
from graphscope.config import Config
from graphscope.gsctl.utils import dict_to_proto_message
from graphscope.proto import error_codes_pb2
from graphscope.proto import interactive_pb2

from gscoordinator.scheduler import schedule
from gscoordinator.servicer.base_service import BaseServiceServicer
from gscoordinator.servicer.interactive.job import GraphImportScheduler
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import delegate_command_to_pod
from gscoordinator.utils import run_kube_cp_command

__all__ = ["InteractiveServiceServicer", "init_interactive_service_servicer"]

logger = logging.getLogger("graphscope")


# There are two workspaces for FLEX Interactive, one residing on "coordinator" node
# and the other on the "interactive" node. These two workspaces are equivalent when
# running mode is "hosts". Hence, INTERACTIVE_WORKSPACE is only effective in
# Kubernetes (k8s) environment.
INTERACTIVE_WORKSPACE = (
    os.environ["INTERACTIVE_WORKSPACE"]
    if "INTERACTIVE_WORKSPACE" in os.environ
    else "/tmp"
)


INTERACTIVE_CONTAINER_NAME = "interactive"


class InteractiveServiceServicer(BaseServiceServicer):
    """Interactive service under flex architecture."""

    def __init__(self, config: Config):
        super().__init__(config)

        # lock to protect the service
        self._lock = threading.RLock()

        # interactive host
        self._interactive_host = None
        self._fetch_interactive_host_impl()
        self._fetch_interactive_host_job = schedule.every(60).seconds.do(
            self._fetch_interactive_host_impl
        )

        # interactive pod list
        self._interactive_pod_list = []

        # job status
        self._job_status = {}

    def cleanup(self):
        pass

    def _fetch_interactive_host_impl(self):
        # TODO: get interactive service endpoint by instance id
        self._interactive_host = "http://47.242.172.5:7777"

    @property
    def interactive_host(self):
        return self._interactive_host

    def register_job_status(self, jobid, status):
        self._job_status[jobid] = status

    def write_and_distribute_file(self, graph, location, raw_data):
        """
        Args:
            graph(str): graph name.
            location(str): file location over coordinator node.
            raw_data(bytes): binary of file data.
        """
        # format: <graph_name>_<basename>
        filename = "{0}_{1}".format(graph, os.path.basename(location))

        # write temp file
        tmp_file = os.path.join(WORKSPACE, filename)
        with open(tmp_file, "wb") as f:
            f.write(raw_data)

        # distribute
        target_file = tmp_file
        if self.launcher_type == "k8s":
            # update file path in interactive pod
            target_file = os.path.join(INTERACTIVE_WORKSPACE, filename)

            for pod in self._interactive_pod_list:
                container = INTERACTIVE_CONTAINER_NAME
                cmd = f"mkdir -p {os.path.dirname(target_file)}"
                logger.debug(delegate_command_to_pod(cmd, pod, container))
                logger.debug(
                    run_kube_cp_command(tmp_file, target_file, pod, container, True)
                )

        return target_file

    def CreateInteractiveGraph(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            # create an instance of the API class
            api_instance = interactive_client.GraphApi(api_client)

            graph_def_dict = MessageToDict(
                request.graph_def, preserving_proto_field_name=True
            )
            graph = interactive_client.Graph.from_dict(graph_def_dict)

            try:
                api_response = api_instance.create_graph(graph)
            except Exception as e:
                logger.warning("Failed to create interactive graph. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def RemoveInteractiveGraph(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.GraphApi(api_client)

            try:
                api_response = api_instance.delete_graph(request.graph_name)
            except Exception as e:
                logger.warning("Failed to remove interactive graph. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def ListInteractiveGraph(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.GraphApi(api_client)

            try:
                api_response = api_instance.list_graphs()
            except Exception as e:
                logger.warning("Failed to list interactive graph. %s", str(e))
                return interactive_pb2.ListInteractiveGraphResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ListInteractiveGraphResponse(
                    code=error_codes_pb2.OK,
                    graphs=[
                        dict_to_proto_message(g.to_dict(), interactive_pb2.GraphProto())
                        for g in api_response
                    ],
                )

    def ImportInteractiveGraph(self, request, context):
        # write raw data to file and copy to interactive workspace
        try:
            schema_mapping_proto = request.schema_mapping
            for mapping in itertools.chain(
                schema_mapping_proto.vertex_mappings, schema_mapping_proto.edge_mappings
            ):
                raw_data_index = 0
                for index, location in enumerate(mapping.inputs):
                    if location.startswith("@"):
                        # write raw data and distribute file to interactive workspace
                        new_location = self.write_and_distribute_file(
                            schema_mapping_proto.graph,
                            location,
                            mapping.raw_data[raw_data_index],
                        )
                        raw_data_index += 1
                        # update the location
                        mapping.inputs[index] = new_location
        except Exception as e:
            logger.warning("Failed to distribute file. %s", str(e))
            return interactive_pb2.ApiResponse(
                code=error_codes_pb2.NETWORK_ERROR, error_msg=str(e)
            )

        # transform proto to dict
        schema_mapping = MessageToDict(
            schema_mapping_proto, preserving_proto_field_name=True
        )

        # create job scheduler
        scheduler = GraphImportScheduler(
            at_time="now",
            repeat=None,
            schema_mapping=schema_mapping,
            servicer=self,
        )
        scheduler.start()

        return interactive_pb2.ApiResponse(code=error_codes_pb2.OK)

    def ListInteractiveJob(self, request, context):
        return interactive_pb2.ListInteractiveJobResponse(
            code=error_codes_pb2.OK,
            job_status=[
                dict_to_proto_message(s.to_dict(), interactive_pb2.JobStatus())
                for _, s in self._job_status.items()
            ],
        )

    def CreateInteractiveProcedure(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            # create an instance of the API class
            api_instance = interactive_client.ProcedureApi(api_client)

            # transform proto to dict
            procedure_def_dict = MessageToDict(
                request.procedure_def, preserving_proto_field_name=True
            )

            graph_name = procedure_def_dict["bound_graph"]
            procedure = interactive_client.Procedure.from_dict(procedure_def_dict)

            try:
                api_response = api_instance.create_procedure(graph_name, procedure)
            except Exception as e:
                logger.warning("Failed to create procedure. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def ListInteractiveProcedure(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ProcedureApi(api_client)

            try:
                api_response = api_instance.list_procedures(request.graph_name)
            except Exception as e:
                logger.warning("Failed to list procedure. %s", str(e))
                return interactive_pb2.ListInteractiveProcedureResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                # change "returns" in the openapi definition to "rlts_meta",
                # as "returns" is a reserved keyword in proto
                procedures = []
                for p in api_response:
                    procedure_dict = p.to_dict()
                    # "returns" -> "rlts_meta"
                    procedure_dict["rlts_meta"] = procedure_dict.pop("returns")
                    # push
                    procedures.append(procedure_dict)

                return interactive_pb2.ListInteractiveProcedureResponse(
                    code=error_codes_pb2.OK,
                    procedures=[
                        dict_to_proto_message(p, interactive_pb2.Procedure())
                        for p in procedures
                    ],
                )

    def UpdateInteractiveProcedure(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ProcedureApi(api_client)

            for p in request.procedures:
                # transform proto to dict
                procedure_def_dict = MessageToDict(p, preserving_proto_field_name=True)

                graph_name = procedure_def_dict["bound_graph"]
                procedure_name = procedure_def_dict["name"]
                procedure = interactive_client.Procedure.from_dict(procedure_def_dict)

                try:
                    api_response = api_instance.update_procedure(
                        graph_name, procedure_name, procedure
                    )
                except Exception as e:
                    logger.warning("Failed to update procedure. %s", str(e))
                    return interactive_pb2.ApiResponse(
                        code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                    )

            return interactive_pb2.ApiResponse(code=error_codes_pb2.OK)

    def RemoveInteractiveProcedure(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ProcedureApi(api_client)

            try:
                api_response = api_instance.delete_procedure(
                    request.graph_name, request.procedure_name
                )
            except Exception as e:
                logger.warning("Failed to remove procedure. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def GetInteractiveServiceStatus(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ServiceApi(api_client)

            try:
                api_response = api_instance.get_service_status()
            except Exception as e:
                logger.warning("Failed to get service status. %s", str(e))
                return interactive_pb2.GetInteractiveServiceStatusResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.GetInteractiveServiceStatusResponse(
                    code=error_codes_pb2.OK,
                    service_status=dict_to_proto_message(
                        api_response.to_dict(), interactive_pb2.ServiceStatus()
                    ),
                )

    def StartInteractiveService(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ServiceApi(api_client)

            service_def_dict = MessageToDict(
                request.service_def, preserving_proto_field_name=True
            )
            service = interactive_client.Service.from_dict(service_def_dict)

            try:
                api_response = api_instance.start_service(service)
            except Exception as e:
                logger.warning("Failed to start service. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def StopInteractiveService(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ServiceApi(api_client)

            try:
                api_response = api_instance.stop_service()
            except Exception as e:
                logger.warning("Failed to stop service. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def RestartInteractiveService(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.ServiceApi(api_client)

            try:
                api_response = api_instance.restart_service()
            except Exception as e:
                logger.warning("Failed to restart service. %s", str(e))
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.ApiResponse(
                    code=error_codes_pb2.OK, error_msg=api_response.message
                )

    def GetNodeStatus(self, request, context):
        with interactive_client.ApiClient(
            interactive_client.Configuration(self._interactive_host)
        ) as api_client:
            api_instance = interactive_client.NodeApi(api_client)

            try:
                api_response = api_instance.get_node_status()
            except Exception as e:
                logger.warning("Failed to get node status. %s", str(e))
                return interactive_pb2.GetNodeStatusResponse(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )
            else:
                return interactive_pb2.GetNodeStatusResponse(
                    code=error_codes_pb2.OK,
                    nodes_status=[
                        dict_to_proto_message(s.to_dict(), interactive_pb2.NodeStatus())
                        for s in api_response
                    ],
                )


def init_interactive_service_servicer(config: Config):
    return InteractiveServiceServicer(config)
