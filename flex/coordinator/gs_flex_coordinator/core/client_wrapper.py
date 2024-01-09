#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import itertools
import logging
import psutil
import socket
import threading
from typing import List, Union

from gs_flex_coordinator.core.config import CLUSTER_TYPE, INSTANCE_NAME, SOLUTION
from gs_flex_coordinator.models import (DeploymentInfo, Graph, ModelSchema,
                                        NodeStatus, Procedure, ServiceStatus,
                                        StartServiceRequest)
from gs_flex_coordinator.core.interactive import init_hiactor_client
from gs_flex_coordinator.version import __version__

logger = logging.getLogger("graphscope")


class ClientWrapper(object):
    """Wrapper of client that interacts with engine"""

    def __init__(self):
        # lock to protect the service
        self._lock = threading.RLock()
        # initialize specific client
        self._client = self._initialize_client()

    def _initialize_client(self):
        service_initializer = {"INTERACTIVE": init_hiactor_client}
        initializer = service_initializer.get(SOLUTION)
        if initializer is None:
            raise RuntimeError(
                "Client initializer of {0} not found.".format(self._solution)
            )
        return initializer()

    def list_graphs(self) -> List[Graph]:
        graphs = self._client.list_graphs()
        # transfer
        rlts = [Graph.from_dict(g.to_dict()) for g in graphs]
        return rlts

    def get_schema_by_name(self, graph_name: str) -> ModelSchema:
        schema = self._client.get_schema_by_name(graph_name)
        # transfer
        rlt = ModelSchema.from_dict(schema.to_dict())
        return rlt

    def create_graph(self, graph: Graph) -> str:
        # there are some tricks here, since schema is a keyword of openapi
        # specification, so it will be converted into the _schema field.
        graph_dict = graph.to_dict()
        if "_schema" in graph_dict:
            graph_dict["schema"] = graph_dict.pop("_schema")
        return self._client.create_graph(graph_dict)

    def delete_graph_by_name(self, graph_name: str) -> str:
        return self._client.delete_graph_by_name(graph_name)

    def create_procedure(self, graph_name: str, procedure: Procedure) -> str:
        procedure_dict = procedure.to_dict()
        return self._client.create_procedure(graph_name, procedure_dict)

    def list_procedures(self, graph_name: Union[None, str]) -> List[Procedure]:
        procedures = self._client.list_procedures(graph_name)
        # transfer
        rlt = [Procedure.from_dict(p.to_dict()) for p in procedures]
        return rlt

    def update_procedure(
        self, graph_name: str, procedure_name: str, procedure: Procedure
    ) -> str:
        procedure_dict = procedure.to_dict()
        return self._client.update_procedure(graph_name, procedure_name, procedure_dict)

    def delete_procedure_by_name(self, graph_name: str, procedure_name: str) -> str:
        return self._client.delete_procedure_by_name(graph_name, procedure_name)

    def get_node_status(self) -> List[NodeStatus]:
        rlt = []
        if CLUSTER_TYPE == "HOSTS":
            disk_info = psutil.disk_usage("/")
            status = {
                "node": socket.gethostname(),
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
                "disk_usage": float(f"{disk_info.used / disk_info.total * 100:.2f}")
            }
            rlt.append(NodeStatus.from_dict(status))
        return rlt

    def get_deployment_info(self) -> DeploymentInfo:
        info = {
            "name": INSTANCE_NAME,
            "cluster_type": CLUSTER_TYPE,
            "version": __version__,
        }
        return DeploymentInfo.from_dict(info)

    def get_service_status(self) -> ServiceStatus:
        return ServiceStatus.from_dict(self._client.get_service_status())

    def stop_service(self) -> str:
        return self._client.stop_service()

    def restart_service(self) -> str:
        return self._client.restart_service()

    def start_service(self, request: StartServiceRequest) -> str:
        return self._client.start_service(request)

    def data_import(self, graph_name, schema_mapping) -> str:
        # there are some tricks here, since property is a keyword of openapi
        # specification, so it will be converted into the _property field.
        schema_mapping_dict = schema_mapping.to_dict()
        for mapping in itertools.chain(
            schema_mapping_dict["vertex_mappings"], schema_mapping_dict["edge_mappings"]
        ):
            for column_mapping in mapping["column_mappings"]:
                if "_property" in column_mapping:
                    column_mapping["property"] = column_mapping.pop("_property")
        print(schema_mapping_dict)
        return self._client.data_import(graph_name, schema_mapping_dict)


client_wrapper = ClientWrapper()
