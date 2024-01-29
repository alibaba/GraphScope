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

import logging
import os
from typing import List, Union

from gs_flex_coordinator.core.config import (CLUSTER_TYPE,
                                             HIACTOR_ADMIN_SERVICE_PORT,
                                             WORKSPACE)
from gs_flex_coordinator.core.utils import get_internal_ip
from gs_flex_coordinator.models import StartServiceRequest

import hiactor_client
from hiactor_client import (Graph, ModelSchema, NodeStatus, Procedure,
                            SchemaMapping, Service)

logger = logging.getLogger("graphscope")


class Hiactor(object):
    """Class used to interact with hiactor engine"""

    def __init__(self):
        # hiactor admin service endpoint
        self._hiactor_endpoint = self._get_hiactor_service_endpoints()
        # workspace
        self._workspace = os.path.join(WORKSPACE, "interactive")
        os.makedirs(self._workspace, exist_ok=True)

    def _get_hiactor_service_endpoints(self):
        if CLUSTER_TYPE == "HOSTS":
            return "http://192.168.0.9:{0}".format(HIACTOR_ADMIN_SERVICE_PORT)

    def list_graphs(self) -> List[Graph]:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.list_graphs()

    def get_schema_by_name(self, graph_name: str) -> ModelSchema:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.get_schema(graph_name)

    def create_graph(self, graph: dict) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.create_graph(Graph.from_dict(graph))

    def delete_graph_by_name(self, graph_name: str) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.delete_graph(graph_name)

    def create_procedure(self, graph_name: str, procedure: dict) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ProcedureApi(api_client)
            return api_instance.create_procedure(
                graph_name, Procedure.from_dict(procedure)
            )

    def list_procedures(self, graph_name: Union[None, str]) -> List[Procedure]:
        if graph_name is not None:
            graph_name_list = [graph_name]
        else:
            # list all procedures
            graphs = self.list_graphs()
            graph_name_list = [g.name for g in graphs]

        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            procedures = []
            api_instance = hiactor_client.ProcedureApi(api_client)
            for graph_name in graph_name_list:
                response = api_instance.list_procedures(graph_name)
                if response is not None:
                    procedures.extend(response)
            return procedures

    def update_procedure(
        self, graph_name: str, procedure_name: str, procedure: dict
    ) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ProcedureApi(api_client)
            return api_instance.update_procedure(
                graph_name, procedure_name, Procedure.from_dict(procedure)
            )

    def delete_procedure_by_name(self, graph_name: str, procedure_name: str) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ProcedureApi(api_client)
            return api_instance.delete_procedure(graph_name, procedure_name)

    def get_service_status(self) -> dict:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ServiceApi(api_client)
            response = api_instance.get_service_status()
            # transfer
            if CLUSTER_TYPE == "HOSTS":
                internal_ip = get_internal_ip()
                return {
                    "status": response.status,
                    "graph_name": response.graph_name,
                    "sdk_endpoints": {
                        "cypher": f"neo4j://{internal_ip}:{response.bolt_port}",
                        "hqps": f"http://{internal_ip}:{response.hqps_port}",
                    },
                }

    def stop_service(self) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ServiceApi(api_client)
            return api_instance.stop_service()

    def restart_service(self) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ServiceApi(api_client)
            return api_instance.restart_service()

    def start_service(self, request: StartServiceRequest) -> str:
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.ServiceApi(api_client)
            return api_instance.start_service(
                Service.from_dict({"graph_name": request.graph_name})
            )

    def data_import(self, graph_name: str, schema_mapping: dict) -> str:
        print(graph_name, schema_mapping)
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_endpoint)
        ) as api_client:
            api_instance = hiactor_client.DataloadingApi(api_client)
            return api_instance.create_dataloading_job(
                graph_name, SchemaMapping.from_dict(schema_mapping)
            )


def init_hiactor_client():
    return Hiactor()
