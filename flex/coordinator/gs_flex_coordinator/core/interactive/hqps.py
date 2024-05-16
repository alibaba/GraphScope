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

import datetime
import logging
import os
import pickle
import time
import requests
from typing import List, Union

import interactive_sdk.openapi
from interactive_sdk.openapi import CreateGraphRequest
from interactive_sdk.openapi import CreateProcedureRequest
from interactive_sdk.openapi import UpdateProcedureRequest
from interactive_sdk.openapi import StartServiceRequest
from interactive_sdk.openapi import SchemaMapping

from gs_flex_coordinator.core.datasource import DataSourceManager
from gs_flex_coordinator.core.config import (
    CLUSTER_TYPE,
    HQPS_ADMIN_SERVICE_PORT,
    WORKSPACE,
)
from gs_flex_coordinator.core.utils import (
    encode_datetime,
    get_internal_ip,
    get_public_ip,
)


class HQPSClient(object):
    """Class used to interact with hqps engine"""

    def __init__(self):
        # hqps admin service endpoint
        self._hqps_endpoint = self._get_hqps_service_endpoints()
        # job configuration
        self._job_config = {}
        # job Configuration path
        self._job_config_pickle_path = os.path.join(WORKSPACE, "job_config.pickle")
        # recover
        self.try_to_recover_from_disk()

    def dump_to_disk(self):
        try:
            with open(self._job_config_pickle_path, "wb") as f:
                pickle.dump(self._job_config, f)
        except Exception as e:
             logging.warn("Failed to dump job config file: %s", str(e))

    def try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._job_config_pickle_path):
                logging.info(
                    "Recover job config from file %s", self._job_config_pickle_path
                )
            with open(self._job_config_pickle_path, "rb") as f:
                self._job_config = pickle.load(f)
        except Exception as e:
            logging.warn("Failed to recover job config: %s", str(e))

    def _get_hqps_service_endpoints(self):
        if CLUSTER_TYPE == "HOSTS":
            logging.info("Connecting to HQPS service ...")
            while True:
                try:
                    requests.get(f"http://192.168.0.9:{HQPS_ADMIN_SERVICE_PORT}")
                except requests.ConnectionError:
                    time.sleep(3)
                else:
                    return f"http://192.168.0.9:{HQPS_ADMIN_SERVICE_PORT}"

    def list_graphs(self) -> List[dict]:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            graphs = [g.to_dict() for g in api_instance.list_graphs()]
            for g in graphs:
                # `schema_update_time` is same to `creation_time` in Interactive
                g["schema_update_time"] = g["creation_time"]
                # we do not have edge's primary key in Interactive
                for edge in g["schema"]["edge_types"]:
                    if "primary_keys" not in edge:
                        edge["primary_keys"] = []
            return graphs

    def get_schema_by_id(self, graph_id: str) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            return api_instance.get_schema(graph_id).to_dict()

    def create_graph(self, graph: dict) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            response = api_instance.create_graph(CreateGraphRequest.from_dict(graph))
            return response.to_dict()

    def delete_graph_by_id(self, graph_id: str) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            rlt = api_instance.delete_graph(graph_id)
            return rlt

    def get_graph_by_id(self, graph_id: str) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            a = api_instance.get_graph(graph_id)
            g = api_instance.get_graph(graph_id).to_dict()
            # `schema_update_time` is same to `creation_time` in Interactive
            g["schema_update_time"] = g["creation_time"]
            # we do not have edge's primary key in Interactive
            for edge in g["schema"]["edge_types"]:
                if "primary_keys" not in edge:
                    edge["primary_keys"] = []
            return g

    def create_vertex_type(self, graph_id: str, vtype: dict):
        raise RuntimeError("Create vertex type is not supported yet!")

    def create_edge_type(self, graph_id: str, etype: dict):
        raise RuntimeError("Create vertex type is not supported yet!")

    def delete_vertex_type_by_name(self, graph_id: str, type_name: str):
        raise RuntimeError("Create vertex type is not supported yet!")

    def delete_edge_type_by_name(self, graph_id: str, edge_type: str, source_vertex_type: str, destination_vertex_type: str):
        raise RuntimeError("Create vertex type is not supported yet!")

    def create_procedure(self, graph_id: str, procedure: dict) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceProcedureManagementApi(api_client)
            response = api_instance.create_procedure(
                graph_id, CreateProcedureRequest.from_dict(procedure)
            )
            return response.to_dict()

    def list_procedures(self, graph_id: str) -> List[dict]:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            procedures = []
            api_instance = interactive_sdk.openapi.AdminServiceProcedureManagementApi(api_client)
            procedures = [p.to_dict() for p in api_instance.list_procedures(graph_id)]
            return procedures

    def update_procedure_by_id(
        self, graph_id: str, procedure_id: str, procedure: dict
    ) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceProcedureManagementApi(api_client)
            return api_instance.update_procedure(
                graph_id, procedure_id, UpdateProcedureRequest.from_dict(procedure)
            )

    def delete_procedure_by_id(self, graph_id: str, procedure_id: str) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceProcedureManagementApi(api_client)
            return api_instance.delete_procedure(graph_id, procedure_id)

    def get_procedure_by_id(self, graph_id: str, procedure_id: str) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceProcedureManagementApi(api_client)
            return api_instance.get_procedure(graph_id, procedure_id).to_dict()

    def get_service_status(self) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceServiceManagementApi(api_client)
            response = api_instance.get_service_status()
            # transfer
            if CLUSTER_TYPE == "HOSTS":
                host = get_public_ip()
                if host is None:
                    host = get_internal_ip()
                status = {
                    "status": response.status,
                    "sdk_endpoints": {
                        "cypher": f"neo4j://{host}:{response.bolt_port}",
                        "hqps": f"http://{host}:{response.hqps_port}",
                        "gremlin": f"ws://{host}:{response.gremlin_port}/gremlin",
                    },
                }
                if response.graph is not None:
                    graph = response.graph.to_dict()
                    # `schema_update_time` is same to `creation_time` in Interactive
                    graph["schema_update_time"] = graph["creation_time"]
                    status["graph"] = graph
                return status


    def stop_service(self) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceServiceManagementApi(api_client)
            return api_instance.stop_service()

    def restart_service(self) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceServiceManagementApi(api_client)
            return api_instance.restart_service()

    def start_service(self, graph_id: str) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceServiceManagementApi(api_client)
            return api_instance.start_service(StartServiceRequest(graph_id=graph_id))

    def list_jobs(self) -> List[dict]:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceJobManagementApi(api_client)
            rlt = []
            for s in api_instance.list_jobs():
                job_status = s.to_dict()
                job_status["start_time"] = encode_datetime(
                    datetime.datetime.fromtimestamp(job_status["start_time"] / 1000)
                )
                if "end_time" in job_status:
                    job_status["end_time"] = encode_datetime(
                        datetime.datetime.fromtimestamp(job_status["end_time"] / 1000)
                    )
                rlt.append(job_status)
            return rlt

    def get_job_by_id(self, job_id: str) -> dict:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceJobManagementApi(api_client)
            job_status = api_instance.get_job_by_id(job_id).to_dict()
            job_status["start_time"] = encode_datetime(
                datetime.datetime.fromtimestamp(job_status["start_time"] / 1000)
            )
            if "end_time" in job_status:
                job_status["end_time"] = encode_datetime(
                    datetime.datetime.fromtimestamp(job_status["end_time"] / 1000)
                )
            return job_status

    def delete_job_by_id(self, job_id: str) -> str:
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceJobManagementApi(api_client)
            return api_instance.delete_job_by_id(job_id)

    def submit_dataloading_job(
        self, graph_id: str, config: dict, ds_manager: DataSourceManager
    ) -> str:
        # schema mapping
        schema_mapping = {
            "loading_config": config["loading_config"],
            "vertex_mappings": [],
            "edge_mappings": [],
        }
        for v in config["vertices"]:
            vds = ds_manager.get_vertex_datasource(graph_id, v["type_name"])
            if vds:
                schema_mapping["vertex_mappings"].append(vds)
        for e in config["edges"]:
            eds = ds_manager.get_edge_datasource(graph_id, e["type_name"], e["source_vertex"], e["destination_vertex"])
            if eds:
                schema_mapping["edge_mappings"].append(eds)
        # set job configuration before submission
        self._job_config[graph_id] = config
        self.dump_to_disk()
        # submit
        with interactive_sdk.openapi.ApiClient(
            interactive_sdk.openapi.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = interactive_sdk.openapi.AdminServiceGraphManagementApi(api_client)
            response = api_instance.create_dataloading_job(
                graph_id, SchemaMapping.from_dict(schema_mapping)
            )
            return response.job_id

    def get_dataloading_job_config(self, graph_id: str) -> dict:
        config = {}
        if graph_id in self._job_config:
            config = self._job_config[graph_id]
        return config


def init_hqps_client():
    return HQPSClient()
