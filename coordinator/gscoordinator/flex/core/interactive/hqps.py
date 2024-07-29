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
from typing import List
from typing import Union

import gs_interactive
import requests
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.create_procedure_request import CreateProcedureRequest
from gs_interactive.models.schema_mapping import SchemaMapping
from gs_interactive.models.start_service_request import StartServiceRequest
from gs_interactive.models.update_procedure_request import UpdateProcedureRequest

from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import HQPS_ADMIN_SERVICE_PORT
from gscoordinator.flex.core.config import WORKSPACE
from gscoordinator.flex.core.datasource import DataSourceManager
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.core.utils import get_internal_ip
from gscoordinator.flex.core.utils import get_public_ip


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
                    "Recover job config from file %s",
                    self._job_config_pickle_path,
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
                    requests.get(f"http://127.0.0.1:{HQPS_ADMIN_SERVICE_PORT}")
                except requests.ConnectionError:
                    time.sleep(3)
                else:
                    return f"http://127.0.0.1:{HQPS_ADMIN_SERVICE_PORT}"

    def list_graphs(self) -> List[dict]:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
            graphs = [g.to_dict() for g in api_instance.list_graphs()]
            for g in graphs:
                g["creation_time"] = encode_datetime(
                    datetime.datetime.fromtimestamp(g["creation_time"] / 1000)
                )
                if g["data_update_time"] == 0:
                     g["data_update_time"] = "null"
                else:
                    g["data_update_time"] = encode_datetime(
                        datetime.datetime.fromtimestamp(g["data_update_time"] / 1000)
                    )
                # `schema_update_time` is same to `creation_time` in gs_interactive
                g["schema_update_time"] = g["creation_time"]
                if "edge_types" not in g["schema"]:
                    g["schema"]["edge_types"] = []
                if "vertex_types" not in g["schema"]:
                    g["schema"]["vertex_types"] = []
                # we do not have edge's primary key in gs_interactive
                for edge in g["schema"]["edge_types"]:
                    if "primary_keys" not in edge:
                        edge["primary_keys"] = []
            return graphs

    def get_schema_by_id(self, graph_id: str) -> dict:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
            schema = api_instance.get_schema(graph_id).to_dict()
            if "vertex_types" not in schema:
                schema["vertex_types"] = []
            if "edge_types" not in schema:
                schema["edge_types"] = []
            return schema

    def create_graph(self, graph: dict) -> dict:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
            response = api_instance.create_graph(CreateGraphRequest.from_dict(graph))
            return response.to_dict()

    def delete_graph_by_id(self, graph_id: str) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
            rlt = api_instance.delete_graph(graph_id)
            return rlt

    def get_graph_by_id(self, graph_id: str) -> dict:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
            g = api_instance.get_graph(graph_id).to_dict()
            g["creation_time"] = encode_datetime(
                datetime.datetime.fromtimestamp(g["creation_time"] / 1000)
            )
            if g["data_update_time"] == 0:
                 g["data_update_time"] = "null"
            else:
                g["data_update_time"] = encode_datetime(
                    datetime.datetime.fromtimestamp(g["data_update_time"] / 1000)
                )
            # `schema_update_time` is same to `creation_time` in Interactive
            g["schema_update_time"] = g["creation_time"]
            if "edge_types" not in g["schema"]:
                g["schema"]["edge_types"] = []
            if "vertex_types" not in g["schema"]:
                g["schema"]["vertex_types"] = []
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

    def delete_edge_type_by_name(
        self,
        graph_id: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ):
        raise RuntimeError("Create vertex type is not supported yet!")

    def create_stored_procedure(self, graph_id: str, stored_procedure: dict) -> dict:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceProcedureManagementApi(
                api_client
            )
            response = api_instance.create_procedure(
                graph_id, CreateProcedureRequest.from_dict(stored_procedure)
            ).to_dict()
            if "procedure_id" in response:
                response["stored_procedure_id"] = response.pop("procedure_id")
            return response

    def list_stored_procedures(self, graph_id: str) -> List[dict]:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            stored_procedures = []
            api_instance = gs_interactive.AdminServiceProcedureManagementApi(
                api_client
            )
            stored_procedures = [
                p.to_dict() for p in api_instance.list_procedures(graph_id)
            ]
            return stored_procedures

    def update_stored_procedure_by_id(
        self, graph_id: str, stored_procedure_id: str, stored_procedure: dict
    ) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceProcedureManagementApi(
                api_client
            )
            return api_instance.update_procedure(
                graph_id,
                stored_procedure_id,
                UpdateProcedureRequest.from_dict(stored_procedure),
            )

    def delete_stored_procedure_by_id(
        self, graph_id: str, stored_procedure_id: str
    ) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceProcedureManagementApi(
                api_client
            )
            return api_instance.delete_procedure(graph_id, stored_procedure_id)

    def get_stored_procedure_by_id(
        self, graph_id: str, stored_procedure_id: str
    ) -> dict:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceProcedureManagementApi(
                api_client
            )
            return api_instance.get_procedure(graph_id, stored_procedure_id).to_dict()

    def list_service_status(self) -> List[dict]:
        # get service status from serving graph
        serving_graph_id = None
        rlts = []
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceServiceManagementApi(
                api_client
            )
            response = api_instance.get_service_status()
            if CLUSTER_TYPE == "HOSTS":
                host = get_internal_ip()
                if response.status == "Running" and response.graph is not None:
                    g = response.graph.to_dict()
                    serving_graph_id = g["id"]
                    service_start_time = encode_datetime(
                        datetime.datetime.fromtimestamp(response.start_time / 1000)
                    )
                    status = {
                        "status": response.status,
                        "sdk_endpoints": {
                            "cypher": f"neo4j://{host}:{response.bolt_port} (internal)",
                            "hqps": f"http://{host}:{response.hqps_port} (internal)",
                            "gremlin": f"ws://{host}:{response.gremlin_port}/gremlin (internal)",
                        },
                        "start_time": service_start_time,
                        "graph_id": g["id"],
                    }
                    rlts.append(status)
        # only one graph is serving at a certain time
        graphs = self.list_graphs()
        for g in graphs:
            if serving_graph_id is None or serving_graph_id != g["id"]:
                status = {
                    "status": "Stopped",
                    "graph_id": g["id"],
                    "start_time": "null",
                }
                rlts.append(status)
        return rlts

    def stop_service(self) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceServiceManagementApi(
                api_client
            )
            return api_instance.stop_service()

    def restart_service(self) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceServiceManagementApi(
                api_client
            )
            return api_instance.restart_service()

    def start_service(self, graph_id: str) -> str:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceServiceManagementApi(
                api_client
            )
            return api_instance.start_service(StartServiceRequest(graph_id=graph_id))

    def list_jobs(self) -> List[dict]:
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceJobManagementApi(
                api_client
            )
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
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceJobManagementApi(
                api_client
            )
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
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceJobManagementApi(
                api_client
            )
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
            eds = ds_manager.get_edge_datasource(
                graph_id,
                e["type_name"],
                e["source_vertex"],
                e["destination_vertex"],
            )
            if eds:
                schema_mapping["edge_mappings"].append(eds)
        # set job configuration before submission
        self._job_config[graph_id] = config
        self.dump_to_disk()
        # submit
        with gs_interactive.ApiClient(
            gs_interactive.Configuration(self._hqps_endpoint)
        ) as api_client:
            api_instance = gs_interactive.AdminServiceGraphManagementApi(
                api_client
            )
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
