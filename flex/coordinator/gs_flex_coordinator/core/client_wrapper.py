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

# import datetime
import itertools
import logging
import os
# import pickle
# import socket
import threading
from typing import List, Union

# import psutil

from gs_flex_coordinator.core.config import (
    CLUSTER_TYPE,
    CREATION_TIME,
    DATASET_WORKSPACE,
    INSTANCE_NAME,
    SOLUTION,
    WORKSPACE,
)
from gs_flex_coordinator.core.insight import init_groot_client
from gs_flex_coordinator.core.interactive import init_hqps_client
from gs_flex_coordinator.core.datasource import DataSourceManager
# from gs_flex_coordinator.core.scheduler import schedule
from gs_flex_coordinator.core.utils import (
    # GraphInfo,
    # decode_datetimestr,
    encode_datetime,
    # get_current_time,
)
from gs_flex_coordinator.models import (
    RunningDeploymentInfo,
    CreateGraphRequest,
    CreateGraphResponse,
    GetGraphSchemaResponse,
    CreateProcedureRequest,
    UpdateProcedureRequest,
    CreateProcedureResponse,
    GetProcedureResponse,
    UploadFileResponse,
    DataloadingJobConfig,
    CreateDataloadingJobResponse,
    CreateEdgeType,
    CreateVertexType,
    # EdgeDataSource,
    # EdgeType,
    GetGraphResponse,
    # GrootDataloadingJobConfig,
    # GrootGraph,
    # GrootSchema,
    JobStatus,
    # ModelSchema,
    # NodeStatus,
    # Procedure,
    SchemaMapping,
    ServiceStatus,
    StartServiceRequest,
    # VertexDataSource,
    # VertexType,
)
from gs_flex_coordinator.version import __version__


class ClientWrapper(object):
    """Wrapper of client that interacts with engine"""

    def __init__(self):
        # lock to protect the service
        self._lock = threading.RLock()
        # initialize specific client
        self._client = self._initialize_client()
        # data source management
        self._datasource_manager = DataSourceManager()
        # graphs info
        # self._graphs_info = {}
        # pickle path
        # self._pickle_path = os.path.join(WORKSPACE, "graphs_info.pickle")
        # # recover
        # self._try_to_recover_from_disk()
        # # sync graphs info every 60s
        # self._sync_graphs_info_job = (
            # schedule.every(60)
            # .seconds.do(self._sync_graphs_info_impl)
            # .tag("sync", "graphs info")
        # )

    # def _try_to_recover_from_disk(self):
        # try:
            # if os.path.exists(self._pickle_path):
                # logging.info("Recover graphs info from file %s", self._pickle_path)
                # with open(self._pickle_path, "rb") as f:
                    # self._graphs_info = pickle.load(f)
        # except Exception as e:
            # logging.warn("Failed to recover graphs info: %s", str(e))
        # # set default graph info
        # self._sync_graphs_info_impl()

    # def _pickle_graphs_info_impl(self):
        # try:
            # with open(self._pickle_path, "wb") as f:
                # pickle.dump(self._graphs_info, f)
        # except Exception as e:
            # logging.warn("Failed to dump graphs info: %s", str(e))

    # def _sync_graphs_info_impl(self):
        # if SOLUTION == "INTERACTIVE":
            # graphs = self.list_graphs()
        # elif SOLUTION == "GRAPHSCOPE_INSIGHT":
            # graphs = self.list_groot_graph()
        # rlts = {}
        # for g in graphs:
            # if g.name in self._graphs_info:
                # rlts[g.name] = self._graphs_info[g.name]
            # else:
                # rlts[g.name] = GraphInfo(name=g.name, creation_time=CREATION_TIME)
        # self._graphs_info = rlts

    def _initialize_client(self):
        service_initializer = {
            "INTERACTIVE": init_hqps_client,
            "GRAPHSCOPE_INSIGHT": init_groot_client,
        }
        initializer = service_initializer.get(SOLUTION)
        if initializer is None:
            raise RuntimeError(f"Client initializer of {SOLUTION} not found.")
        return initializer()

    def list_graphs(self) -> List[GetGraphResponse]:
        graphs = self._client.list_graphs()
        # fix ValueError: Invalid value for `long_text`, must not be `None`
        for g in graphs:
            for item in itertools.chain(g["schema"]["vertex_types"], g["schema"]["edge_types"]):
                for p in item["properties"]:
                    if "string" in p["property_type"] and "long_text" in p["property_type"]["string"]:
                        p["property_type"]["string"]["long_text"] = ""
        # transfer
        rlts = [GetGraphResponse.from_dict(g) for g in graphs]
        return rlts

    def get_schema_by_id(self, graph_id: str) -> GetGraphSchemaResponse:
        schema = self._client.get_schema_by_id(graph_id)
        # fix ValueError: Invalid value for `long_text`, must not be `None`
        for item in itertools.chain(schema["vertex_types"], schema["edge_types"]):
            for p in item["properties"]:
                if "string" in p["property_type"] and "long_text" in p["property_type"]["string"]:
                    p["property_type"]["string"]["long_text"] = ""
        return GetGraphSchemaResponse.from_dict(schema)

    # def get_groot_schema(self, graph_name: str) -> GrootSchema:
        # return GrootSchema.from_dict(self._client.get_groot_schema(graph_name))

    # def import_groot_schema(self, graph_name: str, schema: GrootSchema) -> str:
        # rlt = self._client.import_groot_schema(graph_name, schema.to_dict())
        # self._graphs_info[INSTANCE_NAME].update_time = get_current_time()
        # return rlt

    # def get_current_graph(self) -> GrootGraph:
        # return self._client.get_current_graph()

    def create_graph(self, graph: CreateGraphRequest) -> CreateGraphResponse:
        # there are some tricks here, since schema is a keyword of openapi
        # specification, so it will be converted into the _schema field.
        graph_dict = graph.to_dict()
        if "_schema" in graph_dict:
            graph_dict["schema"] = graph_dict.pop("_schema")
        response = self._client.create_graph(graph_dict)
        return CreateGraphResponse.from_dict(response)

    def create_vertex_type(self, graph_id: str, vtype: CreateVertexType) -> str:
        self._client.create_vertex_type(graph_id, vtype.to_dict())
        return "Create vertex type successfully"
        # if SOLUTION == "GRAPHSCOPE_INSIGHT":
            # graph_name = INSTANCE_NAME
        # vtype_dict = vtype.to_dict()
        # rlt = self._client.create_vertex_type(graph_name, vtype_dict)
        # # self._graphs_info[graph_name].update_time = get_current_time()
        # return rlt

    def create_edge_type(self, graph_id: str, etype: CreateEdgeType) -> str:
        self._client.create_edge_type(graph_id, etype.to_dict())
        return "Create edge type successfully"
        # if SOLUTION == "GRAPHSCOPE_INSIGHT":
            # graph_name = INSTANCE_NAME
        # etype_dict = etype.to_dict()
        # rlt = self._client.create_edge_type(graph_name, etype_dict)
        # self._graphs_info[graph_name].update_time = get_current_time()
        # return rlt

    def delete_vertex_type_by_name(self, graph_id: str, type_name: str) -> str:
        self._client.delete_vertex_type_by_name(graph_id, type_name)
        # remove data source mapping
        self.unbind_vertex_datasource(graph_id, type_name)
        return f"Delete vertex type {type_name} successfully"
        # if SOLUTION == "GRAPHSCOPE_INSIGHT":
            # graph_name = INSTANCE_NAME
        # rlt = self._client.delete_vertex_type(graph_name, vertex_type)
        # self._graphs_info[graph_name].update_time = get_current_time()
        # return rlt

    def delete_edge_type_by_name(
        self,
        graph_id: str,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        self._client.delete_edge_type_by_name(
            graph_id, type_name, source_vertex_type, destination_vertex_type
        )
        # remove data source mapping
        self.unbind_edge_datasource(graph_id, type_name, source_vertex_type, destination_vertex_type)
        return f"Delete edge type ({source_vertex_type})->[{type_name}]->({destination_vertex_type}) successfully"
        # if SOLUTION == "GRAPHSCOPE_INSIGHT":
            # graph_name = INSTANCE_NAME
        # rlt = self._client.delete_edge_type(
            # graph_name, edge_type, source_vertex_type, destination_vertex_type
        # )
        # self._graphs_info[graph_name].update_time = get_current_time()
        # return rlt

    def delete_graph_by_id(self, graph_id: str) -> str:
        rlt = self._client.delete_graph_by_id(graph_id)
        # remove data source mapping
        self._datasource_manager.delete_datasource_by_id(graph_id)
        return rlt

    def get_graph_by_id(self, graph_id: str) -> GetGraphResponse:
        g = self._client.get_graph_by_id(graph_id)
        for item in itertools.chain(g["schema"]["vertex_types"], g["schema"]["edge_types"]):
            for p in item["properties"]:
                if "string" in p["property_type"] and "long_text" in p["property_type"]["string"]:
                    p["property_type"]["string"]["long_text"] = ""
        return GetGraphResponse.from_dict(g)

    def create_procedure(self, graph_id: str, procedure: CreateProcedureRequest) -> CreateProcedureResponse:
        procedure_dict = procedure.to_dict()
        response = self._client.create_procedure(graph_id, procedure_dict)
        return CreateProcedureResponse.from_dict(response)

    def list_procedures(self, graph_id: str) -> List[GetProcedureResponse]:
        procedures = self._client.list_procedures(graph_id)
        # transfer
        rlt = [GetProcedureResponse.from_dict(p) for p in procedures]
        return rlt

    def update_procedure_by_id(
        self, graph_id: str, procedure_id: str, procedure: UpdateProcedureRequest
    ) -> str:
        procedure_dict = procedure.to_dict()
        return self._client.update_procedure_by_id(graph_id, procedure_id, procedure_dict)

    def delete_procedure_by_id(self, graph_id: str, procedure_id: str) -> str:
        return self._client.delete_procedure_by_id(graph_id, procedure_id)

    def get_procedure_by_id(self, graph_id: str, procedure_id: str) -> GetProcedureResponse:
        return GetProcedureResponse.from_dict(
            self._client.get_procedure_by_id(graph_id, procedure_id)
        )

    # def get_node_status(self) -> List[NodeStatus]:
        # rlt = []
        # if CLUSTER_TYPE == "HOSTS":
            # disk_info = psutil.disk_usage("/")
            # status = {
                # "node": socket.gethostname(),
                # "cpu_usage": psutil.cpu_percent(),
                # "memory_usage": psutil.virtual_memory().percent,
                # "disk_usage": float(f"{disk_info.used / disk_info.total * 100:.2f}"),
            # }
            # rlt.append(NodeStatus.from_dict(status))
        # return rlt

    def get_deployment_info(self) -> RunningDeploymentInfo:
        info = {
            "instance_name": INSTANCE_NAME,
            "cluster_type": CLUSTER_TYPE,
            "version": __version__,
            "solution": SOLUTION,
            "creation_time": encode_datetime(CREATION_TIME),
        }
        return RunningDeploymentInfo.from_dict(info)

    def get_service_status(self) -> ServiceStatus:
        status = self._client.get_service_status()
        if "graph" in status:
            # fix ValueError: Invalid value for `long_text`, must not be `None`
            schema = status["graph"]["schema"]
            for item in itertools.chain(schema["vertex_types"], schema["edge_types"]):
                for p in item["properties"]:
                    if "string" in p["property_type"] and "long_text" in p["property_type"]["string"]:
                        p["property_type"]["string"]["long_text"] = ""
        return ServiceStatus.from_dict(status)

    def stop_service(self) -> str:
        return self._client.stop_service()

    def restart_service(self) -> str:
        return self._client.restart_service()

    def start_service(self, request: StartServiceRequest) -> str:
        return self._client.start_service(request.graph_id)

    def list_jobs(self) -> List[JobStatus]:
        return [JobStatus.from_dict(s) for s in self._client.list_jobs()]

    def get_job_by_id(self, job_id: str) -> JobStatus:
        return JobStatus.from_dict(self._client.get_job_by_id(job_id))

    def delete_job_by_id(self, job_id: str) -> str:
        return self._client.delete_job_by_id(job_id)

    def submit_dataloading_job(
        self, graph_id: str, dataloading_job_config: DataloadingJobConfig
    ) -> str:
        config = dataloading_job_config.to_dict()
        job_id = self._client.submit_dataloading_job(graph_id, config, self._datasource_manager)
        return CreateDataloadingJobResponse.from_dict({"job_id": job_id})

    def get_dataloading_job_config(self, graph_id: str) -> DataloadingJobConfig:
        return DataloadingJobConfig.from_dict(self._client.get_dataloading_job_config(graph_id))

    def upload_file(self, filestorage) -> str:
        if CLUSTER_TYPE == "HOSTS":
            filepath = os.path.join(DATASET_WORKSPACE, filestorage.filename)
            filestorage.save(filepath)
            return UploadFileResponse.from_dict({"file_path": filepath})

    # def create_groot_dataloading_job(
        # self, graph_name: str, job_config: GrootDataloadingJobConfig
    # ) -> str:
        # job_id = self._client.create_groot_dataloading_job(
            # graph_name, job_config.to_dict()
        # )
        # return job_id

    # def list_groot_graph(self) -> List[GrootGraph]:
        # graphs = self._client.list_groot_graph()
        # # transfer
        # rlts = [GrootGraph.from_dict(g) for g in graphs]
        # return rlts

    def bind_datasource_in_batch(self, graph_id: str, schema_mapping: SchemaMapping) -> str:
        # there are some tricks here, since property is a keyword of openapi
        # specification, so it will be converted into the _property field.
        schema_mapping_dict = schema_mapping.to_dict()
        for mapping in itertools.chain(
            schema_mapping_dict["vertex_mappings"], schema_mapping_dict["edge_mappings"]
        ):
            for column_mapping in mapping["column_mappings"]:
                if "_property" in column_mapping:
                    column_mapping["property"] = column_mapping.pop("_property")
            if "source_vertex_mappings" in mapping and "destination_vertex_mappings" in mapping:
                for column_mapping in itertools.chain(mapping["source_vertex_mappings"], mapping["destination_vertex_mappings"]):
                    if "_property" in column_mapping:
                        column_mapping["property"] = column_mapping.pop("_property")
        self._datasource_manager.bind_datasource_in_batch(graph_id, schema_mapping_dict)
        return "Bind data source mapping successfully"

    def get_datasource_by_id(self, graph_id: str) -> SchemaMapping:
        return SchemaMapping.from_dict(
            self._datasource_manager.get_datasource_mapping(graph_id)
        )

    def unbind_vertex_datasource(self, graph_id: str, vertex_type: str) -> str:
        self._datasource_manager.unbind_vertex_datasource(graph_id, vertex_type)
        return "Unbind vertex data source successfully"

    def unbind_edge_datasource(
        self,
        graph_id: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        self._datasource_manager.unbind_edge_datasource(
            graph_id, edge_type, source_vertex_type, destination_vertex_type
        )
        return "Unbind edge data source successfully"


client_wrapper = ClientWrapper()
