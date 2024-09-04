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
import os
import threading
from typing import List

from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import DATASET_WORKSPACE
from gscoordinator.flex.core.config import SOLUTION
from gscoordinator.flex.core.datasource import DataSourceManager
from gscoordinator.flex.core.deployment import initialize_deployemnt
from gscoordinator.flex.core.insight import init_groot_client
from gscoordinator.flex.core.interactive import init_hqps_client
from gscoordinator.flex.core.utils import parse_file_metadata
from gscoordinator.flex.models import CreateDataloadingJobResponse
from gscoordinator.flex.models import CreateEdgeType
from gscoordinator.flex.models import CreateGraphRequest
from gscoordinator.flex.models import CreateGraphResponse
from gscoordinator.flex.models import CreateGraphSchemaRequest
from gscoordinator.flex.models import CreateStoredProcRequest
from gscoordinator.flex.models import CreateStoredProcResponse
from gscoordinator.flex.models import CreateVertexType
from gscoordinator.flex.models import DataloadingJobConfig
from gscoordinator.flex.models import DataloadingMRJobConfig
from gscoordinator.flex.models import GetGraphResponse
from gscoordinator.flex.models import GetGraphSchemaResponse
from gscoordinator.flex.models import GetPodLogResponse
from gscoordinator.flex.models import GetResourceUsageResponse
from gscoordinator.flex.models import GetStorageUsageResponse
from gscoordinator.flex.models import GetStoredProcResponse
from gscoordinator.flex.models import JobStatus
from gscoordinator.flex.models import RunningDeploymentInfo
from gscoordinator.flex.models import RunningDeploymentStatus
from gscoordinator.flex.models import SchemaMapping
from gscoordinator.flex.models import ServiceStatus
from gscoordinator.flex.models import StartServiceRequest
from gscoordinator.flex.models import UpdateStoredProcRequest
from gscoordinator.flex.models import UploadFileResponse

logger = logging.getLogger("graphscope")


class ClientWrapper(object):
    """Wrapper of client that interacts with engine"""

    def __init__(self):
        # lock to protect the service
        self._lock = threading.RLock()
        # initialize specific client
        self._client = self._initialize_client()
        # data source management
        self._datasource_manager = DataSourceManager()
        # deployment
        self._deployment = initialize_deployemnt()

    def _initialize_client(self):
        service_initializer = {
            "INTERACTIVE": init_hqps_client,
            "GRAPHSCOPE_INSIGHT": init_groot_client,
        }
        initializer = service_initializer.get(SOLUTION)
        if initializer is None:
            logger.warn(f"Client initializer of {SOLUTION} not found.")
            return None
        return initializer()

    def list_graphs(self) -> List[GetGraphResponse]:
        graphs = self._client.list_graphs()
        # fix ValueError: Invalid value for `long_text`, must not be `None`
        for g in graphs:
            for item in itertools.chain(
                g["schema"]["vertex_types"], g["schema"]["edge_types"]
            ):
                if "properties" in item:
                    for p in item["properties"]:
                        if (
                            "string" in p["property_type"]
                            and "long_text" in p["property_type"]["string"]
                        ):
                            p["property_type"]["string"]["long_text"] = ""
            if "stored_procedures" in g:
                for sp in g["stored_procedures"]:
                    for item in itertools.chain(sp["params"], sp["returns"]):
                        if (
                            "string" in item["type"]
                            and "long_text" in item["type"]["string"]
                        ):
                            item["type"]["string"]["long_text"] = ""
        # transfer
        rlts = [GetGraphResponse.from_dict(g) for g in graphs]
        return rlts

    def get_schema_by_id(self, graph_id: str) -> GetGraphSchemaResponse:
        schema = self._client.get_schema_by_id(graph_id)
        # fix ValueError: Invalid value for `long_text`, must not be `None`
        for item in itertools.chain(schema["vertex_types"], schema["edge_types"]):
            if "properties" in item:
                for p in item["properties"]:
                    if (
                        "string" in p["property_type"]
                        and "long_text" in p["property_type"]["string"]
                    ):
                        p["property_type"]["string"]["long_text"] = ""
        return GetGraphSchemaResponse.from_dict(schema)

    def create_graph(self, graph: CreateGraphRequest) -> CreateGraphResponse:
        # there are some tricks here, since schema is a keyword of openapi
        # specification, so it will be converted into the _schema field.
        graph_dict = graph.to_dict()
        if "_schema" in graph_dict:
            graph_dict["schema"] = graph_dict.pop("_schema")
        response = self._client.create_graph(graph_dict)
        return CreateGraphResponse.from_dict(response)

    def import_schema(self, graph_id: str, schema: CreateGraphSchemaRequest) -> str:
        self._client.import_schema(graph_id, schema.to_dict())
        return "Import schema successfully"

    def create_vertex_type(self, graph_id: str, vtype: CreateVertexType) -> str:
        self._client.create_vertex_type(graph_id, vtype.to_dict())
        return "Create vertex type successfully"

    def create_edge_type(self, graph_id: str, etype: CreateEdgeType) -> str:
        self._client.create_edge_type(graph_id, etype.to_dict())
        return "Create edge type successfully"

    def delete_vertex_type_by_name(self, graph_id: str, type_name: str) -> str:
        self._client.delete_vertex_type_by_name(graph_id, type_name)
        # remove data source mapping
        self.unbind_vertex_datasource(graph_id, type_name)
        return f"Delete vertex type {type_name} successfully"

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
        self.unbind_edge_datasource(
            graph_id, type_name, source_vertex_type, destination_vertex_type
        )
        elabel = f"({source_vertex_type})->[{type_name}]->({destination_vertex_type})"
        return f"Delete edge type {elabel} successfully"

    def delete_graph_by_id(self, graph_id: str) -> str:
        rlt = self._client.delete_graph_by_id(graph_id)
        # remove data source mapping
        self._datasource_manager.delete_datasource_by_id(graph_id)
        return rlt

    def get_graph_by_id(self, graph_id: str) -> GetGraphResponse:
        g = self._client.get_graph_by_id(graph_id)
        for item in itertools.chain(
            g["schema"]["vertex_types"], g["schema"]["edge_types"]
        ):
            if "properties" in item:
                for p in item["properties"]:
                    if (
                        "string" in p["property_type"]
                        and "long_text" in p["property_type"]["string"]
                    ):
                        p["property_type"]["string"]["long_text"] = ""
        if "stored_procedures" in g:
            for sp in g["stored_procedures"]:
                for item in itertools.chain(sp["params"], sp["returns"]):
                    if (
                        "string" in item["type"]
                        and "long_text" in item["type"]["string"]
                    ):
                        item["type"]["string"]["long_text"] = ""
        return GetGraphResponse.from_dict(g)

    def create_stored_procedure(
        self, graph_id: str, stored_procedure: CreateStoredProcRequest
    ) -> CreateStoredProcResponse:
        stored_procedure_dict = stored_procedure.to_dict()
        response = self._client.create_stored_procedure(graph_id, stored_procedure_dict)
        return CreateStoredProcResponse.from_dict(response)

    def list_stored_procedures(self, graph_id: str) -> List[GetStoredProcResponse]:
        stored_procedures = self._client.list_stored_procedures(graph_id)
        for sp in stored_procedures:
            for item in itertools.chain(sp["params"], sp["returns"]):
                if "string" in item["type"] and "long_text" in item["type"]["string"]:
                    item["type"]["string"]["long_text"] = ""
        # transfer
        rlt = [GetStoredProcResponse.from_dict(p) for p in stored_procedures]
        return rlt

    def update_stored_procedure_by_id(
        self,
        graph_id: str,
        stored_procedure_id: str,
        stored_procedure: UpdateStoredProcRequest,
    ) -> str:
        stored_procedure_dict = stored_procedure.to_dict()
        return self._client.update_stored_procedure_by_id(
            graph_id, stored_procedure_id, stored_procedure_dict
        )

    def delete_stored_procedure_by_id(
        self, graph_id: str, stored_procedure_id: str
    ) -> str:
        return self._client.delete_stored_procedure_by_id(graph_id, stored_procedure_id)

    def get_stored_procedure_by_id(
        self, graph_id: str, stored_procedure_id: str
    ) -> GetStoredProcResponse:
        sp = self._client.get_stored_procedure_by_id(graph_id, stored_procedure_id)
        for item in itertools.chain(sp["params"], sp["returns"]):
            if "string" in item["type"] and "long_text" in item["type"]["string"]:
                item["type"]["string"]["long_text"] = ""
        return GetStoredProcResponse.from_dict(sp)

    def get_deployment_info(self) -> RunningDeploymentInfo:
        return RunningDeploymentInfo.from_dict(self._deployment.get_deployment_info())

    def get_deployment_status(self) -> RunningDeploymentStatus:
        return RunningDeploymentStatus.from_dict(
            self._deployment.get_deployment_status()
        )

    def get_deployment_resource_usage(self) -> GetResourceUsageResponse:
        return GetResourceUsageResponse.from_dict(self._deployment.get_resource_usage())

    def get_deployment_pod_log(
        self, pod_name: str, component: str, from_cache: bool
    ) -> GetPodLogResponse:
        return GetPodLogResponse.from_dict(
            {"log": self._deployment.fetch_pod_log(component, pod_name, from_cache)}
        )

    def get_service_status_by_id(self, graph_id: str) -> ServiceStatus:
        status = self._client.list_service_status()
        for s in status:
            if graph_id == s["graph_id"]:
                return ServiceStatus.from_dict(s)
        raise RuntimeError(f"Failed to get service: graph {graph_id} not exists.")

    def list_service_status(self) -> List[ServiceStatus]:
        status = self._client.list_service_status()
        return [ServiceStatus.from_dict(s) for s in status]

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

    def delete_job_by_id(self, job_id: str, delete_scheduler: bool) -> str:
        return self._client.delete_job_by_id(job_id, delete_scheduler)

    def submit_dataloading_job(
        self, graph_id: str, dataloading_job_config: DataloadingJobConfig
    ) -> str:
        config = dataloading_job_config.to_dict()
        job_id = self._client.submit_dataloading_job(
            graph_id, config, self._datasource_manager
        )
        return CreateDataloadingJobResponse.from_dict({"job_id": job_id})

    def get_dataloading_job_config(
        self, graph_id: str, dataloading_job_config: DataloadingJobConfig
    ) -> DataloadingMRJobConfig:
        config = dataloading_job_config.to_dict()
        return DataloadingMRJobConfig.from_dict(
            self._client.get_dataloading_job_config(
                graph_id, config, self._datasource_manager
            )
        )

    def upload_file(self, filestorage) -> str:
        filepath = os.path.join(DATASET_WORKSPACE, filestorage.filename)
        filestorage.save(filepath)
        metadata = parse_file_metadata(filepath)
        return UploadFileResponse.from_dict(
            {"file_path": filepath, "metadata": metadata}
        )

    def bind_datasource_in_batch(
        self, graph_id: str, schema_mapping: SchemaMapping
    ) -> str:
        # there are some tricks here, since property is a keyword of openapi
        # specification, so it will be converted into the _property field.
        schema_mapping_dict = schema_mapping.to_dict()
        for mapping in itertools.chain(
            schema_mapping_dict["vertex_mappings"],
            schema_mapping_dict["edge_mappings"],
        ):
            if "column_mappings" in mapping and mapping["column_mappings"] is not None:
                for column_mapping in mapping["column_mappings"]:
                    if "_property" in column_mapping:
                        column_mapping["property"] = column_mapping.pop("_property")
            if (
                "source_vertex_mappings" in mapping
                and "destination_vertex_mappings" in mapping
                and mapping["source_vertex_mappings"] is not None
                and mapping["destination_vertex_mappings"] is not None
            ):
                for column_mapping in itertools.chain(
                    mapping["source_vertex_mappings"],
                    mapping["destination_vertex_mappings"],
                ):
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

    def get_storage_usage(self) -> GetStorageUsageResponse:
        return GetStorageUsageResponse.from_dict(
            {"storage_usage": self._client.get_storage_usage()}
        )

    def gremlin_service_available(self) -> bool:
        return self._client.gremlin_service_available()


client_wrapper = ClientWrapper()
