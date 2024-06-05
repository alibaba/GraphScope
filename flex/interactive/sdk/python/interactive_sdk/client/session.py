#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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

from abc import ABCMeta, abstractmethod
from typing import Annotated, Any, Dict, List, Optional, Union

from pydantic import Field, StrictStr

from interactive_sdk.client.status import Status
from interactive_sdk.openapi.api.admin_service_graph_management_api import (
    AdminServiceGraphManagementApi,
)
from interactive_sdk.openapi.api.admin_service_job_management_api import (
    AdminServiceJobManagementApi,
)
from interactive_sdk.openapi.api.admin_service_procedure_management_api import (
    AdminServiceProcedureManagementApi,
)
from interactive_sdk.openapi.api.admin_service_service_management_api import (
    AdminServiceServiceManagementApi,
)
from interactive_sdk.openapi.api.graph_service_edge_management_api import (
    GraphServiceEdgeManagementApi,
)
from interactive_sdk.openapi.api.graph_service_vertex_management_api import (
    GraphServiceVertexManagementApi,
)
from interactive_sdk.openapi.api.query_service_api import QueryServiceApi
from interactive_sdk.openapi.api_client import ApiClient
from interactive_sdk.client.result import Result
from interactive_sdk.openapi.configuration import Configuration
from interactive_sdk.openapi.models.create_graph_request import CreateGraphRequest
from interactive_sdk.openapi.models.create_graph_response import CreateGraphResponse
from interactive_sdk.openapi.models.create_procedure_request import (
    CreateProcedureRequest,
)
from interactive_sdk.openapi.models.create_procedure_response import (
    CreateProcedureResponse,
)
from interactive_sdk.openapi.models.edge_request import EdgeRequest
from interactive_sdk.openapi.models.get_graph_response import GetGraphResponse
from interactive_sdk.openapi.models.get_graph_schema_response import (
    GetGraphSchemaResponse,
)
from interactive_sdk.openapi.models.get_procedure_response import GetProcedureResponse
from interactive_sdk.openapi.models.job_response import JobResponse
from interactive_sdk.openapi.models.job_status import JobStatus
from interactive_sdk.openapi.models.schema_mapping import SchemaMapping
from interactive_sdk.openapi.models.service_status import ServiceStatus
from interactive_sdk.openapi.models.start_service_request import StartServiceRequest
from interactive_sdk.openapi.models.update_procedure_request import (
    UpdateProcedureRequest,
)
from interactive_sdk.openapi.models.query_request import QueryRequest
from interactive_sdk.openapi.models.vertex_request import VertexRequest
from interactive_sdk.client.generated.results_pb2 import CollectiveResults


class EdgeInterface(metaclass=ABCMeta):
    @abstractmethod
    def add_edge(self, graph_id: StrictStr, edge_request: EdgeRequest) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def delete_edge(
        self,
        graph_id: StrictStr,
        src_label: Annotated[
            StrictStr, Field(description="The label name of src vertex.")
        ],
        src_primary_key_value: Annotated[
            Any, Field(description="The primary key value of src vertex.")
        ],
        dst_label: Annotated[
            StrictStr, Field(description="The label name of dst vertex.")
        ],
        dst_primary_key_value: Annotated[
            Any, Field(description="The primary key value of dst vertex.")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_edge(
        self,
        graph_id: StrictStr,
        src_label: Annotated[
            StrictStr, Field(description="The label name of src vertex.")
        ],
        src_primary_key_value: Annotated[
            Any, Field(description="The primary key value of src vertex.")
        ],
        dst_label: Annotated[
            StrictStr, Field(description="The label name of dst vertex.")
        ],
        dst_primary_key_value: Annotated[
            Any, Field(description="The primary key value of dst vertex.")
        ],
    ) -> Result[Union[None, EdgeRequest]]:
        raise NotImplementedError

    @abstractmethod
    def update_edge(
        self, graph_id: StrictStr, edge_request: EdgeRequest
    ) -> Result[str]:
        raise NotImplementedError


class VertexInterface(metaclass=ABCMeta):
    @abstractmethod
    def add_vertex(
        self, graph_id: StrictStr, vertex_request: VertexRequest
    ) -> Result[StrictStr]:
        raise NotImplementedError

    @abstractmethod
    def delete_vertex(
        self,
        graph_id: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_vertex(
        self,
        graph_id: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[VertexRequest]:
        raise NotImplementedError

    @abstractmethod
    def update_vertex(
        self, graph_id: StrictStr, vertex_request: VertexRequest
    ) -> Result[str]:
        raise NotImplementedError


class GraphInterface(metaclass=ABCMeta):
    @abstractmethod
    def create_graph(self, graph: CreateGraphRequest) -> Result[CreateGraphResponse]:
        raise NotImplementedError

    @abstractmethod
    def get_graph_schema(
        graph_id: Annotated[
            StrictStr, Field(description="The id of graph to get")
        ],
    ) -> Result[GetGraphSchemaResponse]:
        raise NotImplementedError

    @abstractmethod
    def get_graph_meta(
        graph_id: Annotated[
            StrictStr, Field(description="The id of graph to get")
        ],
    ) -> Result[GetGraphResponse]:
        raise NotImplementedError

    @abstractmethod
    def delete_graph(
        graph_id: Annotated[
            StrictStr, Field(description="The id of graph to delete")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def list_graphs(self) -> Result[List[GetGraphResponse]]:
        raise NotImplementedError

    @abstractmethod
    def bulk_loading(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to load")],
        schema_mapping: SchemaMapping,
    ) -> Result[JobResponse]:
        raise NotImplementedError


class ProcedureInterface(metaclass=ABCMeta):
    @abstractmethod
    def create_procedure(
        self, graph_id: StrictStr, procedure: CreateProcedureRequest
    ) -> Result[CreateProcedureResponse]:
        raise NotImplementedError

    @abstractmethod
    def delete_procedure(
        self, graph_id: StrictStr, procedure_id: StrictStr
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def list_procedures(
        self, graph_id: StrictStr
    ) -> Result[List[GetProcedureResponse]]:
        raise NotImplementedError

    @abstractmethod
    def update_procedure(
        self, graph_id: StrictStr, procedure: UpdateProcedureRequest
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_procedure(
        self, graph_id: StrictStr, procedure_id: StrictStr
    ) -> Result[GetProcedureResponse]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure(
        self, graph_id: StrictStr, params: QueryRequest
    ) -> Result[CollectiveResults]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure_current(
        self, params: QueryRequest
    ) -> Result[CollectiveResults]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure_raw(self, graph_id: StrictStr, params: str) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure_current_raw(self, params: str) -> Result[str]:
        raise NotImplementedError

class QueryServiceInterface:
    @abstractmethod
    def get_service_status(self) -> Result[ServiceStatus]:
        raise NotImplementedError

    @abstractmethod
    def start_service(
        self,
        start_service_request: Annotated[
            Optional[StartServiceRequest],
            Field(description="Start service on a specified graph"),
        ] = None,
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def stop_service(self) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def restart_service(self) -> Result[str]:
        raise NotImplementedError


class JobInterface(metaclass=ABCMeta):
    @abstractmethod
    def get_job(self, job_id: StrictStr) -> Result[JobStatus]:
        raise NotImplementedError

    @abstractmethod
    def list_jobs(self) -> Result[List[JobResponse]]:
        raise NotImplementedError

    @abstractmethod
    def cancel_job(self, job_id: StrictStr) -> Result[str]:
        raise NotImplementedError


class Session(
    VertexInterface,
    EdgeInterface,
    GraphInterface,
    ProcedureInterface,
    JobInterface,
    QueryServiceInterface,
):
    pass


class DefaultSession(Session):
    PROTOCOL_FORMAT = "proto"
    JSON_FORMAT = "json"
    ENCODER_FORMAT = "encoder"
    def __init__(self, uri: str):
        self._client = ApiClient(Configuration(host=uri))

        self._graph_api = AdminServiceGraphManagementApi(self._client)
        self._job_api = AdminServiceJobManagementApi(self._client)
        self._procedure_api = AdminServiceProcedureManagementApi(self._client)
        self._service_api = AdminServiceServiceManagementApi(self._client)
        self._edge_api = GraphServiceEdgeManagementApi(self._client)
        self._vertex_api = GraphServiceVertexManagementApi(self._client)
        # TODO(zhanglei): Get endpoint from service, current implementation is adhoc.
        # get service port
        service_status = self.get_service_status()
        if not service_status.is_ok():
            raise Exception("Failed to get service status")
        service_port = service_status.get_value().hqps_port
        # replace the port in uri
        uri = uri.split(":")
        uri[-1] = str(service_port)
        uri = ":".join(uri)
        self._query_client = ApiClient(Configuration(host=uri))

        self._query_api = QueryServiceApi(self._query_client)

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type=exc_type, exc_value=exc_val, traceback=exc_tb)

    # implementations of the methods from the interfaces
    ################ Vertex Interfaces ##########
    def add_vertex(
        self, graph_id: StrictStr, vertex_request: VertexRequest
    ) -> Result[StrictStr]:
        raise NotImplementedError

    def delete_vertex(
        self,
        graph_id: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    def get_vertex(
        self,
        graph_id: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[VertexRequest]:
        raise NotImplementedError

    def update_vertex(
        self, graph_id: StrictStr, vertex_request: VertexRequest
    ) -> Result[str]:
        raise NotImplementedError

    ################ Edge Interfaces ##########
    def add_edge(self, graph_id: StrictStr, edge_request: EdgeRequest) -> Result[str]:
        raise NotImplementedError

    def delete_edge(
        self,
        graph_id: StrictStr,
        src_label: Annotated[
            StrictStr, Field(description="The label name of src vertex.")
        ],
        src_primary_key_value: Annotated[
            Any, Field(description="The primary key value of src vertex.")
        ],
        dst_label: Annotated[
            StrictStr, Field(description="The label name of dst vertex.")
        ],
        dst_primary_key_value: Annotated[
            Any, Field(description="The primary key value of dst vertex.")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    def get_edge(
        self,
        graph_id: StrictStr,
        src_label: Annotated[
            StrictStr, Field(description="The label name of src vertex.")
        ],
        src_primary_key_value: Annotated[
            Any, Field(description="The primary key value of src vertex.")
        ],
        dst_label: Annotated[
            StrictStr, Field(description="The label name of dst vertex.")
        ],
        dst_primary_key_value: Annotated[
            Any, Field(description="The primary key value of dst vertex.")
        ],
    ) -> Result[Union[None, EdgeRequest]]:
        raise NotImplementedError

    def update_edge(
        self, graph_id: StrictStr, edge_request: EdgeRequest
    ) -> Result[str]:
        raise NotImplementedError

    ################ Graph Interfaces ##########
    def create_graph(self, graph: CreateGraphRequest) -> Result[CreateGraphResponse]:
        try:
            response = self._graph_api.create_graph_with_http_info(graph)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_graph_schema(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphSchemaResponse]:
        try:
            response = self._graph_api.get_schema_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_graph_meta(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphResponse]:
        try:
            response = self._graph_api.get_graph_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def delete_graph(
        self,
        graph_id: Annotated[
            StrictStr, Field(description="The id of graph to delete")
        ],
    ) -> Result[str]:
        try:
            response = self._graph_api.delete_graph_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def list_graphs(self) -> Result[List[GetGraphResponse]]:
        try:
            response = self._graph_api.list_graphs_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def bulk_loading(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to load")],
        schema_mapping: SchemaMapping,
    ) -> Result[JobResponse]:
        try:
            response = self._graph_api.create_dataloading_job_with_http_info(
                graph_id, schema_mapping
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    ################ Procedure Interfaces ##########
    def create_procedure(
        self, graph_id: StrictStr, procedure: CreateProcedureRequest
    ) -> Result[CreateProcedureResponse]:
        try:
            response = self._procedure_api.create_procedure_with_http_info(
                graph_id, procedure
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def delete_procedure(
        self, graph_id: StrictStr, procedure_id: StrictStr
    ) -> Result[str]:
        try:
            response = self._procedure_api.delete_procedure_with_http_info(
                graph_id, procedure_id
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def list_procedures(
        self, graph_id: StrictStr
    ) -> Result[List[GetProcedureResponse]]:
        try:
            response = self._procedure_api.list_procedures_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def update_procedure(
        self, graph_id: StrictStr, procedure: UpdateProcedureRequest
    ) -> Result[str]:
        try:
            response = self._procedure_api.update_procedure_with_http_info(
                graph_id, procedure
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_procedure(
        self, graph_id: StrictStr, procedure_id: StrictStr
    ) -> Result[GetProcedureResponse]:
        try:
            response = self._procedure_api.get_procedure_with_http_info(
                graph_id, procedure_id
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure(
        self, graph_id: StrictStr, params: QueryRequest
    ) -> Result[CollectiveResults]:
        try:
            # Interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in json format
            response = self._query_api.proc_call_with_http_info(
                graph_id = graph_id, 
                x_interactive_request_format = self.JSON_FORMAT,
                body=params.to_json()
            )
            result = CollectiveResults()
            if response.status_code == 200:
                result.ParseFromString(response.data)
                return Result.ok(result)
            else:
                return Result(Status.from_response(response), result)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure_current(
        self, params: QueryRequest
    ) -> Result[CollectiveResults]:
        try:
            # Interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in json format
            response = self._query_api.proc_call_current_with_http_info(
                x_interactive_request_format = self.JSON_FORMAT,
                body = params.to_json()
            )
            result = CollectiveResults()
            if response.status_code == 200:
                result.ParseFromString(response.data)
                return Result.ok(result)
            else:
                return Result(Status.from_response(response), result)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure_raw(self, graph_id: StrictStr, params: str) -> Result[str]:
        try:
            # Interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in encoder/decoder format
            response = self._query_api.proc_call_with_http_info(
                graph_id = graph_id, 
                x_interactive_request_format = self.ENCODER_FORMAT, 
                body = params
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)
        
    def call_procedure_current_raw(self, params: str) -> Result[str]:
        try:
            # Interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in encoder/decoder format
            response = self._query_api.proc_call_current_with_http_info(
                x_interactive_request_format = self.ENCODER_FORMAT, 
                body = params
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    ################ QueryService Interfaces ##########
    def get_service_status(self) -> Result[ServiceStatus]:
        try:
            response = self._service_api.get_service_status_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def start_service(
        self,
        start_service_request: Annotated[
            Optional[StartServiceRequest],
            Field(description="Start service on a specified graph"),
        ] = None,
    ) -> Result[str]:
        try:
            response = self._service_api.start_service_with_http_info(
                start_service_request
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def stop_service(self) -> Result[str]:
        try:
            response = self._service_api.stop_service_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def restart_service(self) -> Result[str]:
        try:
            response = self._service_api.restart_service_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    ################ Job Interfaces ##########
    def get_job(self, job_id: StrictStr) -> Result[JobStatus]:
        try:
            response = self._job_api.get_job_by_id_with_http_info(job_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def list_jobs(self) -> Result[List[JobResponse]]:
        try:
            response = self._job_api.list_jobs_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def cancel_job(self, job_id: StrictStr) -> Result[str]:
        try:
            response = self._job_api.delete_job_by_id_with_http_info(job_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)
