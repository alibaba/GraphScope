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


from pydantic import Field, StrictStr, StrictBytes

from gs_interactive.client.status import Status, StatusCode
from gs_interactive.api.admin_service_graph_management_api import (
    AdminServiceGraphManagementApi,
)
from gs_interactive.api.admin_service_job_management_api import (
    AdminServiceJobManagementApi,
)
from gs_interactive.api.admin_service_procedure_management_api import (
    AdminServiceProcedureManagementApi,
)
from gs_interactive.api.admin_service_service_management_api import (
    AdminServiceServiceManagementApi,
)
from gs_interactive.api.graph_service_edge_management_api import (
    GraphServiceEdgeManagementApi,
)
from gs_interactive.api.graph_service_vertex_management_api import (
    GraphServiceVertexManagementApi,
)
from gs_interactive.api.utils_api import UtilsApi
from gs_interactive.api.query_service_api import QueryServiceApi
from gs_interactive.api_client import ApiClient
from gs_interactive.client.result import Result
from gs_interactive.configuration import Configuration
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.create_graph_response import CreateGraphResponse
from gs_interactive.models.create_procedure_request import (
    CreateProcedureRequest,
)
from gs_interactive.models.create_procedure_response import (
    CreateProcedureResponse,
)
from gs_interactive.models.edge_request import EdgeRequest
from gs_interactive.models.get_graph_response import GetGraphResponse
from gs_interactive.models.get_graph_schema_response import (
    GetGraphSchemaResponse,
)
from gs_interactive.models.get_graph_statistics_response import GetGraphStatisticsResponse
from gs_interactive.models.get_procedure_response import GetProcedureResponse
from gs_interactive.models.job_response import JobResponse
from gs_interactive.models.job_status import JobStatus
from gs_interactive.models.schema_mapping import SchemaMapping
from gs_interactive.models.service_status import ServiceStatus
from gs_interactive.models.start_service_request import StartServiceRequest
from gs_interactive.models.update_procedure_request import (
    UpdateProcedureRequest,
)
from gs_interactive.models.query_request import QueryRequest
from gs_interactive.models.vertex_request import VertexRequest
from gs_interactive.client.generated.results_pb2 import CollectiveResults
from gs_interactive.models.upload_file_response import UploadFileResponse

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
    def get_graph_statistics(
        graph_id: Annotated[
            StrictStr, Field(description="The id of graph to get")
        ],
    ) -> Result[GetGraphStatisticsResponse]:
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

class UiltsInterface(metaclass=ABCMeta):
    @abstractmethod
    def upload_file(self, filestorage: Optional[Union[StrictBytes, StrictStr]]) -> Result[UploadFileResponse]:
        raise NotImplementedError


class Session(
    VertexInterface,
    EdgeInterface,
    GraphInterface,
    ProcedureInterface,
    JobInterface,
    QueryServiceInterface,
    UiltsInterface,
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
        self._utils_api = UtilsApi(self._client)
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
    
    def get_graph_statistics(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphStatisticsResponse]:
        try:
            response = self._graph_api.get_graph_statistic_with_http_info(graph_id)
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
        # First try to upload the input files if they are specified with a starting @
        # return a new schema_mapping with the uploaded files
        upload_res = self.try_upload_files(schema_mapping)
        if not upload_res.is_ok():
            return upload_res
        schema_mapping = upload_res.get_value()
        print("new schema_mapping: ", schema_mapping)
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
            # gs_interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
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
            # gs_interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
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
            # gs_interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
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
            # gs_interactive currently support four type of inputformat, see flex/engines/graph_db/graph_db_session.h
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
    
    def upload_file(self, filestorage: Optional[Union[StrictBytes, StrictStr]]) -> Result[UploadFileResponse]:
        try:
            print("uploading file: ", filestorage)
            response = self._utils_api.upload_file_with_http_info(filestorage)
            print("response: ", response)
            if response.status_code == 200:
                # the response is the path of the uploaded file on server.
                return Result.from_response(response)
            else:
                print("Failed to upload file: ", input)
                return Result.from_response(response)
        except Exception as e:
            print("got exception: ", e)
            return Result.from_exception(e)

    def trim_path(self, path: str) -> str:
        return path[1:] if path.startswith('@') else path
    
    def check_file_mixup(self, schema_mapping: SchemaMapping) -> Result[SchemaMapping]:
        root_dir_marked_with_at = False # Can not mix uploading file and not uploading file
        location=None
        if schema_mapping.loading_config and schema_mapping.loading_config.data_source and schema_mapping.loading_config.data_source.scheme == 'file':
            location = schema_mapping.loading_config.data_source.location
        if location and location.startswith('@'):
            root_dir_marked_with_at = True
        extracted_files = []
        if schema_mapping.vertex_mappings:
            for vertex_mapping in schema_mapping.vertex_mappings:
                if vertex_mapping.inputs:
                    for i, input in enumerate(vertex_mapping.inputs):
                        # First check whether input is valid
                        if location and not root_dir_marked_with_at:
                            if input.startswith('@'):
                                print("Root location given without @, but the input file starts with @" + input)
                                return Result.error(Status(StatusCode.BAD_REQUEST, "Root location given without @, but the input file starts with @" + input), schema_mapping)
                            else:
                                continue
                        if location:
                            vertex_mapping.inputs[i] = location + self.trim_path(input)
                        extracted_files.append(vertex_mapping.inputs[i])
        if schema_mapping.edge_mappings:
            for edge_mapping in schema_mapping.edge_mappings:
                if edge_mapping.inputs:
                    for i, input in enumerate(edge_mapping.inputs):
                        if location and not root_dir_marked_with_at:
                            if input.startswith('@'):
                                print("Root location given without @, but the input file starts with @" + input)
                                return Result.error(Status(StatusCode.BAD_REQUEST, "Root location given without @, but the input file starts with @" + input), schema_mapping)
                            else:
                                continue
                        if location:
                            edge_mapping.inputs[i] = location + self.trim_path(input)
                        extracted_files.append(edge_mapping.inputs[i])
        if extracted_files:
            #count the number of files start with @
            count = 0
            for file in extracted_files:
                if file.startswith('@'):
                    count += 1
            if count == 0:
                print("No file to upload")
                return Result.ok(schema_mapping)
            elif count != len(extracted_files):
                print("Can not mix uploading file and not uploading file")
                return Result.error("Can not mix uploading file and not uploading file")
        return Result.ok(schema_mapping)

    def upload_and_replace_input_inplace(self, schema_mapping: SchemaMapping) -> Result[SchemaMapping]:
        """
        For each input file in schema_mapping, if the file starts with @, upload the file to the server
        and replace the path with the path returned from the server.
        """
        if schema_mapping.vertex_mappings:
            for vertex_mapping in schema_mapping.vertex_mappings:
                if vertex_mapping.inputs:
                    for i, input in enumerate(vertex_mapping.inputs):
                        if input.startswith('@'):
                            res = self.upload_file(input[1:])
                            if not res.is_ok():
                                return Result.error(res.status, schema_mapping)
                            vertex_mapping.inputs[i] = res.get_value().file_path
        if schema_mapping.edge_mappings:
            for edge_mapping in schema_mapping.edge_mappings:
                if edge_mapping.inputs:
                    for i, input in enumerate(edge_mapping.inputs):
                        if input.startswith('@'):
                            res = self.upload_file(input[1:])
                            if not res.is_ok():
                                return Result.error(res.status, schema_mapping)
                            edge_mapping.inputs[i] = res.get_value().file_path
        return Result.ok(schema_mapping)
    
    def try_upload_files(self, schema_mapping: SchemaMapping) -> Result[SchemaMapping]:
        """
        Try to upload the input files if they are specified with a starting @
        for input files in schema_mapping. Replace the path to the uploaded file with the
        path returned from the server.

        The @ can be added to the beginning of data_source.location in schema_mapping.loading_config
        or added to each file in vertex_mappings and edge_mappings.

        1. location: @/path/to/dir
            inputs: 
                - @/path/to/file1
                - @/path/to/file2
        2. location: /path/to/dir
            inputs: 
                - @/path/to/file1
                - @/path/to/file2
        3. location: @/path/to/dir
            inputs: 
                - /path/to/file1
                - /path/to/file2
        4. location: /path/to/dir
            inputs: 
                - /path/to/file1
                - /path/to/file2
        4. location: None
            inputs: 
                - @/path/to/file1
                - @/path/to/file2
        Among the above 4 cases, only the 1, 3, 5 case are valid, for 2,4 the file will not be uploaded
        """
        
        check_mixup_res = self.check_file_mixup(schema_mapping)
        if not check_mixup_res.is_ok():
            return check_mixup_res
        schema_mapping = check_mixup_res.get_value()
        # now try upload the replace inplace
        print("after check_mixup_res: ")
        upload_res = self.upload_and_replace_input_inplace(schema_mapping)
        if not upload_res.is_ok():
            return upload_res
        print("new schema_mapping: ", upload_res.get_value())
        return Result.ok(upload_res.get_value())
        