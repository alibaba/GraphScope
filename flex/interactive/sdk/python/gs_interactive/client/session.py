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

from abc import ABCMeta
from abc import abstractmethod
from typing import Any
from typing import List
from typing import Optional
from typing import Union

from gs_interactive.api import AdminServiceGraphManagementApi
from gs_interactive.api import AdminServiceJobManagementApi
from gs_interactive.api import AdminServiceProcedureManagementApi
from gs_interactive.api import AdminServiceServiceManagementApi
from gs_interactive.api import GraphServiceEdgeManagementApi
from gs_interactive.api import GraphServiceVertexManagementApi
from gs_interactive.api import QueryServiceApi
from gs_interactive.api import UtilsApi
from gs_interactive.api_client import ApiClient
from gs_interactive.configuration import Configuration
from pydantic import Field
from pydantic import StrictBytes
from pydantic import StrictStr
from typing_extensions import Annotated

from gs_interactive.client.generated.results_pb2 import CollectiveResults
from gs_interactive.client.result import Result
from gs_interactive.client.status import Status
from gs_interactive.client.status import StatusCode
from gs_interactive.client.utils import InputFormat
from gs_interactive.client.utils import append_format_byte
from gs_interactive.models import CreateGraphRequest
from gs_interactive.models import CreateGraphResponse
from gs_interactive.models import CreateProcedureRequest
from gs_interactive.models import CreateProcedureResponse
from gs_interactive.models import EdgeRequest
from gs_interactive.models import GetGraphResponse
from gs_interactive.models import GetGraphSchemaResponse
from gs_interactive.models import GetGraphStatisticsResponse
from gs_interactive.models import GetProcedureResponse
from gs_interactive.models import JobResponse
from gs_interactive.models import JobStatus
from gs_interactive.models import QueryRequest
from gs_interactive.models import SchemaMapping
from gs_interactive.models import ServiceStatus
from gs_interactive.models import StartServiceRequest
from gs_interactive.models import StopServiceRequest
from gs_interactive.models import UpdateProcedureRequest
from gs_interactive.models import UploadFileResponse
from gs_interactive.models import VertexData
from gs_interactive.models import VertexEdgeRequest
from gs_interactive.models import VertexRequest


class EdgeInterface(metaclass=ABCMeta):
    @abstractmethod
    def add_edge(
        self, graph_id: StrictStr, edge_request: List[EdgeRequest]
    ) -> Result[str]:
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
        edge_label: Annotated[StrictStr, Field(description="The label name of edge.")],
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
        self,
        graph_id: StrictStr,
        vertex_edge_request: VertexEdgeRequest,
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
    ) -> Result[VertexData]:
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
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphSchemaResponse]:
        raise NotImplementedError

    @abstractmethod
    def get_graph_meta(
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphResponse]:
        raise NotImplementedError

    @abstractmethod
    def get_graph_statistics(
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphStatisticsResponse]:
        raise NotImplementedError

    @abstractmethod
    def delete_graph(
        graph_id: Annotated[StrictStr, Field(description="The id of graph to delete")],
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
        self, graph_id: StrictStr, proc_id: StrictStr, procedure: UpdateProcedureRequest
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
    def call_procedure_current(self, params: QueryRequest) -> Result[CollectiveResults]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure_raw(self, graph_id: StrictStr, params: bytes) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure_current_raw(self, params: bytes) -> Result[str]:
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
    def stop_service(self, graph_id: str) -> Result[str]:
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
    def upload_file(
        self, filestorage: Optional[Union[StrictBytes, StrictStr]]
    ) -> Result[UploadFileResponse]:
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
    """
    The default session implementation for Interactive SDK.
    It provides the implementation of all service APIs.
    """

    def __init__(self, admin_uri: str, stored_proc_uri: str = None):
        """
        Construct a new session using the specified admin_uri and stored_proc_uri.

        Args:
            admin_uri (str): the uri for the admin service.
            stored_proc_uri (str, optional): the uri for the stored procedure service.
                If not provided,the uri will be read from the service status.
        """
        self._client = ApiClient(Configuration(host=admin_uri))

        self._graph_api = AdminServiceGraphManagementApi(self._client)
        self._job_api = AdminServiceJobManagementApi(self._client)
        self._procedure_api = AdminServiceProcedureManagementApi(self._client)
        self._service_api = AdminServiceServiceManagementApi(self._client)
        self._utils_api = UtilsApi(self._client)
        if stored_proc_uri is None:
            service_status = self.get_service_status()
            if not service_status.is_ok():
                raise Exception(
                    "Failed to get service status: ",
                    service_status.get_status_message(),
                )
            service_port = service_status.get_value().hqps_port
            # replace the port in uri
            splitted = admin_uri.split(":")
            splitted[-1] = str(service_port)
            stored_proc_uri = ":".join(splitted)
        self._query_client = ApiClient(Configuration(host=stored_proc_uri))
        self._query_api = QueryServiceApi(self._query_client)
        self._edge_api = GraphServiceEdgeManagementApi(self._query_client)
        self._vertex_api = GraphServiceVertexManagementApi(self._query_client)

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type=exc_type, exc_value=exc_val, traceback=exc_tb)

    # implementations of the methods from the interfaces
    def add_vertex(
        self,
        graph_id: StrictStr,
        vertex_edge_request: VertexEdgeRequest,
    ) -> Result[StrictStr]:
        """
        Add a vertex to the specified graph.
        """
        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._vertex_api.add_vertex_with_http_info(
                graph_id, vertex_edge_request
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

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
    ) -> Result[VertexData]:
        """
        Get a vertex from the specified graph with primary key value.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._vertex_api.get_vertex_with_http_info(
                graph_id, label, primary_key_value
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

    def update_vertex(
        self, graph_id: StrictStr, vertex_request: VertexRequest
    ) -> Result[str]:
        """
        Update a vertex in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._vertex_api.update_vertex_with_http_info(
                graph_id, vertex_request
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

    def add_edge(
        self, graph_id: StrictStr, edge_request: List[EdgeRequest]
    ) -> Result[str]:
        """
        Add an edge to the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._edge_api.add_edge_with_http_info(
                graph_id, edge_request
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

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
        edge_label: Annotated[StrictStr, Field(description="The label name of edge.")],
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
        """
        Get an edge from the specified graph with primary key value.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._edge_api.get_edge_with_http_info(
                graph_id,
                edge_label,
                src_label,
                src_primary_key_value,
                dst_label,
                dst_primary_key_value,
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

    def update_edge(
        self, graph_id: StrictStr, edge_request: EdgeRequest
    ) -> Result[str]:
        """
        Update an edge in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            api_response = self._edge_api.update_edge_with_http_info(
                graph_id, edge_request
            )
            return Result.from_response(api_response)
        except Exception as e:
            return Result.from_exception(e)

    def create_graph(self, graph: CreateGraphRequest) -> Result[CreateGraphResponse]:
        """
        Create a new graph with the specified graph request.
        """

        try:
            response = self._graph_api.create_graph_with_http_info(graph)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_graph_schema(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphSchemaResponse]:
        """Get the schema of a specified graph.

        Parameters:
            graph_id (str): The ID of the graph whose schema is to be retrieved.
        Returns:
            Result[GetGraphSchemaResponse]: The result containing the schema of
                the specified graph.
        """
        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._graph_api.get_schema_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_graph_meta(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphResponse]:
        """
        Get the meta information of a specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._graph_api.get_graph_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_graph_statistics(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to get")],
    ) -> Result[GetGraphStatisticsResponse]:
        """
        Get the statistics of a specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._graph_api.get_graph_statistic_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def delete_graph(
        self,
        graph_id: Annotated[StrictStr, Field(description="The id of graph to delete")],
    ) -> Result[str]:
        """
        Delete a graph with the specified graph id.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._graph_api.delete_graph_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def list_graphs(self) -> Result[List[GetGraphResponse]]:
        """
        List all graphs.
        """

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
        """
        Submit a bulk loading job to the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
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

    def create_procedure(
        self, graph_id: StrictStr, procedure: CreateProcedureRequest
    ) -> Result[CreateProcedureResponse]:
        """
        Create a new procedure in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
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
        """
        Delete a procedure in the specified graph.
        """
        graph_id = self.ensure_param_str("graph_id", graph_id)
        procedure_id = self.ensure_param_str("procedure_id", procedure_id)
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
        """
        List all procedures in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._procedure_api.list_procedures_with_http_info(graph_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def update_procedure(
        self, graph_id: StrictStr, proc_id: StrictStr, procedure: UpdateProcedureRequest
    ) -> Result[str]:
        """
        Update a procedure in the specified graph.
        """
        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            response = self._procedure_api.update_procedure_with_http_info(
                graph_id, proc_id, procedure
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_procedure(
        self, graph_id: StrictStr, procedure_id: StrictStr
    ) -> Result[GetProcedureResponse]:
        """
        Get a procedure in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
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
        """
        Call a procedure in the specified graph.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            # gs_interactive currently support four type of inputformat,
            # see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in json format
            response = self._query_api.call_proc_with_http_info(
                graph_id=graph_id,
                body=append_format_byte(
                    params.to_json().encode(), InputFormat.CYPHER_JSON
                ),
            )
            result = CollectiveResults()
            if response.status_code == 200:
                result.ParseFromString(response.data)
                return Result.ok(result)
            else:
                return Result(Status.from_response(response), result)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure_current(self, params: QueryRequest) -> Result[CollectiveResults]:
        """
        Call a procedure in the current graph.
        """

        try:
            # gs_interactive currently support four type of inputformat,
            # see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in json format
            response = self._query_api.call_proc_current_with_http_info(
                body=append_format_byte(
                    params.to_json().encode(), InputFormat.CYPHER_JSON
                )
            )
            result = CollectiveResults()
            if response.status_code == 200:
                result.ParseFromString(response.data)
                return Result.ok(result)
            else:
                return Result(Status.from_response(response), result)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure_raw(self, graph_id: StrictStr, params: bytes) -> Result[str]:
        """
        Call a procedure in the specified graph with raw bytes.
        """

        graph_id = self.ensure_param_str("graph_id", graph_id)
        try:
            # gs_interactive currently support four type of inputformat,
            # see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in encoder/decoder format
            response = self._query_api.call_proc_with_http_info(
                graph_id=graph_id,
                body=append_format_byte(params, InputFormat.CPP_ENCODER),
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def call_procedure_current_raw(self, params: bytes) -> Result[str]:
        """
        Call a procedure in the current graph with raw bytes.
        """

        try:
            # gs_interactive currently support four type of inputformat,
            # see flex/engines/graph_db/graph_db_session.h
            # Here we add byte of value 1 to denote the input format is in encoder/decoder format
            response = self._query_api.call_proc_current_with_http_info(
                body=append_format_byte(params, InputFormat.CPP_ENCODER)
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_service_status(self) -> Result[ServiceStatus]:
        """
        Get the status of the service.
        """

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
        """
        Start the service on a specified graph.
        """

        try:
            response = self._service_api.start_service_with_http_info(
                start_service_request
            )
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def stop_service(self, graph_id: str = None) -> Result[str]:
        """
        Stop the service.
        """

        try:
            req = StopServiceRequest()
            if graph_id:
                req.graph_id = graph_id
            response = self._service_api.stop_service_with_http_info(req)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def restart_service(self) -> Result[str]:
        """
        Restart the service.
        """

        try:
            response = self._service_api.restart_service_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def get_job(self, job_id: StrictStr) -> Result[JobStatus]:
        """
        Get the status of a job with the specified job id.
        """

        job_id = self.ensure_param_str("job_id", job_id)
        try:
            response = self._job_api.get_job_by_id_with_http_info(job_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def list_jobs(self) -> Result[List[JobResponse]]:
        """
        List all jobs.
        """

        try:
            response = self._job_api.list_jobs_with_http_info()
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def cancel_job(self, job_id: StrictStr) -> Result[str]:
        """
        Cancel a job with the specified job id.
        """

        job_id = self.ensure_param_str("job_id", job_id)
        try:
            response = self._job_api.delete_job_by_id_with_http_info(job_id)
            return Result.from_response(response)
        except Exception as e:
            return Result.from_exception(e)

    def upload_file(
        self, filestorage: Optional[Union[StrictBytes, StrictStr]]
    ) -> Result[UploadFileResponse]:
        """
        Upload a file to the server.
        """

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
        return path[1:] if path.startswith("@") else path

    def preprocess_inputs(
        self, location: str, inputs: List[str], schema_mapping: SchemaMapping
    ):
        root_dir_marked_with_at = False
        if location and location.startswith("@"):
            root_dir_marked_with_at = True
        new_inputs = []
        for i, input in enumerate(inputs):
            # First check whether input is valid
            if location and not root_dir_marked_with_at:
                if input.startswith("@"):
                    print(
                        "Root location given without @, but the input file starts with @"
                        + input
                        + ", index: "
                        + str(i),
                    )
                    return Result.error(
                        Status(
                            StatusCode.BAD_REQUEST,
                            "Root location given without @, but the input file starts with @"
                            + input
                            + ", index: "
                            + str(i),
                        ),
                        new_inputs,
                    )
            if location:
                new_inputs.append(location + "/" + self.trim_path(input))
            else:
                new_inputs.append(input)
        return Result.ok(new_inputs)

    def check_file_mixup(self, schema_mapping: SchemaMapping) -> Result[SchemaMapping]:
        location = None
        if schema_mapping.loading_config and schema_mapping.loading_config.data_source:
            if schema_mapping.loading_config.data_source.scheme != "file":
                print("Only check mixup for file scheme")
                return Result.ok(schema_mapping)
            location = schema_mapping.loading_config.data_source.location

        extracted_files = []
        if schema_mapping.vertex_mappings:
            for vertex_mapping in schema_mapping.vertex_mappings:
                if vertex_mapping.inputs:
                    preprocess_result = self.preprocess_inputs(
                        location, vertex_mapping.inputs, schema_mapping
                    )
                    if not preprocess_result.is_ok():
                        return Result.error(preprocess_result.status, schema_mapping)
                    vertex_mapping.inputs = preprocess_result.get_value()
                    extracted_files.extend(vertex_mapping.inputs)
        if schema_mapping.edge_mappings:
            for edge_mapping in schema_mapping.edge_mappings:
                if edge_mapping.inputs:
                    preprocess_result = self.preprocess_inputs(
                        location, edge_mapping.inputs, schema_mapping
                    )
                    if not preprocess_result.is_ok():
                        return Result.error(preprocess_result.status, schema_mapping)
                    edge_mapping.inputs = preprocess_result.get_value()
                    extracted_files.extend(edge_mapping.inputs)
        if extracted_files:
            # count the number of files start with @
            count = 0
            for file in extracted_files:
                if file.startswith("@"):
                    count += 1
            if count == 0:
                print("No file to upload")
                return Result.ok(schema_mapping)
            elif count != len(extracted_files):
                print("Can not mix uploading file and not uploading file")
                return Result.error("Can not mix uploading file and not uploading file")
        return Result.ok(schema_mapping)

    def upload_and_replace_input_inplace(
        self, schema_mapping: SchemaMapping
    ) -> Result[SchemaMapping]:
        """
        For each input file in schema_mapping, if the file starts with @,
        upload the file to the server, and replace the path with the
        path returned from the server.
        """
        if schema_mapping.vertex_mappings:
            for vertex_mapping in schema_mapping.vertex_mappings:
                if vertex_mapping.inputs:
                    for i, input in enumerate(vertex_mapping.inputs):
                        if input.startswith("@"):
                            res = self.upload_file(input[1:])
                            if not res.is_ok():
                                return Result.error(res.status, schema_mapping)
                            vertex_mapping.inputs[i] = res.get_value().file_path
        if schema_mapping.edge_mappings:
            for edge_mapping in schema_mapping.edge_mappings:
                if edge_mapping.inputs:
                    for i, input in enumerate(edge_mapping.inputs):
                        if input.startswith("@"):
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

        The @ can be added to the beginning of data_source.location
        in schema_mapping.loading_config,or added to each file in vertex_mappings
        and edge_mappings.

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
        Among the above 4 cases, only the 1, 3, 5 case are valid,
        for 2,4 the file will not be uploaded
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

    def ensure_param_str(self, param_name: str, param):
        """
        Ensure the param is a string, otherwise raise an exception
        """
        if not isinstance(param, str):
            # User may input the graph_id as int, convert it to string
            if isinstance(param, int):
                return str(param)
            raise Exception(
                "param should be a string, param_name: "
                + param_name
                + ", param: "
                + str(param)
            )
        return param
