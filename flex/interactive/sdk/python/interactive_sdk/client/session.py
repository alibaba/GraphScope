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
from dataclasses import Field
from typing import Annotated, Any, List
from typing import Optional
from typing import Union
from flex.coordinator.gs_flex_coordinator.models.service_status import ServiceStatus
from flex.interactive.sdk.python.interactive_sdk.client.common.result import Result
from flex.interactive.sdk.python.interactive_sdk.models.graph_schema import GraphSchema
from flex.interactive.sdk.python.interactive_sdk.models.edge_request import EdgeRequest
from flex.interactive.sdk.python.interactive_sdk.models.graph import Graph
from flex.interactive.sdk.python.interactive_sdk.models.job_response import JobResponse
from flex.interactive.sdk.python.interactive_sdk.models.procedure import Procedure
from flex.interactive.sdk.python.interactive_sdk.models.schema_mapping import (
    SchemaMapping,
)
from flex.interactive.sdk.python.interactive_sdk.models.service import Service
from flex.interactive.sdk.python.interactive_sdk.models.vertex_request import (
    VertexRequest,
)
from pydantic import StrictStr


class EdgeInterface(metaclass=ABCMeta):
    @abstractmethod
    def add_edge(self, graph_name: StrictStr, edge_request: EdgeRequest) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def delete_edge(
        self,
        graph_name: StrictStr,
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
        graph_name: StrictStr,
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
        self, graph_name: StrictStr, edge_request: EdgeRequest
    ) -> Result[str]:
        raise NotImplementedError


class VertexInterface(metaclass=ABCMeta):
    @abstractmethod
    def add_vertex(
        self, graph_name: StrictStr, vertex_request: VertexRequest
    ) -> Result[StrictStr]:
        raise NotImplementedError

    @abstractmethod
    def delete_vertex(
        self,
        graph_name: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_vertex(
        self,
        graph_name: StrictStr,
        label: Annotated[StrictStr, Field(description="The label name of vertex.")],
        primary_key_value: Annotated[
            Any, Field(description="The primary key value of vertex.")
        ],
    ) -> Result[VertexRequest]:
        raise NotImplementedError

    @abstractmethod
    def update_vertex(
        self, graph_name: StrictStr, vertex_request: VertexRequest
    ) -> Result[str]:
        raise NotImplementedError


class GraphInterface(metaclass=ABCMeta):
    @abstractmethod
    def create_graph(self, graph: Graph) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_schema(
        graph_name: Annotated[
            StrictStr, Field(description="The name of graph to delete")
        ],
    ) -> Result[GraphSchema]:
        raise NotImplementedError

    @abstractmethod
    def delete_graph(
        graph_name: Annotated[
            StrictStr, Field(description="The name of graph to delete")
        ],
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def list_graphs(self) -> Result[List[Graph]]:
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
        self, graph_name: StrictStr, procedure: Procedure
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def delete_procedure(
        self, graph_name: StrictStr, procedure_name: StrictStr
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def list_procedures(self, graph_name: StrictStr) -> Result[List[Procedure]]:
        raise NotImplementedError

    @abstractmethod
    def update_procedure(
        self, graph_name: StrictStr, procedure: Procedure
    ) -> Result[str]:
        raise NotImplementedError

    @abstractmethod
    def get_procedure(
        self, graph_name: StrictStr, procedure_name: StrictStr
    ) -> Result[Procedure]:
        raise NotImplementedError

    @abstractmethod
    def call_procedure(
        self, graph_name: StrictStr, procedure_name: StrictStr, params: Dict[str, Any]
    ) -> Result[str]:
        raise NotImplementedError


class QueryServiceInterface:
    @abstractmethod
    def get_service_status(self) -> Result[ServiceStatus]:
        raise NotImplementedError

    @abstractmethod
    def start_service(
        self,
        service: Annotated[
            Optional[Service], Field(description="Start service on a specified graph")
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
    def get_job(self, job_id: StrictStr) -> Result[JobResponse]:
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
    def __init__(self) -> None:
        pass

    # implementations of the methods from the interfaces
    ################ Vertex Interfaces ##########
    def add_vertex(
        self, graph_name: StrictStr, vertex_request: VertexRequest
    ) -> Result[StrictStr]:
        pass

    ################ Edge Interfaces ##########
    def add_edge(self, graph_name: StrictStr, edge_request: EdgeRequest) -> Result[str]:
        pass

    ################ Graph Interfaces ##########
    def create_graph(self, graph: Graph) -> Result[str]:
        pass

    ################ Procedure Interfaces ##########
    def create_procedure(
        self, graph_name: StrictStr, procedure: Procedure
    ) -> Result[str]:
        pass

    ################ QueryService Interfaces ##########
    def get_service_status(self) -> Result[ServiceStatus]:
        pass

    ################ Job Interfaces ##########
    def get_job(self, job_id: StrictStr) -> Result[JobResponse]:
        pass

    # ...
    def list_jobs(self) -> Result[List[JobResponse]]:
        pass

    # ...
    def cancel_job(self, job_id: StrictStr) -> Result[str]:
        pass
