#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

from typing import List

import graphscope.flex.rest
from graphscope.flex.rest import CreateEdgeType
from graphscope.flex.rest import CreateGraphRequest
from graphscope.flex.rest import CreateGraphSchemaRequest
from graphscope.flex.rest import CreateVertexType
from graphscope.flex.rest import GetGraphResponse
from graphscope.gsctl.config import get_current_context


def import_schema(graph_identifier: str, schema: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.import_schema_by_id(
            graph_identifier, CreateGraphSchemaRequest.from_dict(schema)
        )


def create_vertex_type(graph_identifier: str, vertex_type: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.create_vertex_type(
            graph_identifier, CreateVertexType.from_dict(vertex_type)
        )


def delete_vertex_type_by_name(graph_identifier: str, vertex_type: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.delete_vertex_type_by_name(graph_identifier, vertex_type)


def create_edge_type(graph_identifier: str, edge_type: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.create_edge_type(
            graph_identifier, CreateEdgeType.from_dict(edge_type)
        )


def delete_edge_type_by_name(
    graph_identifier: str,
    edge_type: str,
    source_vertex_type: str,
    destination_vertex_type: str,
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.delete_edge_type_by_name(
            graph_identifier, edge_type, source_vertex_type, destination_vertex_type
        )


def list_graphs() -> List[GetGraphResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        graphs = api_instance.list_graphs()
        return graphs


def get_graph_by_id(graph_identifier: str) -> GetGraphResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.get_graph_by_id(graph_identifier)


def create_graph(graph: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        response = api_instance.create_graph(CreateGraphRequest.from_dict(graph))
        return response.graph_id


def delete_graph_by_id(graph_identifier: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.GraphApi(api_client)
        return api_instance.delete_graph_by_id(graph_identifier)


def get_graph_id_by_name(name_or_id: str):
    graphs = list_graphs()
    id_candidate = []
    for g in graphs:
        if name_or_id == g.id:
            return name_or_id
        if name_or_id == g.name:
            id_candidate.append(g.id)
    if not id_candidate:
        raise RuntimeError(
            f"Graph '{name_or_id}' not exists, see graph information with `ls` command."
        )
    if len(id_candidate) > 1:
        raise RuntimeError(
            f"Found multiple id candidates {id_candidate} for graph {name_or_id}, please choose one."
        )
    return id_candidate[0]


def get_graph_name_by_id(graph_identifier: str):
    graphs = list_graphs()
    for g in graphs:
        if g.id == graph_identifier:
            return g.name
    return graph_identifier
