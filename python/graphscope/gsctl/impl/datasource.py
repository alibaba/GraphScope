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

import itertools
import os

import graphscope.flex.rest
from graphscope.flex.rest import SchemaMapping
from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.impl.utils import upload_file


def bind_datasource_in_batch(graph_identifier: str, datasource: dict) -> str:
    # upload files
    for mapping in itertools.chain(
        datasource["vertex_mappings"], datasource["edge_mappings"]
    ):
        for index, location in enumerate(mapping["inputs"]):
            # path begin with "@" represents the local file
            if location.startswith("@"):
                location = location[1:]
                mapping["inputs"][index] = upload_file(location)
    # bind data source
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DataSourceApi(api_client)
        return api_instance.bind_datasource_in_batch(
            graph_identifier, SchemaMapping.from_dict(datasource)
        )


def get_datasource_by_id(graph_identifier: str) -> SchemaMapping:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DataSourceApi(api_client)
        return api_instance.get_datasource_by_id(graph_identifier)


def unbind_vertex_datasource(graph_identifier: str, vertex_type: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DataSourceApi(api_client)
        return api_instance.unbind_vertex_datasource(graph_identifier, vertex_type)


def unbind_edge_datasource(
    graph_identifier: str,
    vertex_type: str,
    source_vertex_type: str,
    destination_vertex_type: str,
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DataSourceApi(api_client)
        return api_instance.unbind_edge_datasource(
            graph_identifier, vertex_type, source_vertex_type, destination_vertex_type
        )
