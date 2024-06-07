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
from typing import List

import graphscope.flex.rest
from graphscope.flex.rest import DataSource
from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.impl.utils import upload_file


def import_datasource(graph_name: str, datasource: dict) -> str:
    # upload files
    for item in itertools.chain(
        datasource["vertices_datasource"], datasource["edges_datasource"]
    ):
        location = item["location"]
        if location.startswith("@"):
            location = location[1:]
            filename = os.path.basename(location)
            with open(location, "rb") as f:
                content = f.read()
            path_after_uploaded = upload_file(filename, content, location)
            item["location"] = path_after_uploaded

    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DatasourceApi(api_client)
        return api_instance.import_datasource(
            graph_name=graph_name, data_source=DataSource.from_dict(datasource)
        )


def get_datasource(graph_name: str) -> DataSource:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DatasourceApi(api_client)
        return api_instance.get_datasource(graph_name)


def unbind_vertex_datasource(graph_name: str, vertex_type: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DatasourceApi(api_client)
        return api_instance.unbind_vertex_datasource(graph_name, vertex_type)


def unbind_edge_datasource(
    graph_name: str,
    edge_type: str,
    source_vertex_type: str,
    destination_vertex_type: str,
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DatasourceApi(api_client)
        return api_instance.unbind_edge_datasource(
            graph_name, edge_type, source_vertex_type, destination_vertex_type
        )
