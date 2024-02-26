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

import logging
from typing import List

from gs_flex_coordinator.core.config import CLUSTER_TYPE
from gs_flex_coordinator.core.insight.graph import get_groot_graph

logger = logging.getLogger("graphscope")


class GrootClient(object):
    """Class used to interact with Groot"""

    def __init__(self):
        self._graph = get_groot_graph()
        print(self._graph.to_dict())

    def list_groot_graph(self) -> list:
        rlts = [self._graph.to_dict()]
        return rlts

    def create_vertex_type(self, graph_name: str, vtype_dict: dict) -> str:
        return self._graph.create_vertex_type(vtype_dict)

    def create_edge_type(self, graph_name: str, etype_dict: dict) -> str:
        return self._graph.create_edge_type(etype_dict)

    def delete_vertex_type(self, graph_name: str, vertex_type: str) -> str:
        return self._graph.delete_vertex_type(graph_name, vertex_type)

    def delete_edge_type(
        self,
        graph_name: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        return self._graph.delete_edge_type(
            graph_name, edge_type, source_vertex_type, destination_vertex_type
        )

    def get_groot_schema(self, graph_name: str) -> dict:
        if graph_name == self._graph.name:
            raise RuntimeError(f"Graph {graph_name} not exists")
        return self._graph.schema

    def import_groot_schema(self, graph_name: str, schema: dict) -> str:
        return self._graph.import_schema(schema)

    def list_jobs(self) -> List[dict]:
        return []


def init_groot_client():
    return GrootClient()
