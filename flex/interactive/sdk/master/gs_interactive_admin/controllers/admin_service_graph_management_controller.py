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

import logging
from typing import Dict
from typing import Tuple
from typing import Union

import connexion

from gs_interactive_admin import util
from gs_interactive_admin.models.api_response_with_code import (  # noqa: E501
    APIResponseWithCode,
)
from gs_interactive_admin.models.create_edge_type import CreateEdgeType  # noqa: E501
from gs_interactive_admin.models.create_graph_request import (  # noqa: E501
    CreateGraphRequest,
)
from gs_interactive_admin.models.create_graph_response import (  # noqa: E501
    CreateGraphResponse,
)
from gs_interactive_admin.models.create_vertex_type import (  # noqa: E501
    CreateVertexType,
)
from gs_interactive_admin.models.get_graph_response import (  # noqa: E501
    GetGraphResponse,
)
from gs_interactive_admin.models.get_graph_schema_response import (  # noqa: E501
    GetGraphSchemaResponse,
)
from gs_interactive_admin.models.get_graph_statistics_response import (  # noqa: E501
    GetGraphStatisticsResponse,
)
from gs_interactive_admin.models.job_response import JobResponse  # noqa: E501
from gs_interactive_admin.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_interactive_admin.models.snapshot_status import SnapshotStatus  # noqa: E501

logger = logging.getLogger("interactive")


def create_dataloading_job(graph_id, schema_mapping):  # noqa: E501
    """create_dataloading_job

    Create a dataloading job # noqa: E501

    :param graph_id: The id of graph to do bulk loading.
    :type graph_id: str
    :param schema_mapping:
    :type schema_mapping: dict | bytes

    :rtype: Union[JobResponse, Tuple[JobResponse, int], Tuple[JobResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        schema_mapping = SchemaMapping.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def create_edge_type(graph_id, create_edge_type=None):  # noqa: E501
    """create_edge_type

    Create a edge type # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_edge_type:
    :type create_edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_edge_type = CreateEdgeType.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def create_graph(create_graph_request):  # noqa: E501
    """create_graph

    Create a new graph # noqa: E501

    :param create_graph_request:
    :type create_graph_request: dict | bytes

    :rtype: Union[CreateGraphResponse, Tuple[CreateGraphResponse, int], Tuple[CreateGraphResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_graph_request = CreateGraphRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def create_vertex_type(graph_id, create_vertex_type):  # noqa: E501
    """create_vertex_type

    Create a vertex type # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_vertex_type:
    :type create_vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_vertex_type = CreateVertexType.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def delete_edge_type(
    graph_id, type_name, source_vertex_type, destination_vertex_type
):  # noqa: E501
    """delete_edge_type

    Delete an edge type by name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param type_name:
    :type type_name: str
    :param source_vertex_type:
    :type source_vertex_type: str
    :param destination_vertex_type:
    :type destination_vertex_type: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def delete_graph(graph_id):  # noqa: E501
    """delete_graph

    Delete a graph by id # noqa: E501

    :param graph_id: The id of graph to delete
    :type graph_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def delete_vertex_type(graph_id, type_name):  # noqa: E501
    """delete_vertex_type

    Delete a vertex type by name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param type_name:
    :type type_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def get_graph(graph_id):  # noqa: E501
    """get_graph

    Get a graph by name # noqa: E501

    :param graph_id: The id of graph to get
    :type graph_id: str

    :rtype: Union[GetGraphResponse, Tuple[GetGraphResponse, int], Tuple[GetGraphResponse, int, Dict[str, str]]
    """
    return "do some magic!"


def get_graph_statistic(graph_id):  # noqa: E501
    """get_graph_statistic

    Get the statics info of a graph, including number of vertices for each label, number of edges for each label. # noqa: E501

    :param graph_id: The id of graph to get statistics
    :type graph_id: str

    :rtype: Union[GetGraphStatisticsResponse, Tuple[GetGraphStatisticsResponse, int], Tuple[GetGraphStatisticsResponse, int, Dict[str, str]]
    """
    return "do some magic!"


def get_schema(graph_id):  # noqa: E501
    """get_schema

    Get schema by graph id # noqa: E501

    :param graph_id: The id of graph to get schema
    :type graph_id: str

    :rtype: Union[GetGraphSchemaResponse, Tuple[GetGraphSchemaResponse, int], Tuple[GetGraphSchemaResponse, int, Dict[str, str]]
    """
    return "do some magic!"


def get_snapshot_status(graph_id, snapshot_id):  # noqa: E501
    """get_snapshot_status

    Get the status of a snapshot by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param snapshot_id:
    :type snapshot_id: int

    :rtype: Union[SnapshotStatus, Tuple[SnapshotStatus, int], Tuple[SnapshotStatus, int, Dict[str, str]]
    """
    return "do some magic!"


def list_graphs():  # noqa: E501
    """list_graphs

    List all graphs # noqa: E501


    :rtype: Union[List[GetGraphResponse], Tuple[List[GetGraphResponse], int], Tuple[List[GetGraphResponse], int, Dict[str, str]]
    """
    return "do some magic!"


def update_edge_type(graph_id, create_edge_type):  # noqa: E501
    """update_edge_type

    Update an edge type to add more properties # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_edge_type:
    :type create_edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_edge_type = CreateEdgeType.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def update_vertex_type(graph_id, create_vertex_type):  # noqa: E501
    """update_vertex_type

    Update a vertex type to add more properties # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_vertex_type:
    :type create_vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_vertex_type = CreateVertexType.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"
