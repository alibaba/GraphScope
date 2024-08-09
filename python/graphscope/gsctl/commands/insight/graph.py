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

import click
import yaml

from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.impl import create_edge_type
from graphscope.gsctl.impl import create_vertex_type
from graphscope.gsctl.impl import delete_edge_type_by_name
from graphscope.gsctl.impl import delete_vertex_type_by_name
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import import_schema
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import switch_context
from graphscope.gsctl.utils import TreeDisplay
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import succ
from graphscope.gsctl.utils import terminal_display


@click.group()
def cli():
    pass


@cli.group()
def use():
    """Switch back to the global scope"""
    pass


@cli.group()
def delete():
    """Delete vertex or edge type by name"""
    pass


@cli.group()
def create():
    """Create vertex or edge type from file"""
    pass


@cli.command()
def ls():  # noqa: F811
    """Display schema and data source mapping information"""
    tree = TreeDisplay()
    # context
    current_context = get_current_context()
    try:
        graphs = list_graphs()
        using_graph = None
        for g in graphs:
            if g.id == current_context.context:
                using_graph = g
                break
        # schema
        tree.create_graph_node(using_graph)
        # data source mapping
        datasource_mapping = get_datasource_by_id(using_graph.id)
        tree.create_datasource_mapping_node(using_graph, datasource_mapping)
    except Exception as e:
        err(f"Failed to display graph information: {str(e)}")
    else:
        tree.show(graph_identifier=current_context.context)


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def schema(filename):  # noqa: F811
    """Import schema from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        schema = read_yaml_file(filename)
        import_schema(graph_identifier, schema)
    except Exception as e:
        err(f"Failed to import schema: {str(e)}")
    else:
        succ("Import schema successfully.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def vertex_type(filename):  # noqa: F811
    """Create a vertex type from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        vtype = read_yaml_file(filename)
        create_vertex_type(graph_identifier, vtype)
    except Exception as e:
        err(f"Failed to create vertex type: {str(e)}")
    else:
        succ(f"Create vertex type {vtype['type_name']} successfully.")


@delete.command()
@click.argument("vertex_type", required=True)
def vertex_type(vertex_type):  # noqa: F811
    """Delete a vertex type by name"""
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        delete_vertex_type_by_name(graph_identifier, vertex_type)
    except Exception as e:
        err(f"Failed to delete vertex type {vertex_type}: {str(e)}")
    else:
        succ(f"Delete vertex type {vertex_type} successfully.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def edge_type(filename):  # noqa: F811
    """Create an edge type from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        etype = read_yaml_file(filename)
        create_edge_type(graph_identifier, etype)
    except Exception as e:
        err(f"Failed to create edge type: {str(e)}")
    else:
        succ(f"Create edge type {etype['type_name']} successfully.")


@delete.command()
@click.argument("edge_type", required=True)
@click.option(
    "-s",
    "--source_vertex_type",
    required=True,
    help="Source vertex type of the edge",
)
@click.option(
    "-d",
    "--destination_vertex_type",
    required=True,
    help="Destination vertex type of the edge",
)
def edge_type(edge_type, source_vertex_type, destination_vertex_type):  # noqa: F811
    """Delete an edge type by name"""
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        etype_full_name = (
            f"({source_vertex_type})-[{edge_type}]->({destination_vertex_type})"
        )
        delete_edge_type_by_name(
            graph_identifier, edge_type, source_vertex_type, destination_vertex_type
        )
    except Exception as e:
        err(f"Failed to delete edge type {etype_full_name}: {str(e)}")
    else:
        succ(f"Delete edge type {etype_full_name} successfully.")


@use.command(name="GLOBAL")
def _global():
    """Switch back to the global scope"""
    switch_context("global")
    click.secho("Using GLOBAL", fg="green")
