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
from graphscope.gsctl.impl import (create_edge_type, create_vertex_type,
                                   delete_edge_type, delete_vertex_type,
                                   list_groot_graph)
from graphscope.gsctl.utils import (is_valid_file_path, read_yaml_file,
                                    terminal_display)


@click.group()
def cli():
    pass


@cli.group()
def create():
    """Create a resource from a file"""
    pass


@cli.group()
def delete():
    """Delete a resource by name"""
    pass


@cli.group()
def describe():
    """Show details of a specific resource or group of resources"""
    pass


@cli.group()
def get():
    """Display a specific resource or group of resources"""
    pass


@get.command()
def graph():
    """Display graphs in database"""

    def _construct_and_display_data(graphs):
        if not graphs:
            click.secho("no graph found in database.", fg="blue")
            return
        head = [
            "NAME",
            "CREATION_TIME",
            "VERTEX TYPE SIZE",
            "EDGE TYPE SIZE",
            "GREMLIN ENDPOINT",
            "GRPC ENDPOINT",
        ]
        data = [head]
        for g in graphs:
            data.append(
                [
                    g.name,
                    g.creation_time,
                    len(g.var_schema.vertices),
                    len(g.var_schema.edges),
                    g.gremlin_interface.gremlin_endpoint,
                    g.gremlin_interface.grpc_endpoint,
                ]
            )
        terminal_display(data)

    try:
        graphs = list_groot_graph()
    except Exception as e:
        click.secho(f"Failed to list graphs: {str(e)}", fg="red")
    else:
        _construct_and_display_data(graphs)


@describe.command()
@click.argument("graph_name", required=True)
def graph(graph_name):  # noqa: F811
    """Show details of graphs"""
    try:
        graphs = list_groot_graph()
    except Exception as e:
        click.secho(f"Failed to list graphs: {str(e)}", fg="red")
    else:
        if not graphs:
            click.secho("no graph found in database.", fg="blue")
            return
        specific_graph_exist = False
        for g in graphs:
            if graph_name is not None and g.name != graph_name:
                continue
            # display
            click.secho(yaml.dump(g.to_dict()))
            if graph_name is not None and g.name == graph_name:
                specific_graph_exist = True
                break
        if graph_name is not None and not specific_graph_exist:
            click.secho('graph "{0}" not found.'.format(graph_name), fg="blue")


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create a vertex type",
)
def vtype(filename):  # noqa: F811
    """Create a new vertex type in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        vtype = read_yaml_file(filename)
        # only one graph supported in groot
        create_vertex_type("placeholder", vtype)
    except Exception as e:
        click.secho(f"Failed to create vertex type: {str(e)}", fg="red")
    else:
        click.secho(
            f"Create vertex type {vtype['type_name']} successfully.", fg="green"
        )


@delete.command()
@click.argument("vertex_type", required=True)
def vtype(vertex_type):  # noqa: F811
    """Delete a vertex type in database"""
    try:
        delete_vertex_type("placeholder", vertex_type)
    except Exception as e:
        click.secho(f"Failed to delete vertex type {vertex_type}: {str(e)}", fg="red")
    else:
        click.secho(f"Delete vertex type {vertex_type} successfully.", fg="green")


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
def etype(edge_type, source_vertex_type, destination_vertex_type):  # noqa: F811
    """Delete an edge type in database"""
    try:
        etype_full_name = (
            f"({source_vertex_type})-[{edge_type}]->({destination_vertex_type})"
        )
        delete_edge_type(
            "placeholder", edge_type, source_vertex_type, destination_vertex_type
        )
    except Exception as e:
        click.secho(
            f"Failed to delete edge type {etype_full_name}: {str(e)}",
            fg="red",
        )
    else:
        click.secho(
            f"Delete edge type {etype_full_name} successfully.",
            fg="green",
        )


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create an edge type",
)
def etype(filename):  # noqa: F811
    """Create a new edge type in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        etype = read_yaml_file(filename)
        # only one graph supported in groot
        create_edge_type("placeholder", etype)
    except Exception as e:
        click.secho(f"Failed to create edge type: {str(e)}", fg="red")
    else:
        click.secho(f"Create edge type {etype['type_name']} successfully.", fg="green")


if __name__ == "__main__":
    cli()
