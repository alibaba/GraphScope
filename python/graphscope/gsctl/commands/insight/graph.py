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

from graphscope.gsctl.impl import create_edge_type
from graphscope.gsctl.impl import create_groot_dataloading_job
from graphscope.gsctl.impl import create_vertex_type
from graphscope.gsctl.impl import delete_edge_type
from graphscope.gsctl.impl import delete_job_by_id
from graphscope.gsctl.impl import delete_vertex_type
from graphscope.gsctl.impl import get_datasource
from graphscope.gsctl.impl import get_job_by_id
from graphscope.gsctl.impl import import_datasource
from graphscope.gsctl.impl import import_groot_schema
from graphscope.gsctl.impl import list_groot_graph
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import unbind_edge_datasource
from graphscope.gsctl.impl import unbind_vertex_datasource
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
def create():
    """Create vertex/edge type, data source, loader job from file"""
    pass


@cli.group()
def delete():
    """Delete vertex/edge type, data source, loader job by identifier"""
    pass


@cli.group()
def desc():
    """Show details of job status by identifier"""
    pass


@cli.command()
def ls():  # noqa: F811
    """Display schema, stored procedure, and job information"""
    tree = TreeDisplay()
    try:
        graphs = list_groot_graph()
        using_graph = graphs[0]
        # schema
        tree.create_graph_node_for_groot(using_graph)
        # data source
        datasource = get_datasource("placeholder")
        tree.create_datasource_node(using_graph, datasource)
        # stored procedure
        tree.create_procedure_node(using_graph, [])
        # job
        jobs = list_jobs()
        tree.create_job_node(using_graph, jobs)
    except Exception as e:
        err(f"Failed to display graph information: {str(e)}")
    else:
        tree.show(using_graph.name)


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def schema(filename):  # noqa: F811
    """Import the schema from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    try:
        schema = read_yaml_file(filename)
        # only one graph supported int groot
        import_groot_schema("placeholder", schema)
    except Exception as e:
        err(f"Failed to import schema: {str(e)}")
    else:
        succ("Import schema successfully.")


@create.command(name="vertex_type")
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
    try:
        vtype = read_yaml_file(filename)
        # only one graph supported in groot
        create_vertex_type("placeholder", vtype)
    except Exception as e:
        err(f"Failed to create vertex type: {str(e)}")
    else:
        succ(f"Create vertex type {vtype['type_name']} successfully.")


@delete.command(name="vertex_type")
@click.argument("vertex_type", required=True)
def vertex_type(vertex_type):  # noqa: F811
    """Delete a vertex type, see identifier with `ls` command"""
    try:
        delete_vertex_type("placeholder", vertex_type)
    except Exception as e:
        err(f"Failed to delete vertex type {vertex_type}: {str(e)}")
    else:
        succ(f"Delete vertex type {vertex_type} successfully.")


@create.command(name="edge_type")
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
    try:
        etype = read_yaml_file(filename)
        # only one graph supported in groot
        create_edge_type("placeholder", etype)
    except Exception as e:
        err(f"Failed to create edge type: {str(e)}")
    else:
        succ(f"Create edge type {etype['type_name']} successfully.")


@delete.command(name="edge_type")
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
    """Delete an edge type, see identifier with `ls` command"""
    try:
        etype_full_name = (
            f"({source_vertex_type})-[{edge_type}]->({destination_vertex_type})"
        )
        delete_edge_type(
            "placeholder", edge_type, source_vertex_type, destination_vertex_type
        )
    except Exception as e:
        err(f"Failed to delete edge type {etype_full_name}: {str(e)}")
    else:
        succ(f"Delete edge type {etype_full_name} successfully.")


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file.",
)
def datasource(filename):  # noqa: F811
    """Bind data source from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    try:
        datasource = read_yaml_file(filename)
        # only one graph supported int groot
        import_datasource("placeholder", datasource)
    except Exception as e:
        err(f"Failed to import data source: {str(e)}")
    else:
        succ("Bind data source successfully.")


@delete.command(name="vertex_source")
@click.argument("vertex_type", required=True)
def vertex_source(vertex_type):  # noqa: F811
    """Unbind the data source on vertex type"""
    try:
        unbind_vertex_datasource("placeholder", vertex_type)
    except Exception as e:
        err(f"Failed to unbind data source on {vertex_type}: {str(e)}")
    else:
        succ(f"Unbind data source on {vertex_type} successfully.")


@delete.command(name="edge_source")
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
def edge_source(edge_type, source_vertex_type, destination_vertex_type):  # noqa: F811
    """Unbind the data source on edge type"""
    try:
        etype_full_name = (
            f"({source_vertex_type})-[{edge_type}]->({destination_vertex_type})"
        )
        unbind_edge_datasource(
            "placeholder", edge_type, source_vertex_type, destination_vertex_type
        )
    except Exception as e:
        err(f"Failed to unbind data source on {etype_full_name}: {str(e)}")
    else:
        succ(f"Unbind data source on {etype_full_name} successfully.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def loaderjob(filename):  # noqa: F811
    """Create a dataloading job from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    try:
        config = read_yaml_file(filename)
        jobid = create_groot_dataloading_job("placeholder", config)
    except Exception as e:
        err(f"Failed to create a job: {str(e)}")
    else:
        succ(f"Create job {jobid} successfully.")


@desc.command()
@click.argument("identifier", required=True)
def job(identifier):  # noqa: F811
    """Show details of job, see identifier with `ls` command"""
    try:
        job = get_job_by_id(identifier)
    except Exception as e:
        err(f"Failed to get job: {str(e)}")
    else:
        info(yaml.dump(job.to_dict()))


@delete.command()
@click.argument("identifier", required=True)
def job(identifier):  # noqa: F811
    """Cancel a job, see identifier with `ls` command"""
    try:
        delete_job_by_id(identifier)
    except Exception as e:
        err(f"Failed to cancel job {identifier}: {str(e)}")
    else:
        succ(f"Cancel job {identifier} successfully.")


if __name__ == "__main__":
    cli()
