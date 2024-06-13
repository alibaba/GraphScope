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

from graphscope.gsctl.impl import bind_datasource_in_batch
from graphscope.gsctl.impl import create_graph
from graphscope.gsctl.impl import delete_graph_by_id
from graphscope.gsctl.impl import delete_job_by_id
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import get_graph_id_by_name
from graphscope.gsctl.impl import get_graph_name_by_id
from graphscope.gsctl.impl import get_job_by_id
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import list_service_status
from graphscope.gsctl.impl import list_stored_procedures
from graphscope.gsctl.impl import start_service
from graphscope.gsctl.impl import submit_dataloading_job
from graphscope.gsctl.impl import switch_context
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
    """Create graph, data source, loader job from file"""
    pass


@cli.group()
def delete():
    """Delete graph, data source, loader job by id"""
    pass


@cli.group()
def desc():
    """Show job's details by id"""
    pass


@cli.command()
@click.argument(
    "context", type=click.Choice(["GRAPH"], case_sensitive=False), required=True
)
@click.argument("GRAPH_IDENTIFIER", required=True)
def use(context, graph_identifier):
    """Switch to GRAPH context, see identifier with `ls` command"""
    try:
        graph_identifier = get_graph_id_by_name(graph_identifier)
        graph_name = get_graph_name_by_id(graph_identifier)
        status = list_service_status()
        for s in status:
            if s.graph_id == graph_identifier and s.status != "Running":
                info(f"Starting service on graph {graph_identifier}...")
                start_service(graph_identifier)
        switch_context(graph_identifier, graph_name)
    except Exception as e:
        err(f"Failed to switch context: {str(e)}")
    else:
        click.secho(f"Using GRAPH {graph_name}", fg="green")


@cli.command()
@click.option("-l", is_flag=True, help="List sub resources recursively")
def ls(l):  # noqa: F811, E741
    """Display graph resources in database"""
    tree = TreeDisplay()
    try:
        graphs = list_graphs()
        for g in graphs:
            # schema
            tree.create_graph_node(g, recursive=l)
            if l:
                # data source mapping
                datasource_mapping = get_datasource_by_id(g.id)
                tree.create_datasource_mapping_node(g, datasource_mapping)
                # stored procedure
                stored_procedures = list_stored_procedures(g.id)
                tree.create_stored_procedure_node(g, stored_procedures)
                # job
                jobs = list_jobs()
                tree.create_job_node(g, jobs)
    except Exception as e:
        err(f"Failed to list resources: {str(e)}")
    else:
        tree.show()
        if not l:
            info("Run `gsctl ls -l` to list all resources recursively.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def graph(filename):  # noqa: F811
    """Create a new graph in database"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    try:
        graph = read_yaml_file(filename)
        create_graph(graph)
    except Exception as e:
        err(f"Failed to create graph: {str(e)}")
    else:
        succ(f"Create graph {graph['name']} successfully.")


@delete.command()
@click.argument("graph_identifier", required=True)
def graph(graph_identifier):  # noqa: F811
    """Delete a graph, see graph identifier with `ls` command"""
    try:
        delete_graph_by_id(get_graph_id_by_name(graph_identifier))
    except Exception as e:
        err(f"Failed to delete graph {graph_identifier}: {str(e)}")
    else:
        succ(f"Delete graph {graph_identifier} successfully.")


@create.command
@click.option(
    "-g",
    "--graph_identifier",
    required=True,
    help="See graph identifier with `ls` command",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def datasource(graph_identifier, filename):  # noqa: F811
    """Bind data source mapping from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    graph_identifier = get_graph_id_by_name(graph_identifier)
    try:
        datasource = read_yaml_file(filename)
        bind_datasource_in_batch(graph_identifier, datasource)
    except Exception as e:
        err(f"Failed to bind data source: {str(e)}")
    else:
        succ("Bind data source successfully.")


@delete.command
@click.option(
    "-g",
    "--graph_identifier",
    required=True,
    help="See graph identifier with `ls` command",
)
@click.option(
    "-t",
    "--type",
    required=True,
    help="Vertex or edge type",
)
@click.option(
    "-s",
    "--source_vertex_type",
    required=False,
    help="Source vertex type of the edge [edge only]",
)
@click.option(
    "-d",
    "--destination_vertex_type",
    required=False,
    help="Destination vertex type of the edge [edge only]",
)
def datasource(  # noqa: F811
    graph_identifier, type, source_vertex_type, destination_vertex_type
):
    """Unbind data source mapping on vertex or edge type"""
    try:
        graph_identifier = get_graph_id_by_name(graph_identifier)
        if source_vertex_type is not None and destination_vertex_type is not None:
            unbind_edge_datasource(
                graph_identifier, type, source_vertex_type, destination_vertex_type
            )
        else:
            unbind_vertex_datasource(graph_identifier, type)
    except Exception as e:
        err(f"Failed to unbind data source: {str(e)}")
    else:
        succ("Unbind data source successfully.")


@create.command()
@click.option(
    "-g",
    "--graph_identifier",
    required=True,
    help="See graph identifier with `ls` command",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def loaderjob(graph_identifier, filename):  # noqa: F811
    """Create a data loading job from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    graph_identifier = get_graph_id_by_name(graph_identifier)
    try:
        config = read_yaml_file(filename)
        jobid = submit_dataloading_job(graph_identifier, config)
    except Exception as e:
        err(f"Failed to create a job: {str(e)}")
    else:
        succ(f"Create job {jobid} successfully.")


@delete.command()
@click.argument("identifier", required=True)
def job(identifier):  # noqa: F811
    """Cancel a job, see identifier with `ls` command"""
    try:
        delete_job_by_id(identifier)
    except Exception as e:
        err(f"Failed to delete job {identifier}: {str(e)}")
    else:
        succ(f"Delete job {identifier} successfully.")


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


if __name__ == "__main__":
    cli()
