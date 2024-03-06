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
from graphscope.gsctl.impl import (create_edge_type,
                                   create_groot_dataloading_job,
                                   create_vertex_type, delete_edge_type,
                                   delete_job_by_id, delete_vertex_type,
                                   get_datasource, get_deployment_info,
                                   get_job_by_id, get_node_status,
                                   import_datasource, import_groot_schema,
                                   list_groot_graph, list_jobs,
                                   unbind_edge_datasource,
                                   unbind_vertex_datasource)
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
@click.argument("graph_name", required=False)
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
    help="Path of yaml file to use to create a schema",
)
def schema(filename):  # noqa: F811
    """Batch import schema into database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        schema = read_yaml_file(filename)
        # only one graph supported int groot
        import_groot_schema("placeholder", schema)
    except Exception as e:
        click.secho(f"Failed to import schema: {str(e)}", fg="red")
    else:
        click.secho("Import schema successfully.", fg="green")


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


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to bind data source to the graph",
)
def datasource(filename):  # noqa: F811
    """Bind data source to the graph"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        datasource = read_yaml_file(filename)
        # only one graph supported int groot
        import_datasource("placeholder", datasource)
    except Exception as e:
        click.secho(f"Failed to import data source: {str(e)}", fg="red")
    else:
        click.secho("Import data source successfully.", fg="green")


@get.command()
def datasource():
    """Display data source on graph"""

    def _construct_and_display_data(datasource):
        if not datasource.vertices_datasource and not datasource.edges_datasource:
            click.secho("no data source bound on the graph.", fg="blue")
            return

        head = [
            "TYPE NAME",
            "SOURCE VERTEX TYPE",
            "DESTINATION VERTEX TYPE",
            "DATA SOURCE TYPE",
            "LOCATION",
        ]
        data = [head]
        if datasource.vertices_datasource:
            for v_datasource in datasource.vertices_datasource:
                data.append(
                    [
                        v_datasource.type_name,
                        "-",
                        "-",
                        v_datasource.data_source,
                        v_datasource.location,
                    ]
                )
        if datasource.edges_datasource:
            for e_datasource in datasource.edges_datasource:
                data.append(
                    [
                        e_datasource.type_name,
                        e_datasource.source_vertex,
                        e_datasource.destination_vertex,
                        e_datasource.data_source,
                        e_datasource.location,
                    ]
                )

        terminal_display(data)

    try:
        # only one graph supported int groot
        datasource = get_datasource("placeholder")
    except Exception as e:
        click.secho(f"Failed to list data source: {str(e)}", fg="red")
    else:
        _construct_and_display_data(datasource)


@describe.command()
def datasource():  # noqa: F811
    """Show details of data source"""
    try:
        # only one graph supported int groot
        datasource = get_datasource("placeholder")
    except Exception as e:
        click.secho(f"Failed to get data source: {str(e)}", fg="red")
    else:
        if not datasource.vertices_datasource and not datasource.edges_datasource:
            click.secho("no data source bound on the graph.", fg="blue")
            return
        click.secho(yaml.dump(datasource.to_dict()))


@delete.command()
@click.argument("vertex_type", required=True)
def vdatasource(vertex_type):  # noqa: F811
    """Unbind data source on vertex type"""
    try:
        unbind_vertex_datasource("placeholder", vertex_type)
    except Exception as e:
        click.secho(
            f"Failed to unbind data source on {vertex_type}: {str(e)}", fg="red"
        )
    else:
        click.secho(f"Unbind data source on {vertex_type} successfully.", fg="green")


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
def edatasource(edge_type, source_vertex_type, destination_vertex_type):  # noqa: F811
    """Unbind data source on edge type"""
    try:
        etype_full_name = (
            f"({source_vertex_type})-[{edge_type}]->({destination_vertex_type})"
        )
        unbind_edge_datasource(
            "placeholder", edge_type, source_vertex_type, destination_vertex_type
        )
    except Exception as e:
        click.secho(
            f"Failed to unbind data source on {etype_full_name}: {str(e)}",
            fg="red",
        )
    else:
        click.secho(
            f"Unbind data source on {etype_full_name} successfully.",
            fg="green",
        )


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create a job",
)
def job(filename):  # noqa: F811
    """Create a dataloading job in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        config = read_yaml_file(filename)
        jobid = create_groot_dataloading_job("placeholder", config)
    except Exception as e:
        click.secho(f"Failed to create a job: {str(e)}", fg="red")
    else:
        click.secho(f"Create job {jobid} successfully.", fg="green")


@get.command()
def job():  # noqa: F81
    """Display jobs in database"""

    def _construct_and_display_data(jobs):
        if not jobs:
            click.secho("no job found in database.", fg="blue")
            return
        head = ["JOBID", "TYPE", "STATUS", "START_TIME", "END_TIME"]
        data = [head]
        for j in jobs:
            data.append(
                [
                    j.job_id,
                    j.type,
                    j.status,
                    j.start_time,
                    str(j.end_time),
                ]
            )
        terminal_display(data)

    try:
        jobs = list_jobs()
    except Exception as e:
        click.secho(f"Failed to list jobs: {str(e)}", fg="red")
    else:
        _construct_and_display_data(jobs)


@describe.command()
@click.argument("job_id", required=True)
def job(job_id):  # noqa: F811
    """Show details of job"""
    try:
        job = get_job_by_id(job_id)
    except Exception as e:
        click.secho(f"Failed to get job: {str(e)}", fg="red")
    else:
        click.secho(yaml.dump(job.to_dict()))


@delete.command()
@click.argument("job_id", required=True)
def job(job_id):  # noqa: F811
    """Cancel a job by id in database"""
    try:
        delete_job_by_id(job_id)
    except Exception as e:
        click.secho(f"Failed to delete job {job_id}: {str(e)}", fg="red")
    else:
        click.secho(f"Delete job {job_id} successfully.", fg="green")


@get.command()
def node():
    """Display resource(cpu/memory) usage of nodes"""

    def _construct_and_display_data(nodes):
        head = ["HOSTNAME", "CPU_USAGE", "MEMORY_USAGE", "DISK_USAGE"]
        data = [head]
        for node in nodes:
            data.append(
                [
                    node.node,
                    f"{node.cpu_usage}%",
                    f"{node.memory_usage}%",
                    f"{node.disk_usage}%",
                ]
            )

        terminal_display(data)

    try:
        deployment_info = get_deployment_info()
        if deployment_info.cluster_type != "HOSTS":
            click.secho(
                f"Cluster type {deployment_info.cluster_type} is not support yet."
            )
            return
        nodes = get_node_status()
    except Exception as e:
        click.secho(f"Failed to get node status: {str(e)}", fg="red")
    else:
        _construct_and_display_data(nodes)


if __name__ == "__main__":
    cli()
