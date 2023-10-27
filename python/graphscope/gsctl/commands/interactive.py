#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited. All Rights Reserved.
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

"""Group of interactive commands under the FLEX architecture"""

import itertools
import json

import click
import yaml
from google.protobuf.json_format import MessageToDict

from graphscope.gsctl.client.rpc import get_grpc_client
from graphscope.gsctl.utils import dict_to_proto_message
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import terminal_display
from graphscope.proto import error_codes_pb2
from graphscope.proto import interactive_pb2


@click.group()
def cli():
    pass


@cli.group()
def get():
    """Display one or many resources"""
    pass


@cli.group()
def describe():
    """Show details of a specific resource or group of resources"""
    pass


@cli.group()
def create():
    """Create a resource from a file or from stdin"""
    pass


@cli.group()
def delete():
    """Delete resource by name"""
    pass


@cli.group()
def enable():
    """Enable stored procedures over a given graph"""
    pass


@cli.group()
def disable():
    """Disable stored procedures over a given graph"""
    pass


@cli.group()
def service():
    """Start, restart or stop the database service"""
    pass


def is_success(response):
    if response.code != error_codes_pb2.OK:
        click.secho(
            "{0}: {1}".format(
                error_codes_pb2.Code.Name(response.code), response.error_msg
            ),
            fg="red",
        )
        return False
    return True


@cli.command(name="import")
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph.",
)
@click.option(
    "-c",
    "--config",
    required=True,
    help="Yaml path or raw data for loading graph.",
)
def data_import(graph, config):
    """Load the raw data specified in bulk load file"""

    def _read_and_fill_raw_data(config):
        for mapping in itertools.chain(
            config["vertex_mappings"], config["edge_mappings"]
        ):
            for index, location in enumerate(mapping["inputs"]):
                # location is one of:
                #   1) protocol:///path/to/the/file
                #   2) @/path/to/the/file, which represents the local file
                if location.startswith("@"):
                    if "raw_data" not in mapping:
                        mapping["raw_data"] = []

                    # read file and set raw data
                    with open(location[1:], "rb") as f:
                        content = f.read()
                        mapping["raw_data"].append(content)

    schema_mapping_dict = config
    if is_valid_file_path(config):
        schema_mapping_dict = read_yaml_file(config)

    if graph is not None:
        schema_mapping_dict["graph"] = graph

    _read_and_fill_raw_data(schema_mapping_dict)

    # transfiorm dict to proto message
    schema_mapping = interactive_pb2.SchemaMapping()
    dict_to_proto_message(schema_mapping_dict, schema_mapping)

    grpc_client = get_grpc_client()
    response = grpc_client.import_interactive_graph(schema_mapping)

    if is_success(response):
        click.secho("Create dataloading job successfully.", fg="green")


@create.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
@click.option(
    "-c",
    "--config",
    required=True,
    help="Yaml path or json string of schema for the graph",
)
def graph(graph, config):
    """Create a graph in database, with the provided schema file"""

    graph_def_dict = config
    if is_valid_file_path(config):
        graph_def_dict = read_yaml_file(config)

    # override graph name
    if graph is not None:
        graph_def_dict["name"] = graph

    # transform graph dict to proto message
    graph_def = interactive_pb2.GraphProto()
    dict_to_proto_message(graph_def_dict, graph_def)

    grpc_client = get_grpc_client()
    response = grpc_client.create_interactive_graph(graph_def)

    if is_success(response):
        click.secho(
            "Create interactive graph {0} successfully.".format(graph_def_dict["name"]),
            fg="green",
        )


@create.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
@click.option(
    "-n",
    "--name",
    required=True,
    help="The name of the procedure",
)
@click.option(
    "-i",
    "--sourcefile",
    required=True,
    help="Path of [ .cc, .cypher ] file",
)
@click.option(
    "-d",
    "--description",
    required=False,
    help="Description for the specific procedure",
)
def procedure(graph, name, sourcefile, description):
    """Create and compile procedure over a specific graph"""

    # read source
    with open(sourcefile, "r") as f:
        query = f.read()

    # construct procedure proto
    procedure = interactive_pb2.Procedure(
        name=name, bound_graph=graph, description=description, query=query, enable=True
    )

    grpc_client = get_grpc_client()
    response = grpc_client.create_interactive_procedure(procedure)

    if is_success(response):
        click.secho("Create procedure {0} successfully.".format(name), fg="green")


@delete.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
def graph(graph):
    """Delete a graph, as well as the loaded data by name"""

    grpc_client = get_grpc_client()
    response = grpc_client.remove_interactive_graph(graph)

    if is_success(response):
        click.secho(
            "Delete interactive graph {0} successfully.".format(graph),
            fg="green",
        )


@delete.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
@click.option(
    "-n",
    "--name",
    required=True,
    help="The name of the procedure to be deleted",
)
def procedure(graph, name):
    """Delete a procedure over a specific graph in database"""

    grpc_client = get_grpc_client()
    response = grpc_client.remove_interactive_procedure(graph, name)

    if is_success(response):
        click.secho(
            "Remove procedure {0} on graph {1} successfully.".format(name, graph),
            fg="green",
        )


@enable.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
@click.option(
    "-n",
    "--name",
    required=True,
    help="List of procedure's name to enable, seprated by comma",
)
def procedure(graph, name):
    """Enable stored procedures in a given graph"""

    # remove the last "," if exists
    if name.endswith(","):
        name = name[:-1]
    procedure_list = name.split(",")

    # list current procedures
    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_procedure(graph)

    if is_success(response):
        # enable procedures locally
        procedures = response.procedures
        for p in procedures:
            if p.name in procedure_list:
                p.enable = True
                procedure_list.remove(p.name)

        # check
        if procedure_list:
            click.secho(
                "Procedure {0} not found.".format(str(procedure_list)), fg="red"
            )
            return

        # update procedures
        response = grpc_client.update_interactive_procedure(procedures)
        if is_success(response):
            click.secho("Update procedures successfully.", fg="green")


@disable.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
@click.option(
    "-n",
    "--name",
    required=True,
    help="List of procedure's name to enable, seprated by comma",
)
def proceduree(graph, name):
    """Disable stored procedures in a given graph"""

    # remove the last "," if exists
    if name.endswith(","):
        name = name[:-1]
    procedure_list = name.split(",")

    # list current procedures
    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_procedure(graph)

    if is_success(response):
        # disable procedures locally
        procedures = response.procedures
        for p in procedures:
            if p.name in procedure_list:
                p.enable = False
                procedure_list.remove(p.name)

        # check
        if procedure_list:
            click.secho(
                "Procedure {0} not found.".format(str(procedure_list)), fg="red"
            )
            return

        # update procedures
        response = grpc_client.update_interactive_procedure(procedures)
        if is_success(response):
            click.secho("Update procedures successfully.", fg="green")


@service.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
def start(graph):
    """Stop current service and start a new one on a specified graph"""

    service_def = interactive_pb2.Service(graph_name=graph)

    grpc_client = get_grpc_client()
    response = grpc_client.start_interactive_service(service_def)

    if is_success(response):
        click.secho(
            "Start service on graph {0} successfully.".format(graph), fg="green"
        )


@service.command()
def stop():
    """Stop the current database service"""

    grpc_client = get_grpc_client()
    response = grpc_client.stop_interactive_service()

    if is_success(response):
        click.secho("Service has stopped.", fg="green")


@service.command()
def restart():
    """Restart database service on current graph"""

    grpc_client = get_grpc_client()
    response = grpc_client.restart_interactive_service()

    if is_success(response):
        click.secho("Restart service successfully.", fg="green")


@get.command()
def graph():
    """Display graphs in database"""

    def _construct_and_display_data(graphs):
        if not graphs:
            click.secho("no graph found in database.", fg="blue")
            return

        head = ["", "NAME", "STORE TYPE", "VERTEX LABEL SIZE", "EDGE LABEL SIZE"]
        data = [head]

        for g in graphs:
            # vertex_schema = []
            # for v in g.schema.vertex_types:
            #     vertex_schema.append(v.type_name)
            # edge_schema = []
            # for e in g.schema.edge_types:
            #     edge_schema.append(e.type_name)
            data.append(
                [
                    "*",  # service graph
                    g.name,
                    g.store_type,
                    str(len(g.schema.vertex_types)),
                    str(len(g.schema.edge_types)),
                    # "({0}) {1}".format(len(vertex_schema), ",".join(vertex_schema)),
                    # "({0}) {1}".format(len(edge_schema), ",".join(edge_schema)),
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_graph()

    if is_success(response):
        _construct_and_display_data(response.graphs)
        return response.graphs


@get.command()
def job():
    """Display jobs in database"""

    def _construct_and_display_data(job_status):
        if not job_status:
            click.secho("no job found in database.", fg="blue")
            return

        head = ["JOBID", "STATUS", "START TIME", "END TIME", "DETAIL", "MESSAGE"]
        data = [head]

        for s in job_status:
            detail = {}
            for k, v in s.detail.items():
                detail[k] = v

            message = s.message.replace("\n", "")
            if len(message) > 63:
                message = message[:63] + "..."

            data.append(
                [
                    s.jobid,
                    s.status,
                    s.start_time,
                    s.end_time,
                    json.dumps(detail),
                    message,
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_job()

    if is_success(response):
        _construct_and_display_data(response.job_status)
        return response.job_status


@get.command()
@click.option(
    "-g",
    "--graph",
    required=True,
    help="The name of the graph",
)
def procedure(graph):
    """Display procedures in database"""

    def _construct_and_display_data(procedures):
        if not procedures:
            click.secho("no procedure found in database.", fg="blue")
            return

        head = ["NAME", "TYPE", "ENABLE", "DESCRIPTION"]
        data = [head]

        for procedure in procedures:
            data.append(
                [
                    procedure.name,
                    procedure.type,
                    str(procedure.enable),
                    procedure.description,
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_procedure(graph)

    if is_success(response):
        _construct_and_display_data(response.procedures)
        return response.procedures


@get.command()
def service():
    """Display service status in database"""

    def _construct_and_display_data(service_status):
        head = [
            "STATUS",
            "SERVICE GRAPH",
            "BOLT SERVICE ENDPOINT",
            "HQPS SERVICE ENDPOINT",
        ]
        data = [head]

        data.append(
            [
                service_status.status,
                service_status.graph_name,
                service_status.bolt_port,
                service_status.hqps_port,
            ]
        )

        terminal_display(data)

    grpc_client = get_grpc_client()
    response = grpc_client.get_interactive_service_status()

    if is_success(response):
        _construct_and_display_data(response.service_status)
        return response.service_status


@get.command()
def node():
    """Display resource (CPU/memory) usage of nodes"""

    def _construct_and_display_data(nodes_status):
        head = ["NAME", "CPU USAGE(%)", "MEMORY USAGE(%)", "DISK USAGE(%)"]
        data = [head]

        for s in nodes_status:
            data.append(
                [
                    s.node,
                    s.cpu_usage,
                    s.memory_usage,
                    s.disk_usage,
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    response = grpc_client.get_node_status()

    if is_success(response):
        _construct_and_display_data(response.nodes_status)
        return response.nodes_status


@describe.command()
@click.argument("graph", required=False)
def graph(graph):
    """Show details of graph"""

    grpc_client = get_grpc_client()
    response = grpc_client.list_interactive_graph()

    if is_success(response):
        graphs = response.graphs

        if not graphs:
            click.secho("no graph found in database.", fg="blue")
            return

        specific_graph_exist = False
        for g in graphs:
            if graph is not None and g.name != graph:
                continue

            # display
            click.secho(yaml.dump(MessageToDict(g, preserving_proto_field_name=True)))

            if graph is not None and g.name == graph:
                specific_graph_exist = True
                break

        if graph is not None and not specific_graph_exist:
            click.secho("graphs \"{0}\" not found.".format(graph), fg="blue")


if __name__ == "__main__":
    cli()
