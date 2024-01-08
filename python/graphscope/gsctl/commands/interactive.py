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
from graphscope.proto import flex_pb2


@click.group()
def cli():
    pass


@cli.group()
def get():
    """Display a specific resource or group of resources"""
    pass


@cli.group()
def describe():
    """Show details of a specific resource or group of resources"""
    pass


@cli.group()
def create():
    """Create a resource(graph, procedure) from a file"""
    pass


@cli.group()
def delete():
    """Delete a resource(graph, procedure) by name"""
    pass


@cli.group()
def enable():
    """Enable a stored procedure by name"""
    pass


@cli.group()
def disable():
    """Disable a stored procedure by name"""
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


@create.command()
@click.option(
    "-n",
    "--name",
    required=False,
    help="The name of the graph",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create the graph",
)
def graph(name, filename):
    """Create a new graph in database, with the provided schema file"""

    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return

    graph_def_dict = read_yaml_file(filename)

    # override graph name
    if name is not None:
        graph_def_dict["name"] = name

    # transform graph dict to proto message
    graph_def = flex_pb2.GraphProto()
    dict_to_proto_message(graph_def_dict, graph_def)

    grpc_client = get_grpc_client()
    if not grpc_client.connect():
        return

    response = grpc_client.create_graph(graph_def)

    if is_success(response):
        click.secho(
            "Create graph {0} successfully.".format(graph_def_dict["name"]),
            fg="green",
        )


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create a procedure",
)
def procedure(filename):
    """Create a stored procedure in database"""

    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return

    procedure_dict = read_yaml_file(filename)
    # transform procedure dict to proto message
    procedure_def = flex_pb2.Procedure()
    dict_to_proto_message(procedure_dict, procedure_def)

    grpc_client = get_grpc_client()
    if not grpc_client.connect():
        return

    response = grpc_client.create_procedure(procedure_def)

    if is_success(response):
        click.secho(
            "Create stored procedure successfully", fg="green",
        )


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create the job",
)
def job(filename):
    """Create or schedule a job in database"""

    def _read_and_fill_raw_data(config):
        for mapping in itertools.chain(
            config["vertex_mappings"], config["edge_mappings"]
        ):
            for index, location in enumerate(mapping["inputs"]):
                # path begin with "@" represents the local file
                if location.startswith("@"):
                    if "raw_data" not in mapping:
                        mapping["raw_data"] = []

                    # read file and set raw data
                    with open(location[1:], "rb") as f:
                        content = f.read()
                        mapping["raw_data"].append(content)

    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return

    job = read_yaml_file(filename)

    try:
        _read_and_fill_raw_data(job["description"])

        # job schedule
        schedule = flex_pb2.Schedule()
        dict_to_proto_message(job["schedule"], schedule)

        # job description
        schema_mapping = flex_pb2.SchemaMapping()
        dict_to_proto_message(job["description"], schema_mapping)
        description = flex_pb2.JobDescription(schema_mapping=schema_mapping)

        grpc_client = get_grpc_client()
        if not grpc_client.connect():
            return

        response = grpc_client.create_job(
            type=job["type"],
            schedule=schedule,
            description=description,
        )

        if is_success(response):
            click.secho(
                "Job has been submitted",
                fg="green",
            )
    except Exception as e:
        click.secho(str(e), fg="red")


@delete.command()
@click.argument("graph", required=True)
def graph(graph):
    """Delete a graph by name"""

    grpc_client = get_grpc_client()
    response = grpc_client.delete_graph(graph)

    if is_success(response):
        click.secho(
            "Delete graph {0} successfully.".format(graph),
            fg="green",
        )

@delete.command()
@click.argument("job", required=True)
@click.option(
    '--delete-scheduler/--no-delete-scheduler',
    default=False,
    help="True will delete the job scheduler, otherwise just cancel the next schedule",
)
def job(job, delete_scheduler):
    """Cancel a job by jobid"""

    grpc_client = get_grpc_client()
    response = grpc_client.cancel_job(job, delete_scheduler)

    if is_success(response):
        click.secho(
            "Cancel job {0} successfully.".format(job),
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

    service_def = flex_pb2.Service(graph_name=graph)

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

        head = ["NAME", "STORE TYPE", "VERTEX LABEL SIZE", "EDGE LABEL SIZE"]
        data = [head]

        for g in graphs:
            data.append(
                [
                    g.name,
                    g.store_type,
                    str(len(g.schema.vertex_types)),
                    str(len(g.schema.edge_types)),
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    if not grpc_client.connect():
        return

    response = grpc_client.list_graph()

    if is_success(response):
        _construct_and_display_data(response.graphs)


@get.command()
def job():
    """Display jobs in database"""

    def _construct_and_display_data(job_status):
        if not job_status:
            click.secho("no job found in database", fg="blue")
            return

        head = ["JOBID", "TYPE", "STATUS", "START TIME", "END TIME"]
        data = [head]

        for s in job_status:
            # detail = {}
            # for k, v in s.detail.items():
            # detail[k] = v

            # message = s.message.replace("\n", "")
            # if len(message) > 63:
            # message = message[:63] + "..."

            data.append(
                [
                    s.jobid,
                    s.type,
                    s.status,
                    s.start_time,
                    s.end_time,
                ]
            )

        terminal_display(data)

    grpc_client = get_grpc_client()
    if not grpc_client.connect():
        return

    response = grpc_client.list_job()

    if is_success(response):
        _construct_and_display_data(response.job_status)


@get.command()
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
    response = grpc_client.list_procedure()

    if is_success(response):
        _construct_and_display_data(response.procedures)


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
    if not grpc_client.connect():
        return

    response = grpc_client.list_graph()

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
            click.secho('graph "{0}" not found.'.format(graph), fg="blue")


@describe.command()
@click.argument("job", required=False)
def job(job):
    """Show details of job"""

    grpc_client = get_grpc_client()
    if not grpc_client.connect():
        return

    response = grpc_client.list_job()

    if is_success(response):
        job_status = response.job_status

        if not job_status:
            click.secho("no job found in database", fg="blue")
            return

        specific_job_exist = False
        for s in job_status:
            if job is not None and s.jobid != job:
                continue

            # display
            click.secho(yaml.dump(MessageToDict(s, preserving_proto_field_name=True)))

            if job is not None and s.jobid == job:
                specific_job_exist = True
                break

        if job is not None and not specific_job_exist:
            click.secho('job "{0}" not found.'.format(job), fg="blue")


if __name__ == "__main__":
    cli()
