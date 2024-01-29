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

from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import create_graph
from graphscope.gsctl.impl import create_procedure
from graphscope.gsctl.impl import delete_alert_receiver_by_id
from graphscope.gsctl.impl import delete_alert_rule_by_name
from graphscope.gsctl.impl import delete_graph_by_name
from graphscope.gsctl.impl import delete_procedure_by_name
from graphscope.gsctl.impl import disconnect_coordinator
from graphscope.gsctl.impl import get_deployment_info
from graphscope.gsctl.impl import get_node_status
from graphscope.gsctl.impl import get_schema_by_name
from graphscope.gsctl.impl import get_service_status
from graphscope.gsctl.impl import import_data_to_interactive_graph
from graphscope.gsctl.impl import list_alert_messages
from graphscope.gsctl.impl import list_alert_receivers
from graphscope.gsctl.impl import list_alert_rules
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_procedures
from graphscope.gsctl.impl import register_receiver
from graphscope.gsctl.impl import restart_service
from graphscope.gsctl.impl import start_service
from graphscope.gsctl.impl import stop_service
from graphscope.gsctl.impl import update_alert_messages
from graphscope.gsctl.impl import update_alert_receiver_by_id
from graphscope.gsctl.impl import update_alert_rule
from graphscope.gsctl.impl import update_procedure
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import terminal_display


@click.group()
def cli():
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
def update():
    """Update a resource(procedure) from a file"""
    pass


@cli.group()
def describe():
    """Show details of a specific resource or group of resources"""
    pass


@cli.group()
def get():
    """Display a specific resource or group of resources"""
    pass


@cli.group()
def start():
    """Start database service on a certain graph"""
    pass


@cli.group()
def stop():
    """Stop database service"""
    pass


@cli.group()
def restart():
    """Restart database service on current graph"""
    pass


@cli.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to import data",
)
def dataimport(filename):
    """Import data to Interactive graph (deprecated)"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        config = read_yaml_file(filename)
        import_data_to_interactive_graph(config)
    except Exception as e:
        click.secho(f"Failed to import data: {str(e)}", fg="red")
    else:
        click.secho("Import data successfully.", fg="green")


@get.command()
def graph():
    """Display graphs in database"""

    def _construct_and_display_data(graphs):
        if not graphs:
            click.secho("no graph found in database.", fg="blue")
            return
        head = ["NAME", "STORE TYPE", "VERTEX TYPE SIZE", "EDGE TYPE SIZE"]
        data = [head]
        for g in graphs:
            data.append(
                [
                    g.name,
                    g.store_type,
                    len(g.var_schema.vertex_types),
                    len(g.var_schema.edge_types),
                ]
            )
        terminal_display(data)

    try:
        graphs = list_graphs()
    except Exception as e:
        click.secho(f"Failed to list graphs: {str(e)}", fg="red")
    else:
        _construct_and_display_data(graphs)


@describe.command()
@click.argument("graph_name", required=True)
def graph(graph_name):  # noqa: F811
    """Show details of graphs"""
    try:
        graphs = list_graphs()
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


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create a graph",
)
def graph(filename):  # noqa: F811
    """Create a new graph in database, with the provided schema file"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        graph = read_yaml_file(filename)
        create_graph(graph)
    except Exception as e:
        click.secho(f"Failed to create graph: {str(e)}", fg="red")
    else:
        click.secho(f"Create graph {graph['name']} successfully.", fg="green")


@delete.command()
@click.argument("graph_name", required=True)
def graph(graph_name):  # noqa: F811
    """Delete a graph by name in database"""
    try:
        delete_graph_by_name(graph_name)
    except Exception as e:
        click.secho(f"Failed to delete graph {graph_name}: {str(e)}", fg="red")
    else:
        click.secho(f"Delete graph {graph_name} successfully.", fg="green")


@create.command()
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="Create a stored procedure on a certain graph",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create a procedure",
)
def procedure(graph_name, filename):
    """Create a stored procedure on a certain graph"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        procedure = read_yaml_file(filename)
        # overwrite graph name
        procedure["bound_graph"] = graph_name
        create_procedure(graph_name, procedure)
    except Exception as e:
        click.secho(f"Failed to create stored procedure: {str(e)}", fg="red")
    else:
        click.secho(
            f"Create stored procedure {procedure['name']} successfully.", fg="green"
        )


@get.command()
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="List stored procedures on a certain graph",
)
def procedure(graph_name):  # noqa: F811
    """Display stored procedures in database"""

    def _construct_and_display_data(procedures):
        if not procedures:
            click.secho(f"no stored procedure found on {graph_name}.", fg="blue")
            return
        head = ["NAME", "BOUND_GRAPH", "TYPE", "ENABLE", "RUNNABLE", "DESCRIPTION"]
        data = [head]
        for procedure in procedures:
            data.append(
                [
                    procedure.name,
                    procedure.bound_graph,
                    procedure.type,
                    str(procedure.enable),
                    str(procedure.runnable),
                    procedure.description,
                ]
            )
        terminal_display(data)

    try:
        procedures = list_procedures(graph_name)
    except Exception as e:
        click.secho(f"Failed to list stored procedures: {str(e)}", fg="red")
    else:
        _construct_and_display_data(procedures)


@describe.command()
@click.argument("procedure_name", required=False)
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="Describe stored procedures on a certain graph",
)
def procedure(procedure_name, graph_name):  # noqa: F811
    """Show details of stored procedure"""
    try:
        procedures = list_procedures(graph_name)
    except Exception as e:
        click.secho(f"Failed to list procedures: {str(e)}", fg="red")
    else:
        if not procedures:
            click.secho(f"no stored procedures found on {graph_name}.", fg="blue")
            return
        specific_procedure_exist = False
        for procedure in procedures:
            if procedure_name is not None and procedure.name != procedure_name:
                continue
            # display
            click.secho(yaml.dump(procedure.to_dict()))
            if procedure_name is not None and procedure.name == procedure_name:
                specific_procedure_exist = True
        if procedure_name is not None and not specific_procedure_exist:
            click.secho(
                f"procedure {procedure_name} not found on {graph_name}.", fg="blue"
            )


@update.command()
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="Update a stored procedure on a certain graph",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to update a procedure",
)
def procedure(graph_name, filename):  # noqa: F811
    """Update a stored procedure on a certain graph"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        procedure = read_yaml_file(filename)
        # overwrite graph name
        procedure["bound_graph"] = graph_name
        update_procedure(graph_name, procedure)
    except Exception as e:
        click.secho(f"Failed to update stored procedure: {str(e)}", fg="red")
    else:
        click.secho(
            f"Update stored procedure {procedure['name']} successfully.", fg="green"
        )


@delete.command()
@click.argument("procedure_name", required=True)
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="Delete a stored procedure on a certain graph",
)
def procedure(graph_name, procedure_name):  # noqa: F811
    """Delete a procedure on a certain graph in database"""
    try:
        delete_procedure_by_name(graph_name, procedure_name)
    except Exception as e:
        click.secho(f"Failed to delete stored procedure: {str(e)}", fg="red")
    else:
        click.secho(
            f"Delete stored procedure {procedure_name} on {graph_name} successfully.",
            fg="green",
        )


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


@get.command
def service():
    """Display service status in database"""

    def _construct_and_display_data(status):
        head = ["STATUS", "SERVING_GRAPH", "CYPHER_ENDPOINT", "HQPS_ENDPOINT"]
        data = [head]
        data.append(
            [
                status.status,
                status.graph_name,
                status.sdk_endpoints.cypher,
                status.sdk_endpoints.hqps,
            ]
        )
        terminal_display(data)

    try:
        status = get_service_status()
    except Exception as e:
        click.secho(f"Failed to get service status: {str(e)}", fg="red")
    else:
        _construct_and_display_data(status)


@stop.command
def service():  # noqa: F811
    """Stop database service"""
    try:
        stop_service()
    except Exception as e:
        click.secho(f"Failed to stop service: {str(e)}", fg="red")
    else:
        click.secho("Service stopped.", fg="green")


@start.command
@click.option(
    "-g",
    "--graph_name",
    required=True,
    help="Start service on a certain graph",
)
def service(graph_name):  # noqa: F811
    """Start database service on a certain graph"""
    try:
        start_service(graph_name)
    except Exception as e:
        click.secho(
            f"Failed to start service on graph {graph_name}: {str(e)}", fg="red"
        )
    else:
        click.secho(f"Start service on graph {graph_name} successfully", fg="green")


@restart.command
def service():  # noqa: F811
    """Restart database service on current graph"""
    try:
        restart_service()
    except Exception as e:
        click.secho(f"Failed to restart service: {str(e)}", fg="red")
    else:
        click.secho("Service restarted.", fg="green")


@get.command()
def alertrule():
    """Display alert rules in database"""

    def _construct_and_display_data(rules):
        if not rules:
            click.secho("no alert rules found in database.", fg="blue")
            return
        head = [
            "NAME",
            "SEVERITY",
            "METRIC_TYPE",
            "CONDITIONS_DESCRIPTION",
            "FREQUENCY",
            "ENABLE",
        ]
        data = [head]
        for r in rules:
            data.append(
                [
                    r.name,
                    r.severity,
                    r.metric_type,
                    r.conditions_desription,
                    "{0} Min".format(r.frequency),
                    str(r.enable),
                ]
            )
        terminal_display(data)

    try:
        rules = list_alert_rules()
    except Exception as e:
        click.secho(f"Failed to list alert rules: {str(e)}", fg="red")
    else:
        _construct_and_display_data(rules)


@update.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to update an alertrule",
)
def alertrule(filename):  # noqa: F811
    """Update an alert rule in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        rule = read_yaml_file(filename)
        update_alert_rule(rule)
    except Exception as e:
        click.secho(f"Failed to update alert rule: {str(e)}", fg="red")
    else:
        click.secho(f"Update alert rule {rule['name']} successfully.", fg="green")


@delete.command()
@click.argument("RULE_NAME", required=True)
def alertrule(rule_name):  # noqa: F811
    """Delete an alert rule indatabase"""
    try:
        delete_alert_rule_by_name(rule_name)
    except Exception as e:
        click.secho(f"Failed to delete alert rule: {str(e)}", fg="red")
    else:
        click.secho(
            f"Delete alert rule {rule_name} successfully.",
            fg="green",
        )


@get.command()
@click.option(
    "--status",
    type=click.Choice(["unsolved", "dealing", "solved"]),
    required=False,
)
@click.option(
    "--severity",
    type=click.Choice(["emergency", "warning"]),
    required=False,
)
@click.option("--starttime", required=False, help="format with 2024-01-01-00-00-00")
@click.option("--endtime", required=False, help="format with 2024-01-02-12-30-00")
@click.option("--limit", required=False, default=100)
def alertmessage(status, severity, starttime, endtime, limit):
    """Display alert messages in database"""

    def _construct_and_display_data(messages):
        if not messages:
            click.secho("no alert message found in database.", fg="blue")
            return
        head = [
            "MESSAGE_ID",
            "SEVERITY",
            "METRIC_TYPE",
            "TARGET",
            "TRIGGER_TIME",
            "STATUS",
            "MESSAGE",
        ]
        data = [head]
        for m in messages[:limit]:
            data.append(
                [
                    m.message_id,
                    m.severity,
                    m.metric_type,
                    ",".join(m.target),
                    m.trigger_time,
                    m.status,
                    m.message,
                ]
            )
        terminal_display(data)

    try:
        messages = list_alert_messages(status, severity, starttime, endtime)
    except Exception as e:
        click.secho(f"Failed to list alert messages: {str(e)}", fg="red")
    else:
        _construct_and_display_data(messages)


@update.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to update alert messages in batch",
)
def alertmessage(filename):  # noqa: F811
    """Update alert messages in batch"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        request = read_yaml_file(filename)
        update_alert_messages(request)
    except Exception as e:
        click.secho(f"Failed to update alert messages: {str(e)}", fg="red")
    else:
        click.secho("Update alert messages successfully.", fg="green")


@get.command()
def alertreceiver():
    """Display alert receivers in database"""

    def _construct_and_display_data(receivers):
        if not receivers:
            click.secho("no alert receiver found in database.", fg="blue")
            return
        head = [
            "RECEIVER_ID",
            "TYPE",
            "WEBHOOK_URL",
            "AT_USERS_ID",
            "IS_AT_ALL",
            "ENABLE",
            "MESSAGE",
        ]
        data = [head]
        for r in receivers:
            data.append(
                [
                    r.receiver_id,
                    r.type,
                    r.webhook_url,
                    ",".join(r.at_user_ids),
                    str(r.is_at_all),
                    str(r.enable),
                    r.message,
                ]
            )
        terminal_display(data)

    try:
        receivers = list_alert_receivers()
    except Exception as e:
        click.secho(f"Failed to list alert receivers: {str(e)}", fg="red")
    else:
        _construct_and_display_data(receivers)


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to create an alert receiver",
)
def alertreceiver(filename):  # noqa: F811
    """Create an alert receiver in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        receiver = read_yaml_file(filename)
        print(receiver)
        register_receiver(receiver)
    except Exception as e:
        click.secho(f"Failed to create alert receiver: {str(e)}", fg="red")
    else:
        click.secho("Create alert receiver successfully.", fg="green")


@update.command()
@click.argument("receiver_id", required=True)
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file to use to update an alert receiver",
)
def alertreceiver(receiver_id, filename):  # noqa: F811
    """Update an alert receiver by id in database"""
    if not is_valid_file_path(filename):
        click.secho("Invalid file: {0}".format(filename), fg="blue")
        return
    try:
        receiver = read_yaml_file(filename)
        update_alert_receiver_by_id(receiver_id, receiver)
    except Exception as e:
        click.secho(f"Failed to update the alert receiver: {str(e)}", fg="red")
    else:
        click.secho(f"Update alert receiver {receiver_id} successfully.", fg="green")


@delete.command()
@click.argument("receiver_id", required=True)
def alertreceiver(receiver_id):  # noqa: F811
    """Delete an alert receiver by id in database"""
    try:
        delete_alert_receiver_by_id(receiver_id)
    except Exception as e:
        click.secho(f"Failed to delete alert receiver: {str(e)}", fg="red")
    else:
        click.secho(f"Delete alert receiver {receiver_id} successfully.", fg="green")


if __name__ == "__main__":
    cli()
