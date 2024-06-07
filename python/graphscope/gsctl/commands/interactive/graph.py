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
from graphscope.gsctl.impl import create_dataloading_job
from graphscope.gsctl.impl import create_procedure
from graphscope.gsctl.impl import delete_job_by_id
from graphscope.gsctl.impl import delete_procedure_by_name
from graphscope.gsctl.impl import get_dataloading_config
from graphscope.gsctl.impl import get_job_by_id
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import list_procedures
from graphscope.gsctl.impl import switch_context
from graphscope.gsctl.impl import update_procedure
from graphscope.gsctl.utils import TreeDisplay
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import succ


@click.group()
def cli():
    pass


@cli.group()
def create():
    """Create stored procedure, loader job from file"""
    pass


@cli.group()
def delete():
    """Delete stored procedure, loader job by identifier"""
    pass


@cli.group()
def update():
    """Update stored procedure from file"""
    pass


@cli.group()
def desc():
    """Show details of job status and stored procedure by identifier"""
    pass


@cli.group()
def use():
    """Switch back to the global scope"""
    pass


@cli.command()
def ls():  # noqa: F811
    """Display schema, stored procedure, and job information"""
    tree = TreeDisplay()
    # context
    current_context = get_current_context()
    try:
        graphs = list_graphs()
        using_graph = None
        for g in graphs:
            if g.name == current_context.context:
                using_graph = g
                break
        # schema
        tree.create_graph_node(using_graph)
        # get data source from job configuration
        job_config = get_dataloading_config(using_graph.name)
        tree.create_datasource_node_for_interactive(using_graph, job_config)
        # stored procedure
        procedures = list_procedures(using_graph.name)
        tree.create_procedure_node(using_graph, procedures)
        # job
        jobs = list_jobs()
        tree.create_job_node(using_graph, jobs)
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
def procedure(filename):
    """Create a stored procedure from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_name = current_context.context
    try:
        procedure = read_yaml_file(filename)
        # overwrite graph name
        procedure["bound_graph"] = graph_name
        create_procedure(graph_name, procedure)
    except Exception as e:
        err(f"Failed to create stored procedure: {str(e)}")
    else:
        succ(f"Create stored procedure {procedure['name']} successfully.")


@delete.command()
@click.argument("identifier", required=True)
def procedure(identifier):  # noqa: F811
    """Delete a stored procedure, see identifier with `ls` command"""
    current_context = get_current_context()
    graph_name = current_context.context
    try:
        delete_procedure_by_name(graph_name, identifier)
    except Exception as e:
        err(f"Failed to delete stored procedure: {str(e)}")
    else:
        succ(f"Delete stored procedure {identifier} successfully.")


@update.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def procedure(filename):  # noqa: F811
    """Update a stored procedure from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_name = current_context.context
    try:
        procedure = read_yaml_file(filename)
        # overwrite graph name
        procedure["bound_graph"] = graph_name
        update_procedure(graph_name, procedure)
    except Exception as e:
        err(f"Failed to update stored procedure: {str(e)}")
    else:
        succ(f"Update stored procedure {procedure['name']} successfully.")


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
    current_context = get_current_context()
    graph_name = current_context.context
    try:
        config = read_yaml_file(filename)
        # overwrite graph name
        config["graph"] = graph_name
        jobid = create_dataloading_job(graph_name, config)
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


@desc.command()
@click.argument("identifier", required=True)
def procedure(identifier):  # noqa: F811
    """Show details of stored procedure, see identifier with `ls` command"""
    current_context = get_current_context()
    graph_name = current_context.context
    try:
        procedures = list_procedures(graph_name)
    except Exception as e:
        err(f"Failed to list procedures: {str(e)}")
    else:
        if not procedures:
            info(f"No stored procedures found on {graph_name}.")
            return
        specific_procedure_exist = False
        for procedure in procedures:
            if identifier == procedure.name:
                info(yaml.dump(procedure.to_dict()))
                specific_procedure_exist = True
                break
        if not specific_procedure_exist:
            err(f"Procedure {identifier} not found on {graph_name}.")


@use.command(name="GLOBAL")
def _global():
    """Switch back to the global scope"""
    switch_context("global")
    click.secho("Using GLOBAL", fg="green")


if __name__ == "__main__":
    cli()
