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

import io
import os
import subprocess

import click


def run_shell_cmd(cmd, workingdir):
    """wrapper function to run a shell command/scripts."""
    click.echo(f"run a shell command on cwd={workingdir}. \ncmd=\"{' '.join(cmd)}\"")
    proc = subprocess.Popen(cmd, cwd=workingdir, stdout=subprocess.PIPE)
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        print(line.rstrip())


class GSUtil(object):
    """GraphScope command-line utility

    This is a context for the utility.
    """

    def __init__(self, home=None, debug=False):
        self.home = os.path.abspath(home or ".")
        self.debug = debug


@click.group()
@click.option(
    "--repo-home",
    envvar="REPO_HOME",
    default=".",
    help="GraphScope code repo location.",
)
@click.pass_context
def cli(ctx, repo_home):
    ctx.obj = GSUtil(repo_home)


@click.command()
@click.pass_obj
def install_deps():
    """Install dependencies for building GraphScope."""
    click.echo("install_deps")


@click.command()
@click.argument(
    "component",
    type=click.Choice(
        ["interactive", "analytical", "learning", "coordinator", "client"],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--clean",
    is_flag=True,
    default=False,
    help="Flag indicating whether clean previous build.",
)
@click.option(
    "--install",
    is_flag=True,
    default=False,
    help="Flag indicating whether install after built binaries.",
)
@click.option(
    "--install-prefix",
    type=click.Path(),
    default="/opt/graphscope",
    show_default=True,
    help="Install built binaries to customized location.",
)
@click.option(
    "--with-java",
    is_flag=True,
    default=False,
    help="Whether build analytical engine with Java support.",
)
@click.option(
    "--storage-type",
    type=click.Choice(["experimental", "vineyard"], case_sensitive=False),
    help="Make gie with specified storage type.",
)
@click.pass_obj
def make(repo, component, clean, install, install_prefix, storage_type, with_java):
    """Build executive binaries of COMPONENT. If not given a specific component, build all.

    \f
    TODO: maybe without make?
    """
    if clean:
        click.secho("Cleaning previous build.", fg="green")
        cmd = ["make", "clean"]
        run_shell_cmd(cmd, repo.home)
        return
    click.secho(
        "Before making artifacts, please manually source ENVs from ~/.graphscope_env.",
        fg="yellow",
    )
    click.secho(
        f"Begin the make command, to build components [{component}] of GraphScope, with repo = {repo.home}",
        fg="green",
    )
    cmd = []
    workingdir = repo.home
    if component == "interactive":
        click.secho("Building interactive engine.", fg="green")
        if storage_type == "experimental":
            cmd = ["make", "build", 'QUIET_OPT=""']
            workingdir = os.path.join(repo.home, "interactive_engine", "compiler")
        if storage_type == "vineyard":
            cmd = [
                "mvn",
                "install",
                "-DskipTests",
                "-Drust.compile.mode=release",
                "-P",
                "graphscope,graphscope-assembly",
            ]
            workingdir = os.path.join(repo.home, "interactive_engine")
            run_shell_cmd(cmd, workingdir)
            cmd = ["tar", "xvzf", "graphscope.tar.gz"]
            workingdir = os.path.join(
                repo.home, "interactive_engine", "assembly", "target"
            )
            click.secho(f"Begin to extract, from {workingdir}.", fg="green")
            run_shell_cmd(cmd, workingdir)
            click.secho("GraphScope interactive engine has been built.", fg="green")
        if install is True:
            cmd = [
                "make",
                "interactive-install",
                "INSTALL_PREFIX={}".format(install_prefix),
            ]
            run_shell_cmd(cmd, repo.home)
            click.secho(
                f"GraphScope interactive engine has been installed to {install_prefix}.",
                fg="green",
            )

    if component == "analytical":
        cmd = ["make", "analytical"]
        if with_java:
            cmd = ["make", "analytical-java"]
        run_shell_cmd(cmd, repo.home)
        click.secho("GraphScope analytical engine has been built.", fg="green")
        if install is True:
            cmd = [
                "make",
                "analytical-install",
                "INSTALL_PREFIX={}".format(install_prefix),
            ]
            run_shell_cmd(cmd, repo.home)
            click.secho(
                f"GraphScope analytical engine has been installed to {install_prefix}.",
                fg="green",
            )

    if component == "client":
        cmd = ["make", "client"]
        run_shell_cmd(cmd, repo.home)

    if component == "coordinator":
        cmd = ["make", "coordinator"]
        run_shell_cmd(cmd, repo.home)

    if component is None:
        click.secho("Building all components.", fg="green")
        cmd = ["make", "all"]
        if install is True:
            cmd = ["make", "install", "INSTALL_PREFIX={}".format(install_prefix)]
        run_shell_cmd(cmd, repo.home)


@click.command()
def make_image():
    """Make docker images from source code for deployment.

    \f
    TODO: fulfill this.
    """
    click.echo("make_image")


@click.command()
def test():
    """Trigger tests on built artifacts.

    \f
    TODO: fulfill this."""
    click.echo("test")


cli.add_command(install_deps)
cli.add_command(make)
cli.add_command(make_image)
cli.add_command(test)
