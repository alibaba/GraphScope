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


class GSCtl(object):
    """GraphScope command-line utility

    This is a context for the utility.
    """

    def __init__(self, repo_home=None, debug=False):
        self.home = os.path.abspath(".")
        self.debug = debug


@click.group()
@click.option(
    "--repo-home",
    envvar="REPO_HOME",
    type=click.Path(),
    help="GraphScope code repo location.",
)
@click.pass_context
def cli(ctx, repo_home):
    ctx.obj = GSCtl(repo_home)


@click.command()
@click.argument(
    "tag",
    type=click.Choice(
        ["dev", "client"],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--cn",
    is_flag=True,
    default=False,
    help="Whether to use CN located mirrors to speed up download.",
)
@click.option(
    "--install-prefix",
    type=click.Path(),
    default="/opt/graphscope",
    show_default=True,
    help="Install built binaries to customized location.",
)
@click.option(
    "--deps-prefix",
    type=click.Path(),
    default="/tmp/gs-local-deps",
    show_default=True,
    help="""Install dependency files to [prefix]. By default, './gs install-deps dev'
    will install all the files in '/opt/graphscope/bin', '/opt/graphscope/lib'
    etc. You can specify an installation prefix other than '/opt/graphscope'
    using '--install-prefix', for instance '--install-prefix=$HOME'.""",
)
@click.option(
    "--v6d-version",
    default="main",
    show_default=True,
    help="v6d version to clone.",    
)
@click.option(
    "-j","--jobs",
    default=4,
    help="Concurrent jobs in building, i.e., -j argument passed to make.",    
)
@click.option(
    "--for-analytical",
    is_flag=True,
    default=False,
    help="Only install analytical engine dependencies.",    
)
@click.option(
    "--no-v6d",
    type=bool,
    is_flag=True,
    default=False,
    help="Do not install v6d, for build base docker images, could only be used with '--for-analytical'",    
)
@click.pass_obj
def install_deps(repo,tag,cn,install_prefix,deps_prefix,v6d_version,jobs,for_analytical,no_v6d):
    """Install dependencies for building GraphScope."""
    click.echo("install_deps")
    if tag == "dev":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/install_deps_command.sh",
        "-t", "dev", "-c", str(cn), "-i", install_prefix, "-d", deps_prefix, 
        "-v", str(v6d_version), "-j", str(jobs), "-a", str(for_analytical),
        "-n", str(no_v6d)]
        run_shell_cmd(cmd, repo.home)
        return
    if tag == "client":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/install_deps_command.sh",
        "-t", "client"]
        run_shell_cmd(cmd, repo.home)
        return
    if tag == None:
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/install_deps_command.sh",
        "-t", "dev", "-c", str(cn), "-i", install_prefix, "-d", deps_prefix, 
        "-v", str(v6d_version), "-j", str(jobs), "-a", str(for_analytical),
        "-n", str(no_v6d)]
        run_shell_cmd(cmd, repo.home)
        return

@click.command()
@click.argument(
    "component",
    type=click.Choice(
        ["interactive", "interactive-install","analytical","analytical-java-install,",
         "analytical-install", "learning" ,"learning-install", "coordinator",
         "client","clean","all"],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--install-prefix",
    type=click.Path(),
    default="/opt/graphscope",
    show_default=True,
    help="Install built binaries to customized location.",
)
@click.option(
    "--storage-type",
    type=click.Choice(["experimental", "vineyard"], case_sensitive=False),
    help="Make gie with specified storage type.",
)
@click.pass_obj
def make(repo, component, install_prefix, storage_type):
    """Build executive binaries of COMPONENT. If not given a specific component, build all.

    \f
    TODO: maybe without make?
    """
    if component == "clean":
        click.secho("Cleaning previous build.", fg="green")
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "clean", "-i",install_prefix]
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

    if component == "interactive":
        click.secho("Building interactive engine.", fg="green")
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "interactive", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)
        return
    
    if component == "interactive-install":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "interactive-install", "-i",install_prefix, "-s",storage_type]
        run_shell_cmd(cmd, repo.home)
        return
            
    if component == "analytical":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "analytical", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)
        return
        
    if component == "analytical-install":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "analytical-install", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)
        return  
        
    if component == "learning":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "learning", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)
        return  
       
    if component == "learning-install":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "learning-install", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)
        return  
       
    if component == "client":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "client", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)

    if component == "coordinator":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "coordinator", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)

    if component == "all":
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "all", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)

    if component is None:
        cmd = ["bash", 
        "./python/graphscope/gsctl/scripts/make_command.sh",
        "-c", "all", "-i",install_prefix]
        run_shell_cmd(cmd, repo.home)



@click.command()
def make_image():
    """Make docker images from source code for deployment.

    \f
    TODO: fulfill this.
    """
    click.echo("make_image")


@click.command()
@click.pass_obj
def test(repo):
    """Trigger tests on built artifacts.

    \f
    TODO: fulfill this."""
    click.secho(f"repo.home = {repo.home}", fg="green")
    click.echo("test")


cli.add_command(install_deps)
cli.add_command(make)
cli.add_command(make_image)
cli.add_command(test)

if __name__ == '__main__':
    cli()