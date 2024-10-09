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

"""Group of commands for GraphScope development"""

import io
import os
import subprocess
import sys

import click

# Make sure this file doesn't depend on any graphscope directories for
# installing dependencies, so we can't `import graphscope` here.
version_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "VERSION"
)
with open(version_file_path, "r", encoding="utf-8") as fp:
    __version__ = fp.read().strip()

v6d_version_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "V6D_VERSION"
)
with open(v6d_version_file_path, "r", encoding="utf-8") as fp:
    __v6d_version__ = fp.read().strip()

# Interactive docker container config
INTERACTIVE_DOCKER_CONTAINER_NAME = "gs-interactive-instance"
INTERACTIVE_DOCKER_CONTAINER_LABEL = "flex=interactive"
INTERACTIVE_DOCKER_DEFAULT_CONFIG_PATH = "/opt/flex/share/interactive_config.yaml"

scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "scripts")
install_deps_script = os.path.join(scripts_dir, "install_deps.sh")
default_graphscope_repo_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "..",
    "..",
    "..",
)


def run_shell_cmd(cmd, workingdir):
    """wrapper function to run a shell command/scripts."""
    click.secho(f"run a shell command on cwd={workingdir}. \ncmd=\"{' '.join(cmd)}\"")
    proc = subprocess.Popen(
        cmd,
        cwd=workingdir,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        click.secho(line.rstrip())
    proc.wait()
    return proc.returncode


@click.group()
def cli():
    # nothing happens
    pass


@cli.group()
def flexbuild():
    """Build docker image for Interactive, Insight product."""
    pass


@cli.group()
def instance():
    """Deploy, destroy a Flex instance.

    The `instance` subcommand is responsible for managing the Flex Instances.
    """
    pass


@flexbuild.command()
@click.option(
    "--app",
    type=click.Choice(["docker"]),
    required=True,
    help="Application type of the built artifacts you want to build",
)
@click.option(
    "--graphscope-repo",
    required=False,
    help="GraphScope code repo location.",
)
def insight(app, graphscope_repo):
    """Build GraphScope Insight for BI analysis scenarios"""
    if graphscope_repo is None:
        graphscope_repo = default_graphscope_repo_path
    insight_build_dir = os.path.join(graphscope_repo, "k8s")
    if not os.path.exists(insight_build_dir) or not os.path.isdir(insight_build_dir):
        click.secho(
            f"No such file or directory {insight_build_dir}, try --graphscope-repo param.",
            fg="red",
        )
        return
    cmd = ["make", "graphscope-store", "ENABLE_COORDINATOR=true"]
    sys.exit(run_shell_cmd(cmd, os.path.join(graphscope_repo, insight_build_dir)))


@flexbuild.command()
@click.option(
    "--app",
    type=click.Choice(["docker"]),
    required=True,
    help="Application type of the built artifacts you want to build",
)
@click.option(
    "--graphscope-repo",
    required=False,
    help="GraphScope code repo location.",
)
def interactive(app, graphscope_repo):
    """Build Interactive for high throughput scenarios"""
    if graphscope_repo is None:
        graphscope_repo = default_graphscope_repo_path
    interactive_build_dir = os.path.join(graphscope_repo, "k8s")
    if not os.path.exists(interactive_build_dir) or not os.path.isdir(
        interactive_build_dir
    ):
        click.secho("[FAILED] ", nl=False, fg="red", bold=True)
        click.secho(
            f"No such file or directory {interactive_build_dir}, try --graphscope-repo param.",
        )
        return
    cmd = ["make", "flex-interactive", "ENABLE_COORDINATOR=true"]
    sys.exit(run_shell_cmd(cmd, os.path.join(graphscope_repo, interactive_build_dir)))


@instance.command
@click.option(
    "--type",
    type=click.Choice(["interactive"], case_sensitive=False),
    help="Instance type, e.g. Interactive",
    required=True,
)
@click.option(
    "--coordinator-port",
    help="Mapping port of Coordinator [docker only]",
    default=8080,
    show_default=True,
    required=False,
)
@click.option(
    "--admin-port",
    help="Mapping port of Interactive Admin service [docker only]",
    default=7777,
    show_default=True,
    required=False,
)
@click.option(
    "--storedproc-port",
    help="Mapping port of stored procedure query [docker only]",
    default=10000,
    show_default=True,
    required=False,
)
@click.option(
    "--cypher-port",
    help="Mapping port of cypher query [docker only]",
    default=7687,
    show_default=True,
    required=False,
)
@click.option(
    "--interactive-config",
    help="Interactive config file path [docker only]",
    required=False,
    default=None,
)
@click.option(
    "--gremlin-port",
    help="Mapping port of gremlin query, -1 means disable mapping [docker only]",
    default=-1,
    show_default=True,
    required=False,
)
@click.option(
    "--container-name",
    help="Docker container name [docker only]",
    default=INTERACTIVE_DOCKER_CONTAINER_NAME,
    show_default=True,
    required=False,
)
@click.option(
    "--image-registry",
    help="Docker image registry used to launch instance",
    default="registry.cn-hongkong.aliyuncs.com/graphscope",
    show_default=True,
    required=False,
)
@click.option(
    "--image-tag",
    help="Docker image tag used to launch instance",
    default=__version__,
    show_default=True,
    required=False,
)
def deploy(
    type,
    container_name,
    image_registry,
    image_tag,
    coordinator_port,
    admin_port,
    storedproc_port,
    cypher_port,
    gremlin_port,
    interactive_config,
):  # noqa: F811
    """Deploy a GraphScope Flex instance"""
    cmd = []
    if type == "interactive":
        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--label",
            INTERACTIVE_DOCKER_CONTAINER_LABEL,
            "-p",
            f"{coordinator_port}:8080",
            "-p",
            f"{admin_port}:7777",
            "-p",
            f"{storedproc_port}:10000",
            "-p",
            f"{cypher_port}:7687",
        ]
        if gremlin_port != -1:
            cmd.extend(["-p", f"{gremlin_port}:8182"])
        image = f"{image_registry}/{type}:{image_tag}"
        if interactive_config is not None:
            if not os.path.isfile(interactive_config):
                click.secho(
                    f"Interactive config file {interactive_config} does not exist.",
                    fg="red",
                )
                return
            interactive_config = os.path.abspath(interactive_config)
            cmd.extend(
                ["-v", f"{interactive_config}:{INTERACTIVE_DOCKER_DEFAULT_CONFIG_PATH}"]
            )
        cmd.extend([image, "--enable-coordinator"])
    returncode = run_shell_cmd(cmd, os.getcwd())
    if returncode == 0:
        message = f"""
Coordinator is listening on {coordinator_port} port, you can connect to coordinator by:
    gsctl connect --coordinator-endpoint http://127.0.0.1:{coordinator_port}

Interactive service is ready, you can connect to the interactive service with interactive sdk:
Interactive Admin service is listening at
    http://127.0.0.1:{admin_port},
You can connect to admin service with Interactive SDK, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
"""
        if gremlin_port != -1:
            message += f"""
    export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
"""
        message += """
############################################################################################

See https://graphscope.io/docs/latest/flex/interactive/development/java/java_sdk and
https://graphscope.io/docs/latest/flex/interactive/development/python/python_sdk for more details
about the usage of Interactive SDK.

Apart from interactive sdk, you can also use neo4j native tools(like cypher-shell) to connect to cypher endpoint,
and gremlin console to connect to gremlin endpoint.
        """
        click.secho("[SUCCESS] ", nl=False, fg="green", bold=True)
        click.secho(message, bold=False, fg="blue")


@instance.command
@click.option(
    "--type",
    type=click.Choice(["interactive"], case_sensitive=False),
    help="Instance type, e.g. Interactive",
    required=True,
)
@click.option(
    "--container-name",
    help="Docker container name [docker only]",
    default=INTERACTIVE_DOCKER_CONTAINER_NAME,
    show_default=True,
    required=False,
)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    default=False,
    help="Do not ask for confirmation",
    required=False,
)
def destroy(type, container_name, yes):
    """Destroy Flex Interactive instance"""
    if yes or click.confirm(f"Do you want to destroy {container_name} instance?"):
        cmd = []
        if type == "interactive":
            cmd = [
                "docker",
                "rm",
                "-f",
                container_name,
            ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            click.secho("[SUCCESS] ", nl=False, fg="green", bold=True)
            click.secho(result.stdout, bold=False)
        else:
            click.secho("[FAILED] ", nl=False, fg="red", bold=True)
            click.secho(result.stderr, bold=False)


@instance.command
@click.option(
    "--type",
    type=click.Choice(["interactive"], case_sensitive=False),
    help="Instance type, e.g. Interactive",
    required=True,
)
def status(type):
    """Display instance status"""
    cmd = []
    if type == "interactive":
        cmd = [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label={INTERACTIVE_DOCKER_CONTAINER_LABEL}",
        ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        click.secho("[SUCCESS]\n", fg="green", bold=True)
        click.secho(result.stdout, bold=False)
    else:
        click.secho("[FAILED] ", nl=False, fg="red", bold=True)
        click.secho(result.stderr, bold=False)


@cli.command()
@click.argument(
    "type",
    type=click.Choice(
        [
            "dev",
            "dev-analytical",
            "dev-analytical-java",
            "dev-interactive",
            "dev-learning",
        ],
        case_sensitive=False,
    ),
    required=True,
)
@click.option(
    "--graphscope-repo",
    required=False,
    help="GraphScope code repo location.",
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
    help="Specify the directory on disk to which the file will be installed",
)
@click.option(
    "--v6d-version",
    default=__v6d_version__,
    show_default=True,
    help="vineyard version",
)
@click.option(
    "--no-v6d",
    is_flag=True,
    default=False,
    help="Do not install vineyard, could be used with analytical type",
)
def install_deps(
    type,
    graphscope_repo,
    cn,
    install_prefix,
    v6d_version,
    no_v6d,
):
    """Install dependencies for building GraphScope."""
    cmd = [
        "bash",
        "-e",
        install_deps_script,
        type,
        "--install-prefix",
        install_prefix,
        "--v6d-version",
        str(v6d_version),
    ]
    if no_v6d:
        cmd.append("--no-v6d")
    if cn:
        cmd.append("--cn")
    sys.exit(run_shell_cmd(cmd, graphscope_repo))


if __name__ == "__main__":
    cli()
