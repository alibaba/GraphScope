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

import click
from packaging import version

version_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "VERSION"
)

with open(version_file_path, "r", encoding="utf-8") as fp:
    sv = version.parse(fp.read().strip())
    __is_prerelease__ = sv.is_prerelease
    __version__ = str(sv)


# Interactive docker container config
INTERACTIVE_DOCKER_CONTAINER_NAME = "gs-interactive-instance"
INTERACTIVE_DOCKER_CONTAINER_LABEL = "flex=interactive"


scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "scripts")
install_deps_script = os.path.join(scripts_dir, "install_deps_command.sh")
make_script = os.path.join(scripts_dir, "make_command.sh")
make_image_script = os.path.join(scripts_dir, "make_image_command.sh")
test_script = os.path.join(scripts_dir, "test_command.sh")
default_graphscope_repo_path = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "..",
    "..",
    "..",
)


def run_shell_cmd(cmd, workingdir):
    """wrapper function to run a shell command/scripts."""
    click.echo(f"run a shell command on cwd={workingdir}. \ncmd=\"{' '.join(cmd)}\"")
    proc = subprocess.Popen(
        cmd, cwd=workingdir, env=os.environ.copy(), stdout=subprocess.PIPE
    )
    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        print(line.rstrip())


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
    """Deploy, destroy Interactive instance."""
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
    run_shell_cmd(cmd, os.path.join(graphscope_repo, insight_build_dir))


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
    interactive_build_dir = os.path.join(
        graphscope_repo, "flex", "interactive", "docker"
    )
    if not os.path.exists(interactive_build_dir) or not os.path.isdir(
        interactive_build_dir
    ):
        click.secho(
            f"No such file or directory {interactive_build_dir}, try --graphscope-repo param.",
            fg="red",
        )
        return
    cmd = ["make", "interactive-runtime", "ENABLE_COORDINATOR=true"]
    run_shell_cmd(cmd, os.path.join(graphscope_repo, interactive_build_dir))


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
):  # noqa: F811
    """Deploy Flex Interactive instance"""
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
        cmd.extend([image, "--enable-coordinator"])
    click.secho("Run command: {0}".format(" ".join(cmd)))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        click.secho("[SUCCESS] ", nl=False, fg="green", bold=True)
        click.secho(result.stdout, bold=False)

        message = f"""
Coordinator is listening on {coordinator_port} port, you can connect to coordinator by:
    gsctl connect --coordinator-endpoint http://127.0.0.1:{coordinator_port}

Interactive service is ready, you can connect to the interactive service with interactive sdk:
Interactive Admin service is listening at
    http://127.0.0.1{admin_port},
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
        click.secho(message, bold=False)
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
@click.option(
    "--container-name",
    help="Docker container name [docker only]",
    default=INTERACTIVE_DOCKER_CONTAINER_NAME,
    show_default=True,
    required=False,
)
def destroy(type, container_name):
    """Destroy Flex Interactive instance"""
    if click.confirm(f"Do you want to destroy {container_name} instance?"):
        cmd = []
        if type == "interactive":
            cmd = [
                "docker",
                "rm",
                "-f",
                container_name,
            ]
        click.secho("Run command: {0}".format(" ".join(cmd)))
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


@click.command()
@click.argument(
    "type",
    type=click.Choice(
        ["dev", "client"],
        case_sensitive=False,
    ),
    required=True,
)
@click.option(
    "--graphscope-repo",
    envvar="GRAPHSCOPE_REPO",
    type=click.Path(),
    default=os.path.abspath("."),
    show_default=True,
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
    help="Install built binaries to customized location.",
)
@click.option(
    "--from-local",
    type=click.Path(),
    default="/tmp/gs-local-deps",
    show_default=True,
    help="""Find raw dependencies of GraphScope from a local directory. The raw
    dependencies would then be built and installed to [prefix]. If the directory
    is empty or not exists, dependency files would be downloaded to [directory].""",
)
@click.option(
    "--v6d-version",
    default="main",
    show_default=True,
    help="v6d version to clone.",
)
@click.option(
    "-j",
    "--jobs",
    default="2",
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
    is_flag=True,
    default=False,
    help="Do not install v6d, for build base docker images, could only be used with '--for-analytical'",
)
def install_deps(
    type,
    graphscope_repo,
    cn,
    install_prefix,
    from_local,
    v6d_version,
    jobs,
    for_analytical,
    no_v6d,
):
    """Install dependencies for building GraphScope."""
    cmd = [
        "bash",
        "-e",
        install_deps_script,
        "-t",
        type,
        "-i",
        install_prefix,
        "-d",
        from_local,
        "-v",
        str(v6d_version),
        "-j",
        str(jobs),
    ]
    if for_analytical:
        cmd.append("--for-analytical")
    if no_v6d:
        if not for_analytical:
            # could only be used with '--for-analytical'
            raise RuntimeError("Missing --for-analytical with --no-v6d parameter")
        cmd.append("--no-v6d")
    if cn:
        cmd.append("--cn")
    run_shell_cmd(cmd, graphscope_repo)


@click.command()
@click.argument(
    "component",
    type=click.Choice(
        [
            "interactive",
            "interactive-install",
            "analytical",
            "analytical-java-install",
            "analytical-install",
            "learning",
            "learning-install",
            "coordinator",
            "client",
            "clean",
            "all",
        ],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--graphscope-repo",
    envvar="GRAPHSCOPE_REPO",
    type=click.Path(),
    default=os.path.abspath("."),
    show_default=True,
    help="GraphScope code repo location.",
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
    default="default",
    help="Make gie with specified storage type.",
)
def make(component, graphscope_repo, install_prefix, storage_type):
    """Build executive binaries of COMPONENT. If not given a specific component, build all.
    \f
    TODO: maybe without make?
    """
    click.secho(
        "Before making artifacts, please manually source ENVs from ~/.graphscope_env.",
        fg="yellow",
    )
    click.secho(
        f"Begin the make command, to build components [{component}] of GraphScope, with repo = {graphscope_repo}",
        fg="green",
    )
    if component is None:
        component = "all"

    cmd = [
        "bash",
        "-e",
        make_script,
        "-c",
        component,
        "-i",
        install_prefix,
        "-s",
        storage_type,
    ]
    run_shell_cmd(cmd, graphscope_repo)


@click.command()
@click.argument(
    "component",
    type=click.Choice(
        [
            "all",
            "graphscope-dev",
            "coordinator",
            "analytical",
            "analytical-java",
            "interactive",
            "interactive-frontend",
            "interactive-executor",
            "learning,",
            "vineyard-dev",
            "vineyard-runtime",
            "manylinux2014-ext",
        ],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--graphscope-repo",
    envvar="GRAPHSCOPE_REPO",
    type=click.Path(),
    default=os.path.abspath("."),
    show_default=True,
    help="GraphScope code repo location.",
)
@click.option(
    "--tag",
    default="latest",
    show_default=True,
    help="image tag name to build",
)
@click.option(
    "--registry",
    default="registry.cn-hongkong.aliyuncs.com",
    show_default=True,
    help="registry name",
)
def make_image(component, graphscope_repo, registry, tag):
    """Make docker images from source code for deployment.
    \f
    TODO: fulfill this.
    """
    if component is None:
        component = "all"

    cmd = ["bash", "-e", make_image_script, "-c", component, "-r", registry, "-t", tag]
    run_shell_cmd(cmd, graphscope_repo)


@click.command()
@click.argument(
    "type",
    type=click.Choice(
        [
            "analytical",
            "analytical-java",
            "interactive",
            "learning",
            "local-e2e",
            "k8s-e2e",
            "groot",
        ],
        case_sensitive=False,
    ),
    required=False,
)
@click.option(
    "--graphscope-repo",
    envvar="GRAPHSCOPE_REPO",
    type=click.Path(),
    default=os.path.abspath("."),
    show_default=True,
    help="GraphScope code repo location.",
)
@click.option(
    "--testdata",
    type=click.Path(),
    default="/tmp/gstest",
    show_default=True,
    help="""assign a custom test data location. This could be cloned from
    https://github.com/graphscope/gstest""",
)
@click.option(
    "--local",
    is_flag=True,
    default=False,
    help="Run local tests",
)
@click.option(
    "--storage-type",
    default="default",
    show_default=True,
    help="test gie with specified storage type",
)
@click.option(
    "--k8s",
    is_flag=True,
    default=False,
    help="Run local tests",
)
@click.option(
    "--nx",
    is_flag=True,
    default=False,
    help="Run nx tests",
)
def test(type, graphscope_repo, testdata, local, storage_type, k8s, nx):
    """Trigger tests on built artifacts.

    \f
    TODO: fulfill this."""
    click.secho(f"graphscope_repo = {graphscope_repo}", fg="green")
    click.echo("test")
    if type is None:
        type = ""
    cmd = [
        "bash",
        "-e",
        test_script,
        "-t",
        type,
        "-d",
        testdata,
        "-l",
        str(local),
        "-s",
        storage_type,
        "-k",
        str(k8s),
        "-n",
        str(nx),
    ]
    run_shell_cmd(cmd, graphscope_repo)


cli.add_command(install_deps)
cli.add_command(make)
cli.add_command(make_image)
cli.add_command(test)

if __name__ == "__main__":
    cli()
