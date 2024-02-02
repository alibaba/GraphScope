#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import os
import shutil
import subprocess
import sys
import tempfile
from distutils.cmd import Command

from setuptools import find_packages, setup

NAME = "gs_flex_coordinator"
VERSION = "1.0.0"

pkg_root = os.path.dirname(os.path.abspath(__file__))


def parsed_reqs():
    with open(os.path.join(pkg_root, "requirements.txt"), "r", encoding="utf-8") as fp:
        return fp.read().splitlines()


class GenerateFlexServer(Command):
    description = "generate flex server from openapi specification file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # generate server code, note that controllers are not included here,
        # see from .openapi-generator-ignore
        specification = os.path.join(
            pkg_root, "..", "openapi", "openapi_coordinator.yaml"
        )
        cmd = [
            "openapi-generator-cli",
            "generate",
            "-g",
            "python-flask",
            "-i",
            str(specification),
            "-o",
            str(pkg_root),
            "--package-name",
            "gs_flex_coordinator",
        ]
        print(" ".join(cmd))
        subprocess.check_call(
            cmd,
            env=os.environ.copy(),
        )


class GenerateInteractiveSDK(Command):
    description = "generate interactive client sdk from openapi specification file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # remove
        tempdir = os.path.join("/", tempfile.gettempprefix(), "flex_interactive")
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        targetdir = os.path.join(
            pkg_root, "gs_flex_coordinator", "core", "interactive", "hqps_client"
        )
        if os.path.exists(targetdir):
            shutil.rmtree(targetdir)
        # generate
        specification = os.path.join(
            pkg_root, "..", "openapi", "openapi_interactive.yaml"
        )
        cmd = [
            "openapi-generator-cli",
            "generate",
            "-g",
            "python",
            "-i",
            str(specification),
            "-o",
            str(tempdir),
            "--package-name",
            "hqps_client",
        ]
        print(" ".join(cmd))
        subprocess.check_call(
            cmd,
            env=os.environ.copy(),
        )
        # cp
        subprocess.run(["cp", "-r", os.path.join(tempdir, "hqps_client"), targetdir])


setup(
    name=NAME,
    version=VERSION,
    description="GraphScope FLEX HTTP SERVICE API",
    author_email="graphscope@alibaba-inc.com",
    url="",
    keywords=["OpenAPI", "GraphScope FLEX HTTP SERVICE API"],
    install_requires=parsed_reqs(),
    packages=find_packages(),
    package_data={"": ["openapi/openapi.yaml"]},
    cmdclass={
        "generate_flex_server": GenerateFlexServer,
        "generate_interactive_sdk": GenerateInteractiveSDK,
    },
    include_package_data=True,
    entry_points={
        "console_scripts": ["gs_flex_coordinator=gs_flex_coordinator.__main__:main"]
    },
    long_description="""\
    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)
    """,
)
