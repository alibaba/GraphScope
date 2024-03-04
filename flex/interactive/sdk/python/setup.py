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

NAME = "gs_flex_interactive"
VERSION = "1.0.0"

pkg_root = os.path.dirname(os.path.abspath(__file__))


# def parsed_reqs():
#     with open(os.path.join(pkg_root, "requirements.txt"), "r", encoding="utf-8") as fp:
#         return fp.read().splitlines()


class GenerateInteractiveSDK(Command):
    description = "Generate GraphScope Interactive python sdk from openapi specification file"
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
            pkg_root, "interactive_sdk"
        )
        if os.path.exists(targetdir):
            shutil.rmtree(targetdir)
        # generate
        specification = os.path.join(
            pkg_root, "..", "..", "..", "openapi", "openapi_interactive.yaml"
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
            "interactive_sdk",
        ]
        print(" ".join(cmd))
        subprocess.check_call(
            cmd,
            env=os.environ.copy(),
        )
        # cp
        subprocess.run(["cp", "-r", os.path.join(tempdir, "interactive_sdk"), targetdir])


setup(
    name=NAME,
    version=VERSION,
    description="GraphScope Flex Interactive Python SDK",
    author_email="graphscope@alibaba-inc.com",
    url="",
    keywords=["OpenAPI", "GraphScope Flex Interactive Python SDK"],
    # install_requires=parsed_reqs(),
    packages=find_packages(),
    package_data={"": ["openapi/openapi.yaml"]},
    cmdclass={
        "generate_interactive_sdk": GenerateInteractiveSDK,
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    include_package_data=True,
    long_description="""\
    GraphScope Flex Interactive Python SDK
    """,
)
