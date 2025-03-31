#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
import sys

from setuptools import find_packages
from setuptools import setup

NAME = "gs_interactive_admin"
VERSION = "0.3"
repo_root = os.path.dirname(os.path.abspath(__file__))

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools


def parsed_reqs():
    with open(os.path.join(repo_root, "requirements.txt"), "r", encoding="utf-8") as fp:
        pkgs = fp.read().splitlines()
        return pkgs


setup(
    name=NAME,
    version=VERSION,
    description="GraphScope Interactive API v0.3",
    author_email="graphscope@alibaba-inc.com",
    url="",
    keywords=["OpenAPI", "GraphScope Interactive API v0.3"],
    install_requires=parsed_reqs(),
    packages=find_packages(exclude=["test", "tests"]),
    package_data={"": ["openapi/openapi.yaml", "VERSION"]},
    include_package_data=True,
    license="Apache 2.0",
    entry_points={
        "console_scripts": ["gs_interactive_admin=gs_interactive_admin.__main__:main"]
    },
    long_description="""\
    This is the definition of GraphScope Interactive API, including   - AdminService API   - Vertex/Edge API   - QueryService   AdminService API (with tag AdminService) defines the API for GraphManagement, ProcedureManagement and Service Management.  Vertex/Edge API (with tag GraphService) defines the API for Vertex/Edge management, including creation/updating/delete/retrieve.  QueryService API (with tag QueryService) defines the API for procedure_call, Ahodc query.
    """,  # noqa: E501
)
