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

from setuptools import find_packages  # noqa: H301
from setuptools import setup

pkg_root = os.path.dirname(os.path.abspath(__file__))


def parse_version(root, **kwargs):
    """
    Parse function for setuptools_scm that first tries to read '../VERSION' file
    to get a version number.
    """
    from setuptools_scm.git import parse
    from setuptools_scm.version import meta

    version_file = os.path.join(pkg_root, "..", "VERSION")
    if os.path.isfile(version_file):
        with open(version_file, "r", encoding="utf-8") as fp:
            return meta(fp.read().strip())
    return parse(root, **kwargs)


NAME = "gsctl"
PYTHON_REQUIRES = ">=3.7"
REQUIRES = ["click >= 8.1.6", "graphscope-flex >= 0.27.0", "treelib"]

setup(
    name=NAME,
    description="Command line tool for GraphScope",
    author="GraphScope",
    author_email="graphscope@alibaba-inc.com",
    url="",
    keywords=["GraphScope", "Command-line tool"],
    use_scm_version={
        "root": pkg_root,
        "parse": parse_version,
    },
    install_requires=REQUIRES,
    packages=find_packages(
        include=["graphscope.gsctl", "graphscope.gsctl.*"]
    ),
    include_package_data=True,
    license="Apache 2.0",
    long_description_content_type="text/markdown",
    long_description="""\
    gsctl is a command-line utility for GraphScope. It provides a set of functionalities to make it easy to use GraphScope. These functionalities include building and testing binaries, managing sessions and resources, and more.
    """,  # noqa: E501
    package_data={"graphscope.gsctl": ["scripts/*.sh", "scripts/lib/*.sh", "VERSION"]},
    entry_points={
        "console_scripts": [
            "gsctl = graphscope.gsctl.gsctl:cli",
        ],
    },
)
