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

"""
    GraphScope FLEX HTTP SERVICE API

    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)

    The version of the OpenAPI document: 0.9.1
    Contact: graphscope@alibaba-inc.com
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from setuptools import find_packages  # noqa: H301
from setuptools import setup

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools
NAME = "graphscope-flex"
VERSION = "0.9.1"
PYTHON_REQUIRES = ">=3.7"
REQUIRES = [
    "urllib3 >= 1.25.3, < 2.1.0",
    "python-dateutil",
    "pydantic >= 2",
    "typing-extensions >= 4.7.1",
]

setup(
    name=NAME,
    version=VERSION,
    description="GraphScope FLEX HTTP SERVICE API",
    author="GraphScope",
    author_email="graphscope@alibaba-inc.com",
    url="",
    keywords=["OpenAPI", "OpenAPI-Generator", "GraphScope FLEX HTTP SERVICE API"],
    install_requires=REQUIRES,
    packages=find_packages(include=["graphscope.flex.rest"]),
    include_package_data=True,
    license="Apache 2.0",
    long_description_content_type="text/markdown",
    long_description="""\
    This is a specification for GraphScope FLEX HTTP service based on the OpenAPI 3.0 specification. You can find out more details about specification at [doc](https://swagger.io/specification/v3/).  Some useful links: - [GraphScope Repository](https://github.com/alibaba/GraphScope) - [The Source API definition for GraphScope Interactive](https://github.com/GraphScope/portal/tree/main/httpservice)
    """,  # noqa: E501
    package_data={"graphscope.flex.rest": ["py.typed"]},
)
