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

import datetime
import logging
import os
from typing import List, Union

import interactive_sdk
from interactive_sdk import (
    Graph,
    JobResponse,
    JobStatus,
    Procedure,
    SchemaMapping,
    Service,
)

logger = logging.getLogger("graphscope")



def main():
    api_client = interactive_sdk.ApiClient()
    api_instance = interactive_sdk.AdminServiceGraphManagementApi(api_client)
    response = api_instance.list_graphs()
    print(response)

    # create graph
    graph = Graph.from_dict({"name": "test_graph", "store_type": "mutable_csr"})
    response = api_instance.create_graph(graph)
    print(response)

    # list graph
    response = api_instance.get_schema("test_graph")
    print(response)


if __name__ == "__main__":
    main()
