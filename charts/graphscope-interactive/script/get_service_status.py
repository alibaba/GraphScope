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


import sys

sys.path.append("../../../flex/interactive/sdk/python/")
import time

from gs_interactive.models.edge_mapping_destination_vertex_mappings_inner import (
    EdgeMappingDestinationVertexMappingsInner,
)

import gs_interactive
from gs_interactive.models.column_mapping import ColumnMapping
from gs_interactive.models.edge_mapping_source_vertex_mappings_inner import (
    EdgeMappingSourceVertexMappingsInner,
)
from gs_interactive.models.edge_mapping_source_vertex_mappings_inner_column import (
    EdgeMappingSourceVertexMappingsInnerColumn,
)
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *


def restart_service(sess: Session, graph_id: str):
    resp = sess.start_service(
        start_service_request=StartServiceRequest(graph_id=graph_id)
    )
    assert resp.is_ok()
    print("restart service successfully")


def get_service_status(sess: Session):
    resp = sess.get_service_status()
    assert resp.is_ok()
    print("service status: ", resp.get_value())
    status = resp.get_value()
    print("service running is now running on graph", status.graph.id)


def get_current_running_graph(sess: Session):
    resp = sess.get_service_status()
    assert resp.is_ok()
    status = resp.get_value()
    return status.graph.id


def list_graph(sess: Session):
    resp = sess.list_graphs()
    assert resp.is_ok()
    res = resp.get_value()
    graph_id_arr = [graph.id for graph in res]
    print("list graph: ", graph_id_arr)


if __name__ == "__main__":
    # parse command line args
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")

    # finish
    args = parser.parse_args()
    print(args)
    print("connecting to ", args.endpoint)
    driver = Driver(args.endpoint)
    sess = driver.session()
    # get current running graph

    get_service_status(sess)
    print("-----------------Finish getting service status-----------------")

    list_graph(sess)
