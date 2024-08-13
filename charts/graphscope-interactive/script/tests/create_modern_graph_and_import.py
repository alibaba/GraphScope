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

# sys.path.append("../../../flex/interactive/sdk/python/")
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

import os

script_directory = os.path.dirname(os.path.abspath(__file__))
print("script directory", script_directory)

modern_graph = {
    "name": "modern_graph",
    "version": "v0.1",
    "store_type": "mutable_csr",
    "description": "A graph with 2 vertex types and 2 edge types",
    "schema": {
        "vertex_types": [
            {
                "type_id": 0,
                "type_name": "person",
                "description": "A person vertex type",
                "x_csr_params": {"max_vertex_num": 100},
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_id": 2,
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_id": 1,
                "type_name": "software",
                "description": "A software vertex type",
                "x_csr_params": {"max_vertex_num": 100},
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_id": 2,
                        "property_name": "lang",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
        ],
        "edge_types": [
            {
                "type_id": 0,
                "type_name": "knows",
                "description": "A knows edge type",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                        "x_csr_params": {"sort_on_compaction": "true"},
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
            },
            {
                "type_id": 1,
                "type_name": "created",
                "description": "A created edge type",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
            },
        ],
    },
}


def create_graph(sess: Session, ds: str, report_error: bool):
    # copied_huoyan_graph = huoyan_graph.copy()
    graph_name = f"onecompany_{ds}"
    create_graph_req = CreateGraphRequest.from_dict(modern_graph)
    create_graph_res = sess.create_graph(create_graph_req)
    # CreateGraphRequest.from_dict(copied_huoyan_graph)

    if not create_graph_res.is_ok():
        print("create graph failed: ", create_graph_res.get_status_message())
        raise Exception("fail to create graph")
    create_graph_resp = create_graph_res.get_value()
    print(
        f"create graph {create_graph_resp.graph_id} successfully with name graph_name"
    )
    return create_graph_resp.graph_id


def loading_graph(sess: Session, graph_id: str, report_error: bool):
    schema_mapping = SchemaMapping(
        loading_config=SchemaMappingLoadingConfig(
            data_source=SchemaMappingLoadingConfigDataSource(
                scheme="file",
                location="@//home/graphscope/work/k8s-test/gs/flex/interactive/examples/modern_graph/",
            ),
            import_option="init",
            format=SchemaMappingLoadingConfigFormat(
                type="csv",
                metadata={"batch_reader": True},
            ),
        ),
        vertex_mappings=[
            VertexMapping(
                type_name="person",
                inputs=[f"person.csv"],
            ),
            VertexMapping(
                type_name="software",
                inputs=[f"software.csv"],
            ),
        ],
        edge_mappings=[
            EdgeMapping(
                type_triplet=EdgeMappingTypeTriplet(
                    edge="knows",
                    source_vertex="person",
                    destination_vertex="person",
                ),
                inputs=["person_knows_person.csv"],
            ),
            EdgeMapping(
                type_triplet=EdgeMappingTypeTriplet(
                    edge="created",
                    source_vertex="person",
                    destination_vertex="software",
                ),
                inputs=["person_created_software.csv"],
            ),
        ],
    )
    resp = sess.bulk_loading(graph_id, schema_mapping)
    if not resp.is_ok():
        print("resp: ", resp.get_status_message())
        raise Exception("fail to create loading job")
    print(f"create loading job successfully: {resp.get_value().job_id}")
    return resp.get_value().job_id


def wait_job_finish(sess: Session, job_id: str):
    while True:
        resp = sess.get_job(job_id)
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            break
        elif status == "FAILED":
            print("job failed: ", resp.get_value())
            raise Exception("job failed")
        else:
            time.sleep(10)
    print("Finish loading graph: ", job_id)


def create_procedure(
    sess: Session, graph_id: str, file_path: str, proc_name, report_error: bool
):
    # read file into string
    with open(file_path, "r") as f:
        content = f.read()
    resp = sess.create_procedure(
        graph_id,
        CreateProcedureRequest(
            name=proc_name, description="huo yan app", query=content, type="cpp"
        ),
    )
    print("create procedure result: ", resp)
    if not resp.is_ok():
        raise Exception("fail to create procedure")


def restart_service(sess: Session, graph_id: str):
    resp = sess.start_service(
        start_service_request=StartServiceRequest(graph_id=graph_id)
    )
    if not resp.is_ok():
        print("restart service failed: ", resp.get_status_message())
    print("restart service successfully")


def get_service_status(sess: Session, report_error: bool):
    resp = sess.get_service_status()
    if not resp.is_ok():
        print("get service status failed: ", resp.get_status_message())
        raise Exception("fail to get service status")
    print("service status: ", resp.get_value())
    status = resp.get_value()
    print("service running is now running on graph", status.graph.id)


def get_current_running_graph(sess: Session, report_error: bool):
    resp = sess.get_service_status()
    if not resp.is_ok():
        print("get service status failed: ", resp.get_status_message())
        raise Exception("fail to get service status")
    status = resp.get_value()
    return status.graph.id


def list_graph(sess: Session, report_error: bool):
    resp = sess.list_graphs()
    if not resp.is_ok():
        print("list graph failed: ", resp.get_status_message())
        raise Exception("fail to list graph")
    res = resp.get_value()
    graph_id_arr = [graph.id for graph in res]
    print("list graph: ", graph_id_arr)


if __name__ == "__main__":
    # parse command line args
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")
    parser.add_argument("--proc-name", type=str, default="huoyan")
    # parser.add_argument("--remove-old-graph", type=bool, default=True)
    parser.add_argument("--ds", type=str)
    parser.add_argument("--validate-reporting", type=bool, default=False)
    parser.add_argument("--report-error", type=bool, default=False)

    # finish
    args = parser.parse_args()
    print(args)

    print("connecting to ", args.endpoint)

    report_error = args.report_error

    driver = Driver(args.endpoint)
    sess = driver.session()
    # get current running graph
    old_graph = get_current_running_graph(sess, report_error)
    print("-----------------Finish getting current running graph-----------------")
    print("old graph: ", old_graph)

    graph_id = create_graph(sess, report_error)
    print("-----------------Finish creating graph-----------------")
    print("graph_id: ", graph_id)

    job_id = loading_graph(sess, graph_id, report_error)
    wait_job_finish(sess, job_id)
    print("-----------------Finish loading graph-----------------")

    create_procedure(
        sess, graph_id, script_directory + "/procedure.cc", args.proc_name, report_error
    )
    print("-----------------Finish creating procedure-----------------")

    start_time = time.time()
    restart_service(sess, graph_id)
    end_time = time.time()
    execution_time = end_time - start_time
    print("-----------------Finish restarting service-----------------")
    print(f"restart service cost {execution_time:.6f}seconds")

    get_service_status(sess, report_error)
    print("-----------------Finish getting service status-----------------")

    # if args.remove_old_graph:
    #     print("remove old graph")
    #     delete_graph = sess.delete_graph(old_graph)
    #     print("delete graph res: ", delete_graph)
    # else:
    #     print("keep old graph", old_graph)

    list_graph(sess, report_error)

    print("Bulk loading modern graph finished successfully")
