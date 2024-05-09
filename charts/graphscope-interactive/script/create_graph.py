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
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.schema_mapping import SchemaMapping
from gs_interactive.models.create_procedure_request import CreateProcedureRequest
from gs_interactive.models.start_service_request import StartServiceRequest
from gs_interactive.models.schema_mapping_loading_config import (
    SchemaMappingLoadingConfig,
    SchemaMappingLoadingConfigFormat,
)
from gs_interactive.models.vertex_mapping import VertexMapping
from gs_interactive.models.edge_mapping import EdgeMapping
from gs_interactive.models.edge_mapping_type_triplet import EdgeMappingTypeTriplet
from gs_interactive.models.schema_mapping_loading_config_data_source import (
    SchemaMappingLoadingConfigDataSource,
)


huoyan_graph = {
    "version": "v0.1",
    "name": "onecompany",
    "store_type": "mutable_csr",
    "schema": {
        "vertex_types": [
            {
                "type_name": "company",
                "x_csr_params": {"max_vertex_num": 500000000},
                "properties": [
                    {
                        "property_name": "vertex_id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "vertex_name",
                        # "property_type": {"string": {"var_char": {"max_length": 64}}},
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["vertex_id"],
            },
            {
                "type_name": "person",
                "x_csr_params": {"max_vertex_num": 500000000},
                "properties": [
                    {
                        "property_name": "vertex_id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "vertex_name",
                        # "property_type": {"string": {"var_char": {"max_length": 64}}},
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["vertex_id"],
            },
        ],
        "edge_types": [
            {
                "type_name": "invest",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "company",
                        "destination_vertex": "company",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "rel_type",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    }
                ],
            },
            {
                "type_name": "personInvest",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "company",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "rel_type",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    }
                ],
            },
        ],
    },
}

# huoyan_graph_source = {
#     "graph": "onecompany",
#     "loading_config": {
#         "data_source": {
#             "scheme": "odps",
#         },
#         "import_option": "init",
#         "format": {
#             "type": "arrow",
#             "metadata": {"batch_reader": False, "quoting": True, "delimiter": ","},
#         },
#     },
#     "vertex_mappings": [
#         {
#             "type_name": "company",
#             "inputs": ["grape_dev/lei_huoyan_company/ds=20240613"],
#             "column_mappings": [
#                 {"column": {"index": 0, "name": "vertex_id"}, "property": "vertex_id"},
#                 {
#                     "column": {"index": 1, "name": "vertex_name"},
#                     "property": "vertex_name",
#                 },
#             ],
#         },
#         {
#             "type_name": "person",
#             "inputs": ["grape_dev/lei_huoyan_person/ds=20240613"],
#             "column_mappings": [
#                 {"column": {"index": 0, "name": "vertex_id"}, "property": "vertex_id"},
#                 {
#                     "column": {"index": 1, "name": "vertex_name"},
#                     "property": "vertex_name",
#                 },
#             ],
#         },
#     ],
#     "edge_mappings": [
#         {
#             "type_triplet": {
#                 "edge": "invest",
#                 "source_vertex": "company",
#                 "destination_vertex": "company",
#             },
#             "inputs": ["grape_dev/lei_huoyan_company_invest/ds=20240613"],
#             "source_vertex_mappings": [
#                 {"column": {"index": 0, "name": "src"}, "property": "vertex_id"}
#             ],
#             "destination_vertex_mappings": [
#                 {"column": {"index": 1, "name": "dst"}, "property": "vertex_id"}
#             ],
#             "column_mappings": [
#                 {"column": {"index": 2, "name": "rel_type"}, "property": "rel_type"}
#             ],
#         },
#         {
#             "type_triplet": {
#                 "edge": "personInvest",
#                 "source_vertex": "person",
#                 "destination_vertex": "company",
#             },
#             "inputs": ["grape_dev/lei_huoyan_person_invest/ds=20240613"],
#             "source_vertex_mappings": [
#                 {"column": {"index": 0, "name": "src"}, "property": "vertex_id"}
#             ],
#             "destination_vertex_mappings": [
#                 {"column": {"index": 1, "name": "dst"}, "property": "vertex_id"}
#             ],
#             "column_mappings": [
#                 {"column": {"index": 2, "name": "rel_type"}, "property": "rel_type"}
#             ],
#         },
#     ],
# }


def create_graph(sess: Session):
    graph_id = sess.create_graph(CreateGraphRequest.from_dict(huoyan_graph))
    assert graph_id.is_ok()
    graph_id = graph_id.get_value()
    print(f"create graph {graph_id} successfully")
    return graph_id.graph_id


def loading_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping(
        loading_config=SchemaMappingLoadingConfig(
            data_source=SchemaMappingLoadingConfigDataSource(scheme="odps"),
            import_option="init",
            format=SchemaMappingLoadingConfigFormat(
                type="arrow",
                metadata={"batch_reader": False},
            ),
        ),
        vertex_mappings=[
            VertexMapping(
                type_name="company",
                inputs=["grape_dev/lei_huoyan_company/ds=20240613"],
                column_mappings=[
                    ColumnMapping(
                        var_property="vertex_id",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=0, name="vertex_id"
                        ),
                    ),
                    ColumnMapping(
                        var_property="vertex_name",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=1, name="vertex_name"
                        ),
                    ),
                ],
            ),
            VertexMapping(
                type_name="person",
                inputs=["grape_dev/lei_huoyan_person/ds=20240613"],
                column_mappings=[
                    ColumnMapping(
                        var_property="vertex_id",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=0, name="vertex_id"
                        ),
                    ),
                    ColumnMapping(
                        var_property="vertex_name",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=1, name="vertex_name"
                        ),
                    ),
                ],
            ),
        ],
        edge_mappings=[
            EdgeMapping(
                type_triplet=EdgeMappingTypeTriplet(
                    edge="invest",
                    source_vertex="company",
                    destination_vertex="company",
                ),
                inputs=["grape_dev/lei_huoyan_company_invest/ds=20240613"],
                column_mappings=[
                    ColumnMapping(
                        var_property="rel_type",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="rel_type"
                        ),
                    ),
                ],
                source_vertex_mappings=[
                    EdgeMappingSourceVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=0, name="src"
                        ),
                        var_property="vertex_id",
                    )
                ],
                destination_vertex_mappings=[
                    EdgeMappingDestinationVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=1, name="dst"
                        ),
                        var_property="vertex_id",
                    )
                ],
            ),
            EdgeMapping(
                type_triplet=EdgeMappingTypeTriplet(
                    edge="personInvest",
                    source_vertex="person",
                    destination_vertex="company",
                ),
                inputs=["grape_dev/lei_huoyan_person_invest/ds=20240613"],
                column_mappings=[
                    ColumnMapping(
                        var_property="rel_type",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="rel_type"
                        ),
                    )
                ],
                source_vertex_mappings=[
                    EdgeMappingSourceVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=0, name="src"
                        ),
                        var_property="vertex_id",
                    )
                ],
                destination_vertex_mappings=[
                    EdgeMappingDestinationVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=1, name="dst"
                        ),
                        var_property="vertex_id",
                    )
                ],
            ),
        ],
    )
    resp = sess.bulk_loading(graph_id, schema_mapping)
    if not resp.is_ok():
        print("resp: ", resp.get_status_message())
    assert resp.is_ok()
    print(f"create loading job successfully: {resp.get_value().job_id}")
    return resp.get_value().job_id


def wait_job_finish(sess: Session, job_id: str):
    while True:
        resp = sess.get_job(job_id)
        assert resp.is_ok()
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            break
        elif status == "FAILED":
            print("job failed: ", resp.get_value())
            raise Exception("job failed")
        else:
            time.sleep(1)
    print("Finish loading graph: ", job_id)


def create_procedure(sess: Session, graph_id: str, file_path: str, proc_name):
    # read file into string
    with open(file_path, "r") as f:
        content = f.read()
    resp = sess.create_procedure(
        graph_id,
        CreateProcedureRequest(
            name=proc_name, description="huo yan app", query=content, type="cpp"
        ),
    )
    assert resp.is_ok()
    print("create procedure successfully: ", resp.get_value())


def restart_service(sess: Session, graph_id: str):
    resp = sess.start_service(
        start_service_request=StartServiceRequest(graph_id=graph_id)
    )
    assert resp.is_ok()
    print("restart service successfully")


if __name__ == "__main__":
    # parse command line args
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")
    parser.add_argument("--proc-name", type=str, default="huoyan")
    # finish
    args = parser.parse_args()
    print(args)
    print("connecting to ", args.endpoint)
    driver = Driver(args.endpoint)
    sess = driver.session()
    graph_id = create_graph(sess)
    print("-----------------Finish creating graph-----------------")
    print("graph_id: ", graph_id)

    job_id = loading_graph(sess, graph_id)
    wait_job_finish(sess, job_id)
    print("-----------------Finish loading graph-----------------")

    create_procedure(sess, graph_id, "procedure.cc", args.proc_name)
    print("-----------------Finish creating procedure-----------------")

    restart_service(sess, graph_id)
