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

import time
import hmac
import hashlib
import base64
import urllib.parse

query = """
SELECT  ds
            FROM    onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version
            WHERE   ds = MAX_PT("onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version");
"""
import os
from odps import ODPS

# Make sure environment variable ALIBABA_CLOUD_ACCESS_KEY_ID already set to acquired Access Key ID,
# environment variable ALIBABA_CLOUD_ACCESS_KEY_SECRET set to acquired Access Key Secret
# while environment variable ALIBABA_CLOUD_STS_TOKEN set to acquired STS token.
# Not recommended to hardcode Access Key ID or Access Key Secret in your code.
ODPS_KEY = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
ODPS_SECRETE = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
PROJECT = os.getenv("ODPS_PROJECT")
if ODPS_KEY is None:
    raise Exception("ODPS KEY not set")
if ODPS_SECRETE is None:
    raise Exception("ODPS SECRETE not set")
if PROJECT is None:
    raise Exception("project not set")
o = ODPS(
    ODPS_KEY,
    ODPS_SECRETE,
    project=PROJECT,
    endpoint="http://service-corp.odps.aliyun-inc.com/api",
)
script_directory = os.path.dirname(os.path.abspath(__file__))
print("script directory", script_directory)

uri = "https://oapi.dingtalk.com/robot/send?access_token="
# read token from ${HOME}/.dingtalk_token
token = ""
with open(os.path.expanduser("~/.dingtalk_token"), "r") as f:
    token = f.read().strip()
if token == "":
    raise Exception("token is empty")

secret = ""
with open(os.path.expanduser("~/.dingtalk_secret"), "r") as f:
    secret = f.read().strip()
if secret == "":
    raise Exception("secret is empty")


def get_full_uri():
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode("utf-8")
    string_to_sign = "{}\n{}".format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode("utf-8")
    hmac_code = hmac.new(
        secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    print(timestamp)
    print(sign)
    return uri + token + "&timestamp=" + timestamp + "&sign=" + sign


def report_message(message: str):
    uri = get_full_uri()
    print(uri)
    real_msg = {"msgtype": "text", "text": {"content": message}}
    print(real_msg)
    import requests

    headers = {"Content-Type": "application/json"}
    response = requests.post(uri, json=real_msg, headers=headers)
    print(response.text)


huoyan_graph = {
    "version": "v0.1",
    "name": "onecompany_group",
    "store_type": "mutable_csr",
    "stored_procedures": [
        {
            "name": "huoyan",
            "description": "A stored procedure that does something",
            "library": "/home/graphscope/work/gs/flex/interactive/examples/new_huoyan/plugins/libhuoyan.so",
            "type": "cpp",
        }
    ],
    "schema": {
        "vertex_types": [
            {
                "type_id": 0,
                "type_name": "company",
                "x_csr_params": {"max_vertex_num": 1200000000},
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "vertex_id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "vertex_name",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
                    {
                        "property_id": 2,
                        "property_name": "status",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 3,
                        "property_name": "credit_code",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
                    {
                        "property_id": 4,
                        "property_name": "license_number",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
                ],
                "primary_keys": ["vertex_id"],
            },
            {
                "type_id": 1,
                "type_name": "person",
                "x_csr_params": {"max_vertex_num": 1200000000},
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "vertex_id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "vertex_name",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
                ],
                "primary_keys": ["vertex_id"],
            },
        ],
        "edge_types": [
            {
                "type_id": 0,
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
                        "property_name": "rel_weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "rel_label",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 2,
                        "property_name": "rel_info",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
                ],
            },
            {
                "type_id": 1,
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
                        "property_name": "rel_weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    },
                    {
                        "property_id": 1,
                        "property_name": "rel_label",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_id": 2,
                        "property_name": "rel_info",
                        "property_type": {"string": {"var_char": {"max_length": 64}}},
                    },
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


def create_graph(sess: Session, ds: str, report_error: bool):
    # copied_huoyan_graph = huoyan_graph.copy()
    graph_name = f"onecompany_{ds}"
    create_graph_req = CreateGraphRequest(
        name=graph_name,
        description=f"onecompany graph for {ds}",
        var_schema=CreateGraphSchemaRequest(
            vertex_types=[
                CreateVertexType(
                    type_name="company",
                    x_csr_params=BaseVertexTypeXCsrParams(max_vertex_num=1200000000),
                    properties=[
                        CreatePropertyMeta(
                            property_name="vertex_id",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_SIGNED_INT64")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="vertex_name",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=64))
                                    )
                                )
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="status",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_SIGNED_INT64")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="credit_code",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=64))
                                    )
                                )
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="license_number",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=64))
                                    )
                                )
                            ),
                        ),
                    ],
                    primary_keys=["vertex_id"],
                ),
                CreateVertexType(
                    type_name="person",
                    x_csr_params=BaseVertexTypeXCsrParams(max_vertex_num=500000000),
                    properties=[
                        CreatePropertyMeta(
                            property_name="vertex_id",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_SIGNED_INT64")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="vertex_name",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=64))
                                    )
                                )
                            ),
                        ),
                    ],
                    primary_keys=["vertex_id"],
                ),
            ],
            edge_types=[
                CreateEdgeType(
                    type_name="invest",
                    vertex_type_pair_relations=[
                        BaseEdgeTypeVertexTypePairRelationsInner(
                            source_vertex="company",
                            destination_vertex="company",
                            relation="MANY_TO_MANY",
                        )
                    ],
                    properties=[
                        CreatePropertyMeta(
                            property_name="rel_weight",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_DOUBLE")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="rel_label",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_SIGNED_INT64")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="rel_info",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=16))
                                    )
                                )
                            ),
                        ),
                    ],
                ),
                CreateEdgeType(
                    type_name="personInvest",
                    vertex_type_pair_relations=[
                        BaseEdgeTypeVertexTypePairRelationsInner(
                            source_vertex="person",
                            destination_vertex="company",
                            relation="MANY_TO_MANY",
                        )
                    ],
                    properties=[
                        CreatePropertyMeta(
                            property_name="rel_weight",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_DOUBLE")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="rel_label",
                            property_type=GSDataType(
                                PrimitiveType(primitive_type="DT_SIGNED_INT64")
                            ),
                        ),
                        CreatePropertyMeta(
                            property_name="rel_info",
                            property_type=GSDataType(
                                StringType(
                                    string=StringTypeString(
                                        VarChar(var_char=VarCharVarChar(max_length=64))
                                    )
                                )
                            ),
                        ),
                    ],
                ),
            ],
        ),
    )
    create_graph_res = sess.create_graph(create_graph_req)
    # CreateGraphRequest.from_dict(copied_huoyan_graph)

    if not create_graph_res.is_ok():
        print("create graph failed: ", create_graph_res.get_status_message())
        if report_error:
            report_message(f"fail to create graph with ds {ds}")
        raise Exception("fail to create graph")
    create_graph_resp = create_graph_res.get_value()
    print(
        f"create graph {create_graph_resp.graph_id} successfully with name graph_name"
    )
    return create_graph_resp.graph_id


def loading_graph(sess: Session, graph_id: str, ds: str, report_error: bool):
    schema_mapping = SchemaMapping(
        loading_config=SchemaMappingLoadingConfig(
            data_source=SchemaMappingLoadingConfigDataSource(scheme="odps"),
            import_option="init",
            format=SchemaMappingLoadingConfigFormat(
                type="arrow",
                metadata={"batch_reader": True},
            ),
        ),
        vertex_mappings=[
            VertexMapping(
                type_name="company",
                inputs=[f"onecomp/dwi_oc_rel_vertex_company_interactive_d/ds={ds}"],
                column_mappings=[
                    ColumnMapping(
                        var_property="vertex_id",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="vertex_id"
                        ),
                    ),
                    ColumnMapping(
                        var_property="vertex_name",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=3, name="vertex_name"
                        ),
                    ),
                    ColumnMapping(
                        var_property="status",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=4, name="status"
                        ),
                    ),
                    ColumnMapping(
                        var_property="credit_code",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=5, name="social_credit_code"
                        ),
                    ),
                    ColumnMapping(
                        var_property="license_number",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=6, name="license_number"
                        ),
                    ),
                ],
            ),
            VertexMapping(
                type_name="person",
                inputs=[f"onecomp/dwi_oc_rel_vertex_person_interactive_d/ds={ds}"],
                column_mappings=[
                    ColumnMapping(
                        var_property="vertex_id",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="vertex_id"
                        ),
                    ),
                    ColumnMapping(
                        var_property="vertex_name",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=3, name="vertex_name"
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
                inputs=[f"onecomp/dwi_oc_rel_edge_comp_comp_interactive_d/ds={ds}"],
                column_mappings=[
                    ColumnMapping(
                        var_property="rel_weight",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=4, name="rel_weight"
                        ),
                    ),
                    ColumnMapping(
                        var_property="rel_label",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=5, name="rel_label"
                        ),
                    ),
                    ColumnMapping(
                        var_property="rel_info",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=6, name="rel_info"
                        ),
                    ),
                ],
                source_vertex_mappings=[
                    EdgeMappingSourceVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="onecomp_id"
                        ),
                        var_property="vertex_id",
                    )
                ],
                destination_vertex_mappings=[
                    EdgeMappingDestinationVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=3, name="rel_node_id"
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
                inputs=[f"onecomp/dwi_oc_rel_edge_comp_person_interactive_d/ds={ds}"],
                column_mappings=[
                    ColumnMapping(
                        var_property="rel_weight",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=4, name="rel_weight"
                        ),
                    ),
                    ColumnMapping(
                        var_property="rel_label",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=5, name="rel_label"
                        ),
                    ),
                    ColumnMapping(
                        var_property="rel_info",
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=6, name="rel_info"
                        ),
                    ),
                ],
                source_vertex_mappings=[
                    EdgeMappingSourceVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=2, name="person_id"
                        ),
                        var_property="vertex_id",
                    )
                ],
                destination_vertex_mappings=[
                    EdgeMappingDestinationVertexMappingsInner(
                        column=EdgeMappingSourceVertexMappingsInnerColumn(
                            index=3, name="rel_node_id"
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
        if report_error:
            report_message(f"fail to create loading job with ds {ds}")
        raise Exception("fail to create loading job")
    print(f"create loading job successfully: {resp.get_value().job_id}")
    return resp.get_value().job_id


def wait_job_finish(sess: Session, job_id: str):
    while True:
        resp = sess.get_job(job_id)
        if not resp.is_ok():
            report_message(
                f"when waiting job {job_id} status: {resp.get_value().status}"
            )
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
        if report_error:
            report_message(f"fail to create procedure with name {proc_name}")
        raise Exception("fail to create procedure")


def restart_service(sess: Session, graph_id: str):
    resp = sess.start_service(
        start_service_request=StartServiceRequest(graph_id=graph_id)
    )
    if not resp.is_ok():
        print("restart service failed: ", resp.get_status_message())
        report_message(f"fail to restart service with graph_id {graph_id}")
    print("restart service successfully")


def get_service_status(sess: Session, report_error: bool):
    resp = sess.get_service_status()
    if not resp.is_ok():
        print("get service status failed: ", resp.get_status_message())
        if report_error:
            report_message(f"fail to get service status")
        raise Exception("fail to get service status")
    print("service status: ", resp.get_value())
    status = resp.get_value()
    print("service running is now running on graph", status.graph.id)


def get_current_running_graph(sess: Session, report_error: bool):
    resp = sess.get_service_status()
    if not resp.is_ok():
        print("get service status failed: ", resp.get_status_message())
        if report_error:
            report_message(f"fail to get service status")
        raise Exception("fail to get service status")
    status = resp.get_value()
    return status.graph.id


def list_graph(sess: Session, report_error: bool):
    resp = sess.list_graphs()
    if not resp.is_ok():
        print("list graph failed: ", resp.get_status_message())
        if report_error:
            report_message(f"fail to list graph")
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
    if args.validate_reporting:
        report_message("Testing message")
        sys.exit(0)

    print("connecting to ", args.endpoint)
    if args.ds is None:
        # get the date string of yesterday, yyyymmdd
        #        import datetime

        #        yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
        #        ds = yesterday.strftime("%Y%m%d")
        with o.execute_sql(query).open_reader() as reader:
            pd_df = reader.to_pandas()
            ds = pd_df["ds"][0]
    else:
        ds = args.ds
    print("ds: ", ds)

    report_error = args.report_error

    driver = Driver(args.endpoint)
    sess = driver.session()
    # get current running graph
    old_graph = get_current_running_graph(sess, report_error)
    print("-----------------Finish getting current running graph-----------------")
    print("old graph: ", old_graph)

    graph_id = create_graph(sess, ds, report_error)
    print("-----------------Finish creating graph-----------------")
    print("graph_id: ", graph_id)

    job_id = loading_graph(sess, graph_id, ds, report_error)
    wait_job_finish(sess, job_id)
    print("-----------------Finish loading graph-----------------")

    create_procedure(
        sess, graph_id, script_directory + "/procedure.cc", args.proc_name, report_error
    )
    print("-----------------Finish creating procedure-----------------")

    #    start_time = time.time()
    #    restart_service(sess, graph_id)
    #    end_time = time.time()
    #    execution_time = end_time - start_time
    #    print("-----------------Finish restarting service-----------------")
    #    print(f"restart service cost {execution_time:.6f}seconds")

    get_service_status(sess, report_error)
    print("-----------------Finish getting service status-----------------")

    # if args.remove_old_graph:
    #     print("remove old graph")
    #     delete_graph = sess.delete_graph(old_graph)
    #     print("delete graph res: ", delete_graph)
    # else:
    #     print("keep old graph", old_graph)

    list_graph(sess, report_error)

    report_message(f"Bulk loading graph with date {ds} finished successfully")
