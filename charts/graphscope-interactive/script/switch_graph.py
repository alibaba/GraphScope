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

import time
import hmac
import hashlib
import base64
import urllib.parse

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

query = """
SELECT  ds
            FROM    onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version
            WHERE   ds = MAX_PT("onecomp_risk.ads_fin_rsk_fe_ent_rel_data_version");
"""
import os

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


def restart_service(sess: Session, graph_id: str):
    resp = sess.start_service(
        start_service_request=StartServiceRequest(graph_id=graph_id)
    )
    if not resp.is_ok():
        report_message(f"Failed to restart service, graph_id: {graph_id}")
    print("restart service successfully")


def get_service_status(sess: Session):
    resp = sess.get_service_status()
    if not resp.is_ok():
        report_message("Failed to get service status")
    print("service status: ", resp.get_value())
    status = resp.get_value()
    print("service running is now running on graph", status.graph.id)


def get_current_running_graph(sess: Session):
    resp = sess.get_service_status()
    if not resp.is_ok():
        report_message("Failed to get service status")
    status = resp.get_value()
    return status.graph.id


def list_graph(sess: Session):
    resp = sess.list_graphs()
    if not resp.is_ok():
        report_message("Failed to list graph")
    res = resp.get_value()
    graph_id_arr = [graph.id for graph in res]
    print("list graph: ", graph_id_arr)


def check_graph_exits(sess: Session, graph_id: str):
    resp = sess.get_graph_schema(graph_id=graph_id)
    print("check graph exits: ", resp.is_ok())
    if not resp.is_ok():
        report_message(f"Failed to get graph schema, graph_id: {graph_id}")
    print("graph exits: ", resp.get_value())


if __name__ == "__main__":
    # parse command line args
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")
    parser.add_argument("--graph_id", type=str, default=None, required=False)
    parser.add_argument("--validate-reporting", type=bool, default=False)

    # finish
    args = parser.parse_args()
    print(args)
    if args.validate_reporting:
        report_message("Testing message")
        sys.exit(0)
    print("connecting to ", args.endpoint)
    driver = Driver(args.endpoint)
    sess = driver.session()
    # get current running graph
    old_graph = get_current_running_graph(sess)
    print("-----------------Finish getting current running graph-----------------")
    print("old graph: ", old_graph)

    if args.graph_id not in [None, ""]:
        graph_id = args.graph_id
    else:
        # assume old_graph is a int in string, plus 1
        print("using old graph id to generate new graph id")
        graph_id = str(int(old_graph) + 1)
    print("new graph: ", graph_id)

    # check if graph_id exists
    check_graph_exits(sess, graph_id)

    start_time = time.time()
    restart_service(sess, graph_id)
    end_time = time.time()
    execution_time = end_time - start_time
    print("-----------------Finish restarting service-----------------")
    print(f"restart service cost {execution_time:.6f}seconds")

    get_service_status(sess)
    print("-----------------Finish getting service status-----------------")

    list_graph(sess)

    # after switch to new graph, delete the old graph
    delete_graph = sess.delete_graph(old_graph)
    print("delete graph res: ", delete_graph)
    if not delete_graph.is_ok():
        report_message(f"Failed to delete graph {old_graph}")

    report_message(
        f"Switched to graph {graph_id} successfully, restart service cost {execution_time:.6f}seconds"
    )
