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

sys.path.append("../../python/")
import time
import argparse
import os
from interactive_sdk.client.driver import Driver
from interactive_sdk.client.session import Session
from interactive_sdk.openapi.models.query_request import QueryRequest
from interactive_sdk.openapi.models.gs_data_type import GSDataType
from interactive_sdk.openapi.models.typed_value import TypedValue
from interactive_sdk.openapi.models.primitive_type import PrimitiveType


def get_service_status(sess: Session):
    print("Get service status")
    status = sess.get_service_status()
    print(status)


def get_procedures(sess: Session):
    print("Get procedures")
    procedures = sess.list_procedures("1")
    print(procedures)


def call_procedure(sess: Session):
    print("Call procedure")
    req = QueryRequest(
        query_name="QueryName",
        arguments=[
            TypedValue(
                type=GSDataType(PrimitiveType(primitive_type="DT_SIGNED_INT32")),
                value=1,
            )
        ],
    )
    resp = sess.call_procedure("1", req)
    print(resp)


if __name__ == "__main__":
    # expect one argument: interactive_endpoint
    parser = argparse.ArgumentParser(description="Example Python3 script")

    # Add arguments
    parser.add_argument(
        "--endpoint",
        type=str,
        help="The interactive endpoint to connect",
        required=True,
        default="https://virtserver.swaggerhub.com/GRAPHSCOPE/interactive/1.0.0/",
    )

    # Parse the arguments
    args = parser.parse_args()

    driver = Driver(endpoint=args.endpoint)
    with driver.session() as sess:
        get_service_status(sess)
        get_procedures(sess)
        call_procedure(sess)
