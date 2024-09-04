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

sys.path.append("../../interactive/sdk/python")

from gs_interactive.client.driver import Driver
from gs_interactive.client.utils import Encoder, Decoder
from gs_interactive.models.base_edge_type_vertex_type_pair_relations_inner import (
    BaseEdgeTypeVertexTypePairRelationsInner,
)
from gs_interactive.models.create_edge_type import CreateEdgeType
from gs_interactive.models.create_graph_request import CreateGraphRequest
from gs_interactive.models.create_graph_schema_request import (
    CreateGraphSchemaRequest,
)
from gs_interactive.models.create_procedure_request import (
    CreateProcedureRequest,
)
from gs_interactive.models.create_property_meta import CreatePropertyMeta
from gs_interactive.models.create_vertex_type import CreateVertexType
from gs_interactive.models.edge_mapping import EdgeMapping
from gs_interactive.models.edge_mapping_type_triplet import (
    EdgeMappingTypeTriplet,
)
from gs_interactive.models.gs_data_type import GSDataType
from gs_interactive.models.typed_value import TypedValue
from gs_interactive.models.job_status import JobStatus
from gs_interactive.models.long_text import LongText
from gs_interactive.models.primitive_type import PrimitiveType
from gs_interactive.models.schema_mapping import SchemaMapping
from gs_interactive.models.schema_mapping_loading_config import (
    SchemaMappingLoadingConfig,
)
from gs_interactive.models.schema_mapping_loading_config_format import (
    SchemaMappingLoadingConfigFormat,
)
from gs_interactive.models.start_service_request import StartServiceRequest
from gs_interactive.models.string_type import StringType
from gs_interactive.models.string_type_string import StringTypeString
from gs_interactive.models.vertex_mapping import VertexMapping
from gs_interactive.models.query_request import QueryRequest

# Among the above procedures, the correct input format for each is:
# count_vertex_num: () -> (num: int64), CypherProcedure.
# plus_one: (num: int64) -> (num: int64), CppEncoder
# sample_app: (num: int64) -> (num: int64), kCypherJson


class ProcedureCaller():
    def __init__(self, endpoint):
        self._driver = Driver(endpoint)
        self._sess = self._driver.session()
    
    def call_cypher_queries(self):
        with self._driver.getNeo4jSession() as session:
            result = session.run("CALL count_vertex_num();")
            print("call procedure result: ", result)

    def callProcedureWithJsonFormat(self, graph_id : str):
        # get_person_name
        sample_app_ref = QueryRequest(
            query_name="sample_app",
            arguments=[
                TypedValue(
                    type=GSDataType(
                        PrimitiveType(primitive_type="DT_SIGNED_INT32")
                    ),
                    value = 2
                )
            ]
        )
        resp = self._sess.call_procedure(graph_id, sample_app_ref)
        if not resp.is_ok():
            print("call sample_app failed: ", resp.get_status_message())
            exit(1)
        res = resp.get_value()
        print("call sample_app result: ", res)
        self.call_cypher_queries()
    
    def callProcedureWithEncoder(self, graph_id : str):
        # count_vertex_num, should be with id 1
        # construct a byte array with bytes: 0x01
        encoder = Encoder()
        encoder.put_byte(1)
        resp = self._sess.call_procedure_raw(graph_id, encoder.get_bytes())
        if not resp.is_ok():
            print("call count_vertex_num failed: ", resp.get_status_message())
            exit(1)

        # plus_one, should be with id 2
        encoder2 = Encoder()
        encoder2.put_int(1) # The input value 1
        encoder2.put_byte(2) # The procedure id
        resp = self._sess.call_procedure_raw(graph_id, encoder2.get_bytes())
        if not resp.is_ok():
            print("call plus_one failed: ", resp.get_status_message())
            exit(1)
        res = resp.get_value()
        assert len(res) == 4
        # the four byte represent a integer
        decoder = Decoder(res)
        value = decoder.get_int()
        print("call plus_one result: ", value)
        assert(value == 2)
        assert(decoder.is_empty())

if __name__ == "__main__":
    #parse command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")
    parser.add_argument("--graph_id", type=str, default="1")
    parser.add_argument("--input-format", type=str, default="json")
    #finish
    args = parser.parse_args()
    print(args)
    caller = ProcedureCaller(args.endpoint)
    if args.input_format == "json":
        caller.callProcedureWithJsonFormat(args.graph_id)
    elif args.input_format == "encoder":
        caller.callProcedureWithEncoder(args.graph_id)
    else:
        raise ValueError("Invalid input format: " + args.input_format)

