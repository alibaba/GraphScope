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
from typing import Any
import interactive_sdk
from interactive_sdk import (
    Graph,
    JobResponse,
    JobStatus,
    GraphSchema,
    Procedure,
    SchemaMapping,
    Service,
    VertexType,
    VertexRequest,
    ModelProperty,
    EdgeRequest,
)

def test_create_graph(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.AdminServiceGraphManagementApi(client)
    # User are not supposed to provide property_id, type_id
    json_str = """{
        "name": "test_graph",
        "schema": {
            "vertex_labels": [
                {
                    "type_name": "person",
                    "properties:" : [
                        {
                            
                            "property_name": "id",
                            "property_type": {
                                "primitive_type": "DT_SIGNED_INT64"
                            }
                        },
                        {
                            "property_name": "age",
                            "property_type": {
                                "primitive_type": "DT_SIGNED_INT32"
                            }
                        }
                    ]
                }
            ]
        }
    }
    """
    graph = Graph.from_json(json_str)
    # graph can also be created via the method provided
    vertex_type = VertexType()
    #vertex_type.type_id = 0
    vertex_type.type_name = "person"
    schema = GraphSchema(vertex_types=[vertex_type])
    response = graph_api_instance.create_graph(graph=graph)
    print(response) #str

def test_get_graph(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.AdminServiceGraphManagementApi(client)
    response = graph_api_instance.get_schema("test_graph")
    print(response) #str

def test_create_procedure(client : interactive_sdk.ApiClient):
    procedure_api_instance = interactive_sdk.AdminServiceProcedureManagementApi(client)
    json_str =  """{
        "name": "proc1",
        "bound_graph": "test_graph",
        "description": "a procedure",
        "type": "cypher",
        "query": "MATCH(n: $labelName) RETURN COUNT(n) AS cnt",
        "enable": true,
        "runnable": true,
        "params": [
            {
            "name": "labelName",
            "type": "string"
            }
        ],
        "returns": [
            {
            "name": "cnt",
            "type": "long"
            }
        ]
    }
    """
    procedure = Procedure.from_json(json_str)
    response = procedure_api_instance.create_procedure("test_graph", procedure)
    print(response) #str

def test_start_service(client : interactive_sdk.ApiClient):
    service_api_instance = interactive_sdk.AdminServiceServiceManagementApi(client)
    response = service_api_instance.start_service(
        Service.from_dict({"graph_name": "test_graph"})
    )
    print(response) #str

#TBD
def test_call_procedure(client : interactive_sdk.ApiClient):
    query_api_instance = interactive_sdk.QueryServiceApi(client)
    response = query_api_instance.proc_call(
    """
    {
        "type": "proc_call",
        "payload": "TBD(How to construct the payload)",
    }
    """
    )
    print(response) #str

#TBD
def test_run_adhoc_query(client : interactive_sdk.ApiClient):
    query_api_instance = interactive_sdk.QueryServiceApi(client)
    response = query_api_instance.proc_call(
    """
    {
        "type": "adhoc",
        "payload": "TBD(How to construct the payload)",
    }
    """
    )
    print(response) #str

def test_insert_vertex(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.GraphServiceVertexManagementApi(client)
    # create a dict from str to any
    pk_dict = {
        "id": 123,
    }
    vertex_request = VertexRequest(label = "person", primary_key = ModelProperty.from_dict(pk_dict))
    # user not specifying the graph, cause we can only insert to the running graph
    response = graph_api_instance.add_vertex(vertex_request)
    print(response) #str

def test_insert_edge(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.GraphServiceEdgeManagementApi(client)
    edge_request = EdgeRequest(
        edge_label = "knows",
        src_label = "person",
        src_pk_name = "id",
        src_pk_value = 123,
        dst_label = "person",
        dst_pk_name = "id",
        dst_pk_value = 456,
        properties = []
    )
    # user not specifying the graph, cause we can only insert to the running graph
    response = graph_api_instance.add_edge(edge_request)
    print(response) #str

def test_get_vertex(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.GraphServiceVertexManagementApi(client)
    response = graph_api_instance.get_vertex("person", "id", 123)
    print(response) #str

def test_get_edge(client : interactive_sdk.ApiClient):
    graph_api_instance = interactive_sdk.GraphServiceEdgeManagementApi(client)
    response = graph_api_instance.get_edge(edge_label="knows", 
                                           src_vertex_label="person", src_vertex_pk_name="id", src_vertex_pk_value=123, 
                                           dst_vertex_label="person", dst_vertex_pk_name="id", dst_vertex_pk_value=456)
    print(response) #str

def simple_test(client : interactive_sdk.ApiClient):
    test_create_graph(client)
    test_get_graph(client)
    test_create_procedure(client)
    test_start_service(client)
    test_call_procedure(client)
    test_run_adhoc_query(client)
    test_insert_vertex(client)
    test_insert_edge(client)
    test_get_vertex(client)
    test_get_edge(client)

def main():
    host = "https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0" # localhost:7777 / localhost:10000
    client = interactive_sdk.ApiClient(interactive_sdk.Configuration(host))
    simple_test(client)

if __name__ == "__main__":
    main()