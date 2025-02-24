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

# A simple example to show how to use the Interactive Python SDK to interact with the Groot service.

import argparse
import os
import time
import json
import ast

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

test_graph_def = {
    "name": "test_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            }
        ],
    },
}

def createGraph(sess: Session):
    create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
    resp = sess.create_graph(create_graph_request)
    if resp.is_ok():
        print("The response of create_graph:\n", resp)
        graph_id = resp.get_value().graph_id
        return graph_id
    else:
        raise Exception("create_graph failed with error: %s" % resp.get_status_message())

def getGraphSchema(sess: Session, graph_id: str):
    resp = sess.get_graph_schema(graph_id)
    if resp.is_ok():
        print("The response of get_graph_schema:\n", resp)
        return resp.get_value()
    else:
        raise Exception("get_graph_schema failed with error: %s" % resp.get_status_message())
    
def deleteGraph(sess: Session, graph_id: str):
    resp = sess.delete_graph(graph_id)
    if resp.is_ok():
        print("The response of delete_graph:\n", resp)
    else:
        raise Exception("delete_graph failed with error: %s" % resp.get_status_message())

def addVertexType(sess: Session, graph_id: str):
    create_vertex_type = CreateVertexType(
        type_name="new_person",
        properties=[
            CreatePropertyMeta(
                property_name="id",
                property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT64"}),
            ),
            CreatePropertyMeta(
                property_name="name",
                property_type=GSDataType.from_dict({"string": {"long_text": ""}}),
            ),
        ],
        primary_keys=["id"],
    )
    api_response = sess.create_vertex_type(graph_id, create_vertex_type)
    if api_response.is_ok():
        print("The response of create_vertex_type:\n", api_response)
    else:
        raise Exception(
            "create_vertex_type failed with error: %s" % api_response.get_status_message()
        )

def updateVertexType(sess: Session, graph_id: str):
    update_vertex_type = CreateVertexType(
        type_name="new_person",
        # add a new property "age"
        properties=[
            CreatePropertyMeta(
                property_name="age",
                property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT32"}),
            ),
        ],
        primary_keys=["id"],
    )
    api_response = sess.update_vertex_type(graph_id, update_vertex_type)
    if api_response.is_ok():
        print("The response of update_vertex_type:\n", api_response)
    else:
        raise Exception(
            "update_vertex_type failed with error: %s" % api_response.get_status_message()
        )

def deleteVertexType(sess: Session, graph_id: str):
    api_response = sess.delete_vertex_type(graph_id, "new_person")
    if api_response.is_ok():
        print("The response of delete_vertex_type:\n", api_response)
    else:
        raise Exception(
            "delete_vertex_type failed with error: %s" % api_response.get_status_message()
        )

def addEdgeType(sess: Session, graph_id: str):
    create_edge_type = CreateEdgeType(
        type_name="new_knows",
        vertex_type_pair_relations=[
            BaseEdgeTypeVertexTypePairRelationsInner(
                source_vertex="new_person",
                destination_vertex="new_person",
                relation="MANY_TO_MANY",
            )
        ],
        properties=[
            CreatePropertyMeta(
                property_name="weight",
                property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
            )
        ],
    )
    api_response = sess.create_edge_type(graph_id, create_edge_type)
    if api_response.is_ok():
        print("The response of create_edge_type:\n", api_response)
    else:
        raise Exception(
            "create_edge_type failed with error: %s" % api_response.get_status_message()
        )

def updateEdgeType(sess: Session, graph_id: str):
    update_edge_type = CreateEdgeType(
        type_name="new_knows",
        # add a new property "new_weight"
        properties=[
            CreatePropertyMeta(
                property_name="new_weight",
                property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
            ),
        ],
        vertex_type_pair_relations=[
            BaseEdgeTypeVertexTypePairRelationsInner(
                source_vertex="new_person",
                destination_vertex="new_person",
                relation="MANY_TO_MANY",
            )
        ],
    )
    api_response = sess.update_edge_type(graph_id, update_edge_type)
    if api_response.is_ok():
        print("The response of update_edge_type:\n", api_response)
    else:
        raise Exception(
            "update_edge_type failed with error: %s" % api_response.get_status_message()
        )

def deleteEdgeType(sess: Session, graph_id: str):
    api_response = sess.delete_edge_type(graph_id, "new_knows", "new_person", "new_person")
    if api_response.is_ok():
        print("The response of delete_edge_type:\n", api_response)
    else:
        raise Exception(
            "delete_edge_type failed with error: %s" % api_response.get_status_message()
        )
        
def getSnapShotStatus(sess: Session, graph_id: str, snapshot_id: int):
    api_response = sess.get_snapshot_status(graph_id, snapshot_id)
    if api_response.is_ok():
        print("The response of get_snapshot_status", api_response)
        return api_response
    else:
        raise Exception(
            "get_snapshot_status failed with error: %s" % api_response.get_status_message()
        )
        
def extractValueFromResponse(api_response, value_name):
    value = api_response.get_value()
    if isinstance(value, str):
        try:
            json_value = ast.literal_eval(value)
            if isinstance(json_value, dict):
                return json_value.get(value_name)
        except (ValueError, SyntaxError):
            raise ValueError("String value is not a valid dictionary format")
    elif isinstance(value, dict):
        return value.get(value_name)
    else:
        try:
            return getattr(value, value_name)
        except AttributeError:
            raise AttributeError(f"{type(value).__name__} object has no attribute '{value_name}'")


def addVertex(sess: Session, graph_id: str):
    vertex_request = [
        VertexRequest(
            label="person",
            primary_key_values= [
                ModelProperty(name="id", value=1),
            ],
            properties=[
                ModelProperty(name="name", value="Alice"),
                ModelProperty(name="age", value=20),
            ],
        ),
        VertexRequest(
            label="person",
            primary_key_values= [
                ModelProperty(name="id", value=8),
            ],            
            properties=[
                ModelProperty(name="name", value="mike"),
                ModelProperty(name="age", value=1),
            ],
        ),
    ]
    edge_request = [
        EdgeRequest(
            src_label="person",
            dst_label="person",
            edge_label="knows",
            src_primary_key_values=[ModelProperty(name="id", value=8)],
            dst_primary_key_values=[ModelProperty(name="id", value=1)],
            properties=[ModelProperty(name="weight", value=7)],
        ),
    ]
    params = VertexEdgeRequest(vertex_request=vertex_request, edge_request=edge_request)
    api_response = sess.add_vertex(graph_id, vertex_edge_request=params)
    if api_response.is_ok():
        print("The response of add_vertex:\n", api_response)
        return extractValueFromResponse(api_response, "snapshot_id")
    else:
        raise Exception(
            "add_vertex failed with error: %s" % api_response.get_status_message()
        )

def updateVertex(sess: Session, graph_id: str):
    name_property = ModelProperty(name="name", value="Cindy")
    age_property = ModelProperty(name="age", value=24)
    vertex_request = VertexRequest(
        label="person",
        primary_key_values= [
            ModelProperty(name="id", value=8),
        ],            
        properties=[
            name_property,
            age_property,
        ],
    )
    api_response = sess.update_vertex(graph_id, [vertex_request])
    if api_response.is_ok():
        print("The response of update_vertex:\n", api_response)
        return extractValueFromResponse(api_response, "snapshot_id")
    else:
        raise Exception(
            "update_vertex failed with error: %s" % api_response.get_status_message()
        )
        
def deleteVertex(sess: Session, graph_id: str):
    label = "person"  # str | The label name of querying vertex.
    primary_key_value = ModelProperty(name="id", value=1)  # object | The primary key value of querying vertex.
    delete_vertex_request = DeleteVertexRequest(
        label=label, primary_key_values=[primary_key_value]
    )
    api_response = sess.delete_vertex(graph_id, [delete_vertex_request])
    if api_response.is_ok():
        print("The response of delete_vertex:\n", api_response)
        return extractValueFromResponse(api_response, "snapshot_id")
    else:
        raise Exception(
            "delete_vertex failed with error: %s" % api_response.get_status_message()
        )

def updateEdge(sess: Session, graph_id: str):
    properties = [ModelProperty(name="weight", value=3)]
    edge_request = EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_values=[ModelProperty(name="id", value=1)],
        dst_primary_key_values=[ModelProperty(name="id", value=8)],
        properties=properties,
    )

    resp = sess.update_edge(graph_id, [edge_request])
    if resp.is_ok():
        print("The response of update_edge:\n", resp)
        return extractValueFromResponse(resp, "snapshot_id")
    else:
        raise Exception("update_edge failed with error: %s" % resp.get_status_message())
    
def deleteEdge(sess: Session, graph_id: str):
    src_label = "person"
    dst_label = "person"
    edge_label = "knows"
    src_primary_key_values = [ModelProperty(name="id", value=1)]
    dst_primary_key_values = [ModelProperty(name="id", value=8)]
    delete_edge_request = DeleteEdgeRequest(
        src_label=src_label,
        dst_label=dst_label,
        edge_label=edge_label,
        src_primary_key_values=src_primary_key_values,
        dst_primary_key_values=dst_primary_key_values,
    )
    api_response = sess.delete_edge(
        graph_id,
        [delete_edge_request],
    )
    if api_response.is_ok():
        print("The response of delete_edge:\n", api_response)
        return extractValueFromResponse(api_response, "snapshot_id")
    else:
        raise Exception(
            "delete_edge failed with error: %s" % api_response.get_status_message()
        )

def addEdge(sess: Session, graph_id: str):
    edge_request = [
        EdgeRequest(
            src_label="person",
            dst_label="person",
            edge_label="knows",
            src_primary_key_values=[ModelProperty(name="id", value=1)],
            dst_primary_key_values=[ModelProperty(name="id", value=8)],
            properties=[ModelProperty(name="weight", value=9.123)],
        ),
        EdgeRequest(
            src_label="person",
            dst_label="person",
            edge_label="knows",
            src_primary_key_values=[ModelProperty(name="id", value=2)],
            dst_primary_key_values=[ModelProperty(name="id", value=8)],
            properties=[ModelProperty(name="weight", value=3.233)],
        ),
    ]
    api_response = sess.add_edge(graph_id, edge_request)
    if api_response.is_ok():
        print("The response of add_edge:\n", api_response)
        return extractValueFromResponse(api_response, "snapshot_id")
    else:
        raise Exception(
            "add_edge failed with error: %s" % api_response.get_status_message()
        )


if __name__ == "__main__":
    # expect one argument: interactive_endpoint
    parser = argparse.ArgumentParser(description="Example Python3 script")

    # Parse the arguments
    args = parser.parse_args()

    driver = Driver()
    with driver.session() as sess:
        graph_id = "test_graph"
        # start from a clean graph
        deleteGraph(sess, graph_id)
        current_schema = getGraphSchema(sess, graph_id)
        createGraph(sess)

        # dml operations
        addVertex(sess, graph_id)
        updateVertex(sess, graph_id)
        deleteVertex(sess, graph_id)
        addEdge(sess, graph_id)
        updateEdge(sess, graph_id)
        si = deleteEdge(sess, graph_id)
        # get snapshot status, wait for the snapshot to be ready
        resp = getSnapShotStatus(sess, graph_id, si)
        while extractValueFromResponse(resp, "status") != "AVAILABLE":
            time.sleep(1)
            resp = getSnapShotStatus(sess, graph_id, si)
        
        # ddl operations
        addVertexType(sess, graph_id)
        updateVertexType(sess, graph_id)
        addEdgeType(sess, graph_id)
        updateEdgeType(sess, graph_id)
        deleteEdgeType(sess, graph_id)
        deleteVertexType(sess, graph_id)
        
        