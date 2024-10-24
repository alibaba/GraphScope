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
import argparse
import os
import time

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

MODERN_GRAPH_CSV_DIR = os.path.join(
    os.path.dirname(__file__), "../../../../interactive/examples/modern_graph"
)
# get current dir

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

test_graph_datasource = {
    "loading_config": {"data_source": {"scheme": "file"}, "import_option": "init"},
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": [f"@{MODERN_GRAPH_CSV_DIR}/person.csv"],
            "column_mappings": [
                {"column": {"index": 0, "name": "id"}, "property": "id"},
                {"column": {"index": 1, "name": "name"}, "property": "name"},
                {"column": {"index": 2, "name": "age"}, "property": "age"},
            ],
        }
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": [f"@{MODERN_GRAPH_CSV_DIR}/person_knows_person.csv"],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "person.id"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "person.id"}, "property": "id"}
            ],
            "column_mappings": [
                {"column": {"index": 2, "name": "weight"}, "property": "weight"}
            ],
        }
    ],
}


def createGraph(sess: Session):
    create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
    resp = sess.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    print("Graph id: ", graph_id)
    return graph_id


def bulkLoading(sess: Session, graph_id: str):
    bulk_load_request = SchemaMapping.from_dict(test_graph_datasource)
    resp = sess.bulk_loading(graph_id, bulk_load_request)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    return job_id


def waitJobFinish(sess: Session, job_id: str):
    while True:
        resp = sess.get_job(job_id)
        assert resp.is_ok()
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            break
        elif status == "FAILED":
            raise Exception("job failed")
        else:
            time.sleep(1)

def addVertexImpl(sess: Session, graph_id: str, vertex_pk: int, vertex_name: str, vertex_age: int):
    vertex_request = [
        VertexRequest(
            label="person",
            primary_key_value=vertex_pk,
            properties=[
                ModelProperty(name="name", value=vertex_name),
                ModelProperty(name="age", value=vertex_age),
            ],
        ),
    ]
    params = VertexEdgeRequest(vertex_request=vertex_request,edge_request=[])
    api_response = sess.add_vertex(graph_id, vertex_edge_request=params)
    if api_response.is_ok():
        print("The response of add_vertex:\n", api_response)
    else:
        raise Exception(
            "add_vertex failed with error: %s" % api_response.get_status_message()
        )

def addVertex(sess: Session, graph_id: str):
    addVertexImpl(sess, graph_id, 12, "mike", 1)
    addVertexImpl(sess, graph_id, 13, "zhao", 2)
    addVertexImpl(sess, graph_id, 14, "li", 3)
    addVertexImpl(sess, graph_id, 15, "wang", 4)
    addVertexImpl(sess, graph_id, 16, "zhang", 5)
    addVertexImpl(sess, graph_id, 22, "mike", 1)
    addVertexImpl(sess, graph_id, 23, "zhao", 2)
    addVertexImpl(sess, graph_id, 24, "li", 3)
    addVertexImpl(sess, graph_id, 25, "wang", 4)
    addVertexImpl(sess, graph_id, 26, "zhang", 5)
    # vertex_request = [
    #     VertexRequest(
    #         label="person",
    #         primary_key_value=12,
    #         properties=[
    #             ModelProperty(name="name", value="mike"),
    #             ModelProperty(name="age", value=1),
    #         ],
    #     ),
    # ]
    # edge_request = [
    #     EdgeRequest(
    #         src_label="person",
    #         dst_label="person",
    #         edge_label="knows",
    #         src_primary_key_value=12,
    #         dst_primary_key_value=1,
    #         properties=[ModelProperty(name="weight", value=7)],
    #     ),
    # ]
    # params = VertexEdgeRequest(vertex_request=vertex_request, edge_request=edge_request)
    # api_response = sess.add_vertex(graph_id, vertex_edge_request=params)
    # if api_response.is_ok():
    #     print("The response of add_vertex:\n", api_response)
    # else:
    #     raise Exception(
    #         "add_vertex failed with error: %s" % api_response.get_status_message()
    #     )


def updateVertex(sess: Session, graph_id: str):
    name_property = ModelProperty(name="name", value="Cindy")
    age_property = ModelProperty(name="age", value=24)
    vertex_request = VertexRequest(
        label="person", primary_key_value=1, properties=[name_property, age_property]
    )
    api_response = sess.update_vertex(graph_id, vertex_request=vertex_request)
    if api_response.is_ok():
        print("The response of update_vertex", api_response)
    else:
        raise Exception(
            "update_vertex failed with error: %s" % api_response.get_status_message()
        )


def getVertex(sess: Session, graph_id: str):
    label = "person"  # str | The label name of querying vertex.
    primary_key_value = 1  # object | The primary key value of querying vertex.
    api_response = sess.get_vertex(graph_id, label, primary_key_value)
    if api_response.is_ok():
        print("The response of get_vertex", api_response)
    else:
        raise Exception(
            "get_vertex failed with error: %s" % api_response.get_status_message()
        )



if __name__ == "__main__":
    # expect one argument: interactive_endpoint
    parser = argparse.ArgumentParser(description="Example Python3 script")

    # Parse the arguments
    args = parser.parse_args()

    driver = Driver()
    with driver.session() as sess:
        # graph_id = createGraph(sess)
        # job_id = bulkLoading(sess, graph_id)
        # waitJobFinish(sess, job_id)
        # print("bulk loading finished")
        graph_id = "1"

        # Now start service on the created graph.
        # resp = sess.start_service(
        #     start_service_request=StartServiceRequest(graph_id=graph_id)
        # )
        # assert resp.is_ok()
        # print("restart service on graph ", graph_id)

        addVertex(sess, graph_id)
        # updateVertex(sess, graph_id)
        # getVertex(sess, graph_id)

