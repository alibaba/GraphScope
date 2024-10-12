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

# get the directory of the current file
import os
import time

import pytest
from neo4j import Session as Neo4jSession

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import CreateGraphRequest
from gs_interactive.models import CreateProcedureRequest
from gs_interactive.models import SchemaMapping
from gs_interactive.models import StartServiceRequest
from gs_interactive.models import UpdateProcedureRequest

cur_dir = os.path.dirname(os.path.abspath(__file__))
MODERN_GRAPH_DATA_DIR = os.path.abspath(
    os.path.join(cur_dir, "../../../../examples/modern_graph")
)
print("MODERN_GRAPH_DATA_DIR: ", MODERN_GRAPH_DATA_DIR)


modern_graph_full = {
    "name": "modern_graph",
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
            },
            {
                "type_name": "software",
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
                        "property_name": "lang",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
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
            },
            {
                "type_name": "created",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
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
            },
        ],
    },
}

modern_graph_vertex_only = {
    "name": "modern_graph",
    "description": "This is a test graph, only contains vertex",
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
        "edge_types": [],
    },
}

modern_graph_partial = {
    "name": "modern_graph",
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


modern_graph_full_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        },
        {
            "type_name": "software",
            "inputs": ["software.csv"],
        },
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": ["person_knows_person.csv"],
        },
        {
            "type_triplet": {
                "edge": "created",
                "source_vertex": "person",
                "destination_vertex": "software",
            },
            "inputs": ["person_created_software.csv"],
        },
    ],
}


modern_graph_partial_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        },
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": ["person_knows_person.csv"],
        }
    ],
}

modern_graph_vertex_only_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        }
    ],
}


@pytest.fixture(scope="module")
def interactive_driver():
    driver = Driver()
    yield driver
    driver.close()


@pytest.fixture(scope="module")
def interactive_session(interactive_driver):
    yield interactive_driver.session()


@pytest.fixture(scope="module")
def neo4j_session(interactive_driver):
    _neo4j_sess = interactive_driver.getNeo4jSession()
    yield _neo4j_sess
    _neo4j_sess.close()


@pytest.fixture(scope="function")
def create_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_full)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_vertex_only_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_vertex_only)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_partial_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_partial)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


def wait_job_finish(sess: Session, job_id: str):
    assert job_id is not None
    while True:
        resp = sess.get_job(job_id)
        assert resp.is_ok()
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            return True
        elif status == "FAILED":
            return False
        else:
            time.sleep(1)


def import_data_to_vertex_only_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_vertex_only_modern_graph_no_wait(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    print("job_id: ", job_id)


def import_data_to_partial_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_partial_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_full_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_full_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def submit_query_via_neo4j_endpoint(
    neo4j_sess: Neo4jSession, graph_id: str, query: str
):
    result = neo4j_sess.run(query)
    # check have 1 records, result 0
    result_cnt = 0
    for record in result:
        print("record: ", record)
        result_cnt += 1
    print("result count: ", result_cnt, " for query ", query)


def run_cypher_test_suite(neo4j_sess: Neo4jSession, graph_id: str, queries: list[str]):
    for query in queries:
        submit_query_via_neo4j_endpoint(neo4j_sess, graph_id, query)


def call_procedure(neo4j_sess: Neo4jSession, graph_id: str, proc_name: str, *args):
    query = "CALL " + proc_name + "(" + ",".join([str(item) for item in args]) + ")"
    result = neo4j_sess.run(query)
    for record in result:
        print(record)


def delete_running_graph(sess: Session, graph_id: str):
    # restart the service on graph "1"
    print("delete running graph {}", graph_id)
    # first the the service status, to get the graph id
    service_status = sess.get_service_status()
    assert service_status.is_ok()
    running_graph_id = service_status.get_value().graph.id
    if running_graph_id is None:
        print("No running graph")
        return
    if running_graph_id != graph_id:
        print("The request graph is not running, safe to delete")
    else:
        resp = sess.start_service(StartServiceRequest(graph_id="1"))
        assert resp.is_ok()
    # drop the graph
    resp = sess.delete_graph(graph_id)


def create_procedure(
    sess: Session, graph_id: str, name: str, query: str, description="test proc"
):
    request = CreateProcedureRequest(
        name=name, description=description, type="cypher", query=query
    )

    resp = sess.create_procedure(graph_id, request)
    if not resp.is_ok():
        print("Failed to create procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to create procedure, status: ", resp.get_status_message()
        )
    proc_id = resp.get_value().procedure_id
    return proc_id


def delete_procedure(sess: Session, graph_id: str, proc_id: str):
    resp = sess.delete_procedure(graph_id, proc_id)
    if not resp.is_ok():
        print("Failed to delete procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to delete procedure, status: ", resp.get_status_message()
        )


def update_procedure(sess: Session, graph_id: str, proc_id: str, desc: str):
    request = UpdateProcedureRequest(description=desc)
    resp = sess.update_procedure(graph_id, proc_id, request)
    if not resp.is_ok():
        print("Failed to update procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to update procedure, status: ", resp.get_status_message()
        )


def start_service_on_graph(interactive_session, graph_id: str):
    resp = interactive_session.start_service(StartServiceRequest(graph_id=graph_id))
    assert resp.is_ok()
    # wait one second to let compiler get the new graph
    time.sleep(1)
