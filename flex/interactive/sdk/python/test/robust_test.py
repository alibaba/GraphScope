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
import time
import unittest

import pytest

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

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
            }
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

def create_graph(sess: Session, graph_def):
    create_graph_request = CreateGraphRequest.from_dict(graph_def)
    resp = sess.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    print("Graph id: ", graph_id)
    return graph_id

def create_procedure(sess: Session, graph_id: str, name: str, query: str):
    request = CreateProcedureRequest(
        name=name,
        description="test proc",
        type="cypher",
        query=query)

    resp = sess.create_procedure(graph_id, request)
    assert resp.is_ok()
    proc_id = resp.get_value().procedure_id
    print("Procedure id: ", proc_id)
    return proc_id
    

def delete_running_graph(sess: Session, graph_id):
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
    assert resp.is_ok()

class TestDriver(unittest.TestCase):
    """Test usage of driver"""

    def setUp(self):
        self._driver = Driver()
        self._sess = self._driver.session()
        print("finish setup")

    def tearDown(self):
        pass

    def test_robust(self):
        self.cross_query()
        # self.query_on_empty_graph()
        # self.run_corner_queries()
        # now check whether sever is still running, if not, fails
        service_status = self._sess.get_service_status()
        assert service_status.is_ok()

    def query_on_empty_graph(self):
        """
        Test Query on a graph with only schema defined, no data is imported.
        """
        print("[Query on empty graph]")
        graph_id = create_graph(self._sess, modern_graph_vertex_only)
        # start service on new graph
        resp = self._sess.start_service(StartServiceRequest(graph_id=graph_id))
        assert resp.is_ok()
        # try to query on the graph
        try :
            with self._driver.getNeo4jSession() as neo4j_session:
                query = "MATCH(n) return n"
                result = neo4j_session.run(query)
                # throw exceptions if there is any record
                for record in result:
                    print(record)

                query = "MATCH(n) return count(n)"
                result = neo4j_session.run(query)
                #check have 1 records, result 0
                for record in result:
                    if record[0] != 0:
                        self.fail("Query on empty graph failed")
        except Exception as e:
            print(e)
            self.fail("Query on empty graph failed, exception raised: " + str(e))
        finally:
            # drop the graph
            delete_running_graph(self._sess, graph_id)
        
    def cross_query(self):
        """
        Create a procedure on graph a, and create graph b, and create a procedure with same procedure name.
        Then restart graph on b, and query on graph a's procedure a.
        """
        print("[Cross query]")
        graph_a_id = create_graph(self._sess, modern_graph_full)
        graph_b_id = create_graph(self._sess, modern_graph_vertex_only)
        # start service on graph_a_id
        try :
            resp = self._sess.start_service(StartServiceRequest(graph_id=graph_a_id))
            assert resp.is_ok()
            # create procedure on graph_a_id
            a_proc_id = create_procedure(self._sess, graph_a_id, "test_proc", "MATCH(n: software) return count(n);")
            # start service on graph_a
            resp = self._sess.start_service(StartServiceRequest(graph_id=graph_a_id))
            assert resp.is_ok()
            # query on graph_a
            with self._driver.getNeo4jSession() as neo4j_session:
                query = "CALL test_proc()"
                result = neo4j_session.run(query)
                for record in result:
                    print(record)
            # create procedure on graph_b_id
            b_proc_id = create_procedure(self._sess, graph_b_id, "test_proc", "MATCH(n: person) return count(n);")
            # start service on graph_b
            resp = self._sess.start_service(StartServiceRequest(graph_id=graph_b_id))
            assert resp.is_ok()
            # query on graph_b
            with self._driver.getNeo4jSession() as neo4j_session:
                query = "CALL test_proc()"
                result = neo4j_session.run(query)
                for record in result:
                    print(record)
        except Exception as e:
            print(e)
            self.fail("Cross query failed, exception raised: " + str(e))
        finally:
            # drop the graph
            delete_running_graph(self._sess, graph_a_id)
            delete_running_graph(self._sess, graph_b_id)


    
    def run_corner_queries(self):
        """
        For the graph with 
        """

if __name__ == "__main__":
    unittest.main()

