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
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *
    

from gs_interactive.tests.conftest import create_vertex_only_modern_graph, start_service_on_graph,interactive_driver
from gs_interactive.tests.conftest import interactive_session, create_procedure, delete_running_graph, create_modern_graph, create_partial_modern_graph,run_cypher_test_suite, call_procedure
from gs_interactive.tests.conftest import import_data_to_vertex_only_modern_graph, import_data_to_partial_modern_graph, import_data_to_full_modern_graph


cypher_queries = [
    "MATCH(n) return count(n)",
    "MATCH(n) return n",
    "MATCH(n) return n limit 10",
    "MATCH()-[r]->() return count(r)",
    "MATCH(a)-[b]->(c) return count(b)",
    "MATCH(a)-[b]->(c) return b",
    "MATCH(a)-[b]->(c) return c.id",
]

def test_query_on_vertex_only_graph(interactive_driver, interactive_session, neo4j_session, create_vertex_only_modern_graph):
    """
    Test Query on a graph with only a vertex-only schema defined, no data is imported.
    """
    print("[Query on vertex only graph]")
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    run_cypher_test_suite(neo4j_session, create_vertex_only_modern_graph, cypher_queries)

    start_service_on_graph(interactive_session,"1")
    import_data_to_vertex_only_modern_graph(interactive_session, create_vertex_only_modern_graph)
    run_cypher_test_suite(neo4j_session, create_vertex_only_modern_graph, cypher_queries)

def test_query_on_partial_graph(interactive_driver, interactive_session,neo4j_session, create_partial_modern_graph):
    """
    Test Query on a graph with the partial schema of modern graph defined, no data is imported.
    """
    print("[Query on partial graph]")
    # start service on new graph
    start_service_on_graph(interactive_session, create_partial_modern_graph)
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_partial_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session,"1")
    import_data_to_partial_modern_graph(interactive_session, create_partial_modern_graph)
    run_cypher_test_suite(neo4j_session, create_partial_modern_graph, cypher_queries)
    
def test_query_on_full_modern_graph(interactive_driver, interactive_session, neo4j_session, create_modern_graph):
    """
    Test Query on a graph with full schema of modern graph defined, no data is imported.
    """
    print("[Query on full modern graph]")
    start_service_on_graph(interactive_session,create_modern_graph)
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session,"1")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    run_cypher_test_suite(neo4j_session, create_modern_graph, cypher_queries)

        
def test_service_switching(interactive_session,neo4j_session, create_modern_graph, create_vertex_only_modern_graph ):
    """
    Create a procedure on graph a, and create graph b, and create a procedure with same procedure name.
    Then restart graph on b, and query on graph a's procedure a.
    """
    print("[Cross query]")

    # create procedure on graph_a_id
    a_proc_id = create_procedure(interactive_session, create_modern_graph, "test_proc", "MATCH(n: software) return count(n);")
    print("Procedure id: ", a_proc_id)
    start_service_on_graph(interactive_session, create_modern_graph)
    call_procedure(neo4j_session, create_modern_graph, a_proc_id)

    # create procedure on graph_b_id
    b_proc_id = create_procedure(interactive_session, create_vertex_only_modern_graph, "test_proc", "MATCH(n: person) return count(n);")
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    call_procedure(neo4j_session, create_vertex_only_modern_graph, b_proc_id)


