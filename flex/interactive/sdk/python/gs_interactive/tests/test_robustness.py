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

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from gs_interactive.tests.conftest import call_procedure  # noqa: E402
from gs_interactive.tests.conftest import create_procedure
from gs_interactive.tests.conftest import delete_procedure
from gs_interactive.tests.conftest import import_data_to_full_modern_graph
from gs_interactive.tests.conftest import import_data_to_partial_modern_graph
from gs_interactive.tests.conftest import import_data_to_vertex_only_modern_graph
from gs_interactive.tests.conftest import run_cypher_test_suite
from gs_interactive.tests.conftest import start_service_on_graph
from gs_interactive.tests.conftest import update_procedure

vertex_only_cypher_queries = [
    "MATCH(n) return count(n)",
    "MATCH(n) return n",
    "MATCH(n) return n limit 10",
]

# extend the query list to include queries that are not supported by vertex-only graph
cypher_queries = vertex_only_cypher_queries + [
    # "MATCH()-[e]->() return count(e)", currently not supported by compiler+ffi,
    # see https://github.com/alibaba/GraphScope/issues/4192
    "MATCH(a)-[b]->(c) return count(b)",
    "MATCH(a)-[b]->(c) return b",
    "MATCH(a)-[b]->(c) return c.id",
]


def test_query_on_vertex_only_graph(
    interactive_session, neo4j_session, create_vertex_only_modern_graph
):
    """
    Test Query on a graph with only a vertex-only schema defined, no data is imported.
    """
    print("[Query on vertex only graph]")
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    run_cypher_test_suite(
        neo4j_session, create_vertex_only_modern_graph, vertex_only_cypher_queries
    )

    start_service_on_graph(interactive_session, "1")
    import_data_to_vertex_only_modern_graph(
        interactive_session, create_vertex_only_modern_graph
    )
    run_cypher_test_suite(
        neo4j_session, create_vertex_only_modern_graph, vertex_only_cypher_queries
    )


def test_query_on_partial_graph(
    interactive_session, neo4j_session, create_partial_modern_graph
):
    """
    Test Query on a graph with the partial schema of modern graph defined, no data is imported.
    """
    print("[Query on partial graph]")
    # start service on new graph
    start_service_on_graph(interactive_session, create_partial_modern_graph)
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_partial_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session, "1")
    import_data_to_partial_modern_graph(
        interactive_session, create_partial_modern_graph
    )
    run_cypher_test_suite(neo4j_session, create_partial_modern_graph, cypher_queries)


def test_query_on_full_modern_graph(
    interactive_session, neo4j_session, create_modern_graph
):
    """
    Test Query on a graph with full schema of modern graph defined, no data is imported.
    """
    print("[Query on full modern graph]")
    start_service_on_graph(interactive_session, create_modern_graph)
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session, "1")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    run_cypher_test_suite(neo4j_session, create_modern_graph, cypher_queries)


def test_service_switching(
    interactive_session,
    neo4j_session,
    create_modern_graph,
    create_vertex_only_modern_graph,
):
    """
    Create a procedure on graph a, and create graph b, and
    create a procedure with same procedure name.
    Then restart graph on b, and query on graph a's procedure a.
    """
    print("[Cross query]")

    # create procedure on graph_a_id
    a_proc_id = create_procedure(
        interactive_session,
        create_modern_graph,
        "test_proc",
        "MATCH(n: software) return count(n);",
    )
    print("Procedure id: ", a_proc_id)
    start_service_on_graph(interactive_session, create_modern_graph)
    call_procedure(neo4j_session, create_modern_graph, a_proc_id)

    # create procedure on graph_b_id
    b_proc_id = create_procedure(
        interactive_session,
        create_vertex_only_modern_graph,
        "test_proc",
        "MATCH(n: person) return count(n);",
    )
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    call_procedure(neo4j_session, create_vertex_only_modern_graph, b_proc_id)


def test_procedure_creation(interactive_session, neo4j_session, create_modern_graph):
    print("[Test procedure creation]")

    # create procedure with description contains spaces,',', and
    # special characters '!','@','#','$','%','^','&','*','(',')'
    a_proc_id = create_procedure(
        interactive_session,
        create_modern_graph,
        "test_proc_1",
        "MATCH(n: software) return count(n);",
        "This is a test procedure, with special characters: !@#$%^&*()",
    )
    print("Procedure id: ", a_proc_id)
    start_service_on_graph(interactive_session, create_modern_graph)
    call_procedure(neo4j_session, create_modern_graph, a_proc_id)

    # create procedure with name containing space,
    # should fail, expect to raise exception
    with pytest.raises(Exception):
        create_procedure(
            interactive_session,
            create_modern_graph,
            "test proc",
            "MATCH(n: software) return count(n);",
        )

    # create procedure with invalid cypher query, should fail, expect to raise exception
    with pytest.raises(Exception):
        create_procedure(
            interactive_session,
            create_modern_graph,
            "test_proc2",
            "MATCH(n: IDONTKOWN) return count(n)",
        )


def test_builtin_procedure(interactive_session, neo4j_session, create_modern_graph):
    print("[Test builtin procedure]")
    # Delete the builtin procedure should fail
    with pytest.raises(Exception):
        delete_procedure(interactive_session, create_modern_graph, "count_vertices")
    # Create a procedure with the same name as builtin procedure should fail
    with pytest.raises(Exception):
        create_procedure(
            interactive_session,
            create_modern_graph,
            "count_vertices",
            "MATCH(n: software) return count(n);",
        )
    # Update the builtin procedure should fail
    with pytest.raises(Exception):
        update_procedure(
            interactive_session,
            create_modern_graph,
            "count_vertices",
            "A updated description",
        )
    # Call the builtin procedure
    start_service_on_graph(interactive_session, create_modern_graph)
    call_procedure(neo4j_session, create_modern_graph, "count_vertices", '"person"')


def test_stop_service(interactive_session, create_modern_graph):
    print("[Test stop service]")
    start_service_on_graph(interactive_session, create_modern_graph)
    # stop the service
    stop_res = interactive_session.stop_service(graph_id="A Invalid graph id")
    assert not stop_res.is_ok()
