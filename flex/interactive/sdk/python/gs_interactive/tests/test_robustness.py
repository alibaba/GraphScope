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
import threading
from time import sleep

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from gs_interactive.tests.conftest import call_procedure  # noqa: E402
from gs_interactive.tests.conftest import create_procedure
from gs_interactive.tests.conftest import delete_procedure
from gs_interactive.tests.conftest import ensure_compiler_schema_ready
from gs_interactive.tests.conftest import import_data_to_full_graph_algo_graph
from gs_interactive.tests.conftest import import_data_to_full_modern_graph
from gs_interactive.tests.conftest import import_data_to_new_graph_algo_graph
from gs_interactive.tests.conftest import import_data_to_partial_modern_graph
from gs_interactive.tests.conftest import import_data_to_vertex_only_modern_graph
from gs_interactive.tests.conftest import (
    import_data_to_vertex_only_modern_graph_no_wait,
)
from gs_interactive.tests.conftest import run_cypher_test_suite
from gs_interactive.tests.conftest import send_get_request_periodically
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_vertex_only_modern_graph
    )
    run_cypher_test_suite(
        neo4j_session, create_vertex_only_modern_graph, vertex_only_cypher_queries
    )

    start_service_on_graph(interactive_session, "1")
    import_data_to_vertex_only_modern_graph(
        interactive_session, create_vertex_only_modern_graph
    )
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_vertex_only_modern_graph
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_partial_modern_graph
    )
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_partial_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session, "1")
    import_data_to_partial_modern_graph(
        interactive_session, create_partial_modern_graph
    )
    start_service_on_graph(interactive_session, create_partial_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_partial_modern_graph
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    # try to query on the graph
    run_cypher_test_suite(neo4j_session, create_modern_graph, cypher_queries)
    start_service_on_graph(interactive_session, "1")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    start_service_on_graph(interactive_session, create_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    call_procedure(neo4j_session, create_modern_graph, a_proc_id)

    # create procedure on graph_b_id
    b_proc_id = create_procedure(
        interactive_session,
        create_vertex_only_modern_graph,
        "test_proc",
        "MATCH(n: person) return count(n);",
    )
    start_service_on_graph(interactive_session, create_vertex_only_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_vertex_only_modern_graph
    )
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
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
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
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
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    call_procedure(
        neo4j_session,
        create_modern_graph,
        "count_vertices",
        '"person"',
    )

    call_procedure(
        neo4j_session,
        create_modern_graph,
        "pagerank",
        '"person"',
        '"knows"',
        "0.85",
        "100",
        "0.000001",
    )

    call_procedure(
        neo4j_session,
        create_modern_graph,
        "k_neighbors",
        '"person"',
        "1L",
        "2",
    )

    call_procedure(
        neo4j_session,
        create_modern_graph,
        "shortest_path_among_three",
        '"person"',
        "1L",
        '"person"',
        "2L",
        '"person"',
        "4L",
    )


def test_list_jobs(interactive_session, create_vertex_only_modern_graph):
    print("[Test list jobs]")
    import_data_to_vertex_only_modern_graph_no_wait(
        interactive_session, create_vertex_only_modern_graph
    )
    resp = interactive_session.delete_graph(create_vertex_only_modern_graph)

    resp = interactive_session.list_jobs()
    assert resp.is_ok() and len(resp.get_value()) > 0


@pytest.mark.skipif(
    os.environ.get("RUN_ON_PROTO", None) != "ON", reason="Only works on proto"
)
def test_call_proc_in_cypher(interactive_session, neo4j_session, create_modern_graph):
    print("[Test call procedure in cypher]")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    start_service_on_graph(interactive_session, create_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    result = neo4j_session.run(
        'MATCH(p: person) with p.id as oid CALL k_neighbors("person", oid, 1) return label_name, vertex_oid;'
    )
    cnt = 0
    for record in result:
        cnt += 1
    assert cnt == 8


@pytest.mark.skipif(
    os.environ.get("RUN_ON_PROTO", None) != "ON",
    reason="Scan+Limit fuse only works on proto",
)
def test_scan_limit_fuse(interactive_session, neo4j_session, create_modern_graph):
    print("[Test call procedure in cypher]")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    start_service_on_graph(interactive_session, create_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    result = neo4j_session.run(
        'MATCH(p: person) with p.id as oid CALL k_neighbors("person", oid, 1) return label_name, vertex_oid;'
    )
    cnt = 0
    for record in result:
        cnt += 1
    assert cnt == 8

    # Q: Why we could use this query to verify whether Scan+Limit fuse works?
    # A: If Scan+Limit fuse works, the result of this query should be 2, otherwise it should be 6
    result = neo4j_session.run("MATCH(n) return n.id limit 2")
    cnt = 0
    for record in result:
        cnt += 1
    assert cnt == 2

    result = neo4j_session.run("MATCH(n) return n.id limit 0")
    cnt = 0
    for record in result:
        cnt += 1
    assert cnt == 0


def test_custom_pk_name(
    interactive_session, neo4j_session, create_graph_with_custom_pk_name
):
    print("[Test custom pk name]")
    import_data_to_full_modern_graph(
        interactive_session, create_graph_with_custom_pk_name
    )
    start_service_on_graph(interactive_session, create_graph_with_custom_pk_name)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_graph_with_custom_pk_name
    )
    result = neo4j_session.run(
        "MATCH (n: person) where n.custom_id = 4 return n.custom_id;"
    )
    records = result.fetch(10)
    for record in records:
        print(record)
    assert len(records) == 1

    result = neo4j_session.run(
        "MATCH (n:person)-[e]-(v:person) where v.custom_id = 1 return count(e);"
    )
    records = result.fetch(1)
    assert len(records) == 1 and records[0]["$f0"] == 2

    # another test case that should cover extracting primary key after edge expand.
    result = neo4j_session.run(
        "MATCH (n:person)-[e]-(v:person)-[e2]->(s:software) where n.custom_id = 1 and v.custom_id = 4 return s.name;"
    )
    records = result.fetch(10)
    assert (
        len(records) == 2
        and records[0]["name"] == "lop"
        and records[1]["name"] == "ripple"
    )


def test_complex_query(interactive_session, neo4j_session, create_graph_algo_graph):
    """
    Make sure that long-running queries won't block the admin service.
    """
    print("[Test complex query]")
    import_data_to_full_graph_algo_graph(interactive_session, create_graph_algo_graph)
    start_service_on_graph(interactive_session, create_graph_algo_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_graph_algo_graph
    )
    # start a thread keep get service status, if the request timeout, raise error
    ping_thread = threading.Thread(
        target=send_get_request_periodically,
        args=("{}/v1/service/status".format(interactive_session.admin_uri), 1, 1, 20),
    )
    ping_thread.start()
    # Expect at least 10 seconds to finish
    result = neo4j_session.run("MATCH(n)-[*1..5]-(t) return count(t);")
    ping_thread_2 = threading.Thread(
        target=send_get_request_periodically,
        args=("{}/v1/service/status".format(interactive_session.admin_uri), 1, 1, 20),
    )
    ping_thread_2.start()

    res = result.fetch(1)
    assert res[0]["$f0"] == 9013634

    ping_thread.join()
    ping_thread_2.join()


def test_x_csr_params(
    interactive_session, neo4j_session, create_graph_algo_graph_with_x_csr_params
):
    print("[Test x csr params]")
    import_data_to_new_graph_algo_graph(
        interactive_session, create_graph_algo_graph_with_x_csr_params
    )
    start_service_on_graph(
        interactive_session, create_graph_algo_graph_with_x_csr_params
    )
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_graph_algo_graph_with_x_csr_params
    )
    result = neo4j_session.run('MATCH (n) where n.id <> "" return count(n);')
    # expect return value 0
    records = result.fetch(1)
    print(records[0])
    assert len(records) == 1 and records[0]["$f0"] == 3506

    result = neo4j_session.run("MATCH(n)-[*1..4]-() RETURN count(n),n;")
    records = result.fetch(200)
    assert len(records) == 184

    result = neo4j_session.run("MATCH(n)-[e*1..4]-() RETURN count(n),n;")
    records = result.fetch(200)
    assert len(records) == 184


@pytest.mark.skipif(
    os.environ.get("RUN_ON_PROTO", None) != "ON",
    reason="var_char is only supported in proto",
)
def test_var_char_property(
    interactive_session, neo4j_session, create_graph_with_var_char_property
):
    print("[Test var char property]")
    import_data_to_full_modern_graph(
        interactive_session, create_graph_with_var_char_property
    )
    start_service_on_graph(interactive_session, create_graph_with_var_char_property)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_graph_with_var_char_property
    )
    result = neo4j_session.run("MATCH (n: person) return n.name AS personName;")
    records = result.fetch(10)
    assert len(records) == 4
    for record in records:
        # all string property in this graph is var char with max_length 2
        assert len(record["personName"]) == 2


def test_not_supported_cases(interactive_session, neo4j_session, create_modern_graph):
    """
    There are cases that are not supported by the current implementation.
    In the future, after the implementation is complete, these cases should be supported and should be removed.
    """
    print("[Test not supported cases in cypher]")
    import_data_to_full_modern_graph(interactive_session, create_modern_graph)
    start_service_on_graph(interactive_session, create_modern_graph)
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph
    )
    # expect exception thrown when running cypher query 'MATCH(p)-[e]->(n) return [p,n] as nodes;'
    with pytest.raises(Exception):
        result = neo4j_session.run(
            "MATCH shortestPath(src: person {id: 1})-[e*1..2]-(dst: person) return length(e);"
        )
        result.fetch(1)


def test_multiple_edge_property(
    interactive_session, neo4j_session, create_modern_graph_multiple_edge_property
):
    print("[Test multiple edge property]")
    import_data_to_full_modern_graph(
        interactive_session, create_modern_graph_multiple_edge_property
    )
    start_service_on_graph(
        interactive_session, create_modern_graph_multiple_edge_property
    )
    ensure_compiler_schema_ready(
        interactive_session, neo4j_session, create_modern_graph_multiple_edge_property
    )
    result = neo4j_session.run(
        "MATCH (n: person)-[e]->(m: software) RETURN e.weight AS weight, e.since AS since ORDER BY weight ASC, since ASC;"
    )
    records = result.fetch(10)
    assert len(records) == 4
    expected_result = [
        {"weight": 0.2, "since": 2023},
        {"weight": 0.4, "since": 2020},
        {"weight": 0.4, "since": 2022},
        {"weight": 1.0, "since": 2021},
    ]

    for i in range(len(records)):
        assert records[i]["weight"] == expected_result[i]["weight"]
        assert records[i]["since"] == expected_result[i]["since"]

    result = neo4j_session.run(
        "MATCH (n: person)-[e]->(m: software) RETURN e ORDER BY e.weight ASC, e.since ASC;"
    )
    records = result.fetch(10)
    assert len(records) == 4
    for i in range(len(records)):
        assert records[i]["e"]["weight"] == expected_result[i]["weight"]
        assert records[i]["e"]["since"] == expected_result[i]["since"]
