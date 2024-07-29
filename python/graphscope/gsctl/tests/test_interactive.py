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

import warnings

# Disable warnings
warnings.filterwarnings("ignore", category=Warning)

import time

import pytest
from click.testing import CliRunner

from graphscope.gsctl.impl import bind_datasource_in_batch
from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import create_graph
from graphscope.gsctl.impl import create_stored_procedure
from graphscope.gsctl.impl import delete_graph_by_id
from graphscope.gsctl.impl import delete_stored_procedure_by_id
from graphscope.gsctl.impl import disconnect_coordinator
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import get_job_by_id
from graphscope.gsctl.impl import get_stored_procedure_by_id
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_service_status
from graphscope.gsctl.impl import list_stored_procedures
from graphscope.gsctl.impl import restart_service
from graphscope.gsctl.impl import start_service
from graphscope.gsctl.impl import stop_service
from graphscope.gsctl.impl import submit_dataloading_job
from graphscope.gsctl.impl import unbind_edge_datasource
from graphscope.gsctl.impl import unbind_vertex_datasource
from graphscope.gsctl.impl import update_stored_procedure_by_id
from graphscope.gsctl.impl import upload_file

COORDINATOR_ENDPOINT = "http://127.0.0.1:8080"


modern_graph = {
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


modern_graph_with_empty_edge_property = {
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
                "properties": [],
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


modern_graph_datasource = {
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["@/home/graphscope/alibaba/test/interative/person.csv"],
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
            "inputs": [
                "@/home/graphscope/alibaba/test/interative/person_knows_person.csv"
            ],
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


person_csv_content = "id|name|age\n1|marko|29\n2|vadas|27\n4|josh|32\n6|peter|35"
person_knows_person_csv_content = "person.id|person.id|weight\n1|2|0.5\n1|4|1.0"


class TestE2EInteractive(object):
    def setup_class(self):
        self.deployment_info = connect_coordinator(COORDINATOR_ENDPOINT)

    def test_deployment_info(self):
        assert self.deployment_info.instance_name == "demo"
        assert self.deployment_info.cluster_type == "HOSTS"
        assert self.deployment_info.engine == "Hiactor"
        assert self.deployment_info.storage == "MutableCSR"
        assert self.deployment_info.frontend == "Cypher/Gremlin"
        assert self.deployment_info.version is not None
        assert self.deployment_info.creation_time is not None

    def test_graph(self):
        # test create a new graph
        graph_id = create_graph(modern_graph)
        assert graph_id is not None
        new_graph_exist = False
        graphs = list_graphs()
        for g in graphs:
            if g.name == "modern_graph" and g.id == graph_id:
                new_graph_exist = True
        assert new_graph_exist
        # test delete a graph by id
        delete_graph_by_id(graph_id)
        new_graph_exist = False
        graphs = list_graphs()
        for g in graphs:
            if g.name == "modern_graph" and g.id == graph_id:
                new_graph_exist = True
        assert not new_graph_exist

    def test_bulk_loading(self, tmpdir):
        # person
        person = tmpdir.join("person.csv")
        person.write("id|name|age\n1|marko|29\n2|vadas|27\n4|josh|32\n6|peter|35")
        # person -> knows -> person
        person_knows_person = tmpdir.join("person_knows_person.csv")
        person_knows_person.write("person.id|person.id|weight\n1|2|0.5\n1|4|1.0")
        # data source mapping
        datasource = {
            "vertex_mappings": [
                {
                    "type_name": "person",
                    "inputs": [upload_file(str(person))],
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
                    "inputs": [upload_file(str(person_knows_person))],
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
        # test bind data source
        graph_id = create_graph(modern_graph)
        bind_datasource_in_batch(graph_id, datasource)
        ds = get_datasource_by_id(graph_id)
        assert ds.to_dict() == datasource
        # test bulk loading
        job_config = {
            "loading_config": {
                "import_option": "overwrite",
                "format": {
                    "type": "csv",
                    "metadata": {
                        "delimiter": "|",
                        "header_row": "true",
                    },
                },
            },
            "vertices": [
                {"type_name": "person"},
            ],
            "edges": [
                {
                    "type_name": "knows",
                    "source_vertex": "person",
                    "destination_vertex": "person",
                },
            ],
        }
        job_id = submit_dataloading_job(graph_id, job_config)
        start_time = time.time()
        # waiting for 30s
        while True:
            time.sleep(1)
            status = get_job_by_id(job_id)
            if status.status == "SUCCESS":
                assert status.id == job_id
                assert status.start_time is not None
                assert status.end_time is not None
                assert status.log is not None
                assert status.detail is not None
                break
            if time.time() - start_time > 30:
                raise TimeoutError(f"Waiting timeout for loading job {job_id}")
        # test unbind data source
        unbind_vertex_datasource(graph_id, "person")
        unbind_edge_datasource(graph_id, "knows", "person", "person")
        ds = get_datasource_by_id(graph_id).to_dict()
        assert not ds["vertex_mappings"]
        assert not ds["edge_mappings"]
        delete_graph_by_id(graph_id)

    def test_procedure(self):
        stored_procedure_dict = {
            "name": "procedure_name",
            "description": "This is a test procedure",
            "query": "MATCH (n) RETURN COUNT(n);",
            "type": "cypher",
        }
        # test create a new procedure
        graph_id = create_graph(modern_graph)
        stored_procedure_id = create_stored_procedure(graph_id, stored_procedure_dict)
        assert stored_procedure_id is not None
        new_procedure_exist = False
        procedures = list_stored_procedures(graph_id)
        for p in procedures:
            if p.id == stored_procedure_id and p.name == "procedure_name":
                new_procedure_exist = True
        assert new_procedure_exist
        # test update a procedure
        description = "This is an updated description"
        update_stored_procedure_by_id(
            graph_id, stored_procedure_id, {"description": description}
        )
        procedure = get_stored_procedure_by_id(graph_id, stored_procedure_id)
        assert procedure.description == description
        # test delete a procedure
        delete_stored_procedure_by_id(graph_id, stored_procedure_id)
        new_procedure_exist = False
        procedures = list_stored_procedures(graph_id)
        for p in procedures:
            if p.id == stored_procedure_id and p.name == "procedure_name":
                new_procedure_exist = True
        assert not new_procedure_exist
        delete_graph_by_id(graph_id)

    def test_service(self):
        original_graph_id = None
        status = list_service_status()
        for s in status:
            if s.status == "Running":
                original_graph_id = s.graph_id
        assert original_graph_id is not None
        # start service on a new graph
        new_graph_id = create_graph(modern_graph)
        start_service(new_graph_id)
        status = list_service_status()
        for s in status:
            if s.graph_id == new_graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"
        # restart the service
        restart_service()
        status = list_service_status()
        for s in status:
            if s.graph_id == new_graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"
        # stop the service
        stop_service()
        status = list_service_status()
        for s in status:
            assert s.status == "Stopped"
        # delete graph and switch to original graph
        delete_graph_by_id(new_graph_id)
        start_service(original_graph_id)
        status = list_service_status()
        for s in status:
            if s.graph_id == original_graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"

    def test_suit_case(self):
        # case 1:
        # during deleting a graph, make sure the stored procedures
        # on that graph are deleted at the same time
        stored_procedure_dict = {
            "name": "procedure_name",
            "description": "This is a test procedure",
            "query": "MATCH (n) RETURN COUNT(n);",
            "type": "cypher",
        }
        graph_id = create_graph(modern_graph)
        graph_id_2 = create_graph(modern_graph)
        # create a procedure on graph 1
        stored_procedure_id = create_stored_procedure(graph_id, stored_procedure_dict)
        assert stored_procedure_id == "procedure_name"
        # delete the graph 1, then create a new procedure on graph 2
        delete_graph_by_id(graph_id)
        stored_procedure_id = create_stored_procedure(graph_id_2, stored_procedure_dict)
        assert stored_procedure_id == "procedure_name"
        delete_graph_by_id(graph_id_2)

    def test_corner_case_on_starting_service(self):
        original_graph_id = None
        status = list_service_status()
        for s in status:
            if s.status == "Running":
                original_graph_id = s.graph_id
        assert original_graph_id is not None

        # case 1:
        # start service on vertex only graph
        graph_id = create_graph(modern_graph_vertex_only)
        start_service(graph_id)
        status = list_service_status()
        for s in status:
            if s.graph_id == graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"
        stop_service()
        delete_graph_by_id(graph_id)

        # case 2:
        # start service on graph with empty edge property
        graph_id = create_graph(modern_graph_with_empty_edge_property)
        start_service(original_graph_id)
        status = list_service_status()
        for s in status:
            if s.graph_id == original_graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"
        stop_service()
        delete_graph_by_id(graph_id)

        # switch to original graph
        start_service(original_graph_id)
        status = list_service_status()
        for s in status:
            if s.graph_id == original_graph_id:
                assert s.status == "Running"
            else:
                assert s.status == "Stopped"

    def teardown_class(self):
        disconnect_coordinator()
