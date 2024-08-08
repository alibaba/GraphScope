#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

import os
import time

import pytest

from graphscope.gsctl.impl import bind_datasource_in_batch
from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import create_alert_receiver
from graphscope.gsctl.impl import create_edge_type
from graphscope.gsctl.impl import create_vertex_type
from graphscope.gsctl.impl import delete_alert_receiver_by_id
from graphscope.gsctl.impl import delete_alert_rule_by_id
from graphscope.gsctl.impl import delete_edge_type_by_name
from graphscope.gsctl.impl import delete_vertex_type_by_name
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import get_deployment_pod_log
from graphscope.gsctl.impl import get_deployment_resource_usage
from graphscope.gsctl.impl import get_deployment_status
from graphscope.gsctl.impl import get_graph_by_id
from graphscope.gsctl.impl import get_storage_usage
from graphscope.gsctl.impl import import_schema
from graphscope.gsctl.impl import list_alert_messages
from graphscope.gsctl.impl import list_alert_receivers
from graphscope.gsctl.impl import list_alert_rules
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import submit_dataloading_job
from graphscope.gsctl.impl import update_alert_receiver_by_id
from graphscope.gsctl.impl import update_alert_rule_by_id
from graphscope.gsctl.impl import upload_file


def get_coordinator_endpoint():
    return os.environ.get("COORDINATOR_SERVICE_ENDPOINT", "http://127.0.0.1:8080")


COORDINATOR_ENDPOINT = get_coordinator_endpoint()


modern_graph_person_type = {
    "type_name": "person2",
    "primary_keys": ["id"],
    "properties": [
        {
            "property_name": "id",
            "property_type": {"primitive_type": "DT_SIGNED_INT64"},
            "description": "",
            "property_id": 19,
        },
        {
            "property_name": "name",
            "property_type": {"string": {"long_text": ""}},
            "description": "",
            "property_id": 20,
        },
        {
            "property_name": "age",
            "property_type": {"primitive_type": "DT_SIGNED_INT32"},
            "description": "",
            "property_id": 21,
        },
    ],
    "description": "",
}


modern_graph_knows_type = {
    "type_name": "knows2",
    "vertex_type_pair_relations": [
        {"source_vertex": "person2", "destination_vertex": "person2"}
    ],
    "primary_keys": [],
    "properties": [
        {
            "property_name": "weight",
            "property_type": {"primitive_type": "DT_DOUBLE"},
            "description": "",
            "property_id": 22,
        }
    ],
    "description": "",
}


modern_graph_schema = {
    "vertex_types": [
        {
            "type_name": "person2",
            "primary_keys": ["id"],
            "properties": [
                {
                    "property_name": "id",
                    "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    "description": "",
                    "property_id": 19,
                },
                {
                    "property_name": "name",
                    "property_type": {"string": {"long_text": ""}},
                    "description": "",
                    "property_id": 20,
                },
                {
                    "property_name": "age",
                    "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    "description": "",
                    "property_id": 21,
                },
            ],
            "description": "",
        }
    ],
    "edge_types": [
        {
            "type_name": "knows2",
            "vertex_type_pair_relations": [
                {"source_vertex": "person2", "destination_vertex": "person2"}
            ],
            "primary_keys": [],
            "properties": [
                {
                    "property_name": "weight",
                    "property_type": {"primitive_type": "DT_DOUBLE"},
                    "description": "",
                    "property_id": 22,
                }
            ],
            "description": "",
        }
    ],
}


class TestE2E(object):
    def setup_class(self):
        self.deployment_info = connect_coordinator(COORDINATOR_ENDPOINT)
        # graph id
        graphs = list_graphs()
        assert len(graphs) == 1
        self._graph_id = graphs[0].id
        assert self._graph_id is not None

    @pytest.mark.skipif(
        os.environ.get("RUN_ON_MINIKUBE", None) == "ON",
        reason="Minikube does not support metric API",
    )
    def test_deployment(self):
        assert self.deployment_info.instance_name is not None
        assert self.deployment_info.cluster_type == "KUBERNETES"
        assert self.deployment_info.engine == "Gaia"
        assert self.deployment_info.storage == "MutablePersistent"
        assert self.deployment_info.frontend == "Cypher/Gremlin"
        assert self.deployment_info.version is not None
        assert self.deployment_info.creation_time is not None
        # status
        status = get_deployment_status()
        assert status is not None
        # resource usage
        rlt = get_deployment_resource_usage()
        assert len(rlt.cpu_usage) > 0
        assert len(rlt.memory_usage) > 0
        # storage usage
        rlt = get_storage_usage()
        assert rlt.storage_usage is not None
        # frontend pod log
        frontend_pod = status.pods["frontend"][0]
        # fetch log from k8s
        pod_log = get_deployment_pod_log(
            frontend_pod.name, frontend_pod.component_belong_to, False
        ).to_dict()
        for container, container_log in pod_log["log"].items():
            assert container_log != ""

    def test_schema(self):
        # create vertex type
        create_vertex_type(self._graph_id, modern_graph_person_type)
        graph = get_graph_by_id(self._graph_id).to_dict()
        vertex_type_exists = False
        for vertex in graph["schema"]["vertex_types"]:
            if vertex["type_name"] == modern_graph_person_type["type_name"]:
                vertex_type_exists = True
        assert vertex_type_exists
        # create type edge
        create_edge_type(self._graph_id, modern_graph_knows_type)
        graph = get_graph_by_id(self._graph_id).to_dict()
        edge_type_exists = False
        for edge in graph["schema"]["edge_types"]:
            if edge["type_name"] == modern_graph_knows_type["type_name"]:
                edge_type_exists = True
        assert edge_type_exists
        # delete edge type
        for relation in modern_graph_knows_type["vertex_type_pair_relations"]:
            delete_edge_type_by_name(
                self._graph_id,
                modern_graph_knows_type["type_name"],
                relation["source_vertex"],
                relation["destination_vertex"],
            )
        # delete vertex type
        delete_vertex_type_by_name(
            self._graph_id, modern_graph_person_type["type_name"]
        )

    @pytest.mark.skip(reason="")
    def test_import_schema_in_batch(self):
        import_schema(self._graph_id, modern_graph_schema)
        # check
        graph = get_graph_by_id(self._graph_id).to_dict()
        assert len(graph["schema"]["vertex_types"]) == len(
            modern_graph_schema["vertex_types"]
        )
        assert len(graph["schema"]["edge_types"]) == len(
            modern_graph_schema["edge_types"]
        )
        # delete
        for edge in modern_graph_schema["edge_types"]:
            for relation in edge["vertex_type_pair_relations"]:
                delete_edge_type_by_name(
                    self._graph_id,
                    edge["type_name"],
                    relation["source_vertex"],
                    relation["destination_vertex"],
                )
        for vertex in modern_graph_schema["vertex_types"]:
            delete_vertex_type_by_name(self._graph_id, vertex["type_name"])

    def test_dataloading(self, tmpdir):
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
                    "type_name": "person2",
                    "inputs": [upload_file(str(person))],
                    "column_mappings": [
                        {"column": {"index": 0}, "property": "id"},
                        {"column": {"index": 1}, "property": "name"},
                        {"column": {"index": 2}, "property": "age"},
                    ],
                }
            ],
            "edge_mappings": [
                {
                    "type_triplet": {
                        "edge": "knows2",
                        "source_vertex": "person2",
                        "destination_vertex": "person2",
                    },
                    "inputs": [upload_file(str(person_knows_person))],
                    "source_vertex_mappings": [
                        {"column": {"index": 0}, "property": "id"}
                    ],
                    "destination_vertex_mappings": [
                        {"column": {"index": 1}, "property": "id"}
                    ],
                    "column_mappings": [{"column": {"index": 2}, "property": "weight"}],
                }
            ],
        }
        # test bind data source
        create_vertex_type(self._graph_id, modern_graph_person_type)
        create_edge_type(self._graph_id, modern_graph_knows_type)
        bind_datasource_in_batch(self._graph_id, datasource)
        ds = get_datasource_by_id(self._graph_id).to_dict()
        for vertex_mapping in datasource["vertex_mappings"]:
            for vertex_mapping2 in ds["vertex_mappings"]:
                if vertex_mapping["type_name"] == vertex_mapping2["type_name"]:
                    assert vertex_mapping == vertex_mapping2
        for edge_mapping in datasource["edge_mappings"]:
            for edge_mapping2 in ds["edge_mappings"]:
                if edge_mapping["type_triplet"] == edge_mapping2["type_triplet"]:
                    assert edge_mapping == edge_mapping2
        # test data loading
        job_config = {
            "loading_config": {
                "import_option": "overwrite",
            },
            "vertices": [
                {"type_name": "person2"},
            ],
            "edges": [
                {
                    "type_name": "knows2",
                    "source_vertex": "person2",
                    "destination_vertex": "person2",
                },
            ],
        }
        scheduler_id = submit_dataloading_job(self._graph_id, job_config)
        start_time = time.time()
        # waiting for 30s
        flag = False
        while True:
            time.sleep(1)
            jobs = list_jobs()
            for status in jobs:
                if status.detail["scheduler_id"] == scheduler_id:
                    if status.status == "SUCCESS":
                        flag = True
                        break
            if flag:
                break
            if time.time() - start_time > 30:
                raise TimeoutError(f"Waiting timeout for loading job {scheduler_id}")
        # delete edge type
        for relation in modern_graph_knows_type["vertex_type_pair_relations"]:
            delete_edge_type_by_name(
                self._graph_id,
                modern_graph_knows_type["type_name"],
                relation["source_vertex"],
                relation["destination_vertex"],
            )
        # delete vertex type
        delete_vertex_type_by_name(
            self._graph_id, modern_graph_person_type["type_name"]
        )

    def test_alert(self):
        # list alert rules
        alert_rules = list_alert_rules()
        high_disk_utilication_alert = None
        for rule in alert_rules:
            if rule.name == "HighDiskUtilization":
                high_disk_utilication_alert = rule.to_dict()
        assert high_disk_utilication_alert["enable"] is True
        # update alert rule
        high_disk_utilication_alert["enable"] = False
        update_alert_rule_by_id(
            high_disk_utilication_alert["id"], high_disk_utilication_alert
        )
        alert_rules = list_alert_rules()
        for rule in alert_rules:
            if rule.name == "HighDiskUtilization":
                assert rule.enable is False
        # delete alert rule
        delete_alert_rule_by_id(high_disk_utilication_alert["id"])
        alert_rules = list_alert_rules()
        for rule in alert_rules:
            if rule.name == "HighDiskUtilization":
                assert False
        # list alert messages
        alert_mesages = list_alert_messages()
        assert alert_mesages is not None
        # create alert receiver
        receiver = {
            "type": "webhook",
            "webhook_url": "https://www.abc.com/robot",
            "is_at_all": False,
            "at_user_ids": ["111111"],
            "enable": False,
        }
        create_alert_receiver(receiver)
        # list alert receivers
        alert_receivers = list_alert_receivers()
        assert len(alert_receivers) > 0
        # update alert receiver
        receiver = alert_receivers[0].to_dict()
        receiver["enable"] = True
        receiver["at_user_ids"] = []
        update_alert_receiver_by_id(receiver["id"], receiver)
        alert_receivers = list_alert_receivers()
        for r in alert_receivers:
            if r.id == receiver["id"]:
                assert r.to_dict() == receiver
        # delete alert receiver
        delete_alert_receiver_by_id(receiver["id"])
        alert_receivers = list_alert_receivers()
        for r in alert_receivers:
            if r.id == receiver["id"]:
                assert False

    def teardown_class(self):
        pass
