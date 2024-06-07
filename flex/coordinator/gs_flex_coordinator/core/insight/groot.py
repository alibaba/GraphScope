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

import itertools
import logging
import os
import pickle
from typing import List

from gs_flex_coordinator.core.config import (CLUSTER_TYPE, INSTANCE_NAME,
                                             WORKSPACE)
from gs_flex_coordinator.core.insight.graph import get_groot_graph
from gs_flex_coordinator.core.insight.job import DataloadingJobScheduler
from gs_flex_coordinator.core.scheduler import schedule
from gs_flex_coordinator.models import JobStatus


class GrootClient(object):
    """Class used to interact with Groot"""

    def __init__(self):
        self._graph = get_groot_graph()
        # workspace
        self._workspace = os.path.join(WORKSPACE, "groot")
        os.makedirs(self._workspace, exist_ok=True)
        # data source
        self._data_source = {"vertices_datasource": {}, "edges_datasource": {}}
        # pickle path
        self._datasource_pickle_path = os.path.join(
            self._workspace, "datasource.pickle"
        )
        # job
        self._job_scheduler = {}
        # job status
        self._job_status = {}
        # pickle path
        self._job_status_pickle_path = os.path.join(
            self._workspace, "job_status.pickle"
        )
        # recover
        self._try_to_recover_from_disk()
        # dump job status to disk every 10s
        self._pickle_job_status_job = (
            schedule.every(10)
            .seconds.do(self._pickle_job_status_impl)
            .tag("pickle", "job status")
        )

    def _try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._datasource_pickle_path):
                logging.info(
                    "Recover data source from file %s", self._datasource_pickle_path
                )
                with open(self._datasource_pickle_path, "rb") as f:
                    self._data_source = pickle.load(f)
        except Exception as e:
            logging.warn("Failed to recover data source: %s", str(e))

        try:
            if os.path.exists(self._job_status_pickle_path):
                logging.info(
                    "Recover job status from file %s", self._job_status_pickle_path
                )
                with open(self._job_status_pickle_path, "rb") as f:
                    data = pickle.load(f)
                    for jobid, status in data.items():
                        self._job_status[jobid] = JobStatus.from_dict(status)
        except Exception as e:
            logging.warn("Failed to recover job status: %s", str(e))

    def _pickle_datasource_impl(self):
        try:
            with open(self._datasource_pickle_path, "wb") as f:
                pickle.dump(self._data_source, f)
        except Exception as e:
            logging.warn("Failed to dump data source: %s", str(e))

    def _pickle_job_status_impl(self):
        try:
            rlt = {}
            for jobid, status in self._job_status.items():
                rlt[jobid] = status.to_dict()
            with open(self._job_status_pickle_path, "wb") as f:
                pickle.dump(rlt, f)
        except Exception as e:
            logging.warn("Failed to dump job status: %s", str(e))

    def get_edge_full_label(
        self, type_name: str, source_vertex_type: str, destination_vertex_type: str
    ) -> str:
        return f"{source_vertex_type}_{type_name}_{destination_vertex_type}"

    def get_current_graph(self):
        return self._graph

    def list_groot_graph(self) -> list:
        rlts = [self._graph.to_dict()]
        return rlts

    def create_vertex_type(self, graph_name: str, vtype_dict: dict) -> str:
        return self._graph.create_vertex_type(vtype_dict)

    def create_edge_type(self, graph_name: str, etype_dict: dict) -> str:
        return self._graph.create_edge_type(etype_dict)

    def delete_vertex_type(self, graph_name: str, vertex_type: str) -> str:
        rlt = self._graph.delete_vertex_type(graph_name, vertex_type)
        # unbind data source
        if vertex_type in self._data_source["vertices_datasource"]:
            del self._data_source["vertices_datasource"][vertex_type]
            self._pickle_datasource_impl()
        return rlt

    def delete_edge_type(
        self,
        graph_name: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        rlt = self._graph.delete_edge_type(
            graph_name, edge_type, source_vertex_type, destination_vertex_type
        )
        # unbind data source
        edge_label = self.get_edge_full_label(
            edge_type, source_vertex_type, destination_vertex_type
        )
        if edge_label in self._data_source["edges_datasource"]:
            del self._data_source["edges_datasource"][edge_label]
            self._pickle_datasource_impl()
        return rlt

    def get_groot_schema(self, graph_name: str) -> dict:
        return self._graph.schema

    def import_groot_schema(self, graph_name: str, schema: dict) -> str:
        def _data_type_to_groot(dt):
            if dt == "DT_DOUBLE":
                return "DOUBLE"
            elif dt == "DT_SIGNED_INT64":
                return "LONG"
            elif dt == "DT_STRING":
                return "STRING"
            else:
                return dt

        # transfer to groot data type
        for item in itertools.chain(schema["vertices"], schema["edges"]):
            for p in item["properties"]:
                p["type"] = _data_type_to_groot(p["type"])
        return self._graph.import_schema(schema)

    def list_jobs(self) -> List[dict]:
        rlt = []
        for jobid, status in self._job_status.items():
            rlt.append(status.to_dict())
        return rlt

    def import_datasource(self, graph_name: str, data_source: dict) -> str:
        for vertex_data_source in data_source["vertices_datasource"]:
            self._data_source["vertices_datasource"][
                vertex_data_source["type_name"]
            ] = vertex_data_source
        for edge_data_source in data_source["edges_datasource"]:
            edge_label = self.get_edge_full_label(
                edge_data_source["type_name"],
                edge_data_source["source_vertex"],
                edge_data_source["destination_vertex"],
            )
            self._data_source["edges_datasource"][edge_label] = edge_data_source
        self._pickle_datasource_impl()

    def get_service_status(self) -> dict:
        return {
            "status": "running",
            "graph_name": self._graph.name,
            "sdk_endpoints": {
                "gremlin": self._graph.gremlin_interface["gremlin_endpoint"],
                "grpc": self._graph.gremlin_interface["grpc_endpoint"],
            },
        }

    def get_datasource(self, graph_name: str) -> dict:
        rlts = {"vertices_datasource": [], "edges_datasource": []}
        for _, v in self._data_source["vertices_datasource"].items():
            rlts["vertices_datasource"].append(v)
        for _, e in self._data_source["edges_datasource"].items():
            rlts["edges_datasource"].append(e)
        return rlts

    def bind_vertex_datasource(self, graph_name: str, vertex_data_source: dict) -> str:
        self._data_source["vertices_datasource"][
            vertex_data_source["type_name"]
        ] = vertex_data_source
        self._pickle_datasource_impl()
        return "Bind vertex data source successfully"

    def bind_edge_datasource(self, graph_name: str, edge_data_source: dict) -> str:
        edge_label = self.get_edge_full_label(
            edge_data_source["type_name"],
            edge_data_source["source_vertex"],
            edge_data_source["destination_vertex"],
        )
        self._data_source["edges_datasource"][edge_label] = edge_data_source
        self._pickle_datasource_impl()
        return "Bind edge data source successfully"

    def get_vertex_datasource(self, graph_name: str, vertex_type: str) -> dict:
        if vertex_type not in self._data_source["vertices_datasource"]:
            raise RuntimeError(
                f"Vertex type {vertex_type} does not bind any data source"
            )
        return self._data_source["vertices_datasource"][vertex_type]

    def get_edge_datasource(
        self,
        graph_name: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> dict:
        edge_label = self.get_edge_full_label(
            edge_type, source_vertex_type, destination_vertex_type
        )
        if edge_label not in self._data_source["edges_datasource"]:
            raise RuntimeError(f"Edge type {edge_label} does not bind any data source")
        return self._data_source["edges_datasource"][edge_label]

    def unbind_vertex_datasource(self, graph_name: str, vertex_type: str) -> str:
        # check
        vertex_type_exists = False
        schema = self._graph.schema
        for v in schema["vertices"]:
            if vertex_type == v["label"]:
                vertex_type_exists = True
                break
        if not vertex_type_exists:
            raise RuntimeError(f"Vertex type {vertex_type} not exists")
        if vertex_type in self._data_source["vertices_datasource"]:
            del self._data_source["vertices_datasource"][vertex_type]
            self._pickle_datasource_impl()
        return "unbind data source successfully"

    def unbind_edge_datasource(
        self,
        graph_name: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        # check
        edge_type_exists = False
        schema = self._graph.schema
        for e in schema["edges"]:
            for relation in e["relations"]:
                if (
                    edge_type == e["label"]
                    and source_vertex_type == relation["src_label"]
                    and destination_vertex_type == relation["dst_label"]
                ):
                    edge_type_exists = True
                    break
        if not edge_type_exists:
            raise RuntimeError(
                f"Edge type ({source_vertex_type})-[{edge_type}]->({destination_vertex_type}) not exists"
            )
        edge_label = self.get_edge_full_label(
            edge_type, source_vertex_type, destination_vertex_type
        )
        if edge_label in self._data_source["edges_datasource"]:
            del self._data_source["edges_datasource"][edge_label]
            self._pickle_datasource_impl()
            return "unbind data source successfully"

    def create_groot_dataloading_job(self, graph_name: str, job_config: dict) -> str:
        dataloading_job_scheduler = DataloadingJobScheduler(
            job_config=job_config,
            data_source=self._data_source,
            job_scheduler=self._job_scheduler,
            job_status=self._job_status,
            graph=self._graph,
        )
        return dataloading_job_scheduler.schedulerid

    def get_job_by_id(self, job_id: str) -> dict:
        if job_id not in self._job_status:
            raise RuntimeError(f"Job {job_id} not found")
        return self._job_status[job_id].to_dict()

    def delete_job_by_id(self, job_id: str) -> str:
        if job_id not in self._job_status:
            raise RuntimeError(f"Job {job_id} not found")
        if job_id in self._job_scheduler:
            # we don't have some processes in case of restart the coordinator
            # some processes will not exist if the coordinator is restart
            self._job_scheduler[job_id].cancel()
        return f"Submit cancellation job successfully"

    def get_dataloading_config(self, graph_name: str) -> dict:
        config = {
            "graph": INSTANCE_NAME,
            "loading_config": {},
            "vertex_mappings": [],
            "edge_mappings": [],
        }
        # transfer
        for vtype, ds in self._data_source["vertices_datasource"].items():
            column_mappings = []
            if ds["property_mapping"] is not None:
                for index, property_name in ds["property_mapping"].items():
                    column_mappings.append(
                        {
                            "column": {
                                "index": int(index),
                            },
                            "property": property_name,
                        }
                    )
            config["vertex_mappings"].append(
                {
                    "type_name": vtype,
                    "inputs": [ds["location"]],
                    "column_mappings": column_mappings,
                }
            )
        for etype, ds in self._data_source["edges_datasource"].items():
            source_vertex_mappings = []
            for index, _ in ds["source_pk_column_map"].items():
                source_vertex_mappings.append(
                    {
                        "column": {
                            "index": int(index),
                        }
                    }
                )
            destination_vertex_mappings = []
            for index, _ in ds["destination_pk_column_map"].items():
                destination_vertex_mappings.append(
                    {
                        "column": {
                            "index": int(index),
                        }
                    }
                )
            column_mappings = []
            if ds["property_mapping"] is not None:
                for index, property_name in ds["property_mapping"].items():
                    column_mappings.append(
                        {
                            "column": {
                                "index": int(index),
                            },
                            "property": property_name,
                        }
                    )
            config["edge_mappings"].append(
                {
                    "type_triplet": {
                        "edge": ds["type_name"],
                        "source_vertex": ds["source_vertex"],
                        "destination_vertex": ds["destination_vertex"],
                    },
                    "inputs": [ds["location"]],
                    "source_vertex_mappings": source_vertex_mappings,
                    "destination_vertex_mappings": destination_vertex_mappings,
                    "column_mappings": column_mappings,
                }
            )
        return config


def init_groot_client():
    return GrootClient()
