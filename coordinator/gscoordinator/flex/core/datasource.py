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

import logging
import os
import pickle

from gscoordinator.flex.core.config import WORKSPACE


class DataSourceManager(object):
    """Management class for data source mapping"""

    def __init__(self):
        # graph id -> mapping
        self._datasource_mapping = {}
        self._pickle_path = os.path.join(WORKSPACE, "datasource_mapping.pickle")
        # recover
        self.try_to_recover_from_disk()

    def try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._pickle_path):
                logging.info(
                    "Recover data source mapping from file %s",
                    self._pickle_path,
                )
                with open(self._pickle_path, "rb") as f:
                    self._datasource_mapping = pickle.load(f)
        except Exception as e:
            logging.warn("Failed to recover data source mapping: %s", str(e))

    def dump_to_disk(self):
        try:
            with open(self._pickle_path, "wb") as f:
                pickle.dump(self._datasource_mapping, f)
        except Exception as e:
            logging.warn("Failed to dump data source mapping: %s", str(e))

    def get_edge_full_label(
        self,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        return f"{source_vertex_type}_{type_name}_{destination_vertex_type}"

    def get_vertex_datasource(self, graph_id: str, vertex_type: str) -> dict:
        rlt = {}
        if (
            graph_id in self._datasource_mapping
            and vertex_type in self._datasource_mapping[graph_id]["vertices"]
        ):
            rlt = self._datasource_mapping[graph_id]["vertices"][vertex_type]
        return rlt

    def get_edge_datasource(
        self,
        graph_id: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> dict:
        rlt = {}
        elabel = self.get_edge_full_label(
            edge_type, source_vertex_type, destination_vertex_type
        )
        if (
            graph_id in self._datasource_mapping
            and elabel in self._datasource_mapping[graph_id]["edges"]
        ):
            rlt = self._datasource_mapping[graph_id]["edges"][elabel]
        return rlt

    def bind_datasource_in_batch(self, graph_id: str, schema_mapping: dict):
        datasource_mapping = {"vertices": {}, "edges": {}}
        # vertices
        for vdsm in schema_mapping["vertex_mappings"]:
            datasource_mapping["vertices"][vdsm["type_name"]] = vdsm
        # edge
        for edsm in schema_mapping["edge_mappings"]:
            elabel = self.get_edge_full_label(
                edsm["type_triplet"]["edge"],
                edsm["type_triplet"]["source_vertex"],
                edsm["type_triplet"]["destination_vertex"],
            )
            datasource_mapping["edges"][elabel] = edsm
        # update
        if graph_id not in self._datasource_mapping:
            self._datasource_mapping[graph_id] = datasource_mapping
        else:
            self._datasource_mapping[graph_id]["vertices"].update(
                datasource_mapping["vertices"]
            )
            self._datasource_mapping[graph_id]["edges"].update(
                datasource_mapping["edges"]
            )
        # dump
        self.dump_to_disk()

    def get_datasource_mapping(self, graph_id: str) -> dict:
        datasource_mapping = {"vertex_mappings": [], "edge_mappings": []}
        if graph_id in self._datasource_mapping:
            for _, v_mapping in self._datasource_mapping[graph_id]["vertices"].items():
                datasource_mapping["vertex_mappings"].append(v_mapping)
            for _, e_mapping in self._datasource_mapping[graph_id]["edges"].items():
                datasource_mapping["edge_mappings"].append(e_mapping)
        return datasource_mapping

    def unbind_vertex_datasource(self, graph_id: str, vertex_type: str):
        if graph_id in self._datasource_mapping:
            if vertex_type in self._datasource_mapping[graph_id]["vertices"]:
                del self._datasource_mapping[graph_id]["vertices"][vertex_type]
                self.dump_to_disk()

    def unbind_edge_datasource(
        self,
        graph_id: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ):
        elabel = self.get_edge_full_label(
            edge_type, source_vertex_type, destination_vertex_type
        )
        if graph_id in self._datasource_mapping:
            if elabel in self._datasource_mapping[graph_id]["edges"]:
                del self._datasource_mapping[graph_id]["edges"][elabel]
                self.dump_to_disk()

    def delete_datasource_by_id(self, graph_id: str):
        if graph_id in self._datasource_mapping:
            del self._datasource_mapping[graph_id]
            self.dump_to_disk()
