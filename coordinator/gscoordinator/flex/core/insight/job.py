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

import pandas as pd
from graphscope.framework.record import EdgeRecordKey
from graphscope.framework.record import VertexRecordKey

from gscoordinator.flex.core.scheduler import Scheduler
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.core.utils import get_current_time
from gscoordinator.flex.models import JobStatus


class DataloadingJobScheduler(Scheduler):
    def __init__(self, job_config, data_source, job_scheduler, job_status, graph):
        super().__init__(job_config["schedule"], job_config["repeat"])
        self._type = "Data Import"
        self._job_config = job_config
        self._data_source = data_source
        # groot graph
        self._graph = graph
        # register job
        self._job_scheduler = job_scheduler
        # register the current status
        self._job_status = job_status
        # detailed information
        self._detail = self._construct_detailed_info()
        # start
        self.start()

    def _construct_detailed_info(self):
        label_list = []
        for vlabel in self._job_config["vertices"]:
            label_list.append(vlabel)
        for e in self._job_config["edges"]:
            label_list.append(
                self.get_edge_full_label(
                    e["type_name"], e["source_vertex"], e["destination_vertex"]
                )
            )
        detail = {"graph_name": self._graph.name}
        detail["label"] = ",".join(label_list)
        return detail

    def get_edge_full_label(
        self,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        return f"{source_vertex_type}_{type_name}_{destination_vertex_type}"

    def _import_data_from_local_file(self):
        # construct data
        vertices = []
        for vlabel in self._job_config["vertices"]:
            primary_key = self._graph.get_vertex_primary_key(vlabel)
            datasource = self._data_source["vertices_datasource"][vlabel]
            data = pd.read_csv(
                datasource["location"], sep=",|\|", engine="python"
            )  # noqa: W605
            for record in data.itertuples(index=False):
                primary_key_dict = {}
                property_mapping = {}
                for k, v in datasource["property_mapping"].items():
                    if v == primary_key:
                        property_mapping[v] = record[int(k)]
                    else:
                        property_mapping[v] = record[int(k)]
                vertices.append(
                    [
                        VertexRecordKey(vlabel, primary_key_dict),
                        property_mapping,
                    ]
                )
        edges = []
        for e in self._job_config["edges"]:
            elabel = self.get_edge_full_label(
                e["type_name"], e["source_vertex"], e["destination_vertex"]
            )
            datasource = self._data_source["edges_datasource"][elabel]
            data = pd.read_csv(datasource["location"], sep=",|\|", engine="python")
            for record in data.itertuples(index=False):
                source_pk_column_map = {}
                for k, v in datasource["source_pk_column_map"].items():
                    source_pk_column_map[v] = record[int(k)]
                destination_pk_column_map = {}
                for k, v in datasource["destination_pk_column_map"].items():
                    destination_pk_column_map[v] = record[int(k)]
                property_mapping = {}
                for k, v in datasource["property_mapping"].items():
                    property_mapping[v] = record[int(k)]
                edges.append(
                    [
                        EdgeRecordKey(
                            e["type_name"],
                            VertexRecordKey(e["source_vertex"], source_pk_column_map),
                            VertexRecordKey(
                                e["destination_vertex"],
                                destination_pk_column_map,
                            ),
                        ),
                        property_mapping,
                    ]
                )
        # insert
        if not self.stopped():
            conn = self._graph.conn
            g = conn.g()
            snapshot_id = g.update_vertex_properties_batch(vertices)
            snapshot_id = g.update_edge_properties_batch(edges)
            conn.remote_flush(snapshot_id, timeout_ms=600000)

    def _set_and_update_job_status(
        self, status: str, start_time: str, end_time=None, log=None
    ):
        job_status = {
            "job_id": self.jobid,
            "type": self._type,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "log": log,
            "detail": self._detail,
        }
        # remove None
        job_status = {k: v for k, v in job_status.items() if v is not None}
        # update
        self._job_status[self.jobid] = JobStatus.from_dict(job_status)

    def run(self):
        """This function needs to handle exception by itself"""
        start_time = encode_datetime(self.last_run)
        try:
            # register job
            self._job_scheduler[self.jobid] = self
            # init status
            self._set_and_update_job_status("RUNNING", start_time)
            load_from_odps = True
            # check vertices
            for vlabel in self._job_config["vertices"]:
                if vlabel not in self._data_source["vertices_datasource"]:
                    raise RuntimeError(
                        f"Vertex type {vlabel} does not bind any data source"
                    )
                location = self._data_source["vertices_datasource"][vlabel]["location"]
                load_from_odps = load_from_odps and location.startswith("odps://")
            # check edges
            for e in self._job_config["edges"]:
                elabel = self.get_edge_full_label(
                    e["type_name"], e["source_vertex"], e["destination_vertex"]
                )
                if elabel not in self._data_source["edges_datasource"]:
                    raise RuntimeError(
                        f"Edge type {elabel} does not bind any data source"
                    )
                location = self._data_source["edges_datasource"][elabel]["location"]
                load_from_odps = load_from_odps and location.startswith("odps://")
            if load_from_odps:
                self._import_data_from_odps()
            else:
                self._import_data_from_local_file()
        except Exception as e:
            end_time = encode_datetime(get_current_time())
            self._set_and_update_job_status("FAILED", start_time, end_time, str(e))
        else:
            end_time = encode_datetime(get_current_time())
            if not self.stopped():
                self._set_and_update_job_status("SUCCESS", start_time, end_time)
            else:
                self._set_and_update_job_status("CANCELLED", start_time, end_time)
