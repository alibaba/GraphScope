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

import datetime
import http.client
import json
import time
import urllib.parse

import pandas as pd
from graphscope.framework.record import EdgeRecordKey
from graphscope.framework.record import VertexRecordKey

from gscoordinator.flex.core.config import BASEID
from gscoordinator.flex.core.config import PROJECT
from gscoordinator.flex.core.config import STUDIO_WRAPPER_ENDPOINT
from gscoordinator.flex.core.insight.utils import convert_to_configini
from gscoordinator.flex.core.scheduler import Scheduler
from gscoordinator.flex.core.stoppable_thread import StoppableThread
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.models import JobStatus


class FetchDataloadingJobStatus(object):
    def __init__(self, graph, status: JobStatus):
        self._graph = graph
        self._status = status
        self._fetching_thread = StoppableThread(target=self._fetch_impl, args=())
        self._fetching_thread.daemon = True
        self._fetching_thread.start()

    def _fetch_impl(self):
        if self.completed:
            return
        s = self._status.to_dict()
        conn = http.client.HTTPConnection(STUDIO_WRAPPER_ENDPOINT)
        params = urllib.parse.urlencode(
            {"jobId": s["id"], "project": PROJECT, "baseId": BASEID}
        )
        while not self._fetching_thread.stopped():
            try:
                conn.request(
                    "GET", "{0}?{1}".format("/api/v1/graph/dataloading", params)
                )
                r = conn.getresponse()
                if r.status > 400 and r.status < 600:
                    s["status"] = "FAILED"
                    s["log"] = str(r.read())
                    break
                rlt = json.loads(r.read().decode("utf-8"))
                if rlt["success"]:
                    data = rlt["data"]
                    s["status"] = data["status"].upper()
                    if s["status"] == "SUCCEED":
                        s["status"] = "SUCCESS"
                    s["log"] = data["message"]
                    s["detail"]["progress"] = data["progress"]
                    s["detail"]["build_stage_instance_id"] = data["instanceId"]
                    s["detail"]["build_stage_logview"] = data["logview"]
                    # update the latest dataloading time
                    if s["status"] == "SUCCESS":
                        self._graph.data_update_time = datetime.datetime.now().strftime(
                            "%Y/%m/%d %H:%M:%S"
                        )
                else:
                    s["status"] = "FAILED"
                    s["log"] = rlt["message"]
            except Exception as e:
                s["status"] = "FAILED"
                s["log"] = "Internel error: {0}".format(str(e))
            finally:
                self._status = JobStatus.from_dict(s)
                time.sleep(5)
                if self.completed:
                    break
        s["end_time"] = encode_datetime(datetime.datetime.now())
        self._status = JobStatus.from_dict(s)

    @property
    def completed(self):
        if self._status.status in ["CANCELLED", "SUCCESS", "FAILED"]:
            return True
        if "progress" in self._status.detail and self._status.detail["progress"] == 100:
            return True
        return False

    @property
    def status(self) -> dict:
        return self._status.to_dict()

    def cancel(self):
        """Set the running job thread stoppable and wait for the thread
        to exit properly by using join() method.
        """
        if self._fetching_thread.is_alive():
            self._fetching_thread.stop()
            self._fetching_thread.join()
            s = self._status.to_dict()
            s["status"] = "CANCELLED"
            self._status = JobStatus.from_dict(s)


class DataloadingJobScheduler(Scheduler):
    def __init__(self, config, ds_manager, job_status, graph, at_time, repeat):
        super().__init__(at_time, repeat)
        self._type = "Data Import"
        # job config
        self._config = config
        # data source manager
        self._ds_manager = ds_manager
        # groot graph
        self._graph = graph
        # register the job status
        self._job_status = job_status
        # detailed information
        self._detail = self._construct_detailed_info()
        # used to list dataloading jobs from all the scheduler tasks
        self._tags = [self._type, "detail={0}".format(json.dumps(self._detail))]
        # start
        self.start()

    def _construct_detailed_info(self):
        label_list = []
        for v in self._config["vertices"]:
            label_list.append(v["type_name"])
        for e in self._config["edges"]:
            label_list.append(
                self.get_edge_full_label(
                    e["type_name"], e["source_vertex"], e["destination_vertex"]
                )
            )
        detail = {
            "graph_id": self._graph.id,
            "graph_name": self._graph.name,
            "scheduler_id": self.schedulerid,
        }
        detail["label"] = ", ".join(label_list)
        return detail

    def get_edge_full_label(
        self,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ) -> str:
        return f"{source_vertex_type}_{type_name}_{destination_vertex_type}"

    def generate_job_status(self, status: str, end_time: str, log=None) -> JobStatus:
        return JobStatus.from_dict(
            {
                "id": self.jobid,
                "type": self._type,
                "status": status,
                "start_time": encode_datetime(self.last_run),
                "end_time": encode_datetime(end_time),
                "log": str(log),
                "detail": self._detail,
            }
        )

    def import_data_from_local_file(self):
        # construct data
        vertices = []
        for v in self._config["vertices"]:
            vlabel = v["type_name"]
            primary_key = self._graph.get_vertex_primary_key(vlabel)
            datasource = self._ds_manager.get_vertex_datasource(self._graph.id, vlabel)
            data = pd.read_csv(
                datasource["inputs"][0], sep=",|\|", engine="python"
            )  # noqa: W605
            for record in data.itertuples(index=False):
                primary_key_dict = {}
                property_mapping = {}
                for column_mapping in datasource["column_mappings"]:
                    if primary_key == column_mapping["property"]:
                        primary_key_dict[column_mapping["property"]] = record[
                            column_mapping["column"]["index"]
                        ]
                    else:
                        property_mapping[column_mapping["property"]] = record[
                            column_mapping["column"]["index"]
                        ]
                vertices.append(
                    [
                        VertexRecordKey(vlabel, primary_key_dict),
                        property_mapping,
                    ]
                )
        edges = []
        for e in self._config["edges"]:
            datasource = self._ds_manager.get_edge_datasource(
                self._graph.id,
                e["type_name"],
                e["source_vertex"],
                e["destination_vertex"],
            )
            data = pd.read_csv(datasource["inputs"][0], sep=",|\|", engine="python")
            for record in data.itertuples(index=False):
                source_pk_column_map = {}
                for column_mapping in datasource["source_vertex_mappings"]:
                    source_pk_column_map[column_mapping["property"]] = record[
                        column_mapping["column"]["index"]
                    ]
                destination_pk_column_map = {}
                for column_mapping in datasource["destination_vertex_mappings"]:
                    destination_pk_column_map[column_mapping["property"]] = record[
                        column_mapping["column"]["index"]
                    ]
                property_mapping = {}
                for column_mapping in datasource["column_mappings"]:
                    property_mapping[column_mapping["property"]] = record[
                        column_mapping["column"]["index"]
                    ]
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
            return self.generate_job_status("SUCCESS", end_time=datetime.datetime.now())
        else:
            return self.generate_job_status("CANCELLED", end_time=datetime.datetime.now())

    def run(self):
        """This function needs to handle exception by itself"""
        conn = None
        try:
            load_from_odps = True
            # check vertices
            for v in self._config["vertices"]:
                vertex_type = v["type_name"]
                if not self._ds_manager.vertex_mapping_exists(
                    self._graph.id, vertex_type
                ):
                    raise RuntimeError(
                        f"Vertex type {vertex_type} does not bind any data source"
                    )
                data_source = self._ds_manager.get_vertex_datasource(
                    self._graph.id, vertex_type
                )
                location = data_source["inputs"][0]
                load_from_odps = load_from_odps and location.startswith("odps://")
            # check edges
            for e in self._config["edges"]:
                elabel = self.get_edge_full_label(
                    e["type_name"], e["source_vertex"], e["destination_vertex"]
                )
                if not self._ds_manager.edge_mappings_exists(
                    self._graph.id,
                    e["type_name"],
                    e["source_vertex"],
                    e["destination_vertex"],
                ):
                    raise RuntimeError(
                        f"Edge type {elabel} does not bind any data source"
                    )
                data_source = self._ds_manager.get_edge_datasource(
                    self._graph.id,
                    e["type_name"],
                    e["source_vertex"],
                    e["destination_vertex"],
                )
                location = data_source["inputs"][0]
                load_from_odps = load_from_odps and location.startswith("odps://")

            if not load_from_odps:
                status = self.import_data_from_local_file()
            else:
                # load from odps
                configini = convert_to_configini(
                    self._graph, self._ds_manager, self._config
                )
                # conn
                conn = http.client.HTTPConnection(STUDIO_WRAPPER_ENDPOINT)
                conn.request(
                    "POST",
                    "/api/v1/graph/dataloading",
                    json.dumps(configini),
                    headers={"Content-type": "application/json"},
                )
                r = conn.getresponse()
                if r.status > 400 and r.status < 600:
                    raise RuntimeError(
                        "Failed to submit dataloading job: " + r.read().decode("utf-8")
                    )
                rlt = json.loads(r.read().decode("utf-8"))
                if rlt["success"]:
                    self._jobid = rlt["data"]
                    status = self.generate_job_status(
                        status="RUNNING", end_time=None, log=None
                    )
                else:
                    status = self.generate_job_status(
                        status="FAILED",
                        end_time=datetime.datetime.now(),
                        log=rlt["message"],
                    )
        except Exception as e:
            status = self.generate_job_status(
                status="FAILED", end_time=datetime.datetime.now(), log=str(e)
            )
        finally:
            if isinstance(conn, http.client.HTTPConnection):
                conn.close()
        # register job status
        self._job_status[self.jobid] = FetchDataloadingJobStatus(self._graph, status)
