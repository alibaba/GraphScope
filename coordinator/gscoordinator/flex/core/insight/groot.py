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
import logging
import os
import pickle
import socket
from typing import List

import psutil
from graphscope.config import Config
from gremlin_python.driver.client import Client
from kubernetes import client as kube_client
from kubernetes import config as kube_config

from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import CREATION_TIME
from gscoordinator.flex.core.config import GROOT_STORE_POD_ADMIN_PORT
from gscoordinator.flex.core.config import GROOT_STORE_POD_SUFFIX
from gscoordinator.flex.core.config import INSTANCE_NAME
from gscoordinator.flex.core.config import NAMESPACE
from gscoordinator.flex.core.config import STUDIO_WRAPPER_ENDPOINT
from gscoordinator.flex.core.config import WORKSPACE
from gscoordinator.flex.core.datasource import DataSourceManager
from gscoordinator.flex.core.insight.graph import get_groot_graph
from gscoordinator.flex.core.insight.job import DataloadingJobScheduler
from gscoordinator.flex.core.insight.job import FetchDataloadingJobStatus
from gscoordinator.flex.core.insight.utils import convert_to_configini
from gscoordinator.flex.core.scheduler import cancel_job
from gscoordinator.flex.core.scheduler import schedule
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.core.utils import get_pod_ips
from gscoordinator.flex.core.utils import resolve_api_client
from gscoordinator.flex.models import JobStatus

logger = logging.getLogger("graphscope")


class GrootClient(object):
    """Class used to interact with Groot"""

    def __init__(self):
        self._graph = get_groot_graph()
        # job status
        self._job_status = {}
        # job status pickle path
        self._job_status_pickle_path = os.path.join(WORKSPACE, "job_status.pickle")
        # recover
        self._try_to_recover_from_disk()
        # pickle job status to disk every 10s
        self._pickle_job_status_job = (
            schedule.every(10)
            .seconds.do(self._pickle_job_status_impl)
            .tag("pickle", "job status")
        )

    def _try_to_recover_from_disk(self):
        # we can't pickle class object with thread, so we recover the fetching
        # job from the status
        try:
            status = {}
            if os.path.exists(self._job_status_pickle_path):
                logger.info(
                    "Recover job status from file: %s", self._job_status_pickle_path
                )
                with open(self._job_status_pickle_path, "rb") as f:
                    status = pickle.load(f)
            # recover fetching job
            for jobid, status in status.items():
                self._job_status[jobid] = FetchDataloadingJobStatus(
                    self._graph, JobStatus.from_dict(status)
                )
        except Exception as e:
            logger.warn("Failed to recover job status: %s", str(e))

    def _pickle_job_status_impl(self):
        try:
            status = {}
            # we can't pickle class object with thread, so we pickle the status
            for jobid, fetching in self._job_status.items():
                status[jobid] = fetching.status
            with open(self._job_status_pickle_path, "wb") as f:
                pickle.dump(status, f)
        except Exception as e:
            logger.warn("Pickle job status failed: %s", str(e))
    
    def _restart_pod(self, pod_name, pod_ip, port):
        logger.info(f"Restart groot store pod {pod_name}, ip {pod_ip}")
        conn = http.client.HTTPConnection(pod_ip, port)
        conn.request("POST", "/shutdown")
        # expect the request didn't get any response, since the pod will kill it self
        try: 
            r = conn.getresponse()
            if r.status != 500 or r.status != 503:
                raise RuntimeError("Failed to restart groot store pod: " + r.read().decode("utf-8"))
            else:
                logger.info(f"Restart groot store pod {pod_name} successfully")
        except http.client.RemoteDisconnected:
            logger.info(f"Restart groot store pod {pod_name} successfully")
        except Exception as e:
            raise RuntimeError("Failed to restart groot store pod: " + str(e))
        finally:
            conn.close()

    def check_graph_exists(self, graph_id: str):
        if self._graph.id != graph_id:
            raise RuntimeError(f"Graph {graph_id} not exist.")

    def list_service_status(self) -> List[dict]:
        groot_endpoints = self._graph.groot_endpoints
        res = [
            {
                "graph_id": self._graph.id,
                "status": "Running",
                "start_time": CREATION_TIME,
                "sdk_endpoints": {
                    "gremlin": groot_endpoints["gremlin_endpoint"],
                    "grpc": groot_endpoints["grpc_endpoint"],
                },
            }
        ]
        if "cypher_endpoint" in groot_endpoints and groot_endpoints["cypher_endpoint"]:
            res[0]["sdk_endpoints"]["cypher"] = groot_endpoints["cypher_endpoint"]
        return res

    def stop_service(self) -> str:
        raise RuntimeError("Stop service is not supported yet.")

    def restart_service(self) -> str:
        api_client = resolve_api_client()
        pod_prefix = "{0}-{1}".format(INSTANCE_NAME, GROOT_STORE_POD_SUFFIX)
        ip_names = get_pod_ips(api_client, NAMESPACE, pod_prefix)
        for (ip, name) in ip_names:
            logger.info(f"Restart groot store pod {name}, ip {ip}")
            self._restart_pod(name, ip, GROOT_STORE_POD_ADMIN_PORT)

    def start_service(self, graph_id: str) -> str:
        raise RuntimeError("Start service is not supported yet.")

    def create_graph(self, graph: dict) -> dict:
        raise RuntimeError("Create graph is not supported yet.")

    def create_stored_procedure(self, graph_id: str, stored_procedure: dict) -> dict:
        raise RuntimeError("Stored procedure is not supported yet.")

    def list_stored_procedures(self, graph_id: str) -> List[dict]:
        return []

    def get_graph_by_id(self, graph_id: str) -> dict:
        self.check_graph_exists(graph_id)
        return self._graph.to_dict()

    def list_graphs(self) -> List[dict]:
        return [self._graph.to_dict()]

    def import_schema(self, graph_id, schema: dict):
        self.check_graph_exists(graph_id)
        self._graph.import_schema(schema)

    def create_vertex_type(self, graph_id: str, vertex_type: dict):
        self.check_graph_exists(graph_id)
        self._graph.create_vertex_type(vertex_type)

    def delete_vertex_type_by_name(self, graph_id: str, type_name: str):
        self.check_graph_exists(graph_id)
        self._graph.delete_vertex_type_by_name(type_name)

    def create_edge_type(self, graph_id: str, edge_type: dict):
        self.check_graph_exists(graph_id)
        self._graph.create_edge_type(edge_type)

    def delete_edge_type_by_name(
        self,
        graph_id: str,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ):
        self.check_graph_exists(graph_id)
        self._graph.delete_edge_type_by_name(
            type_name, source_vertex_type, destination_vertex_type
        )

    def get_schema_by_id(self, graph_id: str) -> dict:
        self.check_graph_exists(graph_id)
        return self._graph.schema

    def submit_dataloading_job(
        self, graph_id: str, config: dict, ds_manager: DataSourceManager
    ) -> str:
        self.check_graph_exists(graph_id)
        # schedule
        if "schedule" not in config or config["schedule"] is None:
            at_time = "now"
        else:
            at_time = config["schedule"]
        # repeat
        if "repeat" not in config or config["repeat"] is None:
            repeat = "once"
        else:
            repeat = config["repeat"]
        # scheduler
        job_scheduler = DataloadingJobScheduler(
            config, ds_manager, self._job_status, self._graph, at_time, repeat
        )
        return job_scheduler.schedulerid

    def get_dataloading_job_config(
        self, graph_id: str, config: dict, ds_manager: DataSourceManager
    ) -> dict:
        """Get data loading configuration for MapReduce Task"""
        self.check_graph_exists(graph_id)
        conn = http.client.HTTPConnection(STUDIO_WRAPPER_ENDPOINT)
        params = json.dumps(convert_to_configini(self._graph, ds_manager, config))
        conn.request(
            "POST",
            "/api/v1/graph/dataloading/config",
            params,
            headers={"Content-type": "application/json"},
        )
        r = conn.getresponse()
        if r.status > 400 and r.status < 600:
            raise RuntimeError(
                "Failed to get dataloading config: " + r.read().decode("utf-8")
            )
        data = r.read().decode("utf-8").replace("\n", "\\n")
        rlt = {"config": json.loads(data)["data"]}
        conn.close()
        return rlt

    def list_jobs(self) -> List[dict]:
        def parse_schedule(job):
            if job.at_time is not None:
                if job.unit == "days":
                    return "Every {0} at {1}".format(
                        job.unit[:-1], encode_datetime(job.at_time)
                    )
                elif job.unit == "weeks":
                    return "Every {0} at {1}".format(
                        job.start_day, encode_datetime(job.at_time)
                    )
            return encode_datetime(job.at_time)

        # running or finished job
        rlt = []
        for _, fetching in self._job_status.items():
            rlt.append(fetching.status)
        # job to be scheduled in the future
        schedule_jobs = schedule.get_jobs("Data Import")
        for job in schedule_jobs:
            if datetime.datetime.now() >= job.next_run:
                continue
            # treat scheduler id as jobid
            for tag in job.tags:
                if tag.startswith("detail="):
                    detail = json.loads(tag[7:])
                if tag.startswith("SCHEDULER"):
                    jobid = tag
            detail["schedule"] = parse_schedule(job)
            rlt.append(
                {
                    "id": jobid,
                    "type": "Data Import",
                    "status": "WAITING",
                    "start_time": str(job.next_run),
                    "end_time": encode_datetime(None),
                    "detail": detail,
                }
            )
        return rlt

    def get_job_by_id(self, job_id: str) -> dict:
        jobs = self.list_jobs()
        for job in jobs:
            if job_id == job["id"]:
                return job
        raise RuntimeError(f"Failed to get status: job {job_id} not exists.")

    def delete_job_by_id(self, job_id: str, delete_scheduler: bool) -> str:
        if job_id in self._job_status:
            self._job_status[job_id].cancel()
            return f"Cancel job {job_id} successfully"
        # scheduler job
        job_to_be_cancelled = None
        schedule_jobs = schedule.get_jobs("Data Import")
        for job in schedule_jobs:
            if job_id in job.tags:
                job_to_be_cancelled = job
                break
        if job_to_be_cancelled is not None:
            cancel_job(job_to_be_cancelled, delete_scheduler)

    def get_storage_usage(self) -> dict:
        rlt = {}
        if CLUSTER_TYPE == "HOSTS":
            hostname = socket.gethostname()
            disk_info = psutil.disk_usage("/")
            disk_usage = float(f"{disk_info.used / disk_info.total * 100:.2f}")
            rlt[hostname] = disk_usage
        elif CLUSTER_TYPE == "KUBERNETES":
            store_prefix = "{0}-graphscope-store-store".format(self._graph.name)
            for index, status in self._graph.get_storage_usage().items():
                node = "{0}-{1}".format(store_prefix, index)
                rlt[node] = round(
                    (status.totalSpace - status.usableSpace) / status.totalSpace * 100,
                    2,
                )
        return rlt

    def gremlin_service_available(self) -> bool:
        try:
            groot_endpoints = self._graph.groot_endpoints
            client = Client(
                groot_endpoints["gremlin_endpoint"],
                "g",
                username=groot_endpoints["username"],
                password=groot_endpoints["password"],
            )
            client.submit(
                "g.with('evaluationTimeout', 5000).V().limit(1)"
            ).all().result()
        except Exception as e:
            try:
                client.close()
            except:  # noqa: E722
                pass
            raise RuntimeError(str(e))
        else:
            try:
                client.close()
            except:  # noqa: E722
                pass
            return True
    
    def pod_available(self) -> bool:
        return self._graph.pod_available()
        


def init_groot_client(config: Config):
    return GrootClient()
