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
import itertools
import logging
import os
import pickle
import socket
import threading
from typing import List, Union

import psutil
from gs_flex_coordinator.core.config import (CLUSTER_TYPE,
                                             COORDINATOR_STARTING_TIME,
                                             DATASET_WORKSPACE, INSTANCE_NAME,
                                             SOLUTION, WORKSPACE)
from gs_flex_coordinator.core.interactive import init_hqps_client
from gs_flex_coordinator.core.scheduler import schedule
from gs_flex_coordinator.core.utils import (GraphInfo, decode_datetimestr,
                                            encode_datetime, get_current_time)
from gs_flex_coordinator.models import (DeploymentInfo, Graph, JobStatus,
                                        ModelSchema, NodeStatus, Procedure,
                                        SchemaMapping, ServiceStatus,
                                        StartServiceRequest)
from gs_flex_coordinator.version import __version__

logger = logging.getLogger("graphscope")


class ClientWrapper(object):
    """Wrapper of client that interacts with engine"""

    def __init__(self):
        # lock to protect the service
        self._lock = threading.RLock()
        # initialize specific client
        self._client = self._initialize_client()
        # graphs info
        self._graphs_info = {}
        # pickle path
        self._pickle_path = os.path.join(WORKSPACE, "graphs_info.pickle")
        # recover
        self._try_to_recover_from_disk()

    def _try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._pickle_path):
                logger.info("Recover graphs info from file %s", self._pickle_path)
                with open(self._pickle_path, "rb") as f:
                    self._graphs_info = pickle.load(f)
        except Exception as e:
            logger.warn("Failed to recover graphs info: %s", str(e))
        # set default graph info
        for g in self.list_graphs():
            if g.name not in self._graphs_info:
                self._graphs_info[g.name] = GraphInfo(
                    name=g.name, creation_time=COORDINATOR_STARTING_TIME
                )

    def _pickle_graphs_info_impl(self):
        try:
            with open(self._pickle_path, "wb") as f:
                pickle.dump(self._graphs_info, f)
        except Exception as e:
            logger.warn("Failed to dump graphs info: %s", str(e))

    def _initialize_client(self):
        service_initializer = {"INTERACTIVE": init_hqps_client}
        initializer = service_initializer.get(SOLUTION)
        if initializer is None:
            raise RuntimeError(
                "Client initializer of {0} not found.".format(self._solution)
            )
        return initializer()

    def list_graphs(self) -> List[Graph]:
        graphs = self._client.list_graphs()
        # transfer
        rlts = [Graph.from_dict(g.to_dict()) for g in graphs]
        return rlts

    def get_schema_by_name(self, graph_name: str) -> ModelSchema:
        schema = self._client.get_schema_by_name(graph_name)
        # transfer
        rlt = ModelSchema.from_dict(schema.to_dict())
        return rlt

    def create_graph(self, graph: Graph) -> str:
        # there are some tricks here, since schema is a keyword of openapi
        # specification, so it will be converted into the _schema field.
        graph_dict = graph.to_dict()
        if "_schema" in graph_dict:
            graph_dict["schema"] = graph_dict.pop("_schema")
        rlt = self._client.create_graph(graph_dict)
        self._graphs_info[graph.name] = GraphInfo(
            name=graph.name, creation_time=get_current_time()
        )
        self._pickle_graphs_info_impl()
        return rlt

    def delete_graph_by_name(self, graph_name: str) -> str:
        rlt = self._client.delete_graph_by_name(graph_name)
        del self._graphs_info[graph_name]
        self._pickle_graphs_info_impl()
        return rlt

    def create_procedure(self, graph_name: str, procedure: Procedure) -> str:
        procedure_dict = procedure.to_dict()
        return self._client.create_procedure(graph_name, procedure_dict)

    def list_procedures(self, graph_name: Union[None, str]) -> List[Procedure]:
        procedures = self._client.list_procedures(graph_name)
        # transfer
        rlt = [Procedure.from_dict(p.to_dict()) for p in procedures]
        return rlt

    def update_procedure(
        self, graph_name: str, procedure_name: str, procedure: Procedure
    ) -> str:
        procedure_dict = procedure.to_dict()
        return self._client.update_procedure(graph_name, procedure_name, procedure_dict)

    def delete_procedure_by_name(self, graph_name: str, procedure_name: str) -> str:
        return self._client.delete_procedure_by_name(graph_name, procedure_name)

    def get_node_status(self) -> List[NodeStatus]:
        rlt = []
        if CLUSTER_TYPE == "HOSTS":
            disk_info = psutil.disk_usage("/")
            status = {
                "node": socket.gethostname(),
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
                "disk_usage": float(f"{disk_info.used / disk_info.total * 100:.2f}"),
            }
            rlt.append(NodeStatus.from_dict(status))
        return rlt

    def get_deployment_info(self) -> DeploymentInfo:
        # update graphs info
        for job in self.list_jobs():
            if (
                job.detail["graph_name"] in self._graphs_info
                and job.end_time is not None
            ):
                self._graphs_info[job.detail["graph_name"]].last_dataloading_time = (
                    decode_datetimestr(job.end_time)
                )
        self._pickle_graphs_info_impl()
        graphs_info = {}
        for name, info in self._graphs_info.items():
            graphs_info[name] = info.to_dict()
        info = {
            "name": INSTANCE_NAME,
            "cluster_type": CLUSTER_TYPE,
            "version": __version__,
            "solution": SOLUTION,
            "graphs_info": graphs_info,
        }
        return DeploymentInfo.from_dict(info)

    def get_service_status(self) -> ServiceStatus:
        return ServiceStatus.from_dict(self._client.get_service_status())

    def stop_service(self) -> str:
        return self._client.stop_service()

    def restart_service(self) -> str:
        return self._client.restart_service()

    def start_service(self, request: StartServiceRequest) -> str:
        return self._client.start_service(request)

    def list_jobs(self) -> List[JobStatus]:
        # transfer
        rlt = []
        for job_status_dict in self._client.list_jobs():
            rlt.append(JobStatus.from_dict(job_status_dict))
        return rlt

    def get_job_by_id(self, job_id: str) -> JobStatus:
        job_status_dict = self._client.get_job_by_id(job_id)
        return JobStatus.from_dict(job_status_dict)

    def delete_job_by_id(self, job_id: str) -> str:
        return self._client.delete_job_by_id(job_id)

    def create_dataloading_job(
        self, graph_name: str, schema_mapping: SchemaMapping
    ) -> str:
        # there are some tricks here, since property is a keyword of openapi
        # specification, so it will be converted into the _property field.
        schema_mapping_dict = schema_mapping.to_dict()
        for mapping in itertools.chain(
            schema_mapping_dict["vertex_mappings"], schema_mapping_dict["edge_mappings"]
        ):
            for column_mapping in mapping["column_mappings"]:
                if "_property" in column_mapping:
                    column_mapping["property"] = column_mapping.pop("_property")
        job_id = self._client.create_dataloading_job(graph_name, schema_mapping_dict)
        return job_id

    def upload_file(self, filestorage) -> str:
        if CLUSTER_TYPE == "HOSTS":
            filepath = os.path.join(DATASET_WORKSPACE, filestorage.filename)
            filestorage.save(filepath)
            return str(filepath)


client_wrapper = ClientWrapper()
