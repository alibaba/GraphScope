#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited.
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
import itertools
import logging

import hiactor_client

from google.protobuf.json_format import MessageToDict

from graphscope.config import Config
from graphscope.proto import flex_pb2

from gscoordinator.servicer.flex.interactive.job_scheduler import (
    DataloadingJobScheduler,
)
from gscoordinator.servicer.flex.job import JobStatus

from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import delegate_command_to_pod
from gscoordinator.utils import run_kube_cp_command

__all__ = ["init_interactive_service"]

logger = logging.getLogger("graphscope")


# There are two workspaces for FLEX Interactive, one residing on "coordinator" node
# and the other on the "interactive" node. These two workspaces are equivalent when
# running mode is "hosts". Hence, INTERACTIVE_WORKSPACE is only effective in
# Kubernetes (k8s) environment.
INTERACTIVE_WORKSPACE = (
    os.environ["INTERACTIVE_WORKSPACE"]
    if "INTERACTIVE_WORKSPACE" in os.environ
    else "/tmp"
)


INTERACTIVE_CONTAINER_NAME = "interactive"


class Hiactor(object):
    """Hiactor module used to interact with hiactor engine"""

    def __init__(self, config: Config):
        self._config = config
        # hiactor admin service host
        self._hiactor_host = self._get_hiactor_service_endpoints()
        # workspace
        self._workspace = os.path.join(WORKSPACE, "interactive")
        os.makedirs(self._workspace, exist_ok=True)
        # job status
        self._job_status = {}
        # check heartbeat to interactive engine
        logger.info("Connect to hiactor service at %s", self._hiactor_host)

    def _get_hiactor_service_endpoints(self):
        if self._config.launcher_type == "hosts":
            # TODO change to 127.0.0.1
            endpoint = "http://192.168.0.9:{0}".format(
                os.environ.get("HIACTOR_ADMIN_SERVICE_PORT", 7777)
            )
        return endpoint

    @property
    def hiactor_host(self):
        return self._hiactor_host

    @property
    def job_status(self):
        return self._job_status

    def register_job_status(self, jobid: str, status: JobStatus):
        self._job_status[jobid] = status

    def write_and_distribute_file(
        self, graph_name: str, basename: str, raw_data: bytes
    ):
        # /<workspace>/raw_data/<graphname>
        filedir = os.path.join(self._workspace, "raw_data", graph_name)
        os.makedirs(filedir, exist_ok=True)
        # /<filedir>/basename
        filepath = os.path.join(filedir, basename)
        # dump raw data to file
        with open(filepath, "wb") as f:
            f.write(raw_data)
        # distribute
        target_file = filepath
        if self._config.launcher_type == "k8s":
            # filepath is different in interactive pod
            target_file = os.path.join(
                INTERACTIVE_WORKSPACE, "raw_data", graph_name, basename
            )
            for pod in []:
                container = INTERACTIVE_CONTAINER_NAME
                cmd = f"mkdir -p {os.path.dirname(target_file)}"
                logger.debug(delegate_command_to_pod(cmd, pod, container))
                logger.debug(
                    run_kube_cp_command(tmp_file, target_file, pod, container, True)
                )
        return target_file

    def create_graph(self, graph_def_dict):
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_host)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            graph = hiactor_client.Graph.from_dict(graph_def_dict)
            return api_instance.create_graph(graph)

    def list_graph(self):
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_host)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.list_graphs()

    def delete_graph(self, graph_name: str):
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_host)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.delete_graph(graph_name)

    def create_job(
        self,
        job_type: str,
        schedule_proto: flex_pb2.Schedule,
        description_proto: flex_pb2.JobDescription,
    ):
        if job_type != "DATALOADING":
            raise RuntimeError(
                "Job type {0} is not supported in interacive.".format(job_type)
            )

        # write raw data to file and distribute to interactive workspace
        schema_mapping = description_proto.schema_mapping
        for mapping in itertools.chain(
            schema_mapping.vertex_mappings, schema_mapping.edge_mappings
        ):
            for index, location in enumerate(mapping.inputs):
                if location.startswith("@"):
                    # write raw data and distribute file to interactive workspace
                    new_location = self.write_and_distribute_file(
                        schema_mapping.graph,
                        os.path.basename(location),
                        mapping.raw_data[index],
                    )
                    # clear the raw_data
                    mapping.raw_data[index] = bytes()
                    # update the location
                    mapping.inputs[index] = new_location
        # schedule
        schedule = MessageToDict(
            schedule_proto,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        # job description
        description = MessageToDict(
            description_proto,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        # submit
        if schedule["run_now"]:
            at_time = "now"
            repeat = "null"
        else:
            at_time = schedule["at_time"]
            repeat = schedule["repeat"]

        scheduler = DataloadingJobScheduler(
            at_time=at_time,
            repeat=repeat,
            description=description,
            servicer=self,
        )
        scheduler.start()

    def create_procedure(self, procedure_def_dict):
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_host)
        ) as api_client:
            api_instance = hiactor_client.ProcedureApi(api_client)
            graph_name = procedure_def_dict["bound_graph"]
            procedure = hiactor_client.Procedure.from_dict(procedure_def_dict)
            return api_instance.create_procedure(graph_name, procedure)


def init_interactive_service(config: Config):
    return Hiactor(config)
