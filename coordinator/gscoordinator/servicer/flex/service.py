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

"""Service under FLEX Architecture"""

import atexit
import datetime
import traceback
import functools

# import itertools
import logging
import os
import threading

from google.protobuf.json_format import MessageToDict
from graphscope.config import Config
from graphscope.gsctl.utils import dict_to_proto_message
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import flex_pb2
from graphscope.proto import message_pb2

from gscoordinator.servicer.flex.job import JobStatus
from gscoordinator.servicer.flex.job import JobType
from gscoordinator.servicer.flex.job import Status
from gscoordinator.servicer.flex.scheduler import cancel_job
from gscoordinator.servicer.flex.scheduler import schedule

# from gscoordinator.utils import WORKSPACE
# from gscoordinator.utils import delegate_command_to_pod
# from gscoordinator.utils import run_kube_cp_command

from gscoordinator.servicer.flex.interactive import *

__all__ = ["FlexServiceServicer", "init_flex_service_servicer"]

logger = logging.getLogger("graphscope")


def handle_api_exception(proto_message_response):
    """Decorator to handle api exception occurs during request engine service."""

    def _handle_api_exception(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.warning("Failed to execute %s: %s", str(fn.__name__), str(e))
                traceback.print_exc()
                return proto_message_response(
                    code=error_codes_pb2.API_EXCEPTION_ERROR, error_msg=str(e)
                )

        return wrapper

    return _handle_api_exception


class FlexServiceServicer(coordinator_service_pb2_grpc.CoordinatorServiceServicer):
    """Service under flex architecture."""

    services_initializer = {
        "interactive": init_interactive_service,
    }

    def __init__(self, config: Config):
        self._config = config
        # We use the solution encompasses the various applications and use cases of
        # the product across different industries and business scenarios,
        # e.g. interactive, analytical
        self._solution = self._config.solution.lower()

        atexit.register(self.cleanup)

        # lock to protect the service
        self._lock = threading.RLock()
        # initialize specific service client
        self._service_client = self._initialize_service_client()

    def __del__(self):
        self.cleanup()

    def _initialize_service_client(self):
        initializer = self.services_initializer.get(self._solution)
        if initializer is None:
            raise RuntimeError("Failed to launch {0} service".format(self._solution))
        return initializer(self._config)

    def cleanup(self):
        pass

    def Connect(self, request, context):
        return message_pb2.ConnectResponse(solution=self._solution)

    @handle_api_exception(flex_pb2.ApiResponse)
    def CreateGraph(self, request, context):
        graph_def_dict = MessageToDict(
            request.graph_def,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        api_response = self._service_client.create_graph(graph_def_dict)
        return flex_pb2.ApiResponse(code=error_codes_pb2.OK, error_msg=api_response)

    @handle_api_exception(flex_pb2.ListGraphResponse)
    def ListGraph(self, request, context):
        graphs = self._service_client.list_graph()
        return flex_pb2.ListGraphResponse(
            code=error_codes_pb2.OK,
            graphs=[
                dict_to_proto_message(g.to_dict(), flex_pb2.GraphProto())
                for g in graphs
            ],
        )

    @handle_api_exception(flex_pb2.ApiResponse)
    def DeleteGraph(self, request, context):
        api_response = self._service_client.delete_graph(request.graph_name)
        return flex_pb2.ApiResponse(code=error_codes_pb2.OK, error_msg=api_response)

    @handle_api_exception(flex_pb2.ApiResponse)
    def CreateJob(self, request, context):
        self._service_client.create_job(
            request.type, request.schedule, request.description
        )
        return flex_pb2.ApiResponse(code=error_codes_pb2.OK)

    @handle_api_exception(flex_pb2.ListProcedureResponse)
    def ListProcedure(self, request, context):
        pass

    @handle_api_exception(flex_pb2.ListJobResponse)
    def ListJob(self, request, context):
        # job to be scheduled in the future
        jobs = {}
        for job in schedule.get_jobs():
            if datetime.datetime.now() >= job.next_run:
                continue
            for tag in job.tags:
                if tag.startswith("SCHEDULER"):
                    jobid = tag
            jobs[jobid] = JobStatus.from_dict(
                {
                    "jobid": jobid,
                    "type": JobType.SCHEDULER,
                    "status": Status.WAITING,
                    "start_time": str(job.next_run),
                    "end_time": None,
                    "log": "",
                    "detail": {"tag": str(job.tags)},
                    "message": "",
                }
            )
        jobs.update(self._service_client.job_status)
        return flex_pb2.ListJobResponse(
            code=error_codes_pb2.OK,
            job_status=[
                dict_to_proto_message(s.to_dict(), flex_pb2.JobStatus())
                for _, s in jobs.items()
            ],
        )

    @handle_api_exception(flex_pb2.ApiResponse)
    def CancelJob(self, request, context):
        jobid = request.jobid
        delete_scheduler = request.delete_scheduler
        if jobid in self._service_client.job_status:
            raise RuntimeError("Job cancellation is not supported yet!")
        # cancel job scheduler
        scheduler = None
        for job in schedule.get_jobs():
            if jobid in job.tags:
                scheduler = job
                break
        if scheduler is None:
            raise RuntimeError("Job {0} not found".format(jobid))
        cancel_job(scheduler, delete_scheduler)
        return flex_pb2.ApiResponse(code=error_codes_pb2.OK)

    @handle_api_exception(flex_pb2.ApiResponse)
    def CreateProcedure(self, request, context):
        procedure_def_dict = MessageToDict(
            request.procedure_def,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        api_response = self._service_client.create_procedure(procedure_def_dict)
        return flex_pb2.ApiResponse(code=error_codes_pb2.OK, error_msg=api_response)


def init_flex_service_servicer(config: Config):
    return FlexServiceServicer(config)
