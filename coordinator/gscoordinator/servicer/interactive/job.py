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

import datetime
import logging
from enum import Enum

import interactive_client
from graphscope.proto import interactive_pb2

from gscoordinator.scheduler import Scheduler
from gscoordinator.utils import encode_datetime

logger = logging.getLogger("graphscope")


class JobType(Enum):

    GRAPH_IMPORT = 0


class Status(Enum):

    RUNNING = 0
    CANCELLED = 1
    SUCCESS = 2
    FAILED = 3
    WAITING = 4


class JobStatus(object):
    """Base class of job status for GraphScope FLEX runnable tasks"""

    def __init__(
        self, jobid, status, start_time, end_time=None, detail=dict(), message=""
    ):
        self.jobid = jobid
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        # detail for specific job
        self.detail = detail
        self.message = message

    def to_dict(self):
        return {
            "jobid": self.jobid,
            "status": self.status.name,
            "start_time": encode_datetime(self.start_time),
            "end_time": encode_datetime(self.end_time),
            "detail": self.detail,
            "message": self.message,
        }

    def set_success(self, message=""):
        self.status = Status.SUCCESS
        self.message = message
        self.end_time = datetime.datetime.now()

    def set_failed(self, message=""):
        self.status = Status.FAILED
        self.message = message
        self.end_time = datetime.datetime.now()

    def set_canncelled(self):
        self.status = Status.CANCELLED


class GraphImportScheduler(Scheduler):
    """This class responsible for scheduling and managing the import of interactive graphs."""

    def __init__(self, at_time, repeat, schema_mapping, servicer):
        super().__init__(at_time, repeat)

        self._schema_mapping = schema_mapping
        # we use interactive servicer to get the latest in runtime
        self._servicer = servicer

        self._tags = [JobType.GRAPH_IMPORT]

    def run(self):
        """This function needs to handle exception by itself"""
        graph_name = self._schema_mapping["graph"]

        detail = {"graph name": graph_name, "type": JobType.GRAPH_IMPORT.name}
        status = JobStatus(self.jobid, Status.RUNNING, self.last_run, detail=detail)

        # register status to servicer
        self._servicer.register_job_status(self.jobid, status)

        with interactive_client.ApiClient(
            interactive_client.Configuration(self._servicer.interactive_host)
        ) as api_client:
            # create an instance of the API class
            api_instance = interactive_client.DataloadingApi(api_client)

            try:
                api_response = api_instance.create_dataloading_job(
                    graph_name,
                    interactive_client.SchemaMapping.from_dict(self._schema_mapping),
                )
            except Exception as e:
                logger.warning(
                    "Failed to create dataloading job on graph %s: %s",
                    graph_name,
                    str(e),
                )
                status.set_failed(message=str(e))
            else:
                status.set_success(message=api_response.message)
