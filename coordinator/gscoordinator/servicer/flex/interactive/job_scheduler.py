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

import logging

import hiactor_client

from gscoordinator.servicer.flex.job import JobType
from gscoordinator.servicer.flex.job import JobStatus
from gscoordinator.servicer.flex.scheduler import Scheduler

logger = logging.getLogger("graphscope")


class DataloadingJobScheduler(Scheduler):
    """Class for scheduling the dataloading process of interactive graphs"""

    def __init__(self, at_time, repeat, description, servicer):
        super().__init__(at_time, repeat)

        self._description = description
        self._servicer = servicer

        self._tags = [JobType.GRAPH_IMPORT]

    def run(self):
        """This function needs to handle exception by itself"""

        schema_mapping = self._description["schema_mapping"]
        graph_name = schema_mapping["graph"]

        detail = {"graph name": graph_name}
        status = JobStatus(
            jobid=self.jobid,
            type=JobType.GRAPH_IMPORT,
            start_time=self.last_run,
            detail=detail,
        )

        # register status to servicer
        self._servicer.register_job_status(self.jobid, status)

        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._servicer.hiactor_host)
        ) as api_client:
            # create an instance of the API class
            api_instance = hiactor_client.DataloadingApi(api_client)

            try:
                api_response = api_instance.create_dataloading_job(
                    graph_name,
                    hiactor_client.SchemaMapping.from_dict(schema_mapping),
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
