#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

from typing import List

import graphscope.flex.rest
from graphscope.flex.rest import DataloadingJobConfig
from graphscope.flex.rest import JobStatus
from graphscope.gsctl.config import get_current_context


def submit_dataloading_job(graph_identifier: str, config: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        response = api_instance.submit_dataloading_job(
            graph_identifier, DataloadingJobConfig.from_dict(config)
        )
        return response.job_id


def list_jobs() -> List[JobStatus]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        return api_instance.list_jobs()


def delete_job_by_id(job_identifier: str, delete_scheduler=False) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        return api_instance.delete_job_by_id(job_identifier, delete_scheduler)


def get_job_by_id(job_identifier: str) -> JobStatus:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        return api_instance.get_job_by_id(job_identifier)
