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

import itertools
import os
from typing import List

import graphscope.flex.rest
from graphscope.flex.rest import JobStatus
from graphscope.flex.rest import SchemaMapping
from graphscope.gsctl.config import get_current_context


def list_jobs() -> List[JobStatus]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        jobs = api_instance.list_jobs()
        return jobs


def get_job_by_id(job_id: str) -> JobStatus:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        job = api_instance.get_job_by_id(job_id)
        return job


def create_dataloading_job(graph_name: str, job_config: dict) -> str:
    # upload files
    for mapping in itertools.chain(
        job_config["vertex_mappings"], job_config["edge_mappings"]
    ):
        for index, location in enumerate(mapping["inputs"]):
            # path begin with "@" represents the local file
            if location.startswith("@"):
                location = location[1:]
                filename = os.path.basename(location)
                with open(location, "rb") as f:
                    content = f.read()
                path_after_uploaded = upload_file(filename, content, location)
                mapping["inputs"][index] = path_after_uploaded
    # create job
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        return api_instance.create_dataloading_job(
            graph_name, SchemaMapping.from_dict(job_config)
        )


def delete_job_by_id(job_id: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.JobApi(api_client)
        return api_instance.delete_job_by_id(job_id)


def upload_file(filename: str, content: bytes, location: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.UtilsApi(api_client)
        return api_instance.upload_file(location)
