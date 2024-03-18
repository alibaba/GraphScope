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
from graphscope.flex.rest import GrootDataloadingJobConfig
from graphscope.flex.rest import GrootGraph
from graphscope.flex.rest import GrootSchema
from graphscope.gsctl.config import get_current_context


def list_groot_graph() -> List[GrootGraph]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.LegacyApi(api_client)
        return api_instance.list_groot_graph()


def import_groot_schema(graph_name: str, schema: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.LegacyApi(api_client)
        return api_instance.import_groot_schema(
            graph_name, GrootSchema.from_dict(schema)
        )


def create_groot_dataloading_job(graph_name: str, job_config: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.LegacyApi(api_client)
        a = GrootDataloadingJobConfig.from_dict(job_config)
        print(a)
        return api_instance.create_groot_dataloading_job(
            graph_name, GrootDataloadingJobConfig.from_dict(job_config)
        )
