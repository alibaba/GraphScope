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

import graphscope.flex.rest
from graphscope.flex.rest import GetPodLogResponse
from graphscope.flex.rest import GetResourceUsageResponse
from graphscope.flex.rest import GetStorageUsageResponse
from graphscope.flex.rest import RunningDeploymentStatus
from graphscope.gsctl.config import get_current_context


def get_deployment_status() -> RunningDeploymentStatus:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DeploymentApi(api_client)
        return api_instance.get_deployment_status()


def get_deployment_resource_usage() -> GetResourceUsageResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DeploymentApi(api_client)
        return api_instance.get_deployment_resource_usage()


def get_storage_usage() -> GetStorageUsageResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DeploymentApi(api_client)
        return api_instance.get_storage_usage()


def get_deployment_pod_log(
    pod_name: str, component: str, from_cache: bool
) -> GetPodLogResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DeploymentApi(api_client)
        return api_instance.get_deployment_pod_log(pod_name, component, from_cache)
