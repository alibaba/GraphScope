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

from typing import Union

import graphscope.flex.rest
from graphscope.flex.rest import RunningDeploymentInfo
from graphscope.gsctl.config import Context
from graphscope.gsctl.config import load_gs_config


def connect_coordinator(coordinator_endpoint: str) -> RunningDeploymentInfo:
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.DeploymentApi(api_client)
        deployment_info = api_instance.get_deployment_info()
        # coordinator connected, set the context
        flex = {
            "engine": deployment_info.engine,
            "storage": deployment_info.storage,
            "frontend": deployment_info.frontend,
        }
        context = Context(
            flex=flex,
            coordinator_endpoint=coordinator_endpoint,
        )
        config = load_gs_config()
        config.set_and_write(context)
        return deployment_info


def disconnect_coordinator() -> Union[None, Context]:
    config = load_gs_config()
    current_context = config.current_context()
    if current_context is not None:
        config.remove_and_write(current_context)
    return current_context
