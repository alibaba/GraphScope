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
from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.config import load_gs_config


def upload_file(location: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.UtilsApi(api_client)
        return api_instance.upload_file(location).file_path


def switch_context(context: str, graph_name=None):
    config = load_gs_config()
    current_context = get_current_context()
    current_context.switch_context(context)
    current_context.set_graph_name(graph_name)
    config.update_and_write(current_context)
