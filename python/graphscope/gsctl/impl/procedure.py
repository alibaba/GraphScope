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
from typing import Union

import graphscope.flex.rest
from graphscope.flex.rest import Procedure
from graphscope.gsctl.config import get_current_context


def create_procedure(graph_name: str, procedure: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.create_procedure(graph_name, Procedure.from_dict(procedure))


def list_procedures(graph_name: Union[None, str]) -> List[Procedure]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        if graph_name is None:
            procedures = api_instance.list_procedures()
        else:
            procedures = api_instance.list_procedures_by_graph(graph_name)
        return procedures


def update_procedure(graph_name: str, procedure: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        procedure_name = procedure["name"]
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.update_procedure(
            graph_name, procedure_name, Procedure.from_dict(procedure)
        )


def delete_procedure_by_name(graph_name: str, procedure_name: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.delete_procedure(graph_name, procedure_name)
