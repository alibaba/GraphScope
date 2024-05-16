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
from graphscope.flex.rest import CreateProcedureRequest
from graphscope.flex.rest import GetProcedureResponse
from graphscope.flex.rest import UpdateProcedureRequest
from graphscope.gsctl.config import get_current_context


def create_procedure(graph_identifier: str, procedure: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        response = api_instance.create_procedure(
            graph_identifier, CreateProcedureRequest.from_dict(procedure)
        )
        return response.procedure_id


def list_procedures(graph_identifier: str) -> List[GetProcedureResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.list_procedures(graph_identifier)


def delete_procedure_by_id(graph_identifier: str, procedure_identifier: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.delete_procedure_by_id(
            graph_identifier, procedure_identifier
        )


def get_procedure_by_id(
    graph_identifier: str, procedure_identifier: str
) -> GetProcedureResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.get_procedure_by_id(graph_identifier, procedure_identifier)


def update_procedure_by_id(
    graph_identifier: str, procedure_identifier: str, procedure: dict
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.ProcedureApi(api_client)
        return api_instance.update_procedure_by_id(
            graph_identifier,
            procedure_identifier,
            UpdateProcedureRequest.from_dict(procedure),
        )
