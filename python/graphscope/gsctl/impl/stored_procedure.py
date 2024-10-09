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
from graphscope.flex.rest import CreateStoredProcRequest
from graphscope.flex.rest import GetStoredProcResponse
from graphscope.flex.rest import UpdateStoredProcRequest
from graphscope.gsctl.config import get_current_context


def create_stored_procedure(graph_identifier: str, stored_procedure: dict) -> str:
    # path begin with "@" represents the local file
    if stored_procedure["query"].startswith("@"):
        location = stored_procedure["query"][1:]
        with open(location, "r") as f:
            stored_procedure["query"] = f.read()
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
        response = api_instance.create_stored_procedure(
            graph_identifier, CreateStoredProcRequest.from_dict(stored_procedure)
        )
        return response.stored_procedure_id


def list_stored_procedures(graph_identifier: str) -> List[GetStoredProcResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
        return api_instance.list_stored_procedures(graph_identifier)


def delete_stored_procedure_by_id(
    graph_identifier: str, stored_procedure_identifier: str
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
        return api_instance.delete_stored_procedure_by_id(
            graph_identifier, stored_procedure_identifier
        )


def get_stored_procedure_by_id(
    graph_identifier: str, stored_procedure_identifier: str
) -> GetStoredProcResponse:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
        return api_instance.get_stored_procedure_by_id(
            graph_identifier, stored_procedure_identifier
        )


def update_stored_procedure_by_id(
    graph_identifier: str, stored_procedure_identifier: str, stored_procedure: dict
) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.StoredProcedureApi(api_client)
        return api_instance.update_stored_procedure_by_id(
            graph_identifier,
            stored_procedure_identifier,
            UpdateStoredProcRequest.from_dict(stored_procedure),
        )
