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
from graphscope.flex.rest import CreateAlertReceiverRequest
from graphscope.flex.rest import CreateAlertRuleRequest
from graphscope.flex.rest import GetAlertMessageResponse
from graphscope.flex.rest import GetAlertReceiverResponse
from graphscope.flex.rest import GetAlertRuleResponse
from graphscope.flex.rest import UpdateAlertMessageStatusRequest
from graphscope.gsctl.config import get_current_context


def list_alert_rules() -> List[GetAlertRuleResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_alert_rules()


def update_alert_rule_by_id(rule_id: str, rule: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_alert_rule_by_id(
            rule_id, CreateAlertRuleRequest.from_dict(rule)
        )


def delete_alert_rule_by_id(rule_id: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.delete_alert_rule_by_id(rule_id)


def list_alert_messages(
    alert_type=None,
    status=None,
    severity=None,
    start_time=None,
    end_time=None,
    limit=None,
) -> List[GetAlertMessageResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_alert_messages(
            alert_type, status, severity, start_time, end_time, limit
        )


def update_alert_message_in_batch(message_status: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_alert_message_in_batch(
            UpdateAlertMessageStatusRequest.from_dict(message_status)
        )


def delete_alert_message_in_batch(message_ids: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.delete_alert_message_in_batch(message_ids)


def create_alert_receiver(receiver: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.create_alert_receiver(
            CreateAlertReceiverRequest.from_dict(receiver)
        )


def update_alert_receiver_by_id(receiver_id: str, receiver: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_alert_receiver_by_id(
            receiver_id, CreateAlertReceiverRequest.from_dict(receiver)
        )


def list_alert_receivers() -> List[GetAlertReceiverResponse]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_alert_receivers()


def delete_alert_receiver_by_id(receiver_id: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.delete_alert_receiver_by_id(receiver_id)
