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
from graphscope.flex.rest import AlertMessage
from graphscope.flex.rest import AlertReceiver
from graphscope.flex.rest import AlertRule
from graphscope.flex.rest import UpdateAlertMessagesRequest
from graphscope.gsctl.config import get_current_context


def list_alert_rules() -> List[AlertRule]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_alert_rules()


def update_alert_rule(rule: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        name = rule["name"]
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_alert_rule_by_name(name, AlertRule.from_dict(rule))


def delete_alert_rule_by_name(rule_name: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.delete_alert_rule_by_name(rule_name)


def list_alert_messages(
    status: str, severity: str, starttime: str, endtime: str
) -> List[AlertMessage]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_alert_messages(
            None, status, severity, starttime, endtime
        )


def update_alert_messages(request: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        print(request)
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_alert_messages(
            UpdateAlertMessagesRequest.from_dict(request)
        )


def list_alert_receivers() -> List[AlertReceiver]:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.list_receivers()


def register_receiver(receiver: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.register_receiver(AlertReceiver.from_dict(receiver))


def update_alert_receiver_by_id(receiver_id: str, receiver: dict) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.update_receiver_by_id(
            receiver_id, AlertReceiver.from_dict(receiver)
        )


def delete_alert_receiver_by_id(receiver_id: str) -> str:
    context = get_current_context()
    with graphscope.flex.rest.ApiClient(
        graphscope.flex.rest.Configuration(context.coordinator_endpoint)
    ) as api_client:
        api_instance = graphscope.flex.rest.AlertApi(api_client)
        return api_instance.delete_receiver_by_id(receiver_id)
