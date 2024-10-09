#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import datetime
import logging
import os
import pickle
from typing import List
from typing import Union

from gscoordinator.flex.core.alert.alert_receiver import DingTalkReceiver
from gscoordinator.flex.core.alert.builtin_rules import init_builtin_alert_rules
from gscoordinator.flex.core.alert.message_collector import AlertMessageCollector
from gscoordinator.flex.core.config import ALERT_WORKSPACE
from gscoordinator.flex.core.utils import decode_datetimestr
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.models import CreateAlertReceiverRequest
from gscoordinator.flex.models import CreateAlertRuleRequest
from gscoordinator.flex.models import GetAlertMessageResponse
from gscoordinator.flex.models import GetAlertReceiverResponse
from gscoordinator.flex.models import GetAlertRuleResponse
from gscoordinator.flex.models import UpdateAlertMessageStatusRequest

logger = logging.getLogger("graphscope")


class AlertManager(object):
    def __init__(self):
        # receivers
        self._receivers = {}
        # pickle path
        self._receiver_path = os.path.join(ALERT_WORKSPACE, "receiver", "data.pickle")
        os.makedirs(os.path.dirname(self._receiver_path), exist_ok=True)
        # recover
        self._try_to_recover_from_disk()
        # message collector
        self._message_collector = AlertMessageCollector(self._receivers)
        # builtin alert rules
        self._builtin_alert_rules = init_builtin_alert_rules(self._message_collector)

    def _try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._receiver_path):
                logger.info(
                    "Recover alert receiver from file: %s", self._receiver_path
                )
                with open(self._receiver_path, "rb") as f:
                    self._receivers = pickle.load(f)
        except Exception as e:
            logger.warn("Failed to recover alert receiver: %s", str(e))

    def _pickle_receiver_impl(self):
        try:
            with open(self._receiver_path, "wb") as f:
                pickle.dump(self._receivers, f)
        except Exception as e:
            logger.warn("Failed to dump receiver: %s", str(e))

    def list_alert_rules(self) -> List[GetAlertRuleResponse]:
        rlt = []
        for id, rule in self._builtin_alert_rules.items():
            rlt.append(GetAlertRuleResponse.from_dict(rule.to_dict()))
        return rlt

    def update_alert_rule_by_id(
        self, rule_id: str, alert_rule: CreateAlertRuleRequest
    ) -> str:
        if rule_id not in self._builtin_alert_rules:
            raise RuntimeError(f"Alert rule {rule_id} not exists.")
        self._builtin_alert_rules[rule_id].update(alert_rule.to_dict())
        return "update alert rule successfully"

    def delete_alert_rule_by_id(self, rule_id: str) -> str:
        if rule_id not in self._builtin_alert_rules:
            raise RuntimeError(f"Alert rule {rule_id} not exists.")
        self._builtin_alert_rules[rule_id].stop()
        del self._builtin_alert_rules[rule_id]
        return "delete alert rule successfully"

    def list_alert_messages(
        self,
        type: Union[str, None],
        status: Union[str, None],
        severity: Union[str, None],
        start_time: Union[str, None],
        end_time: Union[str, None],
        limit: Union[int, None],
    ) -> List[GetAlertMessageResponse]:
        enable_filter = True
        if start_time is None and end_time is not None:
            # None -> date, fetch end day's messages, and disable date filter
            enable_filter = False
            end_date_filter = decode_datetimestr(end_time)
            start_date_filter = end_date_filter
        elif start_time is not None and end_time is None:
            # date -> None, fetch messages from start date to now
            start_date_filter = decode_datetimestr(start_time)
            end_date_filter = datetime.datetime.now()
        elif start_time is None and end_time is None:
            # None -> None, fetch today's messages, and disable date filter
            enable_filter = False
            start_date_filter = end_date_filter = datetime.datetime.now()
        else:
            # date -> date
            start_date_filter = decode_datetimestr(start_time)
            end_date_filter = decode_datetimestr(end_time)
        logger.info(
            "Fetch alert messages from %s to %s",
            encode_datetime(start_date_filter),
            encode_datetime(end_date_filter),
        )
        # rlt
        rlt = []
        current_date = end_date_filter
        while current_date.date() >= start_date_filter.date():
            messages = self._message_collector.get_messages_by_date(current_date)
            for message_id, message in messages.items():
                select = True
                if type is not None and message.alert_name != type:
                    select = False
                if status is not None and message.status != status:
                    select = False
                if severity is not None and message.severity != severity:
                    select = False
                if enable_filter:
                    if (
                        message.trigger_time < start_date_filter
                        or message.trigger_time > end_date_filter
                    ):
                        select = False
                if select:
                    rlt.append(GetAlertMessageResponse.from_dict(message.to_dict()))
            current_date -= datetime.timedelta(days=1)
        return rlt[:limit]

    def update_alert_message_in_batch(
        self,
        message_status_request: UpdateAlertMessageStatusRequest,
    ):
        messages = message_status_request.to_dict()
        status = messages["status"]
        for message_id in messages["message_ids"]:
            index = message_id.index("-")
            date = decode_datetimestr(message_id[index + 1 :])
            self._message_collector.update_message_status(date, message_id, status)
        return "Update alert messages successfully"

    def delete_alert_message_in_batch(self, message_ids: str):
        for message_id in message_ids.split(","):
            index = message_id.index("-")
            date = decode_datetimestr(message_id[index + 1 :])
            self._message_collector.delete_message(date, message_id)
        return "Delete alert messages successfully"

    def register_alert_receiver(self, alert_receiver: CreateAlertReceiverRequest) -> str:
        receiver = DingTalkReceiver.from_dict(alert_receiver.to_dict())
        self._receivers[receiver.receiver_id] = receiver
        self._pickle_receiver_impl()
        return "Register alert receiver successfully"

    def list_alert_receivers(self) -> List[GetAlertReceiverResponse]:
        rlt = []
        for _, receiver in self._receivers.items():
            rlt.append(GetAlertReceiverResponse.from_dict(receiver.to_dict()))
        return rlt

    def update_alert_receiver_by_id(
        self, receiver_id: str, alert_receiver: CreateAlertReceiverRequest
    ) -> str:
        if receiver_id not in self._receivers:
            raise RuntimeError(f"Receiver {receiver_id} not exists.")
        self._receivers[receiver_id].update(alert_receiver.to_dict())
        self._pickle_receiver_impl()
        return "Update alert receiver successfully"

    def delete_alert_receiver_by_id(self, receiver_id: str) -> str:
        if receiver_id not in self._receivers:
            raise RuntimeError(f"Receiver {receiver_id} not exists.")
        del self._receivers[receiver_id]
        self._pickle_receiver_impl()
        return "Delete alert receiver successfully"


alert_manager = AlertManager()
