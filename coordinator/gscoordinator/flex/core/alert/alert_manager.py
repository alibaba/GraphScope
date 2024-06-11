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
from gscoordinator.flex.core.scheduler import schedule
from gscoordinator.flex.core.utils import decode_datetimestr
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.models import AlertMessage
from gscoordinator.flex.models import AlertReceiver
from gscoordinator.flex.models import AlertRule


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
        # dump receiver every 60s
        self._pickle_receiver_job = (
            schedule.every(60)
            .seconds.do(self._pickle_receiver_impl)
            .tag("pickle", "alert receiver")
        )

    def _try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._receiver_path):
                logging.info(
                    "Recover alert receiver from file: %s", self._receiver_path
                )
                with open(self._receiver_path, "rb") as f:
                    self._receivers = pickle.load(f)
        except Exception as e:
            logging.warn("Failed to recover alert receiver: %s", str(e))

    def _pickle_receiver_impl(self):
        try:
            with open(self._receiver_path, "wb") as f:
                pickle.dump(self._receivers, f)
        except Exception as e:
            logging.warn("Failed to dump receiver: %s", str(e))

    def list_alert_rules(self) -> List[AlertRule]:
        rlt = []
        for name, rule in self._builtin_alert_rules.items():
            rlt.append(AlertRule.from_dict(rule.to_dict()))
        return rlt

    def update_alert_rule_by_name(self, rule_name: str, alert_rule: AlertRule) -> str:
        if rule_name not in self._builtin_alert_rules:
            raise RuntimeError(f"Alert rule {rule_name} not found.")
        self._builtin_alert_rules[rule_name].update(alert_rule.to_dict())
        return "update alert rule successfully"

    def delete_alert_rule_by_name(self, rule_name: str) -> str:
        if rule_name not in self._builtin_alert_rules:
            raise RuntimeError(f"Alert rule {rule_name} not found.")
        self._builtin_alert_rules[rule_name].stop()
        del self._builtin_alert_rules[rule_name]
        return "delete alert rule successfully"

    def list_alert_messages(
        self,
        type: Union[str, None],
        status: Union[str, None],
        severity: Union[str, None],
        start_time: Union[str, None],
        end_time: Union[str, None],
    ) -> List[AlertMessage]:
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
        logging.info(
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
                    rlt.append(AlertMessage.from_dict(message.to_dict()))
            current_date -= datetime.timedelta(days=1)
        return rlt

    def update_alert_messages(
        self,
        messages: List[AlertMessage],
        batch_status: str,
        batch_delete: bool,
    ):
        for message in messages:
            date = decode_datetimestr(message.trigger_time)
            message_id = message.message_id
            if batch_delete:
                self._message_collector.delete_message(date, message_id)
            else:
                self._message_collector.update_message_status(
                    date, message_id, batch_status
                )
        return "Update alert messages successfully"

    def register_receiver(self, alert_receiver: AlertReceiver) -> str:
        receiver = DingTalkReceiver.from_dict(alert_receiver.to_dict())
        self._receivers[receiver.receiver_id] = receiver
        self._pickle_receiver_impl()
        return "Register alert receiver successfully"

    def list_receivers(self) -> List[AlertReceiver]:
        rlt = []
        for _, receiver in self._receivers.items():
            rlt.append(AlertReceiver.from_dict(receiver.to_dict()))
        return rlt

    def update_receiver_by_id(self, receiver_id, alert_receiver: AlertReceiver) -> str:
        if receiver_id not in self._receivers:
            raise RuntimeError(f"Receiver {receiver_id} not found.")
        self._receivers[receiver_id].update(alert_receiver.to_dict())
        self._pickle_receiver_impl()
        return "Update alert receiver successfully"

    def delete_receiver_by_id(self, receiver_id) -> str:
        if receiver_id not in self._receivers:
            raise RuntimeError(f"Receiver {receiver_id} not found.")
        del self._receivers[receiver_id]
        self._pickle_receiver_impl()
        return "Delete alert receiver successfully"


alert_manager = AlertManager()
