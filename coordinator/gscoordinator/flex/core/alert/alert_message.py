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

from gscoordinator.flex.core.utils import encode_datetime


class AlertMessage(object):
    """Single alarm message"""

    def __init__(
        self,
        message_id,
        alert_name,
        severity,
        metric_type,
        target,
        message,
        status="unsolved",
        trigger_time=datetime.datetime.now(),
    ):
        """
        Args:
            message_id (str): Message ID.
            alert_name (str): Name of alert rule.
            severity (str): optional values are emergency, and warning.
            metric_type (str): optional values are node, gremlin service.
            target (list of str): alert target, such as specified ip 192.168.0.1
            message (str): alert message.
            status (str): optional values are unsolved, dealing, solved.
            trigger_time (datetime.datetime): Time message was generated.
        """

        self.message_id = message_id
        self.alert_name = alert_name
        self.severity = severity
        self.metric_type = metric_type
        self.target = target
        self.message = message
        self.status = status
        self.trigger_time = trigger_time

    def update_status(self, status):
        self.status = status

    def to_dict(self):
        return {
            "message_id": self.message_id,
            "alert_name": self.alert_name,
            "severity": self.severity,
            "metric_type": self.metric_type,
            "target": self.target,
            "trigger_time": encode_datetime(self.trigger_time),
            "status": self.status,
            "message": self.message,
        }
