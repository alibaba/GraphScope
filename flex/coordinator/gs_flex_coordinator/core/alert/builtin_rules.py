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
import socket

import psutil
from gs_flex_coordinator.core.alert.alert_rule import AlertRule
from gs_flex_coordinator.core.alert.message_collector import AlertMessageCollector
from gs_flex_coordinator.core.config import CLUSTER_TYPE, SOLUTION

logger = logging.getLogger("graphscope")


class HighDiskUtilizationAlert(AlertRule):
    def __init__(
        self,
        name,
        severity,
        metric_type,
        conditions_description,
        frequency,
        message_collector,
        threshold,
        enable=True,
    ):
        """
        Args:
            threshold (int): threshold that will trigger alarm if meet.
        """

        self._threshold = threshold
        super().__init__(
            name,
            severity,
            metric_type,
            conditions_description,
            frequency,
            message_collector,
            enable,
        )

    def run_alert(self):
        """This function needs to handle exception by itself"""
        try:
            alert = False
            if CLUSTER_TYPE == "HOSTS":
                target = [socket.gethostname()]
                disk_info = psutil.disk_usage("/")
                disk_usage = float(f"{disk_info.used / disk_info.total * 100:.2f}")
                if disk_usage > self._threshold:
                    message = f"Disk usage {disk_usage}% - exceeds threshold."
                    alert = True
            if alert:
                alert_message = self.generate_alert_message(target, message)
                self.alert(alert_message)
        except Exception as e:
            logger.warn("Failed to get disk usage: %s", str(e))


def init_builtin_alert_rules(message_collector: AlertMessageCollector):
    alert_rules = {}
    # HighDiskUtilization
    alert_rules["HighDiskUtilization"] = HighDiskUtilizationAlert(
        name="HighDiskUtilization",
        severity="warning",
        metric_type="node",
        conditions_description="disk_utilization>80",
        frequency=1,
        message_collector=message_collector,
        threshold=80,
        enable=True,
    )
    return alert_rules
