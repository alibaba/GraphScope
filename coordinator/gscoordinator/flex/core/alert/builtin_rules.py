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
from gremlin_python.driver.client import Client

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core.alert.alert_rule import AlertRule
from gscoordinator.flex.core.alert.message_collector import AlertMessageCollector
from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import SOLUTION

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
            alert_nodes = []
            disk_usages = []
            disk_utils = client_wrapper.get_storage_usage().to_dict()
            for node, usage in disk_utils["storage_usage"].items():
                if float(usage) > self._threshold:
                    alert_nodes.append(node)
                    disk_usages.append(f"{node}: {usage}%")
            if alert_nodes:
                message = "Disk usage {0} - exceeds threshold.".format(
                    ",".join(disk_usages)
                )
                alert_message = self.generate_alert_message(
                    target=alert_nodes, message=message
                )
                # alert
                self.alert(alert_message)
        except Exception as e:
            logger.warn("Failed to get disk usage: %s", str(e))


class GremlinServiceAvailableAlert(AlertRule):
    def __init__(
        self,
        name,
        severity,
        metric_type,
        conditions_description,
        frequency,
        message_collector,
        enable=True,
    ):
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
            available = client_wrapper.gremlin_service_available()
            if not available:
                message = f"Gremlin service unavailable: unknown reason"
        except Exception as e:
            available = False
            message = "Gremlin service unavailable: {0}".format(str(e))
        finally:
            # unable to distinguish whether frontend or excutor is unavailable,
            # so we set the target "-"
            if not available:
                alert_message = self.generate_alert_message("-", message)
                self.alert(alert_message)


def init_builtin_alert_rules(message_collector: AlertMessageCollector):
    alert_rules = {}
    if SOLUTION == "GRAPHSCOPE_INSIGHT":
        # HighDiskUtilization
        high_disk_utilization = HighDiskUtilizationAlert(
            name="HighDiskUtilization",
            severity="warning",
            metric_type="node",
            conditions_description="disk_utilization>80",
            frequency=180,
            message_collector=message_collector,
            threshold=80,
            enable=True,
        )
        alert_rules[high_disk_utilization.id] = high_disk_utilization
        # GremlinServiceAvailable
        gremlin_service_available = GremlinServiceAvailableAlert(
            name="GremlinServiceAvailable",
            severity="emergency",
            metric_type="service",
            conditions_description="g.V().limit(1) failed",
            frequency=5,
            message_collector=message_collector,
            enable=True,
        )
        alert_rules[gremlin_service_available.id] = gremlin_service_available
    return alert_rules
