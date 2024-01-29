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
from abc import ABCMeta, abstractmethod

from gs_flex_coordinator.core.alert.alert_message import AlertMessage
from gs_flex_coordinator.core.scheduler import cancel_job, schedule

logger = logging.getLogger("graphscope")


class AlertRule(metaclass=ABCMeta):
    """Base class for alert rule"""

    def __init__(
        self,
        name,
        severity,
        metric_type,
        conditions_desription,
        frequency,
        message_collector,
        enable=True,
    ):
        """
        Args:
          name (str): Name of alert rule.
          severity (str): optional values are emergency, and warning.
          metric_type (str): optional values are node, gremlin service.
          conditions_desription(str): Description of alarm conditions.
          frequency (int): Alarm frequency, unit by minutes.
          message_collector: Collect messages generated by the alert.
          enable (bool): Enable alert or not.
        """
        self._name = name
        self._severity = severity
        self._metric_type = metric_type
        self._conditions_desription = conditions_desription
        self._frequency = frequency
        self._message_collector = message_collector
        self._enable = enable

        self._alert_job = None
        if self._enable:
            self._alert_job = (
                schedule.every(self._frequency)
                .minutes.do(self.run_alert)
                .tag("alert", self._name)
            )

    def __exit__(self):
        self.stop()

    def update(self, data: dict):
        if "severity" in data:
            self._severity = data["severity"]
        if "metric_type" in data:
            self._metric_type = data["metric_type"]
        if "conditions_desription" in data:
            self._conditions_desription = data["conditions_desription"]
        if "frequency" in data:
            self._frequency = data["frequency"]
        if "enable" in data:
            if data["enable"]:
                self.enable()
            else:
                self.disable()

    def enable(self):
        if not self._enable:
            self._enable = True
            self._alert_job = (
                schedule.every(self._frequency)
                .minutes.do(self.run_alert)
                .tag("alert", self._name)
            )

    def disable(self):
        if self._enable and self._alert_job is not None:
            cancel_job(self._alert_job, delete_scheduler=True)
            self._enable = False

    def to_dict(self):
        return {
            "name": self._name,
            "severity": self._severity,
            "metric_type": self._metric_type,
            "conditions_desription": self._conditions_desription,
            "frequency": self._frequency,
            "enable": self._enable,
        }

    def alert(self, message: AlertMessage):
        self._message_collector.collect(message)

    def generate_alert_message(self, target, message):
        """
        Args:
            target (str): alert target, such as specified ip 192.168.0.1
            message (str): alert message.
        """
        trigger_time = datetime.datetime.now()
        # format with "<alert_name>-2023-11-30-15-02-31"
        message_id = "{0}-{1}".format(
            self._name, trigger_time.strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
        )
        return AlertMessage(
            message_id=message_id,
            alert_name=self._name,
            severity=self._severity,
            metric_type=self._metric_type,
            target=target,
            message=message,
            trigger_time=trigger_time,
        )

    def stop(self):
        try:
            if self._enable and self._alert_job is not None:
                cancel_job(self._alert_job, delete_scheduler=True)
                self._enable = False
        except:
            pass

    @abstractmethod
    def run_alert(self):
        """
        Methods that all subclasses need to implement, note that
        subclass needs to handle exception by itself.
        """
        raise NotImplementedError
