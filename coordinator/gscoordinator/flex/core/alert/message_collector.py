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

from gscoordinator.flex.core.alert.alert_message import AlertMessage
from gscoordinator.flex.core.config import ALERT_WORKSPACE
from gscoordinator.flex.core.scheduler import cancel_job
from gscoordinator.flex.core.scheduler import schedule
from gscoordinator.flex.core.utils import decode_datetimestr


class OneDayAlertMessageCollector(object):
    """Alert messages for a day"""

    def __init__(self, date=datetime.datetime.today(), dump=True):
        """
        Args:
          date (datetime.datetime): The date for which to collect messages.
          dump (bool): True will write messages to disk periodically.
        """

        # format with: 2023-12-01
        self._date = date.date()
        self._messages = {}
        # filepath to store alert messages
        self._pickle_path = os.path.join(ALERT_WORKSPACE, str(self._date))
        # recover
        self._try_to_recover_from_disk()
        # dump alert messages every 60s
        self._pickle_messages_job = None
        if dump:
            self._pickle_messages_job = (
                schedule.every(60)
                .seconds.do(self._pickle_messages_impl)
                .tag("pickle", "alert message")
            )
        logging.info("New alert message collector created: %s", str(self._date))

    def _pickle_messages_impl(self):
        try:
            self.dump_to_disk()
        except Exception as e:
            logging.warn(
                "Failed to dump alert message on date %s: %s",
                str(self._date),
                str(e),
            )

    def _try_to_recover_from_disk(self):
        try:
            if os.path.exists(self._pickle_path):
                logging.info("Recover alert message from file %s", self._pickle_path)

                with open(self._pickle_path, "rb") as f:
                    self._messages = pickle.load(f)  # noqa: B301
        except Exception as e:
            logging.warn(
                "Failed to recover alert message from path %s: %s",
                self._pickle_path,
                str(e),
            )

    @property
    def date(self):
        return self._date

    @property
    def messages(self):
        return self._messages

    def dump_to_disk(self):
        with open(self._pickle_path, "wb") as f:
            pickle.dump(self._messages, f)

    def add_message(self, message: AlertMessage):
        self._messages[message.message_id] = message

    def update_status(self, message_id, status):
        if message_id in self._messages:
            self._messages[message_id].update_status(status)
            self.dump_to_disk()
        return True

    def delete_message(self, message_id):
        if message_id in self._messages:
            del self._messages[message_id]
            self.dump_to_disk()
        return True

    def clean(self):
        try:
            if self._pickle_messages_job is not None:
                cancel_job(self._pickle_messages_job, delete_scheduler=True)
                self._pickle_messages_job = None
                logging.info(
                    "%s: current alert message collector cleaned",
                    str(self._date),
                )
        except:  # noqa: E722, B110
            pass


class AlertMessageCollector(object):
    """Collector class for alert message"""

    def __init__(self, receivers: dict):
        """
        Args:
            receivers (dict): { "receiver_id": receiver }
        """
        self._receivers = receivers
        # current messages
        self._current_message_collector = OneDayAlertMessageCollector()
        # clean alert message more than 3 months
        self._disk_cleanup_job = (
            schedule.every(1).days.do(self._disk_cleanup_impl).tag("cleanup", "disk")
        )

    def _disk_cleanup_impl(self):
        try:
            current_date = datetime.datetime.today().date()
            three_months_ago = current_date - datetime.timedelta(days=90)
            # loop through all files in the directory
            for file_name in os.listdir(ALERT_WORKSPACE):
                file_path = os.path.join(ALERT_WORKSPACE, file_name)

                if os.path.isfile(file_path):
                    # delete the file
                    file_date = decode_datetimestr(file_name)
                    if file_date.date() < three_months_ago:
                        os.remove(file_path)
                        logging.info("Clean alert file: %s", str(file_path))
        except Exception as e:
            logging.warn("Failed to clean the alert file: %s", str(e))

    def is_message_belongs_to_certain_day(self, message, date):
        """
        Args:
            message (AlertMessage): alert message.
            date (datetime.date): certain day.
        """
        return message.trigger_time.date() == date

    def collect(self, message: AlertMessage):
        """Collect message generated by all alert rules."""

        if not self.is_message_belongs_to_certain_day(
            message, self._current_message_collector.date
        ):
            # archive current message collector and construct a new one
            self._current_message_collector.dump_to_disk()
            self._current_message_collector.clean()
            self._current_message_collector = OneDayAlertMessageCollector(
                message.trigger_time
            )

        # add message to current collector
        self._current_message_collector.add_message(message)

        # notify receivers
        for _, receiver in self._receivers.items():
            receiver.send(message)

    def get_message_collector_by_date(
        self, date: datetime.datetime
    ) -> OneDayAlertMessageCollector:
        if date.date() == self._current_message_collector.date:
            message_collector = self._current_message_collector
        else:
            message_collector = OneDayAlertMessageCollector(date, dump=False)
        return message_collector

    def get_messages_by_date(self, date: datetime.datetime) -> dict:
        """
        Returns:
          dict: { "message_id": message }
        """
        message_collector = self.get_message_collector_by_date(date)
        return message_collector.messages

    def update_message_status(
        self, date: datetime.datetime, message_id: str, status: str
    ) -> bool:
        message_collector = self.get_message_collector_by_date(date)
        return message_collector.update_status(message_id, status)

    def delete_message(self, date: datetime.datetime, message_id: str) -> bool:
        message_collector = self.get_message_collector_by_date(date)
        return message_collector.delete_message(message_id)
