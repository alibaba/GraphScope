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

import json
import logging

import requests

from gscoordinator.flex.core.alert.alert_message import AlertMessage
from gscoordinator.flex.core.config import INSTANCE_NAME
from gscoordinator.flex.core.utils import random_string


class DingTalkReceiver(object):
    """DingTalk webhook receiver."""

    def __init__(self, webhook_url, at_user_ids, is_at_all=False, enable=True):
        """
        https://open.dingtalk.com/document/robots/custom-robot-access/#title-7ur-3ok-s1a

        Args:
          webhook_url (str): DingTalk webhook url.
          at_user_ids (list): list of str, each one represents a baseId.
          is_at_all (bool): Whether @all people in group.
          enable (bool): Enable notify or not.
        """
        self._type = "webhook"
        self._receiver_id = "dingtalk_receiver_" + random_string(6)
        self._webhook_url = webhook_url
        self._at_user_ids = list(set(at_user_ids))
        self._is_at_all = is_at_all
        self._enable = enable
        self._error_msg = ""

    @property
    def receiver_id(self):
        return self._receiver_id

    @classmethod
    def from_dict(cls, dikt) -> "DingTalkReceiver":
        return DingTalkReceiver(
            webhook_url=dikt["webhook_url"],
            at_user_ids=dikt["at_user_ids"],
            is_at_all=dikt["at_user_ids"],
            enable=dikt["enable"],
        )

    def update(self, data: dict):
        if "webhook_url" in data:
            self._webhook_url = data["webhook_url"]
        if "at_user_ids" in data:
            self._at_user_ids = list(set(data["at_user_ids"]))
        if "is_at_all" in data:
            self._is_at_all = data["is_at_all"]
        if "enable" in data:
            self._enable = data["enable"]
            if not self._enable:
                self._error_msg = ""

    def to_dict(self):
        return {
            "type": self._type,
            "receiver_id": self._receiver_id,
            "webhook_url": self._webhook_url,
            "at_user_ids": self._at_user_ids,
            "is_at_all": self._is_at_all,
            "enable": self._enable,
            "message": self._error_msg,
        }

    def send(self, message: AlertMessage):
        if not self._enable:
            return

        try:
            content = "[{0}] {1} Alert:\nGraphScope portal instance [{2}]: {3}".format(
                message.severity,
                message.alert_name,
                INSTANCE_NAME,
                message.message,
            )

            headers = {"Content-Type": "application/json"}
            payload = {
                "msgtype": "text",
                "text": {"content": content},
                "at": {
                    "atUserIds": self._at_user_ids,
                    "isAtAll": self._is_at_all,
                },
            }

            response = requests.post(
                self._webhook_url, headers=headers, data=json.dumps(payload)
            )

            rlt = json.loads(response.text)
            if rlt["errcode"] == 0:
                self._error_msg = ""
            else:
                raise RuntimeError(str(rlt))

        except Exception as e:
            logging.warn("Failed to send dingtalk: %s", str(e))
            self._error_msg = str(e)
