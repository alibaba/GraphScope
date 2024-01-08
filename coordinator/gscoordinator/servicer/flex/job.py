#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited.
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
from enum import Enum

from gscoordinator.utils import encode_datetime


class JobType(Enum):
    SCHEDULER = 0
    DATALOADING = 1


class Status(Enum):
    RUNNING = 0
    CANCELLED = 1
    SUCCESS = 2
    FAILED = 3
    WAITING = 4


class JobStatus(object):
    def __init__(
        self,
        jobid,
        type,
        start_time,
        status=Status.RUNNING,
        end_time=None,
        log="",
        detail=dict(),
        message="",
    ):
        self.jobid = jobid
        self.type = type
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.log = log
        # detail for specific job
        self.detail = detail
        self.message = message

    @staticmethod
    def from_dict(data):
        return JobStatus(
            jobid=data["jobid"],
            type=data["type"],
            status=data["status"],
            start_time=data["start_time"],
            end_time=data["end_time"],
            log=data["log"],
            detail=data["detail"],
            message=data["message"],
        )

    def to_dict(self):
        return {
            "jobid": self.jobid,
            "type": self.type.name,
            "status": self.status.name,
            "start_time": encode_datetime(self.start_time),
            "end_time": encode_datetime(self.end_time),
            "log": self.log,
            "detail": self.detail,
            "message": self.message,
        }

    def set_success(self, message=""):
        self.status = Status.SUCCESS
        self.message = message
        self.end_time = datetime.datetime.now()

    def set_failed(self, message=""):
        self.status = Status.FAILED
        self.message = message
        self.end_time = datetime.datetime.now()

    def set_canncelled(self):
        self.status = Status.CANCELLED
