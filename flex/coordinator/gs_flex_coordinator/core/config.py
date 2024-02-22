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
import tempfile

logger = logging.getLogger("graphscope")


# config logging
def config_logging(log_level: str):
    # `NOTSET` is special as it doesn't show log in Python
    log_level = getattr(logging, log_level.upper())
    if log_level == logging.NOTSET:
        log_level = logging.DEBUG
    logger = logging.getLogger("graphscope")
    logger.setLevel(log_level)

    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


config_logging("info")


# workspace
try:
    WORKSPACE = os.environ["GRAPHSCOPE_RUNTIME"]
except KeyError:
    WORKSPACE = os.path.expanduser("~/.graphscope/gs")
# make sure we have permission to create instance workspace
try:
    os.makedirs(os.path.join(WORKSPACE, ".ignore"), exist_ok=True)
except:  # noqa: E722, pylint: disable=bare-except
    WORKSPACE = os.path.join(os.path.join("/", tempfile.gettempprefix()), "gs")
    os.makedirs(os.path.join(WORKSPACE, ".ignore"), exist_ok=True)


# alert workspace
ALERT_WORKSPACE = os.path.join(WORKSPACE, "alert")
os.makedirs(ALERT_WORKSPACE, exist_ok=True)


# dataset workspace
DATASET_WORKSPACE = os.path.join(WORKSPACE, "dataset")
os.makedirs(DATASET_WORKSPACE, exist_ok=True)


# we use the solution encompasses the various applications and use cases of the
# product across different industries and business scenarios, e.g. "INTERACTIVE",
# "GRAPHSCOPE INSIGHT".
SOLUTION = os.environ["SOLUTION"]


# instance
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "demo")


# cluster type, optional from "K8S", "HOSTS"
CLUSTER_TYPE = os.environ.get("CLUSTER_TYPE", "HOSTS")


# interactive configuration
HQPS_ADMIN_SERVICE_PORT = os.environ.get("HIACTOR_ADMIN_SERVICE_PORT", 7777)


# coordinator starting time
COORDINATOR_STARTING_TIME = datetime.datetime.now()
