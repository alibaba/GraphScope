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

import base64
import datetime
import logging
import os
import tempfile

logger = logging.getLogger("graphscope")


def str_to_bool(s):
    if isinstance(s, bool):
        return s
    return s in ["true", "True", "y", "Y", "yes", "Yes", "1"]


# workspace
try:
    WORKSPACE = os.environ["GRAPHSCOPE_RUNTIME"]
except KeyError:
    WORKSPACE = os.path.expanduser("~/.graphscope/runtime/coordinator")
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


# we use the solution encompasses the various applications
# and use cases of the product across different industries
# and business scenarios, e.g. "INTERACTIVE",
# "GRAPHSCOPE INSIGHT".
SOLUTION = os.environ.get("SOLUTION", "GRAPHSCOPE_ONE")
if SOLUTION == "INTERACTIVE":
    FRONTEND_TYPE = "Cypher/Gremlin"
    STORAGE_TYPE = "MutableCSR"
    ENGINE_TYPE = "Hiactor"
elif SOLUTION == "GRAPHSCOPE_INSIGHT":
    FRONTEND_TYPE = "Cypher/Gremlin"
    STORAGE_TYPE = "MutablePersistent"
    ENGINE_TYPE = "Gaia"
else:
    FRONTEND_TYPE = ""
    STORAGE_TYPE = ""
    ENGINE_TYPE = ""


# cluster type, optional from "KUBERNETES", "HOSTS"
CLUSTER_TYPE = os.environ.get("CLUSTER_TYPE", "HOSTS")


# interactive configuration
HQPS_ADMIN_SERVICE_PORT = os.environ.get("HIACTOR_ADMIN_SERVICE_PORT", 7777)


# creation time
CREATION_TIME_PATH = os.path.join(WORKSPACE, "creation_time")
if os.path.exists(CREATION_TIME_PATH):
    with open(CREATION_TIME_PATH, "r") as f:
        time_str = f.readline()
        CREATION_TIME = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
else:
    CREATION_TIME = datetime.datetime.now()
    with open(CREATION_TIME_PATH, "w") as f:
        f.write(CREATION_TIME.strftime("%Y-%m-%d %H:%M:%S"))


# kubernetes
NAMESPACE = os.environ.get("NAMESPACE", "kubetask")
INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "demo")


# groot
GROOT_GRPC_PORT = os.environ.get("GROOT_GRPC_PORT", 55556)
GROOT_GREMLIN_PORT = os.environ.get("GROOT_GREMLIN_PORT", 12312)
GROOT_CYPHER_PORT = os.environ.get("GROOT_CYPHER_PORT", 7687)
GROOT_USERNAME = os.environ.get("GROOT_USERNAME", "")
GROOT_PASSWORD = os.environ.get("GROOT_PASSWORD", "")
try:
    GROOT_PASSWORD = base64.b64decode(GROOT_PASSWORD).decode('utf-8')
except Exception as e:
    logger.warn("Invalid base64-encoded string found, use original value: %s", str(e))
GROOT_FRONTEND_POD_SUFFIX = os.environ.get("GROOT_FRONTEND_POD_SUFFIX", "graphscope-store-frontend")
GROOT_STORE_POD_SUFFIX = os.environ.get("GROOT_STORE_POD_SUFFIX", "graphscope-store-store")
GROOT_COORDINATOR_POD_SUFFIX = os.environ.get("GROOT_COORDINATOR_POD_SUFFIX", "graphscope-store-coordinator")
GROOT_PORTAL_POD_SUFFIX = os.environ.get("GROOT_PORTAL_POD_SUFFIX", "graphscope-store-portal")
GROOT_STORE_POD_ADMIN_PORT = os.environ.get("GROOT_STORE_POD_ADMIN_PORT", 10001)


# dataloading service for groot
STUDIO_WRAPPER_ENDPOINT = os.environ.get("STUDIO_WRAPPER_ENDPOINT", None)


# maximum size of the log queue
MAXSIZE = 1000
# batch size for fetching logs
BATCHSIZE = 4096


# odps
BASEID = os.environ.get("BASEID", None)
PROJECT = os.environ.get("PROJECT", "graphscope")
STUDIO_WRAPPER_ENDPOINT = os.environ.get("STUDIO_WRAPPER_ENDPOINT", None)
