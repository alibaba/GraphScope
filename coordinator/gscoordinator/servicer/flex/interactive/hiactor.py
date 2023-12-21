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

import os
import logging

import hiactor_client

from graphscope.config import Config

__all__ = ["init_interactive_service"]

logger = logging.getLogger("graphscope")


class Hiactor(object):
    """Hiactor module used to interact with hiactor engine"""

    def __init__(self, config: Config):
        self._config = config
        # hiactor admin service host
        self._hiactor_host = self._get_hiactor_service_endpoints()
        logger.info("Connect to hiactor service at %s", self._hiactor_host)

    def _get_hiactor_service_endpoints(self):
        if self._config.launcher_type == "hosts":
            # TODO change to 127.0.0.1
            endpoint = "http://192.168.0.9:{0}".format(
                os.environ.get("HIACTOR_ADMIN_SERVICE_PORT", 7777)
            )
        return endpoint

    def list_graph(self):
        with hiactor_client.ApiClient(
            hiactor_client.Configuration(self._hiactor_host)
        ) as api_client:
            api_instance = hiactor_client.GraphApi(api_client)
            return api_instance.list_graphs()


def init_interactive_service(config: Config):
    return Hiactor(config)
