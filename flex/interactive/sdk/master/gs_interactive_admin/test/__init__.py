#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
import logging

import connexion
from flask_testing import TestCase

from gs_interactive_admin.encoder import JSONEncoder
from gs_interactive_admin.core.service_discovery.service_registry import (
    initialize_service_registry,
)
from gs_interactive_admin.core.config import Config


class BaseTestCase(TestCase):

    def create_app(self):
        logging.basicConfig(level=logging.INFO)
        config = Config()
        config.master.service_registry.ttl = 3

        initialize_service_registry(config)
        logging.getLogger("connexion.operation").setLevel("ERROR")
        logging.getLogger("interactive").setLevel("INFO")
        app = connexion.App(__name__, specification_dir="../openapi/")
        app.app.json_encoder = JSONEncoder
        app.add_api("openapi.yaml", pythonic_params=True)

        return app.app
