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

import os
import warnings

# Disable warnings
warnings.filterwarnings("ignore", category=Warning)

import time

import pytest

from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import disconnect_coordinator
from graphscope.gsctl.impl import upload_file

COORDINATOR_ENDPOINT = "http://127.0.0.1:8080"


class TestFileUploading(object):
    def setup_class(self):
        if "COORDINATOR_ENDPOINT" in os.environ:
            COORDINATOR_ENDPOINT = os.environ["COORDINATOR_ENDPOINT"]
        self.deployment_info = connect_coordinator(COORDINATOR_ENDPOINT)

    def test_upload_file(self):
        """
        This test assumes that coordinator.max_content_length is set to less than 2MB.
        """
        gs_test_dir = os.environ.get("GS_TEST_DIR")
        if gs_test_dir is None:
            raise ValueError("GS_TEST_DIR is not set.")
        upload_file(gs_test_dir + "/modern_graph/person.csv")

        with pytest.raises(Exception):
            upload_file(gs_test_dir + "/modern_graph/person_not_exist.csv")

        with pytest.raises(Exception):
            upload_file(
                gs_test_dir + "/flex/ldbc-sf01-long-date/post_hasCreator_person_0_0.csv"
            )

    def teardown_class(self):
        disconnect_coordinator()
