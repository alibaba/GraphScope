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

import logging
import warnings

# Disable warnings
warnings.filterwarnings("ignore", category=Warning)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s",
    level=logging.INFO,
)

from gscoordinator.flex.core.client_wrapper import client_wrapper  # noqa: F401, E402
from gscoordinator.flex.core.utils import handle_api_exception  # noqa: F401, E402
