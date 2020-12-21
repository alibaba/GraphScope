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


class ConditionalFormatter(logging.Formatter):
    """Provide an option to disable format for some messages.
    Taken from https://stackoverflow.com/questions/34954373/disable-format-for-some-messages
    Examples:
       .. code:: python

            import logging
            import sys

            handler = logging.StreamHandler(sys.stdout)
            formatter = ConditionalFormatter('%(asctime)s %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger = logging.getLogger("graphscope")
            logger.setLevel("INFO")
            logger.addHandler(handler)
            logger.info("with formatting")
            # 2020-12-21 13:44:52,537 INFO - with formatting
            logger.info("without formatting", extra={'simple': True})
            # without formatting
    """

    def format(self, record):
        if hasattr(record, "simple") and record.simple:
            return record.getMessage()
        else:
            return logging.Formatter.format(self, record)
