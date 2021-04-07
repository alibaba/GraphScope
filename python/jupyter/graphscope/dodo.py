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

# this uses https://pydoit.org/ to run tasks/chores
# pip install doit
# $ doit

import re

from graphscope_jupyter._version import __version__ as version


def task_mybinder():
    """Make the mybinder files up to date"""

    def action(targets):
        for filename in targets:
            with open(filename) as f:
                content = f.read()
            content = re.sub(
                "graphin(?P<cmp>[^0-9]*)([0-9\.].*)",  # noqa: W605
                rf"graphin\g<cmp>{version}",
                content,
            )
            with open(filename, "w") as f:
                f.write(content)
            print(f"{filename} updated")

    return {
        "actions": [action],
        "targets": ["environment.yml", "postBuild"],
        "file_dep": ["graphscope_jupyter/_version.py"],
    }
