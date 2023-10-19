#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited. All Rights Reserved.
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

"""Global configuration"""

import os
import random
from string import ascii_letters

import yaml

GS_CONFIG_DEFAULT_LOCATION = os.environ.get(
    "GSCONFIG", os.path.expanduser("~/.graphscope/config")
)


class Context(object):
    def __init__(self, solution, coordinator_endpoint, name=None):
        self.supported_solutions = ["interactive"]
        if solution not in self.supported_solutions:
            raise RuntimeError(
                "The solution {0} in context {1} is not supported yet.".format(
                    solution, name
                )
            )

        if name is None:
            name = "context_" + "".join(random.choices(ascii_letters, k=6))

        self.name = name
        self.solution = solution
        self.coordinator_endpoint = coordinator_endpoint

    def to_dict(self):
        return {
            "name": self.name,
            "solution": self.solution,
            "coordinator_endpoint": self.coordinator_endpoint,
        }


class GSConfig(object):
    def __init__(self, contexts, current_context: str):
        self._contexts = contexts
        self._current_context = current_context

    def current_context(self) -> Context:
        if self._current_context is None:
            return None
        if self._current_context not in self._contexts.keys():
            raise RuntimeError(
                f"Failed to get current context: {self._current_context}"
            )
        return self._contexts[self._current_context]

    def set_and_write(self, context: Context):
        # treat the same endpoint as the same coordinator
        for _, v in self._contexts.items():
            if (
                context.coordinator_endpoint == v.coordinator_endpoint
                and context.solution == v.solution
            ):
                return

        # set
        self._current_context = context.name
        self._contexts[context.name] = context

        # write
        contexts = [v.to_dict() for _, v in self._contexts.items()]
        with open(GS_CONFIG_DEFAULT_LOCATION, "w") as file:
            yaml.dump(
                {"contexts": contexts, "current-context": self._current_context}, file
            )

    def remove_and_write(self, current_context: Context):
        # remove
        del self._contexts[current_context.name]
        self._current_context = None

        # write
        contexts = [v.to_dict() for _, v in self._contexts.items()]
        with open(GS_CONFIG_DEFAULT_LOCATION, "w") as file:
            yaml.dump(
                {"contexts": contexts, "current-context": self._current_context}, file
            )


class GSConfigLoader(object):
    def __init__(self, config_file):
        self._config_file = config_file

    def _parse_config(self, config_dict):
        if not config_dict:
            return {}, None

        current_context = config_dict["current-context"]
        if current_context is None:
            return {}, None

        contexts = {}
        current_context_exists = False
        for c in config_dict["contexts"]:
            if current_context == c["name"]:
                current_context_exists = True
            contexts[c["name"]] = Context(
                name=c["name"],
                solution=c["solution"],
                coordinator_endpoint=c["coordinator_endpoint"],
            )

        if not current_context_exists:
            raise RuntimeError(
                f"Current context {current_context} is not exists in config file {GS_CONFIG_DEFAULT_LOCATION}"
            )

        return contexts, current_context

    def load_config(self):
        config_dict = None
        with open(self._config_file, "r") as file:
            config_dict = yaml.safe_load(file)
        contexts, current_context = self._parse_config(config_dict)
        return GSConfig(contexts, current_context)


def load_gs_config():
    """Loads cluster and context information from gs-config file
    and stores them in Config.
    """
    config_file = GS_CONFIG_DEFAULT_LOCATION

    # create config file is not exists
    if not os.path.exists(config_file):
        workdir = os.path.dirname(config_file)
        os.makedirs(workdir, exist_ok=True)
        with open(config_file, "w") as file:
            yaml.safe_dump({}, file)

    loader = GSConfigLoader(config_file)
    return loader.load_config()


def get_current_context():
    config = load_gs_config()
    return config.current_context()
