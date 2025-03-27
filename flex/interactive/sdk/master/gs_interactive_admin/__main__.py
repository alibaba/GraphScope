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

import base64
import string
import connexion
import argparse
import logging
import random
import os

from gs_interactive_admin.encoder import JSONEncoder
from gs_interactive_admin.core.config import Config
from gs_interactive_admin.core.service_discovery.service_registry import (
    initialize_service_registry,
)
from gs_interactive_admin.core.metadata.metadata_store import (
    init_metadata_store,
    get_metadata_store,
)
from gs_interactive_admin.core.service.service_manager import init_service_manager
from gs_interactive_admin.core.job.job_manager import init_job_manager


def setup_args_parsing():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        help="The base64 encoded config in json format.",
    )
    parser.add_argument(
        "--config-file", type=str, help="The config file path in yaml or json format"
    )
    return parser.parse_args()


def config_logging(log_level):
    """Set log level basic on config.
    Args:
        log_level (str): Log level of stdout handler
    """
    logging.basicConfig(level=logging.INFO)

    # `NOTSET` is special as it doesn't show log in Python
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper())
    if log_level == logging.NOTSET:
        log_level = logging.DEBUG

    logger = logging.getLogger("interactive")
    logger.setLevel(log_level)


def initialize_global_variables(config):
    """
    Initialize global variables. All global variables should have two methods:
    - initialize_xxx: Initialize the global variable
    - get_xxx: Get the global variable
    """
    initialize_service_registry(config)
    init_metadata_store(config)
    # Should be placed after init_metadata_store
    init_service_manager(config)
    init_job_manager(config, get_metadata_store())


def preprocess_config(config: Config):
    if config.master.instance_name is None:
        if os.environ.get("MASTER_INSTANCE_NAME"):
            config.master.instance_name = os.environ.get("MASTER_INSTANCE_NAME")
        else:
            # generate a random instance name with six characters
            config.master.instance_name = "gs-interactive-{}".format(
                "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            )
        logging.info("Generated instance name: %s", config.master.instance_name)


def main():

    args = setup_args_parsing()
    config: Config = None
    if args.config:
        config = base64.b64decode(args.config).decode("utf-8", errors="ignore")
        config = Config.loads_json(config)
    elif args.config_file:
        config = Config.load(args.config_file)
    else:
        raise RuntimeError("Must specify a config or config-file")
    preprocess_config(config)

    config_logging(config.log_level)
    initialize_global_variables(config)

    app = connexion.App(__name__, specification_dir="./openapi/")
    app.add_api(
        "openapi.yaml",
        arguments={"title": "GraphScope Interactive API v0.3"},
        pythonic_params=True,
    )
    app.app.json_encoder = JSONEncoder

    app.run(port=config.master.port, debug=True)


if __name__ == "__main__":
    main()
