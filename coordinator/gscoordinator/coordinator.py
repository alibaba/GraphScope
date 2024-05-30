#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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

"""Coordinator between client and engines"""

import argparse
import base64
import logging
import os
import signal
import sys
import threading
from concurrent import futures

import connexion
import grpc
from graphscope.config import Config
from graphscope.proto import coordinator_service_pb2_grpc

from gscoordinator.flex.encoder import JSONEncoder
from gscoordinator.monitor import Monitor
from gscoordinator.servicer import init_graphscope_one_service_servicer
from gscoordinator.utils import GS_GRPC_MAX_MESSAGE_LENGTH

logger = logging.getLogger("graphscope")


def config_logging(log_level):
    """Set log level basic on config.
    Args:
        log_level (str): Log level of stdout handler
    """
    logging.basicConfig(level=logging.CRITICAL)

    # `NOTSET` is special as it doesn't show log in Python
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper())
    if log_level == logging.NOTSET:
        log_level = logging.DEBUG

    logger = logging.getLogger("graphscope")
    logger.setLevel(log_level)

    vineyard_logger = logging.getLogger("vineyard")
    vineyard_logger.setLevel(log_level)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(log_level)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s"
    )
    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    vineyard_logger.addHandler(stdout_handler)
    vineyard_logger.addHandler(stderr_handler)


def launch_graphscope():
    args = parse_sys_args()
    if args.config:
        config = base64.b64decode(args.config).decode("utf-8", errors="ignore")
        config = Config.loads_json(config)
    elif args.config_file:
        config = Config.load(args.config_file)
    else:
        raise RuntimeError("Must specify a config or config-file")

    config_logging(config.log_level)
    logger.info("Start server with args \n%s", config.dumps_yaml())

    servicer = get_servicer(config)
    start_server(servicer, config)


def parse_sys_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        help="The base64 encoded config in json format.",
    )
    parser.add_argument(
        "--config-file", type=str, help="The config file path in yaml or json format"
    )
    # parser.add_arguments(Config, dest="gs")
    return parser.parse_args()


def get_servicer(config: Config):
    """Get servicer of specified solution under FLEX architecture"""
    service_initializers = {
        "GraphScope One": init_graphscope_one_service_servicer,
    }

    initializer = service_initializers.get(config.solution)
    if initializer is None:
        raise RuntimeError(
            f"Expect {service_initializers.keys()} of solution parameter"
        )

    return initializer(config)


def start_http_service(config):
    app = connexion.App(__name__, specification_dir="./flex/openapi/")
    app.app.json_encoder = JSONEncoder
    app.add_api(
        "openapi.yaml",
        arguments={"title": "GraphScope FLEX HTTP SERVICE API"},
        pythonic_params=True,
    )
    app.run(port=config.coordinator.http_port)


def start_server(
    coordinator_service_servicer: coordinator_service_pb2_grpc.CoordinatorServiceServicer,
    config: Config,
):
    # register gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max(4, os.cpu_count() or 1)),
        options=[
            ("grpc.max_send_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_metadata_size", GS_GRPC_MAX_MESSAGE_LENGTH),
        ],
    )
    coordinator_service_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        coordinator_service_servicer, server
    )
    endpoint = f"0.0.0.0:{config.coordinator.service_port}"
    server.add_insecure_port(endpoint)

    logger.info("Coordinator server listen at %s", endpoint)

    server.start()

    # OpenApi server
    httpservice_t = threading.Thread(target=start_http_service, args=(config,))
    httpservice_t.daemon = True
    httpservice_t.start()

    if config.coordinator.monitor:
        try:
            Monitor.startServer(config.coordinator.monitor_port, "127.0.0.1")
            logger.info(
                "Coordinator monitor server listen at 127.0.0.1:%d",
                config.coordinator.monitor_port,
            )
        except Exception:  # noqa: E722, pylint: disable=broad-except
            logger.exception(
                "Failed to start monitor server on '127.0.0.1:%s'",
                config.coordinator.monitor_port,
            )

    # handle SIGTERM signal
    def terminate(signum, frame):
        server.stop(True)
        coordinator_service_servicer.cleanup()

    signal.signal(signal.SIGTERM, terminate)

    try:
        # GRPC has handled SIGINT
        server.wait_for_termination()
    except KeyboardInterrupt:
        coordinator_service_servicer.cleanup()
        server.stop(True)


if __name__ == "__main__":
    launch_graphscope()
