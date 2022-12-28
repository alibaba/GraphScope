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


import json
import logging
import pickle
import threading
import time

import grpc

from graphscope.client.utils import GS_GRPC_MAX_MESSAGE_LENGTH
from graphscope.client.utils import GRPCUtils
from graphscope.client.utils import handle_grpc_error
from graphscope.client.utils import suppress_grpc_error
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import message_pb2
from graphscope.version import __version__

logger = logging.getLogger("graphscope")


class GRPCClient(object):
    def __init__(self, launcher, endpoint, reconnect=False):
        """Connect to GRAPE engine at the given :code:`endpoint`."""
        # create the gRPC stub
        options = [
            ("grpc.max_send_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_metadata_size", GS_GRPC_MAX_MESSAGE_LENGTH),
        ]
        self._launcher = launcher
        self._grpc_utils = GRPCUtils()
        self._channel = grpc.insecure_channel(endpoint, options=options)
        self._stub = coordinator_service_pb2_grpc.CoordinatorServiceStub(self._channel)
        self._session_id = None
        self._logs_fetching_thread = None
        self._reconnect = reconnect

    def waiting_service_ready(self, timeout_seconds=60):
        begin_time = time.time()
        request = message_pb2.HeartBeatRequest()
        while True:
            if self._launcher:
                code = self._launcher.poll()
                if code is not None and code != 0:
                    raise RuntimeError(
                        f"Start coordinator failed with exit code {code}"
                    )
            try:
                self._stub.HeartBeat(request)
                logger.info("GraphScope coordinator service connected.")
                break
            except grpc.RpcError as e:
                # Cannot connect to coordinator for a short time is expected
                # as the coordinator takes some time to launch
                msg = f"code: {e.code().name}, details: {e.details()}"
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logger.warning("Heart beat analytical engine failed, %s", msg)
                if time.time() - begin_time >= timeout_seconds:
                    raise ConnectionError(f"Connect coordinator timeout, {msg}")
                time.sleep(1)

    def connect(self, cleanup_instance=True, dangling_timeout_seconds=60):
        return self._connect_session_impl(
            cleanup_instance=cleanup_instance,
            dangling_timeout_seconds=dangling_timeout_seconds,
        )

    @property
    def session_id(self):
        return self._session_id

    def __str__(self):
        return "%s" % self._session_id

    def __repr__(self):
        return str(self)

    def run(self, dag_def):
        return self._run_step_impl(dag_def)

    def fetch_logs(self):
        if self._logs_fetching_thread is None:
            self._logs_fetching_thread = threading.Thread(
                target=self._fetch_logs_impl, args=()
            )
            self._logs_fetching_thread.daemon = True
            self._logs_fetching_thread.start()

    def add_lib(self, gar):
        if self._session_id:
            return self._add_lib_impl(gar)
        logger.error("adding lib to a closed session")

    def close(self):
        if self._session_id:
            self._close_session_impl()
            self._session_id = None
        if self._logs_fetching_thread is not None:
            self._logs_fetching_thread.join(timeout=5)

    @handle_grpc_error
    def send_heartbeat(self):
        request = message_pb2.HeartBeatRequest()
        return self._stub.HeartBeat(request)

    @handle_grpc_error
    def _add_lib_impl(self, gar):
        request = message_pb2.AddLibRequest(session_id=self._session_id, gar=gar)
        return self._stub.AddLib(request)

    @handle_grpc_error
    def _connect_session_impl(self, cleanup_instance=True, dangling_timeout_seconds=60):
        """
        Args:
            cleanup_instance (bool, optional): If True, also delete graphscope
                instance (such as pod) in closing process.
            dangling_timeout_seconds (int, optional): After seconds of client
                disconnect, coordinator will kill this graphscope instance.
                Disable dangling check by setting -1.

        """
        request = message_pb2.ConnectSessionRequest(
            cleanup_instance=cleanup_instance,
            dangling_timeout_seconds=dangling_timeout_seconds,
            version=__version__,
            reconnect=self._reconnect,
        )

        response = self._stub.ConnectSession(request)

        self._session_id = response.session_id
        return (
            response.session_id,
            response.cluster_type,
            response.num_workers,
            response.namespace,
            json.loads(response.engine_config),
            response.host_names,
        )

    @suppress_grpc_error
    def _fetch_logs_impl(self):
        request = message_pb2.FetchLogsRequest(session_id=self._session_id)
        responses = self._stub.FetchLogs(request)
        for res in responses:
            info, error = res.info_message.rstrip(), res.error_message.rstrip()
            if info:
                logger.info(info, extra={"simple": True})
            if error:
                logger.error(error, extra={"simple": True})

    @handle_grpc_error
    def _close_session_impl(self):
        request = message_pb2.CloseSessionRequest(session_id=self._session_id)
        response = self._stub.CloseSession(request)
        return response

    @handle_grpc_error(False)  # don't retry the "RunStep" request.
    def _run_step_impl(self, dag_def):
        # note that the "_impl" may be retried, thus the argument cannot be a
        # generator or an iterator.
        runstep_requests = self._grpc_utils.generate_runstep_requests(
            self._session_id, dag_def
        )
        response = self._grpc_utils.parse_runstep_responses(
            self._stub.RunStep(runstep_requests)
        )
        if response.code != error_codes_pb2.OK:
            logger.error(
                "Runstep failed with code: %s, message: %s",
                error_codes_pb2.Code.Name(response.code),
                response.error_msg,
            )
            if response.full_exception:
                exc = pickle.loads(response.full_exception)
                if isinstance(exc, tuple):
                    raise exc[0](*exc[1:])
                else:
                    raise exc
        return response

    def create_analytical_instance(self):
        request = message_pb2.CreateAnalyticalInstanceRequest(
            session_id=self._session_id
        )
        response = self._stub.CreateAnalyticalInstance(request)
        return json.loads(response.engine_config), response.host_names

    def create_interactive_instance(self, object_id, schema_path):
        request = message_pb2.CreateInteractiveInstanceRequest(
            session_id=self._session_id, object_id=object_id, schema_path=schema_path
        )
        response = self._stub.CreateInteractiveInstance(request)
        return response.gremlin_endpoint

    def create_learning_instance(self, object_id, handle, config):
        request = message_pb2.CreateLearningInstanceRequest(session_id=self._session_id)
        request.object_id = object_id
        request.handle = handle
        request.config = config
        response = self._stub.CreateLearningInstance(request)
        return response.handle, response.config, response.endpoints

    def close_analytical_instance(self):
        request = message_pb2.CloseAnalyticalInstanceRequest(
            session_id=self._session_id
        )
        self._stub.CloseAnalyticalInstance(request)

    def close_interactive_instance(self, object_id):
        request = message_pb2.CloseInteractiveInstanceRequest(
            session_id=self._session_id, object_id=object_id
        )
        self._stub.CloseInteractiveInstance(request)

    def close_learning_instance(self, object_id):
        request = message_pb2.CloseLearningInstanceRequest(
            session_id=self._session_id, object_id=object_id
        )
        self._stub.CloseLearningInstance(request)
