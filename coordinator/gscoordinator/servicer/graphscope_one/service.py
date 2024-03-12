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

"""GraphScope One Service"""

import atexit
import base64
import json
import logging
import os
import pickle
import queue
import random
import re
import sys
import threading
import traceback
from string import ascii_letters

import grpc
from packaging import version
from simple_parsing import ArgumentParser

from gscoordinator.io_utils import StdStreamWrapper

# capture system stdout
sys.stdout = StdStreamWrapper(sys.stdout)
sys.stderr = StdStreamWrapper(sys.stderr)

from graphscope.config import Config
from graphscope.framework.utils import PipeMerger
from graphscope.framework.utils import i_to_attr
from graphscope.framework.utils import s_to_attr
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import message_pb2
from graphscope.proto import types_pb2

from gscoordinator.dag_manager import DAGManager
from gscoordinator.dag_manager import GSEngine
from gscoordinator.kubernetes_launcher import KubernetesClusterLauncher
from gscoordinator.launcher import AbstractLauncher
from gscoordinator.local_launcher import LocalLauncher
from gscoordinator.monitor import Monitor
from gscoordinator.object_manager import InteractiveInstanceManager
from gscoordinator.object_manager import LearningInstanceManager
from gscoordinator.object_manager import ObjectManager
from gscoordinator.op_executor import OperationExecutor
from gscoordinator.operator_launcher import OperatorLauncher
from gscoordinator.utils import catch_unknown_errors
from gscoordinator.utils import check_server_ready
from gscoordinator.utils import create_single_op_dag
from gscoordinator.version import __version__

__all__ = ["GraphScopeOneServiceServicer", "init_graphscope_one_service_servicer"]

logger = logging.getLogger("graphscope")


class GraphScopeOneServiceServicer(
    coordinator_service_pb2_grpc.CoordinatorServiceServicer
):
    """Provides methods that implement functionality of master service server.
    Holding:
        1. launcher: the engine launcher.
        2. session_id: the handle for a particular session to engine
        3. object_manager: the object manager for the session
        4. operation_executor: the operation executor for the session
    """

    def __init__(
        self, launcher: AbstractLauncher, dangling_timeout_seconds, log_level="INFO"
    ):
        self._operator_mode = False

        self._object_manager = ObjectManager()

        # only one connection is allowed at the same time
        self._connected = False

        # control log fetching
        self._streaming_logs = False
        self._pipe_merged = PipeMerger(sys.stdout, sys.stderr)

        self._session_id = "session_" + "".join(random.choices(ascii_letters, k=8))

        # dangling check
        self._dangling_timeout_seconds = dangling_timeout_seconds
        self._dangling_detecting_timer = None
        self._cleanup_instance = False
        # the dangling timer should be initialized after the launcher started,
        # otherwise there would be a deadlock if `self._launcher.start()` failed.
        self._set_dangling_timer(cleanup_instance=True)

        # a lock that protects the coordinator
        self._lock = threading.RLock()
        atexit.register(self.cleanup)

        self._launcher = launcher
        self._launcher.set_session_workspace(self._session_id)
        if not self._launcher.start():
            raise RuntimeError("Coordinator launching instance failed.")

        self._operation_executor: OperationExecutor = OperationExecutor(
            self._session_id, self._launcher, self._object_manager
        )

    def __del__(self):
        self.cleanup()

    @Monitor.connectSession
    def ConnectSession(self, request, context):
        if self._launcher.analytical_engine_endpoint is not None:
            engine_config = self._operation_executor.get_analytical_engine_config()
            engine_config.update(self._launcher.get_engine_config())
            host_names = self._launcher.hosts
        else:
            engine_config = {}
            host_names = []

        # A session is already connected.
        if self._connected:
            if getattr(request, "reconnect", False):
                return message_pb2.ConnectSessionResponse(
                    session_id=self._session_id,
                    cluster_type=self._launcher.type(),
                    num_workers=self._launcher.num_workers,
                    namespace=self._launcher.get_namespace(),
                    engine_config=json.dumps(engine_config),
                    host_names=host_names,
                )
            else:
                # connect failed, more than one connection at the same time.
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(
                    "Cannot setup more than one connection at the same time."
                )
                return message_pb2.ConnectSessionResponse()
        # check version compatibility from client
        sv = version.parse(__version__)
        cv = version.parse(request.version)
        if sv.major != cv.major or sv.minor != cv.minor:
            error_msg = f"Version between client and server is inconsistent: {request.version} vs {__version__}"
            logger.warning(error_msg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(error_msg)
            return message_pb2.ConnectSessionResponse()

        # Connect to serving coordinator.
        self._connected = True
        # Cleanup after timeout seconds
        self._dangling_timeout_seconds = request.dangling_timeout_seconds
        # other timeout seconds
        self._comm_timeout_seconds = getattr(request, "comm_timeout_seconds", 120)
        # If true, also delete graphscope instance (such as pods) in closing process
        self._cleanup_instance = request.cleanup_instance

        # Session connected, fetch logs via gRPC.
        self._streaming_logs = True
        sys.stdout.drop(False)
        sys.stderr.drop(False)

        return message_pb2.ConnectSessionResponse(
            session_id=self._session_id,
            cluster_type=self._launcher.type(),
            num_workers=self._launcher.num_workers,
            namespace=self._launcher.get_namespace(),
            engine_config=json.dumps(engine_config),
            host_names=host_names,
        )

    @Monitor.closeSession
    def CloseSession(self, request, context):
        """
        Disconnect session, note that it won't clean up any resources if self._cleanup_instance is False.
        """
        if request.session_id != self._session_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Session handle not matched, {request.session_id} versus {self._session_id}"
            )
            return message_pb2.CloseSessionResponse()

        self._connected = False

        self.cleanup(cleanup_instance=self._cleanup_instance, is_dangling=False)
        if self._cleanup_instance:
            self._session_id = None
            self._operation_executor = None

        # Session closed, stop streaming logs
        sys.stdout.drop(True)
        sys.stderr.drop(True)
        self._streaming_logs = False
        return message_pb2.CloseSessionResponse()

    def HeartBeat(self, request, context):
        self._reset_dangling_timer(self._connected, self._cleanup_instance)
        # analytical engine
        # if self._operation_executor is not None:
        # return self._operation_executor.heart_beat(request)
        return message_pb2.HeartBeatResponse()

    def RunStep(self, request_iterator, context):
        with self._lock:
            for response in self.RunStepWrapped(request_iterator, context):
                yield response

    def _RunStep(self, request_iterator, context):
        from gremlin_python.driver.protocol import GremlinServerError

        # split dag
        dag_manager = DAGManager(request_iterator)
        loader_op_bodies = {}

        # response list for stream
        responses = [
            message_pb2.RunStepResponse(head=message_pb2.RunStepResponseHead())
        ]

        while not dag_manager.empty():
            run_dag_on, dag, dag_bodies = dag_manager.next_dag()
            error_code = error_codes_pb2.COORDINATOR_INTERNAL_ERROR
            head, bodies = None, None
            try:
                # run on analytical engine
                if run_dag_on == GSEngine.analytical_engine:
                    # need dag_bodies to load graph from pandas/numpy
                    error_code = error_codes_pb2.ANALYTICAL_ENGINE_INTERNAL_ERROR
                    head, bodies = self._operation_executor.run_on_analytical_engine(
                        dag, dag_bodies, loader_op_bodies
                    )
                # run on interactive engine
                elif run_dag_on == GSEngine.interactive_engine:
                    error_code = error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR
                    head, bodies = self._operation_executor.run_on_interactive_engine(
                        dag
                    )
                # run on learning engine
                elif run_dag_on == GSEngine.learning_engine:
                    error_code = error_codes_pb2.LEARNING_ENGINE_INTERNAL_ERROR
                    head, bodies = self._operation_executor.run_on_learning_engine(dag)
                # run on coordinator
                elif run_dag_on == GSEngine.coordinator:
                    error_code = error_codes_pb2.COORDINATOR_INTERNAL_ERROR
                    head, bodies = self._operation_executor.run_on_coordinator(
                        dag, dag_bodies, loader_op_bodies
                    )
                # merge the responses
                responses[0].head.results.extend(head.head.results)
                responses.extend(bodies)

            except grpc.RpcError as exc:
                # Not raised by graphscope, maybe socket closed, etc.
                context.set_code(exc.code())
                context.set_details(
                    f"{exc.details()}. The traceback is: {traceback.format_exc()}"
                )

                # stop yield to raise the grpc errors to the client, as the
                # client may consume nothing if error happens
                return

            except GremlinServerError as exc:
                response_head = responses[0]
                response_head.head.code = error_code
                response_head.head.error_msg = f"Error occurred during RunStep. The traceback is: {traceback.format_exc()}"
                if hasattr(exc, "status_message"):
                    exc_message = exc.status_message
                else:
                    exc_message = str(exc)
                response_head.head.full_exception = pickle.dumps(
                    (
                        GremlinServerError,
                        {
                            "code": exc.status_code,
                            "message": exc_message,
                            "attributes": exc.status_attributes,
                        },
                    )
                )
                # stop iteration to propagate the error to client immediately
                break

            except Exception as exc:
                response_head = responses[0]
                response_head.head.code = error_code
                response_head.head.error_msg = f"Error occurred during RunStep, The traceback is: {traceback.format_exc()}"
                response_head.head.full_exception = pickle.dumps(exc)

                # stop iteration to propagate the error to client immediately
                break

        for response in responses:
            yield response

    RunStepWrapped = catch_unknown_errors(
        message_pb2.RunStepResponse(head=message_pb2.RunStepResponseHead()), True
    )(_RunStep)

    def FetchLogs(self, request, context):
        while self._streaming_logs:
            try:
                info_message, error_message = self._pipe_merged.poll(timeout=2)
            except queue.Empty:
                info_message, error_message = "", ""
            except Exception as e:
                info_message, error_message = (
                    f"WARNING: failed to read log: {e}. The traceback is: {traceback.format_exc()}",
                    "",
                )

            if info_message or error_message:
                if self._streaming_logs:
                    yield message_pb2.FetchLogsResponse(
                        info_message=info_message, error_message=error_message
                    )

    def AddLib(self, request, context):
        try:
            self._operation_executor.add_lib(request)
        except Exception as e:
            context.abort(grpc.StatusCode.ABORTED, str(e))
        return message_pb2.AddLibResponse()

    def CreateAnalyticalInstance(self, request, context):
        engine_config = {}
        try:
            # create GAE rpc service
            self._launcher.create_analytical_instance()
            engine_config = self._operation_executor.get_analytical_engine_config()
            engine_config.update(self._launcher.get_engine_config())
        except NotImplementedError:
            # TODO: This is a workaround for that we launching gae unconditionally after session connects,
            # make it an error when above logic has been changed.
            logger.warning("Analytical engine is not enabled.")
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(
                f"Get engine config failed: {e.details()}. The traceback is: {traceback.format_exc()}"
            )
            return message_pb2.CreateAnalyticalInstanceResponse()
        except Exception as e:
            context.abort(
                grpc.StatusCode.ABORTED,
                f"{e}. The traceback is: {traceback.format_exc()}",
            )
            return message_pb2.CreateAnalyticalInstanceResponse()
        return message_pb2.CreateAnalyticalInstanceResponse(
            engine_config=json.dumps(engine_config),
            host_names=self._launcher.hosts,
        )

    def CreateInteractiveInstance(self, request, context):
        def _match_frontend_endpoint(pattern, lines):
            for line in lines.split("\n"):
                rlt = re.findall(pattern, line)
                if rlt:
                    return rlt[0].strip()
            return ""

        # frontend endpoint pattern
        FRONTEND_GREMLIN_PATTERN = re.compile("(?<=FRONTEND_GREMLIN_ENDPOINT:).*$")
        FRONTEND_CYPHER_PATTERN = re.compile("(?<=FRONTEND_CYPHER_ENDPOINT:).*$")
        # frontend external endpoint, for clients that are outside of cluster to connect
        # only available in kubernetes mode, exposed by NodePort or LoadBalancer
        FRONTEND_EXTERNAL_GREMLIN_PATTERN = re.compile(
            "(?<=FRONTEND_EXTERNAL_GREMLIN_ENDPOINT:).*$"
        )
        FRONTEND_EXTERNAL_CYPHER_PATTERN = re.compile(
            "(?<=FRONTEND_EXTERNAL_CYPHER_ENDPOINT:).*$"
        )

        # create instance
        object_id = request.object_id
        schema_path = request.schema_path
        params = request.params
        with_cypher = request.with_cypher
        try:
            proc = self._launcher.create_interactive_instance(
                object_id, schema_path, params, with_cypher
            )
            gie_manager = InteractiveInstanceManager(object_id)
            # Put it to object_manager to ensure it could be killed during coordinator cleanup
            # If coordinator is shutdown by force when creating interactive instance
            self._object_manager.put(object_id, gie_manager)
            # 60 seconds is enough, see also GH#1024; try 120
            # already add errs to outs
            outs, _ = proc.communicate(timeout=120)  # throws TimeoutError
            return_code = proc.poll()
            if return_code != 0:
                raise RuntimeError(f"Error code: {return_code}, message {outs}")
            # match frontend endpoints and check for ready
            gremlin_endpoint = _match_frontend_endpoint(FRONTEND_GREMLIN_PATTERN, outs)
            cypher_endpoint = _match_frontend_endpoint(FRONTEND_CYPHER_PATTERN, outs)
            logger.debug("Got endpoints: %s %s", gremlin_endpoint, cypher_endpoint)
            # coordinator use internal endpoint
            gie_manager.set_endpoint(gremlin_endpoint)
            if check_server_ready(gremlin_endpoint, server="gremlin"):
                logger.info(
                    "Built interactive gremlin frontend: %s for graph %ld",
                    gremlin_endpoint,
                    object_id,
                )

            if with_cypher and check_server_ready(
                cypher_endpoint, server="cypher"
            ):  # throws TimeoutError
                logger.info(
                    "Built interactive cypher frontend: %s for graph %ld",
                    cypher_endpoint,
                    object_id,
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details(
                f"Create interactive instance failed: ${e}. The traceback is: {traceback.format_exc()}"
            )
            self._launcher.close_interactive_instance(object_id)
            self._object_manager.pop(object_id)
            return message_pb2.CreateInteractiveInstanceResponse()
        external_gremlin_endpoint = _match_frontend_endpoint(
            FRONTEND_EXTERNAL_GREMLIN_PATTERN, outs
        )
        external_cypher_endpoint = _match_frontend_endpoint(
            FRONTEND_EXTERNAL_CYPHER_PATTERN, outs
        )
        logger.debug(
            "Got external endpoints: %s %s",
            external_gremlin_endpoint,
            external_cypher_endpoint,
        )

        # client use external endpoint (k8s mode), or internal endpoint (standalone mode)
        gremlin_endpoint = external_gremlin_endpoint or gremlin_endpoint
        cypher_endpoint = external_cypher_endpoint or cypher_endpoint
        return message_pb2.CreateInteractiveInstanceResponse(
            gremlin_endpoint=gremlin_endpoint,
            cypher_endpoint=cypher_endpoint,
            object_id=object_id,
        )

    def CreateLearningInstance(self, request, context):
        object_id = request.object_id
        handle, config, learning_backend = (
            request.handle,
            request.config,
            request.learning_backend,
        )
        try:
            endpoints = self._launcher.create_learning_instance(
                object_id, handle, config, learning_backend
            )
            self._object_manager.put(
                object_id, LearningInstanceManager(object_id, learning_backend)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details(
                f"Create learning instance failed: ${e}. The traceback is: {traceback.format_exc()}"
            )
            self._launcher.close_learning_instance(object_id, learning_backend)
            self._object_manager.pop(object_id)
            return message_pb2.CreateLearningInstanceResponse()
        return message_pb2.CreateLearningInstanceResponse(
            object_id=object_id, handle=handle, config=config, endpoints=endpoints
        )

    def CloseAnalyticalInstance(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("CloseAnalyticalInstance is not implemented")
        return message_pb2.CloseAnalyticalInstanceResponse()

    def CloseInteractiveInstance(self, request, context):
        object_id = request.object_id
        if object_id in self._object_manager:
            self._object_manager.pop(object_id)
            try:
                self._launcher.close_interactive_instance(object_id)
            except Exception as e:
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details(
                    f"Close interactive instance failed: ${e}. The traceback is: {traceback.format_exc()}"
                )
        return message_pb2.CloseInteractiveInstanceResponse()

    def CloseLearningInstance(self, request, context):
        object_id = request.object_id
        if object_id in self._object_manager:
            self._object_manager.pop(object_id)
            logger.info("Close learning instance with object id %ld", object_id)
            try:
                self._launcher.close_learning_instance(
                    object_id, request.learning_backend
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details(
                    f"Close learning instance failed: ${e}. The traceback is: {traceback.format_exc()}"
                )
        return message_pb2.CloseLearningInstanceResponse()

    @Monitor.cleanup
    def cleanup(self, cleanup_instance=True, is_dangling=False):
        # clean up session resources.
        logger.info(
            "Clean up resources, cleanup_instance: %s, is_dangling: %s",
            cleanup_instance,
            is_dangling,
        )
        for _, obj in self._object_manager.items():
            op_type, config = None, {}
            if obj.type == "app":
                op_type = types_pb2.UNLOAD_APP
                config[types_pb2.APP_NAME] = s_to_attr(obj.key)
            elif obj.type == "graph":
                op_type = types_pb2.UNLOAD_GRAPH
                config[types_pb2.GRAPH_NAME] = s_to_attr(obj.key)
                # dynamic graph doesn't have a object id
                if obj.object_id != -1:
                    config[types_pb2.VINEYARD_ID] = i_to_attr(obj.object_id)
            elif obj.type == "gie_manager":
                self._launcher.close_interactive_instance(obj.object_id)
            elif obj.type == "gle_manager":
                self._launcher.close_learning_instance(obj.object_id, 0)
            elif obj.type == "glt_manager":
                self._launcher.close_learning_instance(obj.object_id, 1)

            if op_type is not None:
                dag_def = create_single_op_dag(op_type, config)
                try:
                    self._operation_executor.run_step(dag_def, [])
                except grpc.RpcError as e:
                    logger.error(
                        "Cleanup failed, code: %s, details: %s. The traceback is: %s",
                        e.code().name,
                        e.details(),
                        traceback.format_exc(),
                    )

        self._object_manager.clear()
        self._cancel_dangling_timer()

        if cleanup_instance:
            self._launcher.stop(is_dangling=is_dangling)

    def _set_dangling_timer(self, cleanup_instance: bool):
        if self._dangling_timeout_seconds > 0:
            self._dangling_detecting_timer = threading.Timer(
                interval=self._dangling_timeout_seconds,
                function=self.cleanup,
                args=(
                    cleanup_instance,
                    True,
                ),
            )
            self._dangling_detecting_timer.start()

    def _cancel_dangling_timer(self):
        if self._dangling_detecting_timer is not None:
            self._dangling_detecting_timer.cancel()
            self._dangling_detecting_timer = None

    def _reset_dangling_timer(self, reset: bool, cleanup_instance: bool):
        if reset:
            self._cancel_dangling_timer()
            self._set_dangling_timer(cleanup_instance)


def init_graphscope_one_service_servicer(config: Config):
    type2launcher = {
        "hosts": LocalLauncher,
        "k8s": KubernetesClusterLauncher,
        "operator": OperatorLauncher,
    }

    launcher = type2launcher.get(config.launcher_type)
    if launcher is None:
        raise RuntimeError(f"Expect {type2launcher.keys()} of launcher_type parameter")

    return GraphScopeOneServiceServicer(
        launcher=launcher(config),
        dangling_timeout_seconds=config.session.dangling_timeout_seconds,
        log_level=config.log_level,
    )
