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
import atexit
import functools
import json
import logging
import os
import pickle
import queue
import random
import re
import signal
import string
import sys
import threading
import traceback
from concurrent import futures

import grpc
from packaging import version

from gscoordinator.io_utils import StdStreamWrapper

# capture system stdout
from gscoordinator.launcher import AbstractLauncher
from gscoordinator.local_launcher import LocalLauncher

sys.stdout = StdStreamWrapper(sys.stdout)
sys.stderr = StdStreamWrapper(sys.stderr)

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
from gscoordinator.monitor import Monitor
from gscoordinator.object_manager import InteractiveQueryManager
from gscoordinator.object_manager import LearningInstanceManager
from gscoordinator.object_manager import ObjectManager
from gscoordinator.op_executor import OperationExecutor
from gscoordinator.utils import GS_GRPC_MAX_MESSAGE_LENGTH
from gscoordinator.utils import check_gremlin_server_ready
from gscoordinator.utils import create_single_op_dag
from gscoordinator.utils import str2bool
from gscoordinator.version import __version__


def catch_unknown_errors(response_on_error=None, using_yield=False):
    """A catcher that catches all (unknown) exceptions in gRPC handlers to ensure
    the client not think the coordinator services is crashed.
    """

    def catch_exceptions(handler):
        @functools.wraps(handler)
        def handler_execution(self, request, context):
            try:
                if using_yield:
                    for result in handler(self, request, context):
                        yield result
                else:
                    yield handler(self, request, context)
            except Exception as exc:
                error_message = repr(exc)
                error_traceback = traceback.format_exc()
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details(
                    'Error occurs in handler: "%s", with traceback: ' % error_message
                    + error_traceback
                )
                if response_on_error is not None:
                    yield response_on_error

        return handler_execution

    return catch_exceptions


def config_logging(log_level):
    """Set log level basic on config.
    Args:
        log_level (str): Log level of stdout handler
    """
    logging.basicConfig(level=logging.CRITICAL)

    if log_level:
        log_level = log_level.upper()

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


logger = logging.getLogger("graphscope")


class CoordinatorServiceServicer(
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
        config_logging(log_level)

        self._object_manager = ObjectManager()

        # only one connection is allowed at the same time
        # session id will be generated when connection from client is established
        self._session_id = None
        self._connected = False

        self._launcher = launcher

        # control log fetching
        self._streaming_logs = False
        self._pipe_merged = PipeMerger(sys.stdout, sys.stderr)

        # dangling check
        self._dangling_timeout_seconds = dangling_timeout_seconds
        self._dangling_detecting_timer = None
        self._cleanup_instance = False
        self._set_dangling_timer(cleanup_instance=True)

        self._operation_executor: OperationExecutor = None

        # a lock that protects the coordinator
        self._lock = threading.RLock()
        atexit.register(self.cleanup)

    def __del__(self):
        self.cleanup()

    @Monitor.connectSession
    def ConnectSession(self, request, context):
        if self._launcher.analytical_engine_process is not None:
            engine_config = self._operation_executor.get_analytical_engine_config()
            engine_config.update(self._launcher.get_engine_config())
            host_names = self._launcher.hosts.split(",")
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
        # If true, also delete graphscope instance (such as pods) in closing process
        self._cleanup_instance = request.cleanup_instance

        # Session connected, fetch logs via gRPC.
        self._streaming_logs = True
        sys.stdout.drop(False)

        if self._session_id is None:  # else reuse previous session.
            self._session_id = self._generate_session_id()
            self._launcher.set_session_workspace(self._session_id)

            self._operation_executor = OperationExecutor(
                self._session_id, self._launcher, self._object_manager
            )
            if not self._launcher.start():
                # connect failed, more than one connection at the same time.
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details("Create GraphScope cluster failed")
                return message_pb2.ConnectSessionResponse()

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
        if not self._check_session_consistency(request, context):
            return message_pb2.CloseSessionResponse()

        self._connected = False

        self.cleanup(cleanup_instance=self._cleanup_instance, is_dangling=False)
        if self._cleanup_instance:
            self._session_id = None
            self._operation_executor = None

        # Session closed, stop streaming logs
        sys.stdout.drop(True)
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
                context.set_details(exc.details())

                # stop yield to raise the grpc errors to the client, as the
                # client may consume nothing if error happens
                return

            except GremlinServerError as exc:
                response_head = responses[0]
                response_head.head.code = error_code
                response_head.head.error_msg = f"Error occurred during RunStep, The traceback is: {traceback.format_exc()}"
                response_head.head.full_exception = pickle.dumps(
                    (
                        GremlinServerError,
                        {
                            "code": exc.status_code,
                            "message": exc.status_message,
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
                info_message, error_message = f"WARNING: failed to read log: {e}", ""

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
            context.set_details("Get engine config failed: " + e.details())
            return message_pb2.CreateAnalyticalInstanceResponse()
        except Exception as e:
            context.abort(grpc.StatusCode.ABORTED, str(e))
            return message_pb2.CreateAnalyticalInstanceResponse()
        return message_pb2.CreateAnalyticalInstanceResponse(
            engine_config=json.dumps(engine_config),
            host_names=self._launcher.hosts.split(","),
        )

    def CreateInteractiveInstance(self, request, context):
        def _match_frontend_endpoint(pattern, lines):
            for line in lines.split("\n"):
                rlt = re.findall(pattern, line)
                if rlt:
                    return rlt[0].strip()
            return ""

        # frontend endpoint pattern
        FRONTEND_PATTERN = re.compile("(?<=FRONTEND_ENDPOINT:).*$")
        # frontend external endpoint, for clients that are outside of cluster to connect
        # only available in kubernetes mode, exposed by NodePort or LoadBalancer
        FRONTEND_EXTERNAL_PATTERN = re.compile("(?<=FRONTEND_EXTERNAL_ENDPOINT:).*$")

        # create instance
        object_id = request.object_id
        schema_path = request.schema_path
        try:
            proc = self._launcher.create_interactive_instance(object_id, schema_path)
            gie_manager = InteractiveQueryManager(object_id)
            # Put it to object_manager to ensure it could be killed during coordinator cleanup
            # If coordinator is shutdown by force when creating interactive instance
            self._object_manager.put(object_id, gie_manager)
            # 60 seconds is enough, see also GH#1024; try 120
            # already add errs to outs
            outs, _ = proc.communicate(timeout=120)  # throws TimeoutError
            return_code = proc.poll()
            if return_code != 0:
                raise RuntimeError(f"Error code: {return_code}, message {outs}")
            # match frontend endpoint and check for ready
            endpoint = _match_frontend_endpoint(FRONTEND_PATTERN, outs)
            # coordinator use internal endpoint
            gie_manager.set_endpoint(endpoint)
            if check_gremlin_server_ready(endpoint):  # throws TimeoutError
                logger.info(
                    "Built interactive frontend %s for graph %ld", endpoint, object_id
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Create interactive instance failed: " + str(e))
            self._launcher.close_interactive_instance(object_id)
            self._object_manager.pop(object_id)
            return message_pb2.CreateInteractiveInstanceResponse()
        external_endpoint = _match_frontend_endpoint(FRONTEND_EXTERNAL_PATTERN, outs)
        # client use external endpoint (k8s mode), or internal endpoint (standalone mode)
        endpoint = external_endpoint or endpoint
        return message_pb2.CreateInteractiveInstanceResponse(
            gremlin_endpoint=endpoint, object_id=object_id
        )

    def CreateLearningInstance(self, request, context):
        object_id = request.object_id
        logger.info("Create learning instance with object id %ld", object_id)
        handle, config = request.handle, request.config
        try:
            endpoints = self._launcher.create_learning_instance(
                object_id, handle, config
            )
            self._object_manager.put(object_id, LearningInstanceManager(object_id))
        except Exception as e:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Create learning instance failed: " + str(e))
            self._launcher.close_learning_instance(object_id)
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
                context.set_details("Close interactive instance failed: " + str(e))
        return message_pb2.CloseInteractiveInstanceResponse()

    def CloseLearningInstance(self, request, context):
        object_id = request.object_id
        if object_id in self._object_manager:
            self._object_manager.pop(object_id)
            logger.info("Close learning instance with object id %ld", object_id)
            try:
                self._launcher.close_learning_instance(object_id)
            except Exception as e:
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details("Close learning instance failed: " + str(e))
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
                self._launcher.close_learning_instance(obj.object_id)

            if op_type is not None:
                dag_def = create_single_op_dag(op_type, config)
                try:
                    self._operation_executor.run_step(dag_def, [])
                except grpc.RpcError as e:
                    logger.error(
                        "Cleanup failed, code: %s, details: %s",
                        e.code().name,
                        e.details(),
                    )

        self._object_manager.clear()
        self._cancel_dangling_timer()

        if cleanup_instance:
            self._launcher.stop(is_dangling=is_dangling)

    @staticmethod
    def _generate_session_id():
        return "session_" + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(8)]
        )

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

    def _check_session_consistency(self, request, context):
        if request.session_id != self._session_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Session handle not matched, {request.session_id} versus {self._session_id}"
            )
            return False
        else:
            return True


def parse_sys_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        default=4,
        help="The number of graphscope engine workers.",
    )
    parser.add_argument(
        "--preemptive",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="Support resource preemption or resource guarantee",
    )
    parser.add_argument(
        "--instance_id",
        type=str,
        help="Unique id for each GraphScope instance.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=63800,
        help="Coordinator service port.",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="info",
        help="Log level, choose from 'info' or 'debug'.",
    )
    parser.add_argument(
        "--hosts",
        type=str,
        default="localhost",
        help="list of comma seperated hostnames of graphscope engine workers.",
    )
    parser.add_argument(
        "--vineyard_socket",
        type=str,
        default=None,
        help="Vineyard IPC socket path, a socket suffixed by timestamp will be created in '/tmp' if not given.",
    )
    parser.add_argument(
        "--cluster_type",
        type=str,
        default="k8s",
        help="Cluster type of deploying, choose from 'k8s' or 'local'.",
    )
    parser.add_argument(
        "--k8s_namespace",
        type=str,
        default="graphscope",
        help="The namespace to create all resource, which must exist in advance.",
    )
    parser.add_argument(
        "--k8s_image_registry", type=str, default="", help="k8s image registry"
    )
    parser.add_argument(
        "--k8s_image_repository",
        type=str,
        default="graphscope",
        help="k8s image repository",
    )
    parser.add_argument(
        "--k8s_image_tag", type=str, default=__version__, help="k8s image tag"
    )
    parser.add_argument(
        "--k8s_service_type",
        type=str,
        default="NodePort",
        help="Service type, choose from 'NodePort' or 'LoadBalancer'.",
    )
    parser.add_argument(
        "--k8s_coordinator_name",
        type=str,
        default="",
        help="Coordinator name of graphscope instance.",
    )
    parser.add_argument(
        "--k8s_coordinator_service_name",
        type=str,
        default="",
        help="Coordinator service name of graphscope instance.",
    )
    parser.add_argument(
        "--k8s_image_pull_policy",
        type=str,
        default="IfNotPresent",
        help="Kubernetes image pull policy.",
    )
    parser.add_argument(
        "--k8s_image_pull_secrets",
        type=str,
        default="",
        help="A list of comma separated secrets to pull image.",
    )
    parser.add_argument(
        "--k8s_vineyard_daemonset",
        type=str,
        default=None,
        help="Use the existing vineyard DaemonSet with name 'k8s_vineyard_daemonset'.",
    )
    parser.add_argument(
        "--k8s_vineyard_cpu",
        type=float,
        default=1.0,
        help="CPU cores of vineyard container.",
    )
    parser.add_argument(
        "--k8s_vineyard_image",
        type=str,
        default=None,
        help="Image for vineyard container",
    )
    parser.add_argument(
        "--k8s_vineyard_mem",
        type=str,
        default="256Mi",
        help="Memory of vineyard container, suffix with ['Mi', 'Gi', 'Ti'].",
    )
    parser.add_argument(
        "--vineyard_shared_mem",
        type=str,
        default="8Gi",
        help="Plasma memory in vineyard, suffix with ['Mi', 'Gi', 'Ti'].",
    )
    parser.add_argument(
        "--k8s_engine_cpu",
        type=float,
        default=1.0,
        help="CPU cores of engine container, default: 1.0",
    )
    parser.add_argument(
        "--k8s_engine_mem",
        type=str,
        default="256Mi",
        help="Memory of engine container, suffix with ['Mi', 'Gi', 'Ti'].",
    )
    parser.add_argument(
        "--etcd_addrs",
        type=str,
        default=None,
        help="The addr of external etcd cluster, with formats like 'etcd01:port,etcd02:port,etcd03:port' ",
    )
    parser.add_argument(
        "--etcd_listening_client_port",
        type=int,
        default=2379,
        help="The port that etcd server will beind to for accepting client connections. Defaults to 2379.",
    )
    parser.add_argument(
        "--etcd_listening_peer_port",
        type=int,
        default=2380,
        help="The port that etcd server will beind to for accepting peer connections. Defaults to 2380.",
    )
    parser.add_argument(
        "--k8s_with_analytical",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="Enable analytical engine or not.",
    )
    parser.add_argument(
        "--k8s_with_analytical_java",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="Enable analytical engine with java or not.",
    )
    parser.add_argument(
        "--k8s_with_interactive",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="Enable interactive engine or not.",
    )
    parser.add_argument(
        "--k8s_with_learning",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="Enable learning engine or not.",
    )
    parser.add_argument(
        "--k8s_with_mars",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Enable mars or not.",
    )
    parser.add_argument(
        "--k8s_mars_worker_cpu",
        type=float,
        default=0.5,
        help="CPU cores of mars worker container, default: 0.5",
    )
    parser.add_argument(
        "--k8s_mars_worker_mem",
        type=str,
        default="4Gi",
        help="Memory of Mars worker container, default: 4Gi",
    )
    parser.add_argument(
        "--k8s_mars_scheduler_cpu",
        type=float,
        default=0.5,
        help="CPU cores of Mars scheduler container, default: 0.5",
    )
    parser.add_argument(
        "--k8s_mars_scheduler_mem",
        type=str,
        default="2Gi",
        help="Memory of Mars scheduler container, default: 2Gi",
    )
    parser.add_argument(
        "--k8s_engine_pod_node_selector",
        type=str,
        default="",
        help="Node selector for engine pods, default is None",
    )
    parser.add_argument(
        "--k8s_volumes",
        type=str,
        default="",
        help="A json string specifies the kubernetes volumes to mount.",
    )
    parser.add_argument(
        "--k8s_delete_namespace",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Delete the namespace that created by graphscope.",
    )
    parser.add_argument(
        "--timeout_seconds",
        type=int,
        default=600,
        help="The length of time to wait before giving up launching graphscope.",
    )
    parser.add_argument(
        "--dangling_timeout_seconds",
        type=int,
        default=600,
        help="The length of time to wait starting from client disconnected before killing the graphscope instance",
    )
    parser.add_argument(
        "--waiting_for_delete",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Wait until the graphscope instance has been deleted successfully",
    )
    parser.add_argument(
        "--k8s_with_dataset",
        type=str2bool,
        nargs="?",
        const=False,
        default=False,
        help="Mount the aliyun dataset bucket as a volume by ossfs.",
    )
    parser.add_argument(
        "--monitor",
        type=str2bool,
        nargs="?",
        const=False,
        default=False,
        help="Enable or disable prometheus exporter.",
    )
    parser.add_argument(
        "--monitor_port",
        type=int,
        default=9968,
        help="Coordinator prometheus exporter service port.",
    )
    return parser.parse_args()


def launch_graphscope():
    args = parse_sys_args()
    launcher = get_launcher(args)
    start_server(launcher, args)


def get_launcher(args):
    if args.cluster_type == "k8s":
        launcher = KubernetesClusterLauncher(
            coordinator_name=args.k8s_coordinator_name,
            coordinator_service_name=args.k8s_coordinator_service_name,
            delete_namespace=args.k8s_delete_namespace,
            engine_cpu=args.k8s_engine_cpu,
            engine_mem=args.k8s_engine_mem,
            engine_pod_node_selector=args.k8s_engine_pod_node_selector,
            image_pull_policy=args.k8s_image_pull_policy,
            image_pull_secrets=args.k8s_image_pull_secrets,
            image_registry=args.k8s_image_registry,
            image_repository=args.k8s_image_repository,
            image_tag=args.k8s_image_tag,
            instance_id=args.instance_id,
            log_level=args.log_level,
            mars_worker_cpu=args.k8s_mars_worker_cpu,
            mars_worker_mem=args.k8s_mars_worker_mem,
            mars_scheduler_cpu=args.k8s_mars_scheduler_cpu,
            mars_scheduler_mem=args.k8s_mars_scheduler_mem,
            with_dataset=args.k8s_with_dataset,
            namespace=args.k8s_namespace,
            num_workers=args.num_workers,
            preemptive=args.preemptive,
            service_type=args.k8s_service_type,
            timeout_seconds=args.timeout_seconds,
            vineyard_cpu=args.k8s_vineyard_cpu,
            vineyard_daemonset=args.k8s_vineyard_daemonset,
            vineyard_image=args.k8s_vineyard_image,
            vineyard_mem=args.k8s_vineyard_mem,
            vineyard_shared_mem=args.vineyard_shared_mem,
            volumes=args.k8s_volumes,
            waiting_for_delete=args.waiting_for_delete,
            with_mars=args.k8s_with_mars,
            with_analytical=args.k8s_with_analytical,
            with_analytical_java=args.k8s_with_analytical_java,
            with_interactive=args.k8s_with_interactive,
            with_learning=args.k8s_with_learning,
        )
    elif args.cluster_type == "hosts":
        launcher = LocalLauncher(
            num_workers=args.num_workers,
            hosts=args.hosts,
            etcd_addrs=args.etcd_addrs,
            etcd_listening_client_port=args.etcd_listening_client_port,
            etcd_listening_peer_port=args.etcd_listening_peer_port,
            vineyard_socket=args.vineyard_socket,
            shared_mem=args.vineyard_shared_mem,
            log_level=args.log_level,
            instance_id=args.instance_id,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        raise RuntimeError("Expect hosts or k8s of cluster_type parameter")
    return launcher


def start_server(launcher, args):
    coordinator_service_servicer = CoordinatorServiceServicer(
        launcher=launcher,
        dangling_timeout_seconds=args.dangling_timeout_seconds,
        log_level=args.log_level,
    )

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
    server.add_insecure_port(f"0.0.0.0:{args.port}")

    logger.info("Start server with args %s", args)

    logger.info("Coordinator server listen at 0.0.0.0:%d", args.port)

    server.start()

    if args.monitor:
        try:
            Monitor.startServer(args.monitor_port, "0.0.0.0")
            logger.info(
                "Coordinator monitor server listen at 0.0.0.0:%d", args.monitor_port
            )
        except Exception as e:
            logger.error(
                "Failed to start monitor server 0.0.0.0:%d : %s", args.monitor_port, e
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


if __name__ == "__main__":
    launch_graphscope()
