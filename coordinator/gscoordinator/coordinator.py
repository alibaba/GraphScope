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
import datetime
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
import urllib.parse
import urllib.request
import zipfile
from concurrent import futures
from io import BytesIO

import grpc
from packaging import version

from gscoordinator.io_utils import StdStreamWrapper

# capture system stdout
sys.stdout = StdStreamWrapper(sys.stdout)
sys.stderr = StdStreamWrapper(sys.stderr)

from graphscope.client.utils import GRPCUtils
from graphscope.framework import utils
from graphscope.framework.dag_utils import create_graph
from graphscope.framework.dag_utils import create_loader
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.graph_utils import normalize_parameter_edges
from graphscope.framework.graph_utils import normalize_parameter_vertices
from graphscope.framework.loader import Loader
from graphscope.framework.utils import PipeMerger
from graphscope.framework.utils import get_tempdir
from graphscope.framework.utils import normalize_data_type_str
from graphscope.proto import attr_value_pb2
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import engine_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

from gscoordinator.cluster import KubernetesClusterLauncher
from gscoordinator.dag_manager import DAGManager
from gscoordinator.dag_manager import GSEngine
from gscoordinator.dag_manager import split_op_result
from gscoordinator.launcher import LocalLauncher
from gscoordinator.object_manager import GraphMeta
from gscoordinator.object_manager import GremlinResultSet
from gscoordinator.object_manager import InteractiveQueryManager
from gscoordinator.object_manager import LearningInstanceManager
from gscoordinator.object_manager import LibMeta
from gscoordinator.object_manager import ObjectManager
from gscoordinator.utils import ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH
from gscoordinator.utils import ANALYTICAL_ENGINE_JAVA_JVM_OPTS
from gscoordinator.utils import GRAPHSCOPE_HOME
from gscoordinator.utils import RESOURCE_DIR_NAME
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import check_gremlin_server_ready
from gscoordinator.utils import compile_app
from gscoordinator.utils import compile_graph_frame
from gscoordinator.utils import create_single_op_dag
from gscoordinator.utils import dump_string
from gscoordinator.utils import get_app_sha256
from gscoordinator.utils import get_graph_sha256
from gscoordinator.utils import get_lib_path
from gscoordinator.utils import op_pre_process
from gscoordinator.utils import str2bool
from gscoordinator.utils import to_maxgraph_schema
from gscoordinator.version import __version__

# endpoint of prelaunch analytical engine
GS_DEBUG_ENDPOINT = os.environ.get("GS_DEBUG_ENDPOINT", "")

# 2 GB
GS_GRPC_MAX_MESSAGE_LENGTH = 2 * 1024 * 1024 * 1024 - 1

logger = logging.getLogger("graphscope")


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
                context.set_code(error_codes_pb2.COORDINATOR_INTERNAL_ERROR)
                context.set_details(
                    'Error occurs in handler: "%s", with traceback: ' % error_message
                    + error_traceback
                )
                if response_on_error is not None:
                    yield response_on_error

        return handler_execution

    return catch_exceptions


class CoordinatorServiceServicer(
    coordinator_service_pb2_grpc.CoordinatorServiceServicer
):
    """Provides methods that implement functionality of master service server.
    Holding:
        1. process: the grape-engine process.
        2. session_id: the handle for a particular session to engine
        3. vineyard_ipc_socket: returned by grape-engine
        4. vineyard_rpc_socket: returned by grape-engine
        5. engine_endpoint: the endpoint of grape-engine
        6. engine_servicer: grpc connection to grape-engine

    """

    def __init__(self, launcher, dangling_timeout_seconds, log_level="INFO"):
        self._launcher = launcher

        self._request = None
        self._object_manager = ObjectManager()
        self._grpc_utils = GRPCUtils()
        self._dangling_detecting_timer = None
        self._config_logging(log_level)

        # only one connection is allowed at the same time
        # generate session id  when a client connection is established
        self._session_id = None

        # launch engines
        if len(GS_DEBUG_ENDPOINT) > 0:
            logger.info(
                "Coordinator will connect to engine with endpoint: " + GS_DEBUG_ENDPOINT
            )
            self._launcher._analytical_engine_endpoint = GS_DEBUG_ENDPOINT
        else:
            if not self._launcher.start():
                raise RuntimeError("Coordinator Launching failed.")

        self._launcher_type = self._launcher.type()
        self._instance_id = self._launcher.instance_id
        # string of a list of hosts, comma separated
        self._engine_hosts = self._launcher.hosts
        self._k8s_namespace = ""
        if self._launcher_type == types_pb2.K8S:
            self._k8s_namespace = self._launcher.get_namespace()

        # analytical engine
        self._analytical_engine_stub = self._create_grpc_stub()
        self._analytical_engine_config = None
        self._analytical_engine_endpoint = None

        self._builtin_workspace = os.path.join(WORKSPACE, "builtin")
        # udf app workspace should be bound to a specific session when client connect.
        self._udf_app_workspace = None
        # java class path should contains
        # 1) java runtime path
        # 2) add resources, the recents added resource will be placed first.
        self._java_class_path = ANALYTICAL_ENGINE_JAVA_INIT_CLASS_PATH
        logger.info("Java initial class path set to: {}".format(self._java_class_path))
        self._jvm_opts = ANALYTICAL_ENGINE_JAVA_JVM_OPTS

        # control log fetching
        self._streaming_logs = True
        self._pipe_merged = PipeMerger(sys.stdout, sys.stderr)

        # dangling check
        self._dangling_timeout_seconds = dangling_timeout_seconds
        if self._dangling_timeout_seconds >= 0:
            self._dangling_detecting_timer = threading.Timer(
                interval=self._dangling_timeout_seconds,
                function=self._cleanup,
                args=(
                    True,
                    True,
                ),
            )
            self._dangling_detecting_timer.start()

        # a lock that protects the coordinator
        self._lock = threading.Lock()

        atexit.register(self._cleanup)

    def __del__(self):
        self._cleanup()

    def _generate_session_id(self):
        return "session_" + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(8)]
        )

    def _config_logging(self, log_level):
        """Set log level basic on config.
        Args:
            log_level (str): Log level of stdout handler
        """
        logging.basicConfig(level=logging.CRITICAL)

        if log_level:
            log_level = log_level.upper()

        logger = logging.getLogger("graphscope")
        logger.setLevel(log_level)

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

    def ConnectSession(self, request, context):
        for result in self.ConnectSessionWrapped(request, context):
            return result

    def _ConnectSession(self, request, context):
        # A session is already connected.
        if self._request:
            if getattr(request, "reconnect", False):
                return message_pb2.ConnectSessionResponse(
                    session_id=self._session_id,
                    cluster_type=self._launcher.type(),
                    num_workers=self._launcher.num_workers,
                    engine_config=json.dumps(self._analytical_engine_config),
                    pod_name_list=self._engine_hosts.split(","),
                    namespace=self._k8s_namespace,
                )
            # connect failed, more than one connection at the same time.
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(
                "Cannot setup more than one connection at the same time."
            )
            return message_pb2.ConnectSessionResponse()
        # Connect to serving coordinator.
        self._key_to_op = {}
        # dict of op_def_pb2.OpResult
        self._op_result_pool = {}
        self._request = request
        try:
            self._analytical_engine_config = self._get_engine_config()
        except grpc.RpcError as e:
            logger.error(
                "Get engine config failed, code: %s, details: %s",
                e.code().name,
                e.details(),
            )
            context.set_code(e.code())
            context.set_details(e.details())
            return message_pb2.ConnectSessionResponse()
        # Generate session id
        self._session_id = self._generate_session_id()
        self._udf_app_workspace = os.path.join(
            WORKSPACE, self._instance_id, self._session_id
        )
        self._resource_dir = os.path.join(
            WORKSPACE, self._instance_id, self._session_id, RESOURCE_DIR_NAME
        )
        self._launcher.set_session_workspace(self._session_id)

        # Session connected, fetch logs via gRPC.
        self._streaming_logs = True
        sys.stdout.drop(False)

        # check version compatibility from client
        sv = version.parse(__version__)
        cv = version.parse(self._request.version)
        if sv.major != cv.major or sv.minor != cv.minor:
            error_msg = f"Version between client and server is inconsistent: {self._request.version} vs {__version__}"
            logger.warning(error_msg)
            context.set_code(error_codes_pb2.CONNECTION_ERROR)
            context.set_details(error_msg)
            return message_pb2.ConnectSessionResponse()

        return message_pb2.ConnectSessionResponse(
            session_id=self._session_id,
            cluster_type=self._launcher.type(),
            num_workers=self._launcher.num_workers,
            engine_config=json.dumps(self._analytical_engine_config),
            pod_name_list=self._engine_hosts.split(","),
            namespace=self._k8s_namespace,
        )

    ConnectSessionWrapped = catch_unknown_errors(message_pb2.ConnectSessionResponse())(
        _ConnectSession
    )

    def HeartBeat(self, request, context):
        for result in self.HeartBeatWrapped(request, context):
            return result

    def _HeartBeat(self, request, context):
        if self._request and self._request.dangling_timeout_seconds >= 0:
            # Reset dangling detect timer
            if self._dangling_detecting_timer:
                self._dangling_detecting_timer.cancel()

            self._dangling_detecting_timer = threading.Timer(
                interval=self._request.dangling_timeout_seconds,
                function=self._cleanup,
                args=(
                    self._request.cleanup_instance,
                    True,
                ),
            )
            self._dangling_detecting_timer.start()

        # analytical engine
        request = message_pb2.HeartBeatRequest()
        return self._analytical_engine_stub.HeartBeat(request)

    HeartBeatWrapped = catch_unknown_errors(message_pb2.HeartBeatResponse())(_HeartBeat)

    def run_on_analytical_engine(  # noqa: C901
        self,
        dag_def: op_def_pb2.DagDef,
        dag_bodies,
        loader_op_bodies: dict,
    ):
        def _generate_runstep_request(session_id, dag_def, dag_bodies):
            runstep_requests = []
            # head
            runstep_requests.append(
                message_pb2.RunStepRequest(
                    head=message_pb2.RunStepRequestHead(
                        session_id=session_id, dag_def=dag_def
                    )
                )
            )
            runstep_requests.extend(dag_bodies)
            for item in runstep_requests:
                yield item

        # preprocess of op before run on analytical engine
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(
                op,
                self._op_result_pool,
                self._key_to_op,
                engine_hosts=self._engine_hosts,
                engine_config=self._analytical_engine_config,
                engine_java_class_path=self._java_class_path,  # may be needed in CREATE_GRAPH or RUN_APP
                engine_jvm_opts=self._jvm_opts,
            )

            # Handle op that depends on loader (data source)
            if op.op == types_pb2.CREATE_GRAPH or op.op == types_pb2.ADD_LABELS:
                for key_of_parent_op in op.parents:
                    parent_op = self._key_to_op[key_of_parent_op]
                    if parent_op.op == types_pb2.DATA_SOURCE:
                        # handle bodies of loader op
                        if parent_op.key in loader_op_bodies:
                            dag_bodies.extend(loader_op_bodies[parent_op.key])

            # Compile app or not.
            if op.op == types_pb2.BIND_APP:
                op, app_sig, app_lib_path = self._maybe_compile_app(op)

            # Compile graph or not
            # arrow property graph and project graph need to compile
            # If engine crashed, we will get a SocketClosed grpc Exception.
            # In that case, we should notify client the engine is dead.
            if (
                (
                    op.op == types_pb2.CREATE_GRAPH
                    and op.attr[types_pb2.GRAPH_TYPE].i == graph_def_pb2.ARROW_PROPERTY
                )
                or op.op == types_pb2.TRANSFORM_GRAPH
                or op.op == types_pb2.PROJECT_TO_SIMPLE
                or op.op == types_pb2.ADD_LABELS
            ):
                op = self._maybe_register_graph(op, self._session_id)
        # generate runstep requests, and run on analytical engine
        requests = _generate_runstep_request(self._session_id, dag_def, dag_bodies)
        # response
        response_head = None
        response_bodies = []
        try:
            responses = self._analytical_engine_stub.RunStep(requests)
            for response in responses:
                if response.HasField("head"):
                    response_head = response
                else:
                    response_bodies.append(response)
        except grpc.RpcError as e:
            logger.error(
                "Engine RunStep failed, code: %s, details: %s",
                e.code().name,
                e.details(),
            )
            if e.code() == grpc.StatusCode.INTERNAL:
                # TODO: make the stacktrace seperated from normal error messages
                # Too verbose.
                if len(e.details()) > 3072:  # 3k bytes
                    msg = f"{e.details()[:3072]} ... [truncated]"
                else:
                    msg = e.details()
                raise AnalyticalEngineInternalError(msg)
            else:
                raise

        # handle result from response stream
        if response_head is None:
            raise AnalyticalEngineInternalError(
                "Missing head from the response stream."
            )
        for op_result in response_head.head.results:
            # record result in coordinator, which doesn't contains large data
            self._op_result_pool[op_result.key] = op_result
            # get the op corresponding to the result
            op = self._key_to_op[op_result.key]
            # register graph and dump graph schema
            if op.op in (
                types_pb2.CREATE_GRAPH,
                types_pb2.PROJECT_GRAPH,
                types_pb2.ADD_LABELS,
                types_pb2.ADD_COLUMN,
            ):
                schema_path = os.path.join(
                    get_tempdir(), op_result.graph_def.key + ".json"
                )
                vy_info = graph_def_pb2.VineyardInfoPb()
                op_result.graph_def.extension.Unpack(vy_info)
                self._object_manager.put(
                    op_result.graph_def.key,
                    GraphMeta(
                        op_result.graph_def.key,
                        vy_info.vineyard_id,
                        op_result.graph_def,
                        schema_path,
                    ),
                )
                if op_result.graph_def.graph_type == graph_def_pb2.ARROW_PROPERTY:
                    dump_string(
                        to_maxgraph_schema(vy_info.property_schema_json),
                        schema_path,
                    )
                    vy_info.schema_path = schema_path
                    op_result.graph_def.extension.Pack(vy_info)
            # register app
            elif op.op == types_pb2.BIND_APP:
                self._object_manager.put(
                    app_sig,
                    LibMeta(op_result.result.decode("utf-8"), "app", app_lib_path),
                )
            # unregister graph
            elif op.op == types_pb2.UNLOAD_GRAPH:
                self._object_manager.pop(op.attr[types_pb2.GRAPH_NAME].s.decode())
            # unregister app
            elif op.op == types_pb2.UNLOAD_APP:
                self._object_manager.pop(op.attr[types_pb2.APP_NAME].s.decode())
        return response_head, response_bodies

    def run_on_interactive_engine(self, dag_def: op_def_pb2.DagDef):
        response_head = message_pb2.RunStepResponse(
            head=message_pb2.RunStepResponseHead()
        )
        response_bodies = []
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(
                op,
                self._op_result_pool,
                self._key_to_op,
                engine_hosts=self._engine_hosts,
                engine_config=self._analytical_engine_config,
            )
            if op.op == types_pb2.CREATE_INTERACTIVE_QUERY:
                op_result = self._create_interactive_instance(op)
            elif op.op == types_pb2.GREMLIN_QUERY:
                op_result = self._execute_gremlin_query(op)
            elif op.op == types_pb2.FETCH_GREMLIN_RESULT:
                op_result = self._fetch_gremlin_result(op)
            elif op.op == types_pb2.CLOSE_INTERACTIVE_QUERY:
                op_result = self._close_interactive_instance(op)
            elif op.op == types_pb2.SUBGRAPH:
                op_result = self._gremlin_to_subgraph(op)
            else:
                raise RuntimeError("Unsupport op type: " + str(op.op))
            splited_result = split_op_result(op_result)
            response_head.head.results.append(op_result)
            for i, chunk in enumerate(splited_result):
                has_next = True
                if i + 1 == len(splited_result):
                    has_next = False
                response_bodies.append(
                    message_pb2.RunStepResponse(
                        body=message_pb2.RunStepResponseBody(
                            chunk=chunk, has_next=has_next
                        )
                    )
                )
            # record op result
            self._op_result_pool[op.key] = op_result
        return response_head, response_bodies

    def run_on_learning_engine(self, dag_def: op_def_pb2.DagDef):
        response_head = message_pb2.RunStepResponse(
            head=message_pb2.RunStepResponseHead()
        )
        response_bodies = []
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(
                op,
                self._op_result_pool,
                self._key_to_op,
                engine_hosts=self._engine_hosts,
                engine_config=self._analytical_engine_config,
            )
            if op.op == types_pb2.CREATE_LEARNING_INSTANCE:
                op_result = self._create_learning_instance(op)
            elif op.op == types_pb2.CLOSE_LEARNING_INSTANCE:
                op_result = self._close_learning_instance(op)
            else:
                raise RuntimeError("Unsupport op type: " + str(op.op))
            response_head.head.results.append(op_result)
            self._op_result_pool[op.key] = op_result
        return response_head, response_bodies

    def run_on_coordinator(
        self,
        dag_def: op_def_pb2.DagDef,
        dag_bodies,
        loader_op_bodies: dict,
    ):
        response_head = message_pb2.RunStepResponse(
            head=message_pb2.RunStepResponseHead()
        )
        response_bodies = []
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            op_pre_process(
                op,
                self._op_result_pool,
                self._key_to_op,
                engine_hosts=self._engine_hosts,
                engine_config=self._analytical_engine_config,
            )
            if op.op == types_pb2.DATA_SOURCE:
                op_result = self._process_data_source(op, dag_bodies, loader_op_bodies)
            elif op.op == types_pb2.DATA_SINK:
                op_result = self._process_data_sink(op)
            else:
                raise RuntimeError("Unsupport op type: " + str(op.op))
            response_head.head.results.append(op_result)
            self._op_result_pool[op.key] = op_result
        return response_head, response_bodies

    def RunStep(self, request_iterator, context):
        with self._lock:
            for response in self.RunStepWrapped(request_iterator, context):
                yield response

    def _RunStep(self, request_iterator, context):
        # split dag
        dag_manager = DAGManager(request_iterator)
        loader_op_bodies = {}

        # response list for stream
        responses = []
        # head
        responses.append(
            message_pb2.RunStepResponse(head=message_pb2.RunStepResponseHead())
        )

        while not dag_manager.empty():
            run_dag_on, dag, dag_bodies = dag_manager.next_dag()
            error_code = error_codes_pb2.COORDINATOR_INTERNAL_ERROR
            head = None
            bodies = None
            try:
                # run on analytical engine
                if run_dag_on == GSEngine.analytical_engine:
                    # need dag_bodies to load graph from pandas/numpy
                    error_code = error_codes_pb2.ANALYTICAL_ENGINE_INTERNAL_ERROR
                    head, bodies = self.run_on_analytical_engine(
                        dag, dag_bodies, loader_op_bodies
                    )
                # run on interactive engine
                elif run_dag_on == GSEngine.interactive_engine:
                    error_code = error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR
                    head, bodies = self.run_on_interactive_engine(dag)
                # run on learning engine
                elif run_dag_on == GSEngine.learning_engine:
                    error_code = error_codes_pb2.LEARNING_ENGINE_INTERNAL_ERROR
                    head, bodies = self.run_on_learning_engine(dag)
                # run on coordinator
                elif run_dag_on == GSEngine.coordinator:
                    error_code = error_codes_pb2.COORDINATOR_INTERNAL_ERROR
                    head, bodies = self.run_on_coordinator(
                        dag, dag_bodies, loader_op_bodies
                    )
                # merge the responses
                responses[0].head.results.extend(head.head.results)
                responses.extend(bodies)
            except grpc.RpcError as exc:
                # Not raised by graphscope, maybe socket closed, etc
                context.set_code(exc.code())
                context.set_details(exc.details())
                for response in responses:
                    yield response
            except Exception as exc:
                response_head = responses[0]
                response_head.head.code = error_code
                response_head.head.error_msg = (
                    "Error occurred during preprocessing, The traceback is: {0}".format(
                        traceback.format_exc()
                    )
                )
                response_head.head.full_exception = pickle.dumps(exc)
                for response in responses:
                    yield response

        for response in responses:
            yield response

    RunStepWrapped = catch_unknown_errors(
        message_pb2.RunStepResponse(head=message_pb2.RunStepResponseHead()), True
    )(_RunStep)

    def _maybe_compile_app(self, op):
        app_sig = get_app_sha256(op.attr, self._java_class_path)
        # try to get compiled file from GRAPHSCOPE_HOME/precompiled
        space = os.path.join(GRAPHSCOPE_HOME, "precompiled", "builtin")
        app_lib_path = get_lib_path(os.path.join(space, app_sig), app_sig)
        if not os.path.isfile(app_lib_path):
            space = self._builtin_workspace
            if (types_pb2.GAR in op.attr) or (
                op.attr[types_pb2.APP_ALGO].s.decode("utf-8").startswith("giraph:")
            ):
                space = self._udf_app_workspace
            # try to get compiled file from workspace
            app_lib_path = get_lib_path(os.path.join(space, app_sig), app_sig)
            if not os.path.isfile(app_lib_path):
                # compile and distribute
                compiled_path = self._compile_lib_and_distribute(
                    compile_app, app_sig, op
                )
                if app_lib_path != compiled_path:
                    raise RuntimeError(
                        f"Computed application library path not equal to compiled path, {app_lib_path} versus {compiled_path}"
                    )
        op.attr[types_pb2.APP_LIBRARY_PATH].CopyFrom(
            attr_value_pb2.AttrValue(s=app_lib_path.encode("utf-8"))
        )
        return op, app_sig, app_lib_path

    def _maybe_register_graph(self, op, session_id):
        graph_sig = get_graph_sha256(op.attr)
        # try to get compiled file from GRAPHSCOPE_HOME/precompiled
        space = os.path.join(GRAPHSCOPE_HOME, "precompiled", "builtin")
        graph_lib_path = get_lib_path(os.path.join(space, graph_sig), graph_sig)
        if not os.path.isfile(graph_lib_path):
            space = self._builtin_workspace
            # try to get compiled file from workspace
            graph_lib_path = get_lib_path(os.path.join(space, graph_sig), graph_sig)
            if not os.path.isfile(graph_lib_path):
                # compile and distribute
                compiled_path = self._compile_lib_and_distribute(
                    compile_graph_frame, graph_sig, op
                )
                if graph_lib_path != compiled_path:
                    raise RuntimeError(
                        f"Computed graph library path not equal to compiled path, {graph_lib_path} versus {compiled_path}"
                    )
        if graph_sig not in self._object_manager:
            # register graph
            op_def = op_def_pb2.OpDef(op=types_pb2.REGISTER_GRAPH_TYPE)
            op_def.attr[types_pb2.GRAPH_LIBRARY_PATH].CopyFrom(
                attr_value_pb2.AttrValue(s=graph_lib_path.encode("utf-8"))
            )
            op_def.attr[types_pb2.TYPE_SIGNATURE].CopyFrom(
                attr_value_pb2.AttrValue(s=graph_sig.encode("utf-8"))
            )
            op_def.attr[types_pb2.GRAPH_TYPE].CopyFrom(
                attr_value_pb2.AttrValue(i=op.attr[types_pb2.GRAPH_TYPE].i)
            )
            dag_def = op_def_pb2.DagDef()
            dag_def.op.extend([op_def])
            try:
                response_head, _ = self.run_on_analytical_engine(dag_def, [], {})
            except grpc.RpcError as e:
                logger.error(
                    "Register graph failed, code: %s, details: %s",
                    e.code().name,
                    e.details(),
                )
                if e.code() == grpc.StatusCode.INTERNAL:
                    raise AnalyticalEngineInternalError(e.details())
                else:
                    raise
            self._object_manager.put(
                graph_sig,
                LibMeta(
                    response_head.head.results[0].result,
                    "graph_frame",
                    graph_lib_path,
                ),
            )
        op.attr[types_pb2.TYPE_SIGNATURE].CopyFrom(
            attr_value_pb2.AttrValue(s=graph_sig.encode("utf-8"))
        )
        return op

    def FetchLogs(self, request, context):
        while self._streaming_logs:
            try:
                info_message, error_message = self._pipe_merged.poll(timeout=2)
            except queue.Empty:
                info_message, error_message = "", ""
            except Exception as e:
                info_message, error_message = "WARNING: failed to read log: %s" % e, ""

            if info_message or error_message:
                if self._streaming_logs:
                    yield message_pb2.FetchLogsResponse(
                        info_message=info_message, error_message=error_message
                    )

    def AddLib(self, request, context):
        for result in self.AddLibWrapped(request, context):
            return result

    def _AddLib(self, request, context):
        if request.session_id != self._session_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Session handle not matched, {request.session_id} versus {self._session_id}"
            )
        os.makedirs(self._resource_dir, exist_ok=True)
        gar = request.gar
        fp = BytesIO(gar)
        filename = None
        with zipfile.ZipFile(fp, "r") as zip_ref:
            zip_ref.extractall(self._resource_dir)
            logger.info(
                "Coordinator recieved add lib request contains file {}".format(
                    zip_ref.namelist()
                )
            )
            if len(zip_ref.namelist()) != 1:
                raise RuntimeError("Expect only one resource in one gar")
            filename = zip_ref.namelist()[0]
        full_filename = os.path.join(self._resource_dir, filename)
        self._launcher.distribute_file(full_filename)
        logger.info("Successfully distributed {}".format(full_filename))
        if full_filename.endswith(".jar"):
            logger.info("adding lib to java class path since it ends with .jar")
            self._java_class_path = full_filename + ":" + self._java_class_path
            logger.info("current java class path: {}".format(self._java_class_path))
        return message_pb2.AddLibResponse()

    AddLibWrapped = catch_unknown_errors(message_pb2.AddLibResponse())(_AddLib)

    def CloseSession(self, request, context):
        for result in self.CloseSessionWrapped(request, context):
            return result

    def _CloseSession(self, request, context):
        """
        Disconnect session, note that it doesn't clean up any resources.
        """
        if request.session_id != self._session_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Session handle not matched, {request.session_id} versus {self._session_id}"
            )

        self._cleanup(
            cleanup_instance=self._request.cleanup_instance, is_dangling=False
        )
        self._request = None

        # Session closed, stop streaming logs
        sys.stdout.drop(True)
        self._streaming_logs = False
        return message_pb2.CloseSessionResponse()

    CloseSessionWrapped = catch_unknown_errors(message_pb2.CloseSessionResponse())(
        _CloseSession
    )

    def _create_interactive_instance(self, op: op_def_pb2.OpDef):
        def _match_frontend_endpoint(pattern, lines):
            for line in lines.split("\n"):
                rlt = re.findall(pattern, line)
                if rlt:
                    return rlt[0].strip()
            return ""

        # vineyard object id of graph
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        # maxgraph endpoint pattern
        MAXGRAPH_FRONTEND_PATTERN = re.compile("(?<=MAXGRAPH_FRONTEND_ENDPOINT:).*$")
        MAXGRAPH_FRONTEND_EXTERNAL_PATTERN = re.compile(
            "(?<=MAXGRAPH_FRONTEND_EXTERNAL_ENDPOINT:).*$"
        )
        # maxgraph endpoint
        maxgraph_endpoint = None
        # maxgraph external endpoint, for client and gremlin function test
        maxgraph_external_endpoint = None
        # create instance
        proc = self._launcher.create_interactive_instance(op.attr)
        try:
            # 60 seconds is enough, see also GH#1024; try 120
            # already add errs to outs
            outs, _ = proc.communicate(timeout=120)
            return_code = proc.poll()
            if return_code == 0:
                # match maxgraph endpoint and check for ready
                maxgraph_endpoint = _match_frontend_endpoint(
                    MAXGRAPH_FRONTEND_PATTERN, outs
                )
                if check_gremlin_server_ready(maxgraph_endpoint):
                    logger.info(
                        "build maxgraph frontend %s for graph %ld",
                        maxgraph_endpoint,
                        object_id,
                    )
                maxgraph_external_endpoint = _match_frontend_endpoint(
                    MAXGRAPH_FRONTEND_EXTERNAL_PATTERN, outs
                )

                self._object_manager.put(
                    op.key,
                    InteractiveQueryManager(op.key, maxgraph_endpoint, object_id),
                )
                endpoint = maxgraph_external_endpoint or maxgraph_endpoint
                result = {"endpoint": endpoint, "object_id": object_id}
                return op_def_pb2.OpResult(
                    code=error_codes_pb2.OK,
                    key=op.key,
                    result=json.dumps(result).encode("utf-8"),
                )
            raise RuntimeError("Error code: {0}, message {1}".format(return_code, outs))
        except Exception as e:
            proc.kill()
            self._launcher.close_interactive_instance(object_id)
            raise RuntimeError("Create interactive instance failed.") from e

    def _execute_gremlin_query(self, op: op_def_pb2.OpDef):
        message = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        key_of_parent_op = op.parents[0]

        gremlin_client = self._object_manager.get(key_of_parent_op)
        try:
            rlt = gremlin_client.submit(message, request_options=request_options)
        except Exception as e:
            raise RuntimeError("Gremlin query failed.") from e
        self._object_manager.put(op.key, GremlinResultSet(op.key, rlt))
        return op_def_pb2.OpResult(code=error_codes_pb2.OK, key=op.key)

    def _fetch_gremlin_result(self, op: op_def_pb2.OpDef):
        fetch_result_type = op.attr[types_pb2.GIE_GREMLIN_FETCH_RESULT_TYPE].s.decode()
        key_of_parent_op = op.parents[0]
        result_set = self._object_manager.get(key_of_parent_op).result_set
        try:
            if fetch_result_type == "one":
                rlt = result_set.one()
            elif fetch_result_type == "all":
                rlt = result_set.all().result()
        except Exception as e:
            raise RuntimeError("Fetch gremlin result failed") from e
        meta = op_def_pb2.OpResult.Meta(has_large_result=True)
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
            meta=meta,
            result=pickle.dumps(rlt),
        )

    def _process_data_sink(self, op: op_def_pb2.OpDef):
        import vineyard
        import vineyard.io

        storage_options = json.loads(op.attr[types_pb2.STORAGE_OPTIONS].s.decode())
        fd = op.attr[types_pb2.FD].s.decode()
        df = op.attr[types_pb2.VINEYARD_ID].s.decode()
        engine_config = self._analytical_engine_config
        vineyard_endpoint = engine_config["vineyard_rpc_endpoint"]
        vineyard_ipc_socket = engine_config["vineyard_socket"]
        deployment, hosts = self._launcher.get_vineyard_stream_info()
        dfstream = vineyard.io.open(
            "vineyard://" + str(df),
            mode="r",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            deployment=deployment,
            hosts=hosts,
        )
        vineyard.io.open(
            fd,
            dfstream,
            mode="w",
            vineyard_ipc_socket=vineyard_ipc_socket,
            vineyard_endpoint=vineyard_endpoint,
            storage_options=storage_options,
            deployment=deployment,
            hosts=hosts,
        )
        return op_def_pb2.OpResult(code=error_codes_pb2.OK, key=op.key)

    def _process_data_source(
        self, op: op_def_pb2.OpDef, dag_bodies, loader_op_bodies: dict
    ):
        def _spawn_vineyard_io_stream(source, storage_options, read_options):
            import vineyard
            import vineyard.io

            engine_config = self._analytical_engine_config
            vineyard_endpoint = engine_config["vineyard_rpc_endpoint"]
            vineyard_ipc_socket = engine_config["vineyard_socket"]
            deployment, hosts = self._launcher.get_vineyard_stream_info()
            num_workers = self._launcher.num_workers
            stream_id = repr(
                vineyard.io.open(
                    source,
                    mode="r",
                    vineyard_endpoint=vineyard_endpoint,
                    vineyard_ipc_socket=vineyard_ipc_socket,
                    hosts=hosts,
                    num_workers=num_workers,
                    deployment=deployment,
                    read_options=read_options,
                    storage_options=storage_options,
                )
            )
            return "vineyard", stream_id

        def _process_loader_func(loader):
            # loader is type of attr_value_pb2.Chunk
            protocol = loader.attr[types_pb2.PROTOCOL].s.decode()
            if protocol in ("hdfs", "hive", "oss", "s3"):
                source = loader.attr[types_pb2.SOURCE].s.decode()
                storage_options = json.loads(
                    loader.attr[types_pb2.STORAGE_OPTIONS].s.decode()
                )
                read_options = json.loads(
                    loader.attr[types_pb2.READ_OPTIONS].s.decode()
                )
                new_protocol, new_source = _spawn_vineyard_io_stream(
                    source, storage_options, read_options
                )
                loader.attr[types_pb2.PROTOCOL].CopyFrom(utils.s_to_attr(new_protocol))
                loader.attr[types_pb2.SOURCE].CopyFrom(utils.s_to_attr(new_source))

        for loader in op.large_attr.chunk_meta_list.items:
            # handle vertex or edge loader
            if loader.attr[types_pb2.CHUNK_TYPE].s.decode() == "loader":
                # set op bodies, this is for loading graph from numpy/pandas
                op_bodies = []
                for bodies in dag_bodies:
                    if bodies.body.op_key == op.key:
                        op_bodies.append(bodies)
                loader_op_bodies[op.key] = op_bodies
                _process_loader_func(loader)

        return op_def_pb2.OpResult(code=error_codes_pb2.OK, key=op.key)

    def _close_interactive_instance(self, op: op_def_pb2.OpDef):
        try:
            key_of_parent_op = op.parents[0]
            gremlin_client = self._object_manager.get(key_of_parent_op)
            object_id = gremlin_client.object_id
            proc = self._launcher.close_interactive_instance(object_id)
            # 60s is enough
            proc.wait(timeout=60)
            gremlin_client.close()
        except Exception as e:
            raise RuntimeError(
                f"Failed to close interactive instance {object_id}"
            ) from e
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
        )

    def _gremlin_to_subgraph_v2(self, op: op_def_pb2.OpDef):
        gremlin_script = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        oid_type = op.attr[types_pb2.OID_TYPE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        key_of_parent_op = op.parents[0]
        gremlin_client = self._object_manager.get(key_of_parent_op)

        def create_global_graph_builder(graph_name, num_workers):
            import vineyard

            vineyard_client = vineyard.connect(
                self._analytical_engine_config["vineyard_rpc_endpoint"]
            )

            # build the vineyard::GlobalPGStream
            metadata = vineyard.ObjectMeta()
            metadata.set_global(True)
            metadata["typename"] = "vineyard::GlobalPGStream"
            metadata["total_stream_chunks"] = num_workers

            # build the parallel stream for edge
            edge_metadata = vineyard.ObjectMeta()
            edge_metadata.set_global(True)
            edge_metadata["typename"] = "vineyard::ParallelStream"
            edge_metadata["__streams_-size"] = num_workers

            # build the parallel stream for vertex
            vertex_metadata = vineyard.ObjectMeta()
            vertex_metadata.set_global(True)
            vertex_metadata["typename"] = "vineyard::ParallelStream"
            vertex_metadata["__streams_-size"] = num_workers

            for worker in range(num_workers):
                edge_metadata = vineyard.ObjectMeta()
                edge_metadata["typename"] = "vineyard::RecordBatchStream"
                edge_metadata["nbytes"] = 0
                edge_metadata["params_"] = json.dumps(
                    {
                        "graph_name": graph_name,
                        "kind": "edge",
                    }
                )
                edge = vineyard_client.create_metadata(edge_metadata)
                vineyard_client.persist(edge.id)
                edge_metadata.add_member("__streams_-%d" % worker, edge)

                vertex_metadata = vineyard.ObjectMeta()
                vertex_metadata["typename"] = "vineyard::RecordBatchStream"
                vertex_metadata["nbytes"] = 0
                vertex_metadata["params_"] = json.dumps(
                    {
                        "graph_name": graph_name,
                        "kind": "vertex",
                    }
                )
                vertex = vineyard_client.create_metadata(vertex_metadata)
                vineyard_client.persist(vertex.id)
                edge_metadata.add_member("__streams_-%d" % worker, vertex)

                metadata = vineyard.ObjectMeta()
                metadata["typename"] = "vineyard::PropertyGraphOutStream"
                metadata["graph_name"] = graph_name
                metadata["graph_schema"] = "{}"
                metadata["nbytes"] = 0
                metadata["stream_index"] = worker
                metadata["edge_stream"] = edge
                metadata["vertex_stream"] = vertex
                chunk = vineyard_client.create_metadata(metadata)
                vineyard_client.persist(chunk.id)
                metadata.add_member("stream_chunk_%d" % worker, chunk)

            # build the vineyard::GlobalPGStream
            graph = vineyard_client.create_metadata(metadata)
            vineyard_client.persist(graph.id)
            vineyard_client.put_name(graph.id, graph_name)

            # build the parallel stream for edge
            edge = vineyard_client.create_metadata(metadata)
            vineyard_client.persist(edge.id)
            vineyard_client.put_name(edge.id, "__%s_edge_stream" % graph_name)

            # build the parallel stream for vertex
            vertex = vineyard_client.create_metadata(metadata)
            vineyard_client.persist(vertex.id)
            vineyard_client.put_name(vertex.id, "__%s_vertex_stream" % graph_name)

            return repr(graph.id), repr(edge.id), repr(vertex.id)

        def load_subgraph(oid_type, edge_stream_id, vertex_stream_id):
            import vineyard

            vertices = [Loader(vineyard.ObjectID(vertex_stream_id))]
            edges = [Loader(vineyard.ObjectID(edge_stream_id))]
            oid_type = normalize_data_type_str(oid_type)
            v_labels = normalize_parameter_vertices(vertices, oid_type)
            e_labels = normalize_parameter_edges(edges, oid_type)
            loader_op = create_loader(v_labels + e_labels)
            config = {
                types_pb2.DIRECTED: utils.b_to_attr(True),
                types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
                types_pb2.GENERATE_EID: utils.b_to_attr(False),
                types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
                types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
            }
            new_op = create_graph(
                self._session_id,
                graph_def_pb2.ARROW_PROPERTY,
                inputs=[loader_op],
                attrs=config,
            )
            # spawn a vineyard stream loader on coordinator
            loader_op_def = loader_op.as_op_def()
            coordinator_dag = op_def_pb2.DagDef()
            coordinator_dag.op.extend([loader_op_def])
            # set the same key from subgraph to new op
            new_op_def = new_op.as_op_def()
            new_op_def.key = op.key
            dag = op_def_pb2.DagDef()
            dag.op.extend([new_op_def])
            self.run_on_coordinator(coordinator_dag, [], {})
            response_head, _ = self.run_on_analytical_engine(dag, [], {})
            logger.info("subgraph has been loaded")
            return response_head.head.results[-1]

        # generate a random graph name
        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_num = random.randint(0, 10000000)
        graph_name = "%s_%s" % (str(now_time), str(random_num))

        (
            _graph_builder_id,
            edge_stream_id,
            vertex_stream_id,
        ) = create_global_graph_builder(graph_name, self._launcher.num_workers)

        # start a thread to launch the graph
        pool = futures.ThreadPoolExecutor()
        subgraph_task = pool.submit(
            load_subgraph,
            oid_type,
            edge_stream_id,
            vertex_stream_id,
        )

        # add subgraph vertices and edges
        subgraph_script = "{0}.subgraph('{1}').outputVineyard('{2}')".format(
            gremlin_script, graph_name, graph_name
        )
        gremlin_client.submit(
            subgraph_script, request_options=request_options
        ).all().result()

        return subgraph_task.result()

    def _gremlin_to_subgraph(self, op: op_def_pb2.OpDef):
        gremlin_script = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        oid_type = op.attr[types_pb2.OID_TYPE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        key_of_parent_op = op.parents[0]
        gremlin_client = self._object_manager.get(key_of_parent_op)

        def load_subgraph(oid_type, name):
            import vineyard

            vertices = [Loader(vineyard.ObjectName("__%s_vertex_stream" % name))]
            edges = [Loader(vineyard.ObjectName("__%s_edge_stream" % name))]
            oid_type = normalize_data_type_str(oid_type)
            v_labels = normalize_parameter_vertices(vertices, oid_type)
            e_labels = normalize_parameter_edges(edges, oid_type)
            loader_op = create_loader(v_labels + e_labels)
            config = {
                types_pb2.DIRECTED: utils.b_to_attr(True),
                types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
                types_pb2.GENERATE_EID: utils.b_to_attr(False),
                types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
                types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
            }
            new_op = create_graph(
                self._session_id,
                graph_def_pb2.ARROW_PROPERTY,
                inputs=[loader_op],
                attrs=config,
            )
            # spawn a vineyard stream loader on coordinator
            loader_op_def = loader_op.as_op_def()
            coordinator_dag = op_def_pb2.DagDef()
            coordinator_dag.op.extend([loader_op_def])
            # set the same key from subgraph to new op
            new_op_def = new_op.as_op_def()
            new_op_def.key = op.key
            dag = op_def_pb2.DagDef()
            dag.op.extend([new_op_def])
            self.run_on_coordinator(coordinator_dag, [], {})
            response_head, _ = self.run_on_analytical_engine(dag, [], {})
            logger.info("subgraph has been loaded")
            return response_head.head.results[-1]

        # generate a random graph name
        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_num = random.randint(0, 10000000)
        graph_name = "%s_%s" % (str(now_time), str(random_num))

        # create a graph handle by name
        gremlin_client.submit(
            "g.createGraph('{0}').with('graphType', 'vineyard')".format(graph_name),
            request_options=request_options,
        ).all().result()

        # start a thread to launch the graph
        pool = futures.ThreadPoolExecutor()
        subgraph_task = pool.submit(
            load_subgraph,
            oid_type,
            graph_name,
        )

        # add subgraph vertices and edges
        subgraph_script = "{0}.subgraph('{1}').outputVineyard('{2}')".format(
            gremlin_script, graph_name, graph_name
        )
        gremlin_client.submit(
            subgraph_script, request_options=request_options
        ).all().result()

        return subgraph_task.result()

    def _create_learning_instance(self, op: op_def_pb2.OpDef):
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        logger.info(
            "Coordinator create learning instance with object id %ld",
            object_id,
        )
        handle = op.attr[types_pb2.GLE_HANDLE].s.decode("utf-8")
        config = op.attr[types_pb2.GLE_CONFIG].s.decode("utf-8")
        endpoints = self._launcher.create_learning_instance(object_id, handle, config)
        self._object_manager.put(op.key, LearningInstanceManager(op.key, object_id))
        result = {
            "handle": handle,
            "config": config,
            "endpoints": endpoints,
            "object_id": object_id,
        }
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
            result=json.dumps(result).encode("utf-8"),
        )

    def _close_learning_instance(self, op: op_def_pb2.OpDef):
        key_of_parent_op = op.parents[0]
        learning_instance_manager = self._object_manager.get(key_of_parent_op)
        object_id = learning_instance_manager.object_id
        logger.info(
            "Coordinator close learning instance with object id %ld",
            object_id,
        )
        self._launcher.close_learning_instance(object_id)
        learning_instance_manager.closed = True
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
        )

    def _cleanup(self, cleanup_instance=True, is_dangling=False):
        # clean up session resources.
        for key in self._object_manager.keys():
            obj = self._object_manager.get(key)
            obj_type = obj.type
            unload_type, config = None, None

            if obj_type == "app":
                unload_type = types_pb2.UNLOAD_APP
                config = {
                    types_pb2.APP_NAME: attr_value_pb2.AttrValue(
                        s=obj.key.encode("utf-8")
                    )
                }
            elif obj_type == "graph":
                unload_type = types_pb2.UNLOAD_GRAPH
                config = {
                    types_pb2.GRAPH_NAME: attr_value_pb2.AttrValue(
                        s=obj.key.encode("utf-8")
                    )
                }
                # dynamic graph doesn't have a vineyard id
                if obj.vineyard_id != -1:
                    config[types_pb2.VINEYARD_ID] = attr_value_pb2.AttrValue(
                        i=obj.vineyard_id
                    )
            elif obj_type == "gie_manager":
                if not obj.closed:
                    self._close_interactive_instance(
                        op=op_def_pb2.OpDef(
                            op=types_pb2.CLOSE_INTERACTIVE_QUERY, parents=[key]
                        )
                    )

            elif obj_type == "gle_manager":
                if not obj.closed:
                    self._close_learning_instance(
                        op=op_def_pb2.OpDef(
                            op=types_pb2.CLOSE_LEARNING_INSTANCE,
                            parents=[key],
                        )
                    )

            if unload_type:
                dag_def = create_single_op_dag(unload_type, config)
                request = self._grpc_utils.generate_runstep_requests(
                    session_id=self._session_id, dag_def=dag_def
                )
                try:
                    self._analytical_engine_stub.RunStep(request)
                except grpc.RpcError as e:
                    logger.error(
                        "Cleanup failed, code: %s, details: %s",
                        e.code().name,
                        e.details(),
                    )

        self._object_manager.clear()

        self._request = None

        # cancel dangling detect timer
        if self._dangling_detecting_timer:
            self._dangling_detecting_timer.cancel()
            self._dangling_detecting_timer = None

        # close engines
        if cleanup_instance:
            self._analytical_engine_stub = None
            self._analytical_engine_endpoint = None
            self._launcher.stop(is_dangling=is_dangling)

        self._session_id = None

    def _create_grpc_stub(self):
        options = [
            ("grpc.max_send_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", GS_GRPC_MAX_MESSAGE_LENGTH),
            ("grpc.max_metadata_size", GS_GRPC_MAX_MESSAGE_LENGTH),
        ]

        channel = grpc.insecure_channel(
            self._launcher.analytical_engine_endpoint, options=options
        )
        return engine_service_pb2_grpc.EngineServiceStub(channel)

    def _get_engine_config(self):
        dag_def = create_single_op_dag(types_pb2.GET_ENGINE_CONFIG)
        try:
            response_head, _ = self.run_on_analytical_engine(dag_def, [], {})
        except grpc.RpcError as e:
            logger.error(
                "Get engine config failed, code: %s, details: %s",
                e.code().name,
                e.details(),
            )
            if e.code() == grpc.StatusCode.INTERNAL:
                raise AnalyticalEngineInternalError(e.details())
            else:
                raise
        config = json.loads(response_head.head.results[0].result.decode("utf-8"))
        config.update(self._launcher.get_engine_config())
        return config

    def _compile_lib_and_distribute(self, compile_func, lib_name, op):
        space = self._builtin_workspace
        if (types_pb2.GAR in op.attr) or (
            op.attr[types_pb2.APP_ALGO].s.decode("utf-8").startswith("giraph:")
        ):
            space = self._udf_app_workspace
        app_lib_path, java_jar_path, java_ffi_path, app_type = compile_func(
            space,
            lib_name,
            op.attr,
            self._analytical_engine_config,
            self._java_class_path,
        )
        # for java app compilation, we need to distribute the jar and ffi generated
        if app_type == "java_pie":
            self._launcher.distribute_file(java_jar_path)
            self._launcher.distribute_file(java_ffi_path)
        self._launcher.distribute_file(app_lib_path)
        return app_lib_path


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
        "--k8s_service_type",
        type=str,
        default="NodePort",
        help="Service type, choose from 'NodePort' or 'LoadBalancer'.",
    )
    parser.add_argument(
        "--k8s_gs_image",
        type=str,
        default=f"registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:{__version__}",
        help="Docker image of graphscope engines.",
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
        "--k8s_etcd_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/etcd:v3.4.13",
        help="Docker image of etcd, needed by vineyard.",
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
        default="graphscope",
        help="A list of comma sparated secrets to pull image.",
    )
    parser.add_argument(
        "--k8s_vineyard_daemonset",
        type=str,
        default="",
        help="Use the existing vineyard DaemonSet with name 'k8s_vineyard_daemonset'.",
    )
    parser.add_argument(
        "--k8s_vineyard_cpu",
        type=float,
        default=1.0,
        help="CPU cores of vinayard container.",
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
        "--k8s_etcd_num_pods",
        type=int,
        default=3,
        help="The number of etcd pods.",
    )
    parser.add_argument(
        "--k8s_etcd_cpu",
        type=float,
        default=1.0,
        help="CPU cores of etcd pod, default: 1.0",
    )
    parser.add_argument(
        "--k8s_etcd_mem",
        type=str,
        default="256Mi",
        help="Memory of etcd pod, suffix with ['Mi', 'Gi', 'Ti'].",
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
        "--k8s_volumes",
        type=str,
        default="{}",
        help="A json string spcifies the kubernetes volumes to mount.",
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
        "--k8s_delete_namespace",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Delete the namespace that created by graphscope.",
    )
    parser.add_argument(
        "--mount_dataset",
        type=str,
        default=None,
        help="Mount the aliyun dataset bucket as a volume by ossfs.",
    )
    parser.add_argument(
        "--k8s_dataset_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/dataset:{__version__}",
        help="Docker image to mount the dataset bucket",
    )
    return parser.parse_args()


def launch_graphscope():
    args = parse_sys_args()
    logger.info("Launching with args %s", args)

    if args.cluster_type == "k8s":
        launcher = KubernetesClusterLauncher(
            namespace=args.k8s_namespace,
            service_type=args.k8s_service_type,
            gs_image=args.k8s_gs_image,
            etcd_image=args.k8s_etcd_image,
            dataset_image=args.k8s_dataset_image,
            coordinator_name=args.k8s_coordinator_name,
            coordinator_service_name=args.k8s_coordinator_service_name,
            etcd_addrs=args.etcd_addrs,
            etcd_num_pods=args.k8s_etcd_num_pods,
            etcd_cpu=args.k8s_etcd_cpu,
            etcd_mem=args.k8s_etcd_mem,
            engine_cpu=args.k8s_engine_cpu,
            engine_mem=args.k8s_engine_mem,
            vineyard_daemonset=args.k8s_vineyard_daemonset,
            vineyard_cpu=args.k8s_vineyard_cpu,
            vineyard_mem=args.k8s_vineyard_mem,
            vineyard_shared_mem=args.vineyard_shared_mem,
            mars_worker_cpu=args.k8s_mars_worker_cpu,
            mars_worker_mem=args.k8s_mars_worker_mem,
            mars_scheduler_cpu=args.k8s_mars_scheduler_cpu,
            mars_scheduler_mem=args.k8s_mars_scheduler_mem,
            with_mars=args.k8s_with_mars,
            image_pull_policy=args.k8s_image_pull_policy,
            image_pull_secrets=args.k8s_image_pull_secrets,
            volumes=args.k8s_volumes,
            mount_dataset=args.mount_dataset,
            num_workers=args.num_workers,
            preemptive=args.preemptive,
            instance_id=args.instance_id,
            log_level=args.log_level,
            timeout_seconds=args.timeout_seconds,
            waiting_for_delete=args.waiting_for_delete,
            delete_namespace=args.k8s_delete_namespace,
        )
    elif args.cluster_type == "hosts":
        launcher = LocalLauncher(
            num_workers=args.num_workers,
            hosts=args.hosts,
            etcd_addrs=args.etcd_addrs,
            vineyard_socket=args.vineyard_socket,
            shared_mem=args.vineyard_shared_mem,
            log_level=args.log_level,
            instance_id=args.instance_id,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        raise RuntimeError("Expect hosts or k8s of cluster_type parameter")

    coordinator_service_servicer = CoordinatorServiceServicer(
        launcher=launcher,
        dangling_timeout_seconds=args.dangling_timeout_seconds,
        log_level=args.log_level,
    )

    # register gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(os.cpu_count() or 1),
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
    logger.info("Coordinator server listen at 0.0.0.0:%d", args.port)

    server.start()

    # handle SIGTERM signal
    def terminate(signum, frame):
        coordinator_service_servicer._cleanup()

    signal.signal(signal.SIGTERM, terminate)

    try:
        # Grpc has handled SIGINT
        server.wait_for_termination()
    except KeyboardInterrupt:
        coordinator_service_servicer._cleanup()


if __name__ == "__main__":
    launch_graphscope()
