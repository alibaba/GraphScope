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
import json
import logging
import os
import pickle
import queue
import random
import signal
import socket
import string
import sys
import threading
import urllib.parse
import urllib.request
from concurrent import futures

import grpc
from packaging import version

from gscoordinator.io_utils import StdoutWrapper

# capture system stdout
sys.stdout = StdoutWrapper(sys.stdout)

from graphscope.framework import utils
from graphscope.framework.dag_utils import create_graph
from graphscope.framework.dag_utils import create_loader
from graphscope.framework.graph_utils import normalize_parameter_edges
from graphscope.framework.graph_utils import normalize_parameter_vertices
from graphscope.framework.loader import Loader
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
from gscoordinator.launcher import LocalLauncher
from gscoordinator.object_manager import GraphMeta
from gscoordinator.object_manager import GremlinResultSet
from gscoordinator.object_manager import InteractiveQueryManager
from gscoordinator.object_manager import LearningInstanceManager
from gscoordinator.object_manager import LibMeta
from gscoordinator.object_manager import ObjectManager
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

COORDINATOR_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRAPHSCOPE_HOME = os.path.join(COORDINATOR_HOME, "..")

WORKSPACE = "/tmp/gs"
DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
ANALYTICAL_ENGINE_HOME = os.path.join(GRAPHSCOPE_HOME, "analytical_engine")
ANALYTICAL_ENGINE_PATH = os.path.join(ANALYTICAL_ENGINE_HOME, "build", "grape_engine")
TEMPLATE_DIR = os.path.join(COORDINATOR_HOME, "gscoordinator", "template")
BUILTIN_APP_RESOURCE_PATH = os.path.join(
    COORDINATOR_HOME, "gscoordinator", "builtin/app/builtin_app.gar"
)
GS_DEBUG_ENDPOINT = os.environ.get("GS_DEBUG_ENDPOINT", "")

ENGINE_CONTAINER = "engine"
VINEYARD_CONTAINER = "vineyard"

# 2 GB
GS_GRPC_MAX_MESSAGE_LENGTH = 2 * 1024 * 1024 * 1024 - 1

logger = logging.getLogger("graphscope")


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

        # control log fetching
        self._streaming_logs = True

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
        if log_level:
            log_level = log_level.upper()
        logger = logging.getLogger("graphscope")
        logger.setLevel(logging.DEBUG)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(log_level)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s"
        )
        stdout_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)

    def ConnectSession(self, request, context):
        # A session is already connected.
        if self._request:
            if getattr(request, "reconnect", False):
                self._make_response(
                    message_pb2.ConnectSessionResponse,
                    code=error_codes_pb2.OK,
                    session_id=self._session_id,
                    cluster_type=self._launcher.type(),
                    num_workers=self._launcher.num_workers,
                    engine_config=json.dumps(self._analytical_engine_config),
                    pod_name_list=self._engine_hosts.split(","),
                    namespace=self._k8s_namespace,
                )
            else:
                return self._make_response(
                    message_pb2.ConnectSessionResponse,
                    code=error_codes_pb2.CONNECTION_ERROR,
                    error_msg="Cannot setup more than one connection at the same time.",
                )

        # Connect to serving coordinator.
        self._request = request
        self._analytical_engine_config = self._get_engine_config()

        # Generate session id
        self._session_id = self._generate_session_id()
        self._key_to_op = dict()
        # dict of op_def_pb2.OpResult
        self._op_result_pool = dict()
        self._udf_app_workspace = os.path.join(WORKSPACE, self._session_id)

        # Session connected, fetch logs via gRPC.
        self._streaming_logs = True
        sys.stdout.drop(False)

        # check version compatibility from client
        sv = version.parse(__version__)
        cv = version.parse(self._request.version)
        if sv.major != cv.major or sv.minor != cv.minor:
            logger.warning(
                "Version between client and server is inconsistent: %s vs %s",
                self._request.version,
                __version__,
            )

        return self._make_response(
            message_pb2.ConnectSessionResponse,
            code=error_codes_pb2.OK,
            session_id=self._session_id,
            cluster_type=self._launcher.type(),
            num_workers=self._launcher.num_workers,
            engine_config=json.dumps(self._analytical_engine_config),
            pod_name_list=self._engine_hosts.split(","),
            namespace=self._k8s_namespace,
        )

    def HeartBeat(self, request, context):
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
        try:
            self._analytical_engine_stub.HeartBeat(request)
        except Exception as e:
            return self._make_response(
                message_pb2.HeartBeatResponse,
                error_codes_pb2.CONNECTION_ERROR,
                "connect analytical engine failed: {}".format(str(e)),
            )
        else:
            return self._make_response(
                message_pb2.HeartBeatResponse, error_codes_pb2.OK
            )

    def run_on_analytical_engine(  # noqa: C901
        self, session_id, dag_def: op_def_pb2.DagDef, op_results: list
    ):
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            try:
                op_pre_process(
                    op,
                    self._op_result_pool,
                    self._key_to_op,
                    engine_hosts=self._engine_hosts,
                    engine_config=self._get_engine_config(),
                )
            except Exception as e:
                error_msg = (
                    "Failed to pre process op {0} with error message {1}".format(
                        op, str(e)
                    )
                )
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COORDINATOR_INTERNAL_ERROR,
                    error_msg,
                    full_exception=pickle.dumps(e),
                    results=op_results,
                )

            # Compile app or not.
            if op.op == types_pb2.BIND_APP:
                try:
                    op, app_sig, app_lib_path = self._maybe_compile_app(op)
                except Exception as e:
                    error_msg = "Failed to compile app: {0}".format(str(e))
                    logger.error(error_msg)
                    return self._make_response(
                        message_pb2.RunStepResponse,
                        error_codes_pb2.COMPILATION_ERROR,
                        error_msg,
                        results=op_results,
                    )
            # Compile graph or not
            # arrow property graph and project graph need to compile
            # If engine crashed, we will get a SocketClosed grpc Exception.
            # In that case, we should notify client the engine is dead.
            if (
                (
                    op.op == types_pb2.CREATE_GRAPH
                    and op.attr[types_pb2.GRAPH_TYPE].graph_type
                    == graph_def_pb2.ARROW_PROPERTY
                )
                or op.op == types_pb2.TRANSFORM_GRAPH
                or op.op == types_pb2.PROJECT_TO_SIMPLE
                or op.op == types_pb2.ADD_LABELS
            ):
                try:
                    op = self._maybe_register_graph(op, session_id)
                except grpc.RpcError as e:
                    logger.error("self._launcher.poll() = %s", self._launcher.poll())
                    if self._launcher.poll() is not None:
                        message = (
                            "Analytical engine exited with %s" % self._launcher.poll()
                        )
                    else:
                        message = str(e)
                    return self._make_response(
                        message_pb2.RunStepResponse,
                        error_codes_pb2.COMPILATION_ERROR,
                        message,
                        results=op_results,
                    )
                except Exception as e:
                    error_msg = "Graph compile error: {}".format(str(e))
                    logger.error(error_msg)
                    return self._make_response(
                        message_pb2.RunStepResponse,
                        error_codes_pb2.COMPILATION_ERROR,
                        error_msg,
                        results=op_results,
                    )

        request = message_pb2.RunStepRequest(
            session_id=self._session_id, dag_def=dag_def
        )
        try:
            response = self._analytical_engine_stub.RunStep(request)
        except grpc.RpcError as e:
            logger.error("self._launcher.poll() = %s", self._launcher.poll())
            if self._launcher.poll() is not None:
                message = "Analytical engine exited with %s" % self._launcher.poll()
            else:
                message = str(e)
            # op_results.extend(response.results)
            return self._make_response(
                message_pb2.RunStepResponse,
                error_codes_pb2.FATAL_ERROR,
                message,
                results=op_results,
            )
        except Exception as e:
            # op_results.extend(response.results)
            return self._make_response(
                message_pb2.RunStepResponse,
                error_codes_pb2.UNKNOWN,
                str(e),
                results=op_results,
            )

        op_results.extend(response.results)
        for r in response.results:
            op = self._key_to_op[r.key]
            if op.op not in (
                types_pb2.CONTEXT_TO_NUMPY,
                types_pb2.CONTEXT_TO_DATAFRAME,
                types_pb2.REPORT_GRAPH,
            ):
                self._op_result_pool[r.key] = r

        if response.status.code == error_codes_pb2.OK:
            for op_result in response.results:
                key = op_result.key
                op = self._key_to_op[key]
                if op.op in (
                    types_pb2.CREATE_GRAPH,
                    types_pb2.PROJECT_GRAPH,
                    types_pb2.ADD_LABELS,
                    types_pb2.ADD_COLUMN,
                ):
                    schema_path = os.path.join(
                        "/tmp", op_result.graph_def.key + ".json"
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
                elif op.op == types_pb2.BIND_APP:
                    self._object_manager.put(
                        app_sig,
                        LibMeta(op_result.result.decode("utf-8"), "app", app_lib_path),
                    )
                elif op.op == types_pb2.UNLOAD_GRAPH:
                    self._object_manager.pop(op.attr[types_pb2.GRAPH_NAME].s.decode())
                elif op.op == types_pb2.UNLOAD_APP:
                    self._object_manager.pop(op.attr[types_pb2.APP_NAME].s.decode())
        return response

    def run_on_interactive_engine(
        self, session_id, dag_def: op_def_pb2.DagDef, op_results: list
    ):
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            try:
                op_pre_process(
                    op,
                    self._op_result_pool,
                    self._key_to_op,
                    engine_hosts=self._engine_hosts,
                    engine_config=self._get_engine_config(),
                )
            except Exception as e:
                error_msg = (
                    "Failed to pre process op {0} with error message {1}".format(
                        op, str(e)
                    )
                )
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COORDINATOR_INTERNAL_ERROR,
                    error_msg,
                    full_exception=pickle.dumps(e),
                    results=op_results,
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
                logger.error("Unsupport op type: %s", str(op.op))
            op_results.append(op_result)
            if op_result.code == error_codes_pb2.OK:
                # don't record the results of these ops to avoid
                # taking up too much memory in coordinator
                if op.op not in [types_pb2.FETCH_GREMLIN_RESULT]:
                    self._op_result_pool[op.key] = op_result
            else:
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR,
                    error_msg=op_result.error_msg,
                    results=op_results,
                )
        return self._make_response(
            message_pb2.RunStepResponse, error_codes_pb2.OK, results=op_results
        )

    def RunStep(self, request, context):
        op_results = list()
        # split dag
        dag_manager = DAGManager(request.dag_def)
        while not dag_manager.empty():
            next_dag = dag_manager.get_next_dag()
            run_dag_on, dag_def = next_dag
            if run_dag_on == GSEngine.analytical_engine:
                ret = self.run_on_analytical_engine(
                    request.session_id, dag_def, op_results
                )
            if run_dag_on == GSEngine.interactive_engine:
                ret = self.run_on_interactive_engine(
                    request.session_id, dag_def, op_results
                )

            if run_dag_on == GSEngine.learning_engine:
                ret = self.run_on_learning_engine(
                    request.session_id, dag_def, op_results
                )
            if run_dag_on == GSEngine.coordinator:
                ret = self.run_on_coordinator(request.session_id, dag_def, op_results)
            if ret.status.code != error_codes_pb2.OK:
                return ret
        return self._make_response(
            message_pb2.RunStepResponse, error_codes_pb2.OK, results=op_results
        )

    def run_on_learning_engine(
        self, session_id, dag_def: op_def_pb2.DagDef, op_results: list
    ):
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            try:
                op_pre_process(
                    op,
                    self._op_result_pool,
                    self._key_to_op,
                    engine_hosts=self._engine_hosts,
                    engine_config=self._get_engine_config(),
                )
            except Exception as e:
                error_msg = (
                    "Failed to pre process op {0} with error message {1}".format(
                        op, str(e)
                    )
                )
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COORDINATOR_INTERNAL_ERROR,
                    error_msg,
                    full_exception=pickle.dumps(e),
                    results=op_results,
                )
            if op.op == types_pb2.CREATE_LEARNING_INSTANCE:
                op_result = self._create_learning_instance(op)
            elif op.op == types_pb2.CLOSE_LEARNING_INSTANCE:
                op_result = self._close_learning_instance(op)
            else:
                logger.error("Unsupport op type: %s", str(op.op))
            op_results.append(op_result)
            if op_result.code == error_codes_pb2.OK:
                self._op_result_pool[op.key] = op_result
        return self._make_response(
            message_pb2.RunStepResponse, error_codes_pb2.OK, results=op_results
        )

    def run_on_coordinator(
        self, session_id, dag_def: op_def_pb2.DagDef, op_results: list
    ):
        for op in dag_def.op:
            self._key_to_op[op.key] = op
            try:
                op_pre_process(
                    op,
                    self._op_result_pool,
                    self._key_to_op,
                    engine_hosts=self._engine_hosts,
                    engine_config=self._get_engine_config(),
                )
            except Exception as e:
                error_msg = (
                    "Failed to pre process op {0} with error message {1}".format(
                        op, str(e)
                    )
                )
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COORDINATOR_INTERNAL_ERROR,
                    error_msg,
                    full_exception=pickle.dumps(e),
                    results=op_results,
                )
            if op.op == types_pb2.DATA_SOURCE:
                op_result = self._process_data_source(op)
            elif op.op == types_pb2.OUTPUT:
                op_result = self._output(op)
            else:
                logger.error("Unsupport op type: %s", str(op.op))
            op_results.append(op_result)
            if op_result.code == error_codes_pb2.OK:
                self._op_result_pool[op.key] = op_result
        return self._make_response(
            message_pb2.RunStepResponse, error_codes_pb2.OK, results=op_results
        )

    def _maybe_compile_app(self, op):
        app_sig = get_app_sha256(op.attr)
        space = self._builtin_workspace
        if types_pb2.GAR in op.attr:
            space = self._udf_app_workspace
        app_lib_path = get_lib_path(os.path.join(space, app_sig), app_sig)
        if not os.path.isfile(app_lib_path):
            compiled_path = self._compile_lib_and_distribute(compile_app, app_sig, op)
            if app_lib_path != compiled_path:
                raise RuntimeError("Computed path not equal to compiled path.")

        op.attr[types_pb2.APP_LIBRARY_PATH].CopyFrom(
            attr_value_pb2.AttrValue(s=app_lib_path.encode("utf-8"))
        )
        return op, app_sig, app_lib_path

    def _maybe_register_graph(self, op, session_id):
        graph_sig = get_graph_sha256(op.attr)
        space = self._builtin_workspace
        graph_lib_path = get_lib_path(os.path.join(space, graph_sig), graph_sig)
        if not os.path.isfile(graph_lib_path):
            compiled_path = self._compile_lib_and_distribute(
                compile_graph_frame, graph_sig, op
            )
            if graph_lib_path != compiled_path:
                raise RuntimeError("Computed path not equal to compiled path.")
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
                attr_value_pb2.AttrValue(
                    graph_type=op.attr[types_pb2.GRAPH_TYPE].graph_type
                )
            )
            dag_def = op_def_pb2.DagDef()
            dag_def.op.extend([op_def])
            register_request = message_pb2.RunStepRequest(
                session_id=session_id, dag_def=dag_def
            )
            register_response = self._analytical_engine_stub.RunStep(register_request)

            if register_response.status.code == error_codes_pb2.OK:
                self._object_manager.put(
                    graph_sig,
                    LibMeta(
                        register_response.results[0].result,
                        "graph_frame",
                        graph_lib_path,
                    ),
                )
            else:
                raise RuntimeError("Error occur when register graph")
        op.attr[types_pb2.TYPE_SIGNATURE].CopyFrom(
            attr_value_pb2.AttrValue(s=graph_sig.encode("utf-8"))
        )
        return op

    def FetchLogs(self, request, context):
        while self._streaming_logs:
            try:
                message = sys.stdout.poll(timeout=3)
            except queue.Empty:
                pass
            else:
                if self._streaming_logs:
                    yield self._make_response(
                        message_pb2.FetchLogsResponse,
                        error_codes_pb2.OK,
                        message=message,
                    )

    def CloseSession(self, request, context):
        """
        Disconnect session, note that it doesn't clean up any resources.
        """
        if request.session_id != self._session_id:
            return self._make_response(
                message_pb2.CloseSessionResponse,
                error_codes_pb2.INVALID_ARGUMENT_ERROR,
                "Session handle does not match",
            )

        self._cleanup(
            cleanup_instance=self._request.cleanup_instance, is_dangling=False
        )
        self._request = None

        # Session closed, stop streaming logs
        sys.stdout.drop(True)
        self._streaming_logs = False

        return self._make_response(message_pb2.CloseSessionResponse, error_codes_pb2.OK)

    def _create_interactive_instance(self, op: op_def_pb2.OpDef):
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        schema_path = op.attr[types_pb2.SCHEMA_PATH].s.decode()
        gremlin_server_cpu = op.attr[types_pb2.GIE_GREMLIN_SERVER_CPU].f
        gremlin_server_mem = op.attr[types_pb2.GIE_GREMLIN_SERVER_MEM].s.decode()
        if types_pb2.GIE_GREMLIN_ENGINE_PARAMS in op.attr:
            engine_params = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_ENGINE_PARAMS].s.decode()
            )
        else:
            engine_params = {}
        enable_gaia = op.attr[types_pb2.GIE_ENABLE_GAIA].b
        with open(schema_path) as file:
            schema_json = file.read()

        params = {
            "graphName": "%s" % object_id,
            "zookeeperIp": socket.gethostbyname(socket.gethostname()),
        }

        if self._launcher_type == types_pb2.K8S:
            post_url = "{0}/instance/create".format(self._launcher.get_manager_host())
            params.update(
                {
                    "schemaJson": schema_json,
                    "podNameList": self._engine_hosts,
                    "containerName": ENGINE_CONTAINER,
                    "preemptive": str(self._launcher.preemptive),
                    "gremlinServerCpu": str(gremlin_server_cpu),
                    "gremlinServerMem": gremlin_server_mem,
                    "enableGaia": str(enable_gaia),
                }
            )
            engine_params = [
                "{}:{}".format(key, value) for key, value in engine_params.items()
            ]
            params["engineParams"] = "'{}'".format(";".join(engine_params))
        else:
            manager_host = self._launcher.graph_manager_endpoint
            params.update(
                {
                    "vineyardIpcSocket": self._launcher.vineyard_socket,
                    "schemaPath": schema_path,
                    "zookeeperPort": str(self._launcher.zookeeper_port),
                    "enableGaia": str(enable_gaia),
                }
            )
            post_url = "http://%s/instance/create_local" % manager_host

        post_data = urllib.parse.urlencode(params).encode("utf-8")
        create_res = urllib.request.urlopen(url=post_url, data=post_data)
        res_json = json.load(create_res)
        error_code = res_json["errorCode"]
        if error_code == 0:
            maxgraph_endpoint = f"{res_json['frontHost']}:{res_json['frontPort']}"
            logger.info(
                "build maxgraph frontend %s for graph %ld", maxgraph_endpoint, object_id
            )
            endpoints = [maxgraph_endpoint]
            if enable_gaia:
                gaia_endpoint = (
                    f"{res_json['gaiaFrontHost']}:{res_json['gaiaFrontPort']}"
                )
                endpoints.append(gaia_endpoint)
                logger.info(
                    "build gaia frontend %s for graph %ld", gaia_endpoint, object_id
                )
            self._object_manager.put(
                op.key,
                InteractiveQueryManager(op.key, endpoints, object_id),
            )
            return op_def_pb2.OpResult(
                code=error_codes_pb2.OK,
                key=op.key,
                result=",".join(endpoints).encode("utf-8"),
                extra_info=str(object_id).encode("utf-8"),
            )
        else:
            error_message = (
                "create interactive instance for object id %ld failed with error code %d message %s"
                % (object_id, error_code, res_json["errorMessage"])
            )
            logger.error(error_message)
            return op_def_pb2.OpResult(
                code=error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR,
                key=op.key,
                error_msg=error_message,
            )

    def _execute_gremlin_query(self, op: op_def_pb2.OpDef):
        message = op.attr[types_pb2.GIE_GREMLIN_QUERY_MESSAGE].s.decode()
        request_options = None
        if types_pb2.GIE_GREMLIN_REQUEST_OPTIONS in op.attr:
            request_options = json.loads(
                op.attr[types_pb2.GIE_GREMLIN_REQUEST_OPTIONS].s.decode()
            )
        query_gaia = op.attr[types_pb2.GIE_QUERY_GAIA].b
        key_of_parent_op = op.parents[0]

        gremlin_client = self._object_manager.get(key_of_parent_op)
        try:
            rlt = gremlin_client.submit(
                message, request_options=request_options, query_gaia=query_gaia
            )
        except Exception as e:
            error_message = "Gremlin query failed with error message {0}".format(str(e))
            logger.error(error_message)
            return op_def_pb2.OpResult(
                code=error_codes_pb2.GREMLIN_QUERY_ERROR,
                key=op.key,
                error_msg=error_message,
            )
        else:
            self._object_manager.put(op.key, GremlinResultSet(op.key, rlt))
            return op_def_pb2.OpResult(
                code=error_codes_pb2.OK,
                key=op.key,
            )

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
            error_message = "Fetch gremlin result failed with error message {0}".format(
                e
            )
            logger.error(error_message)
            return op_def_pb2.OpResult(
                code=error_codes_pb2.GREMLIN_QUERY_ERROR,
                key=op.key,
                error_msg=error_message,
            )
        else:
            return op_def_pb2.OpResult(
                code=error_codes_pb2.OK, key=op.key, result=pickle.dumps(rlt)
            )

    def _output(self, op: op_def_pb2.OpDef):
        import vineyard
        import vineyard.io

        storage_options = json.loads(op.attr[types_pb2.STORAGE_OPTIONS].s.decode())
        fd = op.attr[types_pb2.FD].s.decode()
        df = op.attr[types_pb2.VINEYARD_ID].s.decode()
        engine_config = self._get_engine_config()
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

    def _process_data_source(self, op: op_def_pb2.OpDef):
        def _spawn_vineyard_io_stream(source, storage_options, read_options):
            import vineyard
            import vineyard.io

            engine_config = self._get_engine_config()
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

        def _process_loader_func(func):
            protocol = func.attr[types_pb2.PROTOCOL].s.decode()
            if protocol in ("hdfs", "hive", "oss", "s3"):
                source = func.attr[types_pb2.VALUES].s.decode()
                storage_options = json.loads(
                    func.attr[types_pb2.STORAGE_OPTIONS].s.decode()
                )
                read_options = json.loads(func.attr[types_pb2.READ_OPTIONS].s.decode())
                new_protocol, new_source = _spawn_vineyard_io_stream(
                    source, storage_options, read_options
                )
                func.attr[types_pb2.PROTOCOL].CopyFrom(utils.s_to_attr(new_protocol))
                func.attr[types_pb2.VALUES].CopyFrom(utils.s_to_attr(new_source))

        for label in op.attr[types_pb2.ARROW_PROPERTY_DEFINITION].list.func:
            # vertex label or edge label
            if types_pb2.LOADER in label.attr:
                loader_func = label.attr[types_pb2.LOADER].func
                if loader_func.name == "loader":
                    _process_loader_func(loader_func)
            if types_pb2.SUB_LABEL in label.attr:
                for func in label.attr[types_pb2.SUB_LABEL].list.func:
                    if types_pb2.LOADER in func.attr:
                        loader_func = func.attr[types_pb2.LOADER].func
                        if loader_func.name == "loader":
                            _process_loader_func(loader_func)

        return op_def_pb2.OpResult(code=error_codes_pb2.OK, key=op.key)

    def _close_interactive_instance(self, op: op_def_pb2.OpDef):
        try:
            key_of_parent_op = op.parents[0]
            gremlin_client = self._object_manager.get(key_of_parent_op)
            object_id = gremlin_client.object_id
            if self._launcher_type == types_pb2.K8S:
                manager_host = self._launcher.get_manager_host()
                close_url = "%s/instance/close?graphName=%ld&podNameList=%s&containerName=%s&waitingForDelete=%s" % (
                    manager_host,
                    object_id,
                    self._engine_hosts,
                    ENGINE_CONTAINER,
                    str(self._launcher.waiting_for_delete()),
                )
            else:
                manager_host = self._launcher.graph_manager_endpoint
                close_url = "http://%s/instance/close_local?graphName=%ld" % (
                    manager_host,
                    object_id,
                )
            logger.info(
                "Coordinator close interactive instance with url[%s]" % close_url
            )
            close_res = urllib.request.urlopen(close_url).read()
            gremlin_client.closed = True
        except Exception as e:
            logger.error("Failed to close interactive instance: %s", str(e))
        else:
            res_json = json.loads(close_res.decode("utf-8", errors="ignore"))
            error_code = res_json["errorCode"]
            if error_code != 0:
                error_message = (
                    "Failed to close interactive instance for object id %ld with error code %d message %s"
                    % (object_id, error_code, res_json["errorMessage"])
                )
                logger.error("Failed to close interactive instance: %s", error_message)
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
        )

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
            self.run_on_coordinator(self._session_id, coordinator_dag, [])
            resp = self.run_on_analytical_engine(self._session_id, dag, [])
            logger.info("subgraph has been loaded")
            return resp.results[-1]

        # generate a random graph name
        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_num = random.randint(0, 10000000)
        graph_name = "%s_%s" % (str(now_time), str(random_num))

        try:
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
        except Exception as e:
            error_message = "Failed to create subgraph from gremlin query with error message: {0}".format(
                str(e)
            )
            logger.error(error_message)
            return op_def_pb2.OpResult(
                code=error_codes_pb2.GREMLIN_QUERY_ERROR,
                key=op.key,
                error_msg=error_message,
            )

    def _create_learning_instance(self, op: op_def_pb2.OpDef):
        object_id = op.attr[types_pb2.VINEYARD_ID].i
        logger.info(
            "Coordinator create learning instance with object id %ld",
            object_id,
        )
        handle = op.attr[types_pb2.GLE_HANDLE].s
        config = op.attr[types_pb2.GLE_CONFIG].s
        endpoints = self._launcher.create_learning_instance(
            object_id, handle.decode("utf-8"), config.decode("utf-8")
        )
        self._object_manager.put(op.key, LearningInstanceManager(op.key, object_id))
        return op_def_pb2.OpResult(
            code=error_codes_pb2.OK,
            key=op.key,
            handle=handle,
            config=config,
            result=",".join(endpoints).encode("utf-8"),
            extra_info=str(object_id).encode("utf-8"),
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

    @staticmethod
    def _make_response(
        resp_cls, code, error_msg="", op=None, full_exception=None, **kwargs
    ):
        resp = resp_cls(
            status=message_pb2.ResponseStatus(code=code, error_msg=error_msg), **kwargs
        )
        if op:
            resp.status.op.CopyFrom(op)
        elif full_exception:
            # bytes
            resp.status.full_exception = full_exception
        return resp

    def _cleanup(self, cleanup_instance=True, is_dangling=False):
        # clean up session resources.
        for key in self._object_manager.keys():
            obj = self._object_manager.get(key)
            obj_type = obj.type
            unload_type = None

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
                request = message_pb2.RunStepRequest(
                    session_id=self._session_id, dag_def=dag_def
                )
                self._analytical_engine_stub.RunStep(request)

        self._object_manager.clear()

        self._request = None

        # cancel dangling detect timer
        if self._dangling_detecting_timer:
            self._dangling_detecting_timer.cancel()

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
        ]

        channel = grpc.insecure_channel(
            self._launcher.analytical_engine_endpoint, options=options
        )
        return engine_service_pb2_grpc.EngineServiceStub(channel)

    def _get_engine_config(self):
        op_def = op_def_pb2.OpDef(op=types_pb2.GET_ENGINE_CONFIG)
        dag_def = op_def_pb2.DagDef()
        dag_def.op.extend([op_def])
        fetch_request = message_pb2.RunStepRequest(
            session_id=self._session_id, dag_def=dag_def
        )
        fetch_response = self._analytical_engine_stub.RunStep(fetch_request)

        config = json.loads(fetch_response.results[0].result.decode("utf-8"))
        config.update(self._launcher.get_engine_config())
        return config

    def _compile_lib_and_distribute(self, compile_func, lib_name, op):
        if self._analytical_engine_config is None:
            # fetch NETWORKX compile option from engine
            self._analytical_engine_config = self._get_engine_config()
        space = self._builtin_workspace
        if types_pb2.GAR in op.attr:
            space = self._udf_app_workspace
        app_lib_path = compile_func(
            space, lib_name, op.attr, self._analytical_engine_config
        )
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
        help="The number of engine workers.",
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
        help="Log level, info or debug.",
    )
    parser.add_argument(
        "--hosts",
        type=str,
        default="localhost",
        help="A list of hostname, comma separated.",
    )
    parser.add_argument(
        "--vineyard_socket",
        type=str,
        default=None,
        help="Socket path to connect to vineyard, random socket will be created if param missing.",
    )
    parser.add_argument(
        "--cluster_type",
        type=str,
        default="k8s",
        help="Deploy graphscope components on local or kubernetes cluster.",
    )
    parser.add_argument(
        "--k8s_namespace",
        type=str,
        default="graphscope",
        help="Contains the namespace to create all resource inside, namespace must be exist.",
    )
    parser.add_argument(
        "--k8s_service_type",
        type=str,
        default="NodePort",
        help="Valid options are NodePort, and LoadBalancer.",
    )
    parser.add_argument(
        "--k8s_gs_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:{}".format(
            __version__
        ),
        help="Docker image of graphscope engines.",
    )
    parser.add_argument(
        "--k8s_coordinator_name",
        type=str,
        default="",
        help="Coordinator name in graphscope instance.",
    )
    parser.add_argument(
        "--k8s_coordinator_service_name",
        type=str,
        default="",
        help="Coordinator service name in graphscope instance.",
    )
    parser.add_argument(
        "--k8s_etcd_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/etcd:v3.4.13",
        help="Docker image of etcd, used by vineyard.",
    )
    parser.add_argument(
        "--k8s_gie_graph_manager_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:{}".format(
            __version__
        ),
        help="Graph Manager image of graph interactive engine.",
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
        help="A list of secret name, comma separated.",
    )
    parser.add_argument(
        "--k8s_vineyard_daemonset",
        type=str,
        default="",
        help="Try to use the existing vineyard DaemonSet with name 'k8s_vineyard_daemonset'.",
    )
    parser.add_argument(
        "--k8s_vineyard_cpu",
        type=float,
        default=1.0,
        help="Cpu cores of vinayard container.",
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
        help="Cpu cores of engine container, default: 1.0",
    )
    parser.add_argument(
        "--k8s_engine_mem",
        type=str,
        default="256Mi",
        help="Memory of engine container, suffix with ['Mi', 'Gi', 'Ti'].",
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
        help="Cpu cores of etcd pod, default: 1.0",
    )
    parser.add_argument(
        "--k8s_etcd_mem",
        type=str,
        default="256Mi",
        help="Memory of etcd pod, suffix with ['Mi', 'Gi', 'Ti'].",
    )
    parser.add_argument(
        "--k8s_gie_graph_manager_cpu",
        type=float,
        default=1.0,
        help="Cpu cores of graph manager container, default: 1.0",
    )
    parser.add_argument(
        "--k8s_gie_graph_manager_mem",
        type=str,
        default="256Mi",
        help="Memory of graph manager container, suffix with ['Mi', 'Gi', 'Ti'].",
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
        help="Cpu cores of mars worker container, default: 0.5",
    )
    parser.add_argument(
        "--k8s_mars_worker_mem",
        type=str,
        default="4Gi",
        help="Memory of mars worker container, default: 4Gi",
    )
    parser.add_argument(
        "--k8s_mars_scheduler_cpu",
        type=float,
        default=0.5,
        help="Cpu cores of mars scheduler container, default: 0.5",
    )
    parser.add_argument(
        "--k8s_mars_scheduler_mem",
        type=str,
        default="2Gi",
        help="Memory of mars scheduler container, default: 2Gi",
    )
    parser.add_argument(
        "--k8s_volumes",
        type=str,
        default="{}",
        help="A json string for kubernetes volumes.",
    )
    parser.add_argument(
        "--timeout_seconds",
        type=int,
        default=600,
        help="Launch failed after waiting timeout seconds.",
    )
    parser.add_argument(
        "--dangling_timeout_seconds",
        type=int,
        default=600,
        help="Kill graphscope instance after seconds of client disconnect.",
    )
    parser.add_argument(
        "--waiting_for_delete",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Waiting for delete graphscope instance.",
    )
    parser.add_argument(
        "--k8s_delete_namespace",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Delete namespace or not.",
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
            gie_graph_manager_image=args.k8s_gie_graph_manager_image,
            coordinator_name=args.k8s_coordinator_name,
            coordinator_service_name=args.k8s_coordinator_service_name,
            etcd_num_pods=args.k8s_etcd_num_pods,
            etcd_cpu=args.k8s_etcd_cpu,
            etcd_mem=args.k8s_etcd_mem,
            gie_graph_manager_cpu=args.k8s_gie_graph_manager_cpu,
            gie_graph_manager_mem=args.k8s_gie_graph_manager_mem,
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
        ],
    )
    coordinator_service_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        coordinator_service_servicer, server
    )
    server.add_insecure_port("0.0.0.0:{}".format(args.port))
    logger.info("Coordinator server listen at 0.0.0.0:%d", args.port)

    server.start()

    # handle SIGTERM signal
    def terminate(signum, frame):
        global coordinator_service_servicer
        coordinator_service_servicer._cleanup()

    signal.signal(signal.SIGTERM, terminate)

    try:
        # Grpc has handled SIGINT
        server.wait_for_termination()
    except KeyboardInterrupt:
        coordinator_service_servicer._cleanup()


if __name__ == "__main__":
    launch_graphscope()
