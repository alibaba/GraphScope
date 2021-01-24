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
import hashlib
import json
import logging
import os
import queue
import random
import signal
import string
import sys
import threading
import time
import urllib.request
from concurrent import futures
from io import StringIO

import grpc

from gscoordinator.io_utils import StdoutWrapper

# capture system stdout
sys.stdout = StdoutWrapper(sys.stdout)

from graphscope.proto import attr_value_pb2
from graphscope.proto import coordinator_service_pb2_grpc
from graphscope.proto import engine_service_pb2_grpc
from graphscope.proto import error_codes_pb2
from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

from gscoordinator.cluster import KubernetesClusterLauncher
from gscoordinator.launcher import LocalLauncher
from gscoordinator.object_manager import GraphMeta
from gscoordinator.object_manager import LibMeta
from gscoordinator.object_manager import ObjectManager
from gscoordinator.utils import compile_app
from gscoordinator.utils import compile_graph_frame
from gscoordinator.utils import create_single_op_dag
from gscoordinator.utils import distribute_lib_on_k8s
from gscoordinator.utils import distribute_lib_via_hosts
from gscoordinator.utils import dump_string
from gscoordinator.utils import generate_graph_type_sig
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
MAXGRAPH_MANAGER_HOST = "http://%s.%s.svc.cluster.local:8080"

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

    def __init__(self, launcher, dangling_seconds, log_level="INFO"):
        self._launcher = launcher

        self._request = None
        self._object_manager = ObjectManager()
        self._dangling_detecting_timer = None
        self._config_logging(log_level)

        # only one connection is allowed at the same time
        self._session_id = "session_" + "".join(
            [random.choice(string.ascii_lowercase) for _ in range(8)]
        )

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
        if self._launcher_type == types_pb2.K8S:
            self._pods_list = self._launcher.get_pods_list()
            self._k8s_namespace = self._launcher.get_namespace()
            self._gie_graph_manager_service_name = (
                self._launcher.get_gie_graph_manager_service_name()
            )
        else:
            self._pods_list = []  # locally launched

        # analytical engine
        self._analytical_engine_stub = self._create_grpc_stub()
        self._analytical_engine_config = None
        self._analytical_engine_endpoint = None

        self._builtin_workspace = os.path.join(WORKSPACE, "builtin")
        self._udf_app_workspace = os.path.join(WORKSPACE, self._session_id)

        # control log fetching
        self._closed = False

        # dangling check
        self._dangling_seconds = dangling_seconds
        if (self._dangling_seconds != -1):
            self._dangling_detecting_timer = threading.Timer(
                interval=self._dangling_seconds, function=self._cleanup, args=(True,)
            )
            self._dangling_detecting_timer.start()

        atexit.register(self._cleanup)

    def __del__(self):
        self._cleanup()

    def _config_logging(self, log_level):
        """Set log level basic on config.
        Args:
            log_level (str): Log level of stdout handler
        """
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
            return self._make_response(
                message_pb2.ConnectSessionResponse,
                code=error_codes_pb2.CONNECTION_ERROR,
                error_msg="Cannot setup more than one connection at the same time.",
            )

        # Connect to serving coordinator.
        self._request = request
        self._analytical_engine_config = self._get_engine_config()

        return self._make_response(
            message_pb2.ConnectSessionResponse,
            code=error_codes_pb2.OK,
            session_id=self._session_id,
            engine_config=json.dumps(self._analytical_engine_config),
            pod_name_list=self._pods_list,
        )

    def HeartBeat(self, request, context):
        if self._dangling_seconds != -1:
            # Reset dangling detect timer
            self._dangling_detecting_timer.cancel()
            self._dangling_detecting_timer = threading.Timer(
                interval=self._dangling_seconds, function=self._cleanup, args=(True,)
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

    def RunStep(self, request, context):  # noqa: C901
        # only one op in one step is allowed.
        if len(request.dag_def.op) != 1:
            return self._make_response(
                message_pb2.RunStepResponse,
                error_codes_pb2.INVALID_ARGUMENT_ERROR,
                "Request's op size is not equal to 1.",
            )

        op = request.dag_def.op[0]

        # Compile app or not.
        if op.op == types_pb2.CREATE_APP:
            try:
                op, app_sig, app_lib_path = self._maybe_compile_app(op)
            except Exception as e:
                error_msg = "Failed to compile app: {}".format(str(e))
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COMPILATION_ERROR,
                    error_msg,
                    op,
                )

        # If engine crashed, we will get a SocketClosed grpc Exception.
        # In that case, we should notify client the engine is dead.

        # Compile graph or not
        # arrow property graph and project graph need to compile
        if (
            (
                op.op == types_pb2.CREATE_GRAPH
                and op.attr[types_pb2.GRAPH_TYPE].graph_type == types_pb2.ARROW_PROPERTY
            )
            or op.op == types_pb2.TRANSFORM_GRAPH
            or op.op == types_pb2.PROJECT_GRAPH
        ):
            try:
                op = self._maybe_register_graph(op, request.session_id)
            except grpc.RpcError as e:
                logger.error("self._launcher.poll() = %s", self._launcher.poll())
                if self._launcher.poll() is not None:
                    message = "Analytical engine exited with %s" % self._launcher.poll()
                else:
                    message = str(e)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.FATAL_ERROR,
                    message,
                    op,
                )
            except Exception as e:
                error_msg = "Graph compile error: {}".format(str(e))
                logger.error(error_msg)
                return self._make_response(
                    message_pb2.RunStepResponse,
                    error_codes_pb2.COMPILATION_ERROR,
                    error_msg,
                    op,
                )

        try:
            response = self._analytical_engine_stub.RunStep(request)
        except grpc.RpcError as e:
            logger.error("self._launcher.poll() = %s", self._launcher.poll())
            if self._launcher.poll() is not None:
                message = "Analytical engine exited with %s" % self._launcher.poll()
            else:
                message = str(e)
            return self._make_response(
                message_pb2.RunStepResponse, error_codes_pb2.FATAL_ERROR, message, op
            )
        except Exception as e:
            return self._make_response(
                message_pb2.RunStepResponse, error_codes_pb2.UNKNOWN, str(e), op
            )

        if response.status.code == error_codes_pb2.OK:
            if op.op == types_pb2.CREATE_GRAPH:
                schema_path = os.path.join("/tmp", response.graph_def.key + ".json")
                self._object_manager.put(
                    response.graph_def.key,
                    GraphMeta(
                        response.graph_def.key,
                        response.graph_def.schema_def,
                        schema_path,
                    ),
                )
                if response.graph_def.graph_type == types_pb2.ARROW_PROPERTY:
                    dump_string(
                        to_maxgraph_schema(
                            response.graph_def.schema_def.property_schema_json
                        ),
                        schema_path,
                    )
                    response.graph_def.schema_path = schema_path
            elif op.op == types_pb2.CREATE_APP:
                self._object_manager.put(
                    app_sig,
                    LibMeta(response.result.decode("utf-8"), "app", app_lib_path),
                )
            elif op.op == types_pb2.UNLOAD_GRAPH:
                self._object_manager.pop(op.attr[types_pb2.GRAPH_NAME].s.decode())
            elif op.op == types_pb2.UNLOAD_APP:
                self._object_manager.pop(op.attr[types_pb2.APP_NAME].s.decode())

        return response

    def _maybe_compile_app(self, op):
        app_sig = self._generate_app_sig(op.attr)
        if app_sig in self._object_manager:
            app_lib_path = self._object_manager.get(app_sig).lib_path
        else:
            app_lib_path = self._compile_lib_and_distribute(compile_app, app_sig, op)

        op.attr[types_pb2.APP_LIBRARY_PATH].CopyFrom(
            attr_value_pb2.AttrValue(s=app_lib_path.encode("utf-8"))
        )
        return op, app_sig, app_lib_path

    def _maybe_register_graph(self, op, session_id):
        graph_sig = self._generate_graph_sig(op.attr)
        if graph_sig in self._object_manager:
            lib_meta = self._object_manager.get(graph_sig)
            graph_lib_path = lib_meta.lib_path
        else:
            graph_lib_path = self._compile_lib_and_distribute(
                compile_graph_frame, graph_sig, op
            )

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
                    LibMeta(register_response.result, "graph_frame", graph_lib_path),
                )
            else:
                raise RuntimeError("Error occur when register graph")
        op.attr[types_pb2.TYPE_SIGNATURE].CopyFrom(
            attr_value_pb2.AttrValue(s=graph_sig.encode("utf-8"))
        )
        return op

    def FetchLogs(self, request, context):
        while not self._closed:
            try:
                message = sys.stdout.poll(timeout=3)
            except queue.Empty:
                pass
            else:
                yield self._make_response(
                    message_pb2.FetchLogsResponse, error_codes_pb2.OK, message=message
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

        if not self._closed:
            self._cleanup()
            self._request = None
            self._closed = True
            return self._make_response(
                message_pb2.CloseSessionResponse, error_codes_pb2.OK
            )

    def CreateInteractiveInstance(self, request, context):
        logger.info(
            "Coordinator create interactive instance with object id %ld",
            request.object_id,
        )
        object_id = request.object_id
        manager_host = MAXGRAPH_MANAGER_HOST % (
            self._gie_graph_manager_service_name,
            self._k8s_namespace,
        )
        with open(request.schema_path) as file:
            schema_json = file.read()
            post_url = "%s/instance/create" % manager_host
            params = {
                "graphName": "%s" % object_id,
                "schemaJson": schema_json,
                "podNameList": ",".join(self._pods_list),
                "containerName": ENGINE_CONTAINER,
            }
            post_data = urllib.parse.urlencode(params).encode("utf-8")
            create_res = urllib.request.urlopen(url=post_url, data=post_data)
            res_json = json.load(create_res)
            error_code = res_json["errorCode"]
            if error_code == 0:
                front_host = res_json["frontHost"]
                front_port = res_json["frontPort"]
                logger.info(
                    "build frontend %s:%d for graph %ld",
                    front_host,
                    front_port,
                    object_id,
                )
                return message_pb2.CreateInteractiveResponse(
                    status=message_pb2.ResponseStatus(code=error_codes_pb2.OK),
                    frontend_host=front_host,
                    frontend_port=front_port,
                    object_id=object_id,
                )
            else:
                error_message = (
                    "create interactive instance for object id %ld failed with error code %d message %s"
                    % (object_id, error_code, res_json["errorMessage"])
                )
                logger.error(error_message)
                return message_pb2.CreateInteractiveResponse(
                    status=message_pb2.ResponseStatus(
                        code=error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR,
                        error_msg=error_message,
                    ),
                    frontend_host="",
                    frontend_port=0,
                    object_id=object_id,
                )

    def CloseInteractiveInstance(self, request, context):
        object_id = request.object_id
        pod_name_list = ",".join(self._pods_list)
        manager_host = MAXGRAPH_MANAGER_HOST % (
            self._gie_graph_manager_service_name,
            self._k8s_namespace,
        )
        close_url = (
            "%s/instance/close?graphName=%ld&podNameList=%s&containerName=%s"
            % (manager_host, object_id, pod_name_list, ENGINE_CONTAINER)
        )
        logger.info("Coordinator close interactive instance with url[%s]" % close_url)
        try:
            close_res = urllib.request.urlopen(close_url).read()
        except Exception as e:
            logger.error("Failed to close interactive instance: %s", e)
            return message_pb2.CloseInteractiveResponse(
                status=message_pb2.ResponseStatus(
                    code=error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR,
                    error_msg="Internal error during close interactive instance: %d, %s"
                    % (400, e),
                )
            )
        res_json = json.loads(close_res.decode("utf-8", errors="ignore"))
        error_code = res_json["errorCode"]
        if 0 == error_code:
            return message_pb2.CloseInteractiveResponse(
                status=message_pb2.ResponseStatus(code=error_codes_pb2.OK)
            )
        else:
            error_message = (
                "Failed to close interactive instance for object id %ld with error code %d message %s"
                % (object_id, error_code, res_json["errorMessage"])
            )
            logger.error("Failed to close interactive instance: %s", error_message)
            return message_pb2.CloseInteractiveResponse(
                status=message_pb2.ResponseStatus(
                    code=error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR,
                    error_msg=error_message,
                )
            )

    def CreateLearningInstance(self, request, context):
        logger.info(
            "Coordinator create learning instance with object id %ld",
            request.object_id,
        )
        object_id = request.object_id
        handle = request.handle
        config = request.config
        endpoints = self._launcher.create_learning_instance(object_id, handle, config)
        return message_pb2.CreateLearningInstanceResponse(
            status=message_pb2.ResponseStatus(code=error_codes_pb2.OK),
            endpoints=",".join(endpoints),
        )

    def CloseLearningInstance(self, request, context):
        logger.info(
            "Coordinator close learning instance with object id %ld",
            request.object_id,
        )
        self._launcher.close_learning_instance(request.object_id)
        return message_pb2.CloseLearningInstanceResponse(
            status=message_pb2.ResponseStatus(code=error_codes_pb2.OK)
        )

    @staticmethod
    def _make_response(resp_cls, code, error_msg="", op=None, **args):
        resp = resp_cls(
            status=message_pb2.ResponseStatus(code=code, error_msg=error_msg), **args
        )
        if op:
            resp.status.op.CopyFrom(op)
        return resp

    def _cleanup(self, is_dangling=False):
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

            if unload_type:
                dag_def = create_single_op_dag(unload_type, config)
                request = message_pb2.RunStepRequest(
                    session_id=self._session_id, dag_def=dag_def
                )
                self._analytical_engine_stub.RunStep(request)

        self._object_manager.clear()
        self._analytical_engine_stub = None

        # cancel dangling detect timer
        if self._dangling_detecting_timer:
            self._dangling_detecting_timer.cancel()

        # close engines
        self._launcher.stop(is_dangling=is_dangling)

        self._session_id = None
        self._analytical_engine_endpoint = None

    def _create_grpc_stub(self):
        options = [
            ("grpc.max_send_message_length", 2147483647),
            ("grpc.max_receive_message_length", 2147483647),
        ]

        channel = grpc.insecure_channel(
            self._launcher.get_analytical_engine_endpoint(), options=options
        )
        return engine_service_pb2_grpc.EngineServiceStub(channel)

    def _generate_app_sig(self, attr):
        return hashlib.sha256(
            attr[types_pb2.APP_SIGNATURE].s + attr[types_pb2.GRAPH_SIGNATURE].s
        ).hexdigest()

    def _generate_graph_sig(self, attr: dict):
        graph_signature = generate_graph_type_sig(attr)
        return hashlib.sha256(graph_signature.encode("utf-8")).hexdigest()

    def _get_engine_config(self):
        op_def = op_def_pb2.OpDef(op=types_pb2.GET_ENGINE_CONFIG)
        dag_def = op_def_pb2.DagDef()
        dag_def.op.extend([op_def])
        fetch_request = message_pb2.RunStepRequest(
            session_id=self._session_id, dag_def=dag_def
        )
        fetch_response = self._analytical_engine_stub.RunStep(fetch_request)
        config = json.loads(fetch_response.result.decode("utf-8"))
        if self._launcher_type == types_pb2.K8S:
            config["vineyard_service_name"] = self._launcher.get_vineyard_service_name()
            config["vineyard_rpc_endpoint"] = self._launcher.get_vineyard_rpc_endpoint()
        else:
            config['engine_hosts'] = self._launcher.hosts
        return config

    def _compile_lib_and_distribute(self, compile_func, lib_name, op):
        if self._analytical_engine_config is None:
            # fetch experimental_on compile option from engine
            self._analytical_engine_config = self._get_engine_config()
        space = self._builtin_workspace
        if types_pb2.GAR in op.attr:
            space = self._udf_app_workspace
        app_lib_path = compile_func(
            space, lib_name, op.attr, self._analytical_engine_config
        )
        if self._launcher_type == types_pb2.K8S:
            distribute_lib_on_k8s(",".join(self._pods_list), app_lib_path)
        else:
            distribute_lib_via_hosts(self._launcher.hosts, app_lib_path)
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
        "--enable_k8s",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="Deploy graphscope components on kubernetes.",
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
        "--k8s_zookeeper_image",
        type=str,
        default="registry.cn-hongkong.aliyuncs.com/graphscope/zookeeper:3.4.10",
        help="Docker image of zookeeper, used by graph interactive engine.",
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
        "--k8s_vineyard_shared_mem",
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
        "--timeout_seconds",
        type=int,
        default=60,
        help="Launch failed after waiting timeout seconds Or cleanup graphscope instance after seconds of client disconnect.",
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

    if args.enable_k8s:
        launcher = KubernetesClusterLauncher(
            namespace=args.k8s_namespace,
            service_type=args.k8s_service_type,
            gs_image=args.k8s_gs_image,
            etcd_image=args.k8s_etcd_image,
            zookeeper_image=args.k8s_zookeeper_image,
            gie_graph_manager_image=args.k8s_gie_graph_manager_image,
            coordinator_name=args.k8s_coordinator_name,
            coordinator_service_name=args.k8s_coordinator_service_name,
            engine_cpu=args.k8s_engine_cpu,
            engine_mem=args.k8s_engine_mem,
            vineyard_cpu=args.k8s_vineyard_cpu,
            vineyard_mem=args.k8s_vineyard_mem,
            vineyard_shared_mem=args.k8s_vineyard_shared_mem,
            image_pull_policy=args.k8s_image_pull_policy,
            image_pull_secrets=args.k8s_image_pull_secrets,
            num_workers=args.num_workers,
            instance_id=args.instance_id,
            log_level=args.log_level,
            timeout_seconds=args.timeout_seconds,
            waiting_for_delete=args.waiting_for_delete,
            delete_namespace=args.k8s_delete_namespace,
        )
    else:
        launcher = LocalLauncher(
            num_workers=args.num_workers,
            hosts=args.hosts,
            vineyard_socket=args.vineyard_socket,
            log_level=args.log_level,
            timeout_seconds=args.timeout_seconds,
        )

    coordinator_service_servicer = CoordinatorServiceServicer(
        launcher=launcher,
        dangling_seconds=args.timeout_seconds,
        log_level=args.log_level,
    )

    # after GraphScope ready, fetch logs via gRPC.
    sys.stdout.drop(False)

    # register gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(os.cpu_count() or 1))
    coordinator_service_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        coordinator_service_servicer, server
    )
    server.add_insecure_port("0.0.0.0:{}".format(args.port))
    logger.info("Coordinator server listen at 0.0.0.0:%d", args.port)

    server.start()
    try:
        # Grpc has handled SIGINT/SIGTERM
        server.wait_for_termination()
    except KeyboardInterrupt:
        del coordinator_service_servicer


if __name__ == "__main__":
    launch_graphscope()
