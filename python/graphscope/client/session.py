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

""" Manage sessions to the GraphScope coordinator.
"""

import atexit
import base64
import contextlib
import json
import logging
import os
import pickle
import signal
import threading
import time
import uuid
import warnings

try:
    from kubernetes import client as kube_client
    from kubernetes import config as kube_config
except ImportError:
    kube_client = None
    kube_config = None

import graphscope
from graphscope.analytical.udf.utils import InMemoryZip
from graphscope.client.archive import OutArchive
from graphscope.client.rpc import GRPCClient
from graphscope.client.utils import CaptureKeyboardInterrupt
from graphscope.client.utils import GSLogger
from graphscope.client.utils import SignalIgnore
from graphscope.client.utils import set_defaults
from graphscope.config import GSConfig as gs_config
from graphscope.deploy.hosts.cluster import HostsClusterLauncher
from graphscope.deploy.kubernetes.cluster import KubernetesClusterLauncher
from graphscope.deploy.kubernetes.utils import resolve_api_client
from graphscope.framework.dag import Dag
from graphscope.framework.errors import FatalError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import K8sError
from graphscope.framework.graph import Graph
from graphscope.framework.graph import GraphDAGNode
from graphscope.framework.operation import Operation
from graphscope.framework.utils import decode_dataframe
from graphscope.framework.utils import decode_numpy
from graphscope.interactive.query import InteractiveQuery
from graphscope.proto import graph_def_pb2
from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

DEFAULT_CONFIG_FILE = os.environ.get(
    "GS_CONFIG_PATH", os.path.expanduser("~/.graphscope/session.json")
)

_session_dict = {}

logger = logging.getLogger("graphscope")


class _FetchHandler(object):
    """Handler for structured fetches.
    This class takes care of extracting a sub-DAG as targets for a user-provided structure for fetches,
    which can be used for a low level `run` call of grpc_client.

    Given the results of the low level run call, this class can also rebuild a result structure matching
    the user-provided structure for fetches, but containing the corresponding results.
    """

    def __init__(self, dag, fetches):
        self._fetches = fetches
        self._ops = list()
        self._unpack = False
        if not isinstance(self._fetches, (list, tuple)):
            self._fetches = [self._fetches]
            self._unpack = True
        for fetch in self._fetches:
            if hasattr(fetch, "op"):
                fetch = fetch.op
            if not isinstance(fetch, Operation):
                raise ValueError("Expect an `Operation` in sess run method.")
            self._ops.append(fetch)
        # extract sub dag
        self._sub_dag = dag.extract_subdag_for(self._ops)
        if "GRAPHSCOPE_DEBUG" in os.environ:
            logger.info("sub_dag: %s", self._sub_dag)

    @property
    def targets(self):
        return self._sub_dag

    def _rebuild_graph(self, seq, op_result: op_def_pb2.OpResult):
        if isinstance(self._fetches[seq], Operation):
            # for nx Graph
            return op_result.graph_def
        # get graph dag node as base
        graph_dag_node = self._fetches[seq]
        # construct graph
        g = Graph(graph_dag_node)
        # update graph flied from graph_def
        g.update_from_graph_def(op_result.graph_def)
        return g

    def _rebuild_app(self, seq, op_result: op_def_pb2.OpResult):
        from graphscope.framework.app import App

        # get app dag node as base
        app_dag_node = self._fetches[seq]
        # construct app
        app = App(app_dag_node, op_result.result.decode("utf-8"))
        return app

    def _rebuild_context(self, seq, op_result: op_def_pb2.OpResult):
        from graphscope.framework.context import Context

        # get context dag node as base
        context_dag_node = self._fetches[seq]
        ret = json.loads(op_result.result.decode("utf-8"))
        context_type = ret["context_type"]
        if context_type == "dynamic_vertex_data":
            # for nx
            from graphscope.framework.context import DynamicVertexDataContext

            return DynamicVertexDataContext(context_dag_node, ret["context_key"])
        return Context(context_dag_node, ret["context_key"], ret["context_schema"])

    def _rebuild_gremlin_results(self, seq, op_result: op_def_pb2.OpResult):
        from graphscope.interactive.query import ResultSet

        # get result set node as base
        result_set_dag_node = self._fetches[seq]
        return ResultSet(result_set_dag_node)

    def wrap_results(self, response: message_pb2.RunStepResponse):  # noqa: C901
        rets = list()
        for seq, op in enumerate(self._ops):
            for op_result in response.results:
                if op.key == op_result.key:
                    if op.output_types == types_pb2.RESULTS:
                        if op.type == types_pb2.RUN_APP:
                            rets.append(self._rebuild_context(seq, op_result))
                        elif op.type == types_pb2.FETCH_GREMLIN_RESULT:
                            rets.append(pickle.loads(op_result.result))
                        elif op.type == types_pb2.REPORT_GRAPH:
                            rets.append(OutArchive(op_result.result))
                        else:
                            # for nx Graph
                            rets.append(op_result.result.decode("utf-8"))
                    if op.output_types == types_pb2.GREMLIN_RESULTS:
                        rets.append(self._rebuild_gremlin_results(seq, op_result))
                    if op.output_types == types_pb2.GRAPH:
                        rets.append(self._rebuild_graph(seq, op_result))
                    if op.output_types == types_pb2.APP:
                        rets.append(None)
                    if op.output_types == types_pb2.BOUND_APP:
                        rets.append(self._rebuild_app(seq, op_result))
                    if op.output_types in (
                        types_pb2.VINEYARD_TENSOR,
                        types_pb2.VINEYARD_DATAFRAME,
                    ):
                        rets.append(
                            json.loads(op_result.result.decode("utf-8"))["object_id"]
                        )
                    if op.output_types in (types_pb2.TENSOR, types_pb2.DATAFRAME):
                        if (
                            op.type == types_pb2.CONTEXT_TO_DATAFRAME
                            or op.type == types_pb2.GRAPH_TO_DATAFRAME
                        ):
                            rets.append(decode_dataframe(op_result.result))
                        if (
                            op.type == types_pb2.CONTEXT_TO_NUMPY
                            or op.type == types_pb2.GRAPH_TO_NUMPY
                        ):
                            rets.append(decode_numpy(op_result.result))
                    if op.output_types == types_pb2.NULL_OUTPUT:
                        rets.append(None)
                    break
        return rets[0] if rets and self._unpack else rets

    def get_dag_for_unload(self):
        """Unload operations (graph, app, context) in dag which are not
        existed in fetches.
        """
        unload_dag = op_def_pb2.DagDef()
        keys_of_fetches = set([op.key for op in self._ops])
        mapping = {
            types_pb2.CREATE_GRAPH: types_pb2.UNLOAD_GRAPH,
            types_pb2.CREATE_APP: types_pb2.UNLOAD_APP,
            types_pb2.RUN_APP: types_pb2.UNLOAD_CONTEXT,
        }
        for op_def in self._sub_dag.op:
            if op_def.op in mapping and op_def.key not in keys_of_fetches:
                unload_op_def = op_def_pb2.OpDef(
                    op=mapping[op_def.op], key=uuid.uuid4().hex
                )
                unload_op_def.parents.extend([op_def.key])
                unload_dag.op.extend([unload_op_def])
        return unload_dag


class Session(object):
    """A class for interacting with GraphScope graph computation service cluster.

    A :class:`Session` object encapsulates the environment in which :class:`Operation`
    objects are executed/evaluated.

    A session may own resources. It is important to release these resources when
    they are no longer required. To do this, invoke the :meth:`close` method
    on the session.

    A Session can register itself as default session with :meth:`as_default`, and all operations
    after that will use the default session. Session deregister itself as a default session
    when closed.

    The following example demonstrates its usage:

    .. code:: python

        >>> import graphscope as gs

        >>> # use session object explicitly
        >>> sess = gs.session()
        >>> g = sess.g()
        >>> pg = g.project(vertices={'v': []}, edges={'e': ['dist']})
        >>> r = gs.sssp(g, 4)
        >>> sess.close()

        >>> # or use a session as default
        >>> sess = gs.session().as_default()
        >>> g = gs.g()
        >>> pg = g.project(vertices={'v': []}, edges={'e': ['dist']})
        >>> r = gs.sssp(pg, 4)
        >>> sess.close()

    We support setup a service cluster and create a RPC session in following ways:

    - GraphScope graph computation service run in cluster managed by kubernetes.

        >>> s = graphscope.session()

        Also, :class:`Session` provides several keyword params for users to define the cluster.
        You may use the param :code:`k8s_gs_image` to specify the image for all engine pod, and
        param :code:`k8s_engine_cpu` or :code:`k8s_engine_mem` to specify the resources. More,
        you can find all params detail in :meth:`__init__` method.

        >>> s = graphscope.session(
        ...         k8s_vineyard_cpu=0.1,
        ...         k8s_vineyard_mem="256Mi",
        ...         vineyard_shared_mem="4Gi",
        ...         k8s_engine_cpu=0.1,
        ...         k8s_engine_mem="256Mi")

    - or all params can be provided by a json configuration file or configuration dict.

        >>> s = graphscope.session(config='/tmp/config.json')
        >>> # Or
        >>> s = graphscope.session(config={'k8s_engine_cpu': 5, 'k8s_engine_mem': '5Gi'})
    """

    @set_defaults(gs_config)
    def __init__(
        self,
        config=None,
        addr=gs_config.addr,
        mode=gs_config.mode,
        cluster_type=gs_config.cluster_type,
        num_workers=gs_config.num_workers,
        preemptive=gs_config.preemptive,
        k8s_namespace=gs_config.k8s_namespace,
        k8s_service_type=gs_config.k8s_service_type,
        k8s_image_registry=gs_config.k8s_image_registry,
        k8s_image_repository=gs_config.k8s_image_repository,
        k8s_image_tag=gs_config.k8s_image_tag,
        k8s_image_pull_policy=gs_config.k8s_image_pull_policy,
        k8s_image_pull_secrets=gs_config.k8s_image_pull_secrets,
        k8s_coordinator_cpu=gs_config.k8s_coordinator_cpu,
        k8s_coordinator_mem=gs_config.k8s_coordinator_mem,
        etcd_addrs=gs_config.etcd_addrs,
        etcd_listening_client_port=gs_config.etcd_listening_client_port,
        etcd_listening_peer_port=gs_config.etcd_listening_peer_port,
        k8s_vineyard_image=gs_config.k8s_vineyard_image,
        k8s_vineyard_daemonset=gs_config.k8s_vineyard_daemonset,
        k8s_vineyard_cpu=gs_config.k8s_vineyard_cpu,
        k8s_vineyard_mem=gs_config.k8s_vineyard_mem,
        vineyard_shared_mem=gs_config.vineyard_shared_mem,
        k8s_engine_cpu=gs_config.k8s_engine_cpu,
        k8s_engine_mem=gs_config.k8s_engine_mem,
        k8s_mars_worker_cpu=gs_config.mars_worker_cpu,
        k8s_mars_worker_mem=gs_config.mars_worker_mem,
        k8s_mars_scheduler_cpu=gs_config.mars_scheduler_cpu,
        k8s_mars_scheduler_mem=gs_config.mars_scheduler_mem,
        k8s_coordinator_pod_node_selector=gs_config.k8s_coordinator_pod_node_selector,
        k8s_engine_pod_node_selector=gs_config.k8s_engine_pod_node_selector,
        k8s_volumes=gs_config.k8s_volumes,
        k8s_waiting_for_delete=gs_config.k8s_waiting_for_delete,
        timeout_seconds=gs_config.timeout_seconds,
        dangling_timeout_seconds=gs_config.dangling_timeout_seconds,
        with_mars=gs_config.with_mars,
        with_analytical=gs_config.with_analytical,
        with_analytical_java=gs_config.with_analytical_java,
        with_interactive=gs_config.with_interactive,
        with_learning=gs_config.with_learning,
        with_dataset=gs_config.with_dataset,
        reconnect=False,
        hosts=["localhost"],
        **kw,
    ):
        """Construct a new GraphScope session.

        Args:
            config (dict or str, optional): The configuration dict or file about how to launch the GraphScope instance.
                For str, it will identify it as a path and read the configuration file to build a
                session if file exist. If not specified, the global default configuration
                :code:`DEFAULT_CONFIG_FILE` will be used, which get value of GS_CONFIG_PATH
                in environment. Note that it will overwrite explicit parameters. Defaults to None.

            addr (str, optional): The endpoint of a pre-launched GraphScope instance with '<ip>:<port>' format.
                A new session id will be generated for each session connection.

            mode (str, optional): optional values are eager and lazy. Defaults to eager.
                Eager execution is a flexible platform for research and experimentation, it provides:
                    An intuitive interface: Quickly test on small data.
                    Easier debugging: Call ops directly to inspect running models and test changes.
                Lazy execution means GraphScope does not process the data till it has to. It just gathers all the
                    information to a DAG that we feed into it, and processes only when we execute :code:`sess.run(fetches)`

            cluster_type (str, optional): Deploy GraphScope instance on hosts or k8s cluster. Defaults to k8s.
                Available options: "k8s" and "hosts". Note that only support deployed on localhost with hosts mode.

            num_workers (int, optional): The number of workers to launch GraphScope engine. Defaults to 2.

            preemptive (bool, optional): If True, GraphScope instance will treat resource params (e.g. k8s_coordinator_cpu)
                as limits and provide the minimum available value as requests, but this will make pod has a `Burstable` QOS,
                which can be preempted by other pods with high QOS. Otherwise, it will set both requests and limits with the
                same value.

            k8s_namespace (str, optional): Contains the namespace to create all resource inside.
                If param missing, it will try to read namespace from kubernetes context, or
                a random namespace will be created and deleted if namespace not exist.
                Defaults to None.

            k8s_service_type (str, optional): Type determines how the GraphScope service is exposed.
                Valid options are NodePort, and LoadBalancer. Defaults to NodePort.

            k8s_image_registry (str, optional): The GraphScope image registry.

            k8s_image_repository (str, optional): The GraphScope image repository.

            k8s_image_tag (str, optional): The GraphScope image tag.

            k8s_image_pull_policy (str, optional): Kubernetes image pull policy. Defaults to "IfNotPresent".

            k8s_image_pull_secrets (list[str], optional): A list of secret name used to authorize pull image.

            k8s_vineyard_image (str, optional): The image of vineyard.

            k8s_vineyard_daemonset (str, optional): The name of vineyard Helm deployment to use. GraphScope will try to
                discovery the daemonset from kubernetes cluster, then use it if exists, and fallback to launching
                a bundled vineyard container otherwise.

            k8s_vineyard_cpu (float, optional): Minimum number of CPU cores request for vineyard container. Defaults to 0.5.

            k8s_vineyard_mem (str, optional): Minimum number of memory request for vineyard container. Defaults to '512Mi'.

            vineyard_shared_mem (str, optional): Init size of vineyard shared memory. Defaults to '4Gi'.

            k8s_engine_cpu (float, optional): Minimum number of CPU cores request for engine container. Defaults to 0.5.

            k8s_engine_mem (str, optional): Minimum number of memory request for engine container. Defaults to '4Gi'.

            k8s_coordinator_cpu (float, optional): Minimum number of CPU cores request for coordinator pod. Defaults to 1.0.

            k8s_coordinator_mem (str, optional): Minimum number of memory request for coordinator pod. Defaults to '4Gi'.

            etcd_addrs (str, optional): The addr of external etcd cluster,
                with formats like 'etcd01:port,etcd02:port,etcd03:port'

            k8s_mars_worker_cpu (float, optional):
                Minimum number of CPU cores request for mars worker container. Defaults to 0.5.

            k8s_mars_worker_mem (str, optional):
                Minimum number of memory request for mars worker container. Defaults to '4Gi'.

            k8s_mars_scheduler_cpu (float, optional):
                Minimum number of CPU cores request for mars scheduler container. Defaults to 0.5.

            k8s_mars_scheduler_mem (str, optional):
                Minimum number of memory request for mars scheduler container. Defaults to '2Gi'.

            k8s_coordinator_pod_node_selector (dict, optional):
                Node selector to the coordinator pod on k8s. Default is None.
                See also: https://tinyurl.com/3nx6k7ph

            k8s_engine_pod_node_selector = None
                Node selector to the engine pod on k8s. Default is None.
                See also: https://tinyurl.com/3nx6k7ph

            with_mars (bool, optional):
                Launch graphscope with mars. Defaults to False.

            with_analytical (bool, optional):
                Launch graphscope with analytical engine. Defaults to True.

            with_analytical_java (bool, optional):
                Launch graphscope with analytical engine with java support. Defaults to False.

            with_interactive (bool, optional):
                Launch graphscope with interactive engine. Defaults to True.

            with_learning (bool, optional):
                Launch graphscope with learning engine. Defaults to True.

            with_dataset (bool, optional):
                Create a container and mount aliyun demo dataset bucket to the path `/dataset`.

            k8s_volumes (dict, optional): A dict of k8s volume which represents a directory containing data, accessible to the
                containers in a pod. Defaults to {}.

                For example, you can mount host path with:

                k8s_volumes = {
                    "my-data": {
                        "type": "hostPath",
                        "field": {
                            "path": "<path>",
                            "type": "Directory"
                        },
                        "mounts": [
                            {
                                "mountPath": "<path1>"
                            },
                            {
                                "mountPath": "<path2>"
                            }
                        ]
                    }
                }

                Or you can mount PVC with:

                k8s_volumes = {
                    "my-data": {
                        "type": "persistentVolumeClaim",
                        "field": {
                            "claimName": "your-pvc-name"
                        },
                        "mounts": [
                            {
                                "mountPath": "<path1>"
                            }
                        ]
                    }
                }

                Also, you can mount a single volume with:

                k8s_volumes = {
                    "my-data": {
                        "type": "hostPath",
                        "field": {xxx},
                        "mounts": {
                            "mountPath": "<path1>"
                        }
                    }
                }

            timeout_seconds (int, optional): For waiting service ready (or waiting for delete if
                k8s_waiting_for_delete is True).

            dangling_timeout_seconds (int, optional): After seconds of client disconnect,
                coordinator will kill this graphscope instance. Defaults to 600.
                Expect this value to be greater than 5 (heartbeat interval).
                Disable dangling check by setting -1.

            k8s_waiting_for_delete (bool, optional): Waiting for service delete or not. Defaults to False.

            **kw (dict, optional): Other optional parameters will be put to :code:`**kw`.
                - k8s_client_config (dict, optional):
                    Provide configurable parameters for connecting to remote k8s,
                    which strongly relies on the `kube_config.load_kube_config` function.
                    eg: {
                        "config_file": "~/.kube/config",
                        "context": None,
                        "client_configuration": None,
                        "persist_config": True
                    }
                    config_file: Name of the kube-config file.
                    context: set the active context. If is set to None, current_context from config file will be used.
                    client_configuration: The kubernetes.client.Configuration to set configs to.
                    persist_config: If True, config file will be updated when changed(e.g GCP token refresh).

                - log_level: Deprecated.
                    Move this param as a global configuration. Set via `graphscope.set_option(log_level='DEBUG')`

                - show_log: Deprecated.
                    Move this param as a global configuration.Set via `graphscope.set_option(show_log=True)`

            reconnect (bool, optional): When connecting to a pre-launched GraphScope cluster with :code:`addr`,
                the connect request would be rejected with there is still an existing session connected. There
                are cases where the session still exists and user's client has lost connection with the backend,
                e.g., in a jupyter notebook. We have a :code:`dangling_timeout_seconds` for it, but a more
                deterministic behavior would be better.

                If :code:`reconnect` is True, the existing session will be reused. It is the user's responsibility
                to ensure there's no such an active client.

                Defaults to :code:`False`.

        Raises:
            TypeError: If the given argument combination is invalid and cannot be used to create
                a GraphScope session.
        """

        # supress the grpc warnings, see also grpc/grpc#29103
        os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "false"

        self._config_params = {}
        self._accessable_params = (
            "addr",
            "mode",
            "cluster_type",
            "num_workers",
            "preemptive",
            "k8s_namespace",
            "k8s_service_type",
            "k8s_image_registry",
            "k8s_image_repository",
            "k8s_image_tag",
            "k8s_image_pull_policy",
            "k8s_image_pull_secrets",
            "k8s_coordinator_cpu",
            "k8s_coordinator_mem",
            "etcd_addrs",
            "etcd_listening_client_port",
            "etcd_listening_peer_port",
            "k8s_vineyard_image",
            "k8s_vineyard_daemonset",
            "k8s_vineyard_cpu",
            "k8s_vineyard_mem",
            "vineyard_shared_mem",
            "k8s_engine_cpu",
            "k8s_engine_mem",
            "k8s_mars_worker_cpu",
            "k8s_mars_worker_mem",
            "k8s_mars_scheduler_cpu",
            "k8s_mars_scheduler_mem",
            "k8s_coordinator_pod_node_selector",
            "k8s_engine_pod_node_selector",
            "with_mars",
            "with_analytical",
            "with_analytical_java",
            "with_interactive",
            "with_learning",
            "reconnect",
            "k8s_volumes",
            "k8s_waiting_for_delete",
            "timeout_seconds",
            "dangling_timeout_seconds",
            "with_dataset",
            "hosts",
        )
        self._deprecated_params = (
            "show_log",
            "log_level",
        )
        saved_locals = locals()
        for param in self._accessable_params:
            self._config_params[param] = saved_locals[param]

        # parse config, which should be a path to config file, or dict
        # config has the highest priority
        if isinstance(config, dict):
            self._config_params.update(config)
        elif isinstance(config, str):
            self._load_config(config, silent=False)
        elif DEFAULT_CONFIG_FILE:
            self._load_config(DEFAULT_CONFIG_FILE)

        # update other optional params
        self._config_params.update(kw)

        # initial setting of cluster_type
        self._cluster_type = self._parse_cluster_type()

        # initial dag
        self._dag = Dag()

        # mars cannot work with run-on-local mode
        if self._cluster_type == types_pb2.HOSTS and self._config_params["with_mars"]:
            raise NotImplementedError(
                "Mars cluster cannot be launched along with local GraphScope deployment"
            )
        if with_analytical and with_analytical_java:
            logger.warning(
                "Cannot setup `with_analytical` and `with_analytical_java` at the same time"
            )
            logger.warning("Disabled `analytical`.")
            self._config_params["with_analytical"] = False

        # deprecated params handle
        for param in self._deprecated_params:
            if param in kw:
                warnings.warn(
                    f"The `{param}` parameter has been deprecated and has no effect.",
                    category=DeprecationWarning,
                )
                if param == "show_log" or param == "log_level":
                    warnings.warn(
                        f"Please use `graphscope.set_option({param}={kw.pop(param, None)})` instead",
                        category=DeprecationWarning,
                    )
                kw.pop(param, None)

        # update k8s_client_config params
        self._config_params["k8s_client_config"] = kw.pop("k8s_client_config", {})

        # There should be no more custom keyword arguments.
        if kw:
            raise ValueError("Value not recognized: ", list(kw.keys()))

        if self._config_params["addr"]:
            logger.info(
                "Connecting graphscope session with address: %s",
                self._config_params["addr"],
            )
        else:
            logger.info(
                "Initializing graphscope session with parameters: %s",
                self._config_params,
            )

        self._closed = False

        # coordinator service endpoint
        self._coordinator_endpoint = None

        self._launcher = None
        self._heartbeat_sending_thread = None

        self._grpc_client: GRPCClient = None
        self._session_id: str = None  # unique identifier across sessions
        # engine config:
        #
        #   {
        #       "experiment": "ON/OFF",
        #       "vineyard_socket": "...",
        #       "vineyard_rpc_endpoint": "..."
        #   }
        self._engine_config: {} = None

        # interactive instance related graph map
        self._interactive_instance_dict = {}
        # learning engine related graph map
        self._learning_instance_dict = {}

        self._default_session = None

        atexit.register(self.close)
        # create and connect session
        with CaptureKeyboardInterrupt(self.close):
            self._connect()

        self._disconnected: bool = False

        # heartbeat
        self._heartbeat_interval_seconds: int = 5
        self._heartbeat_sending_thread = threading.Thread(
            target=self._send_heartbeat, args=()
        )
        self._heartbeat_sending_thread.daemon = True
        self._heartbeat_sending_thread.start()
        self._heartbeat_maximum_failures: int = 3

        # networkx module
        self._nx = None

    def __repr__(self):
        return str(self.info)

    def __str__(self):
        return repr(self)

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def dag(self):
        return self._dag

    def _load_config(self, path, silent=True):
        config_path = os.path.expandvars(os.path.expanduser(path))
        try:
            with open(config_path, "r") as f:
                data = json.load(f)
                self._config_params.update(data)
        except Exception as exp:  # noqa
            if not silent:
                raise exp

    def _parse_cluster_type(self):
        # get the cluster type after connecting
        cluster_type = types_pb2.UNDEFINED

        if self._config_params["addr"] is None:
            if self._config_params["cluster_type"] == "hosts":
                self._run_on_local()
                cluster_type = types_pb2.HOSTS
            elif self._config_params["cluster_type"] == "k8s":
                cluster_type = types_pb2.K8S
            else:
                raise ValueError("Expect 'hosts' or 'k8s' for cluster_type parameter")
        return cluster_type

    @property
    def engine_config(self):
        """Show the engine configuration associated with session in json format."""
        return self._engine_config

    @property
    def info(self):
        """Show all resource info associated with session in json format."""
        info = {}
        if self._closed:
            info["status"] = "closed"
        elif self._grpc_client is None or self._disconnected:
            info["status"] = "disconnected"
        else:
            info["status"] = "active"

        if self._cluster_type == types_pb2.K8S:
            info["type"] = "k8s"
            info["engine_hosts"] = ",".join(self._pod_name_list)
            info["namespace"] = self._config_params["k8s_namespace"]
        else:
            info["type"] = "hosts"
            info["engine_hosts"] = self._engine_config["engine_hosts"]

        info["cluster_type"] = types_pb2.ClusterType.Name(self._cluster_type)
        info["session_id"] = self.session_id
        info["num_workers"] = self._config_params["num_workers"]
        info["coordinator_endpoint"] = self._coordinator_endpoint
        info["engine_config"] = self._engine_config
        return info

    @property
    def closed(self):
        return self._closed

    def eager(self):
        return self._config_params["mode"] == "eager"

    def _send_heartbeat(self):
        # >1: failure, 0: reset when success
        heartbeat_failure_count = 0
        while not self._closed:
            if self._grpc_client:
                try:
                    self._grpc_client.send_heartbeat()
                except Exception as exc:
                    if heartbeat_failure_count == 0:
                        logger.warning(exc)
                    heartbeat_failure_count = heartbeat_failure_count + 1
                    if heartbeat_failure_count > self._heartbeat_maximum_failures:
                        logger.error(
                            "The connection between coordinator has lost after %d times "
                            "of heartbeat failure, closing the session ...",
                            heartbeat_failure_count,
                        )
                        self.close()
                    self._disconnected = True
                else:
                    heartbeat_failure_count = 0
                    self._disconnected = False
            time.sleep(self._heartbeat_interval_seconds)

    def connected(self) -> bool:
        """Check if the session is still connected and available.

        Returns: True or False

        """
        return not self._disconnected

    def close(self):
        """Closes this session.

        This method frees all resources associated with the session.

        Note that closing will ignore SIGINT and SIGTERM signal and recover later.
        """
        if threading.currentThread() is threading.main_thread():
            with SignalIgnore([signal.SIGINT, signal.SIGTERM]):
                self._close()
        else:
            self._close()

    def _close(self):  # noqa: C901
        if self._closed:
            return
        self._closed = True
        self._coordinator_endpoint = None

        self._unregister_default()

        if self._heartbeat_sending_thread:
            try:
                self._heartbeat_sending_thread.join(
                    timeout=self._heartbeat_interval_seconds
                )
            except RuntimeError:  # ignore the "cannot join current thread" error
                pass
            self._heartbeat_sending_thread = None

        self._disconnected = True

        # close all interactive instances
        for instance in self._interactive_instance_dict.values():
            try:
                instance.close()
            except Exception:
                pass
        self._interactive_instance_dict.clear()

        # close all learning instances
        for instance in self._learning_instance_dict.values():
            try:
                instance.close()
            except Exception:
                pass
        self._learning_instance_dict.clear()

        if self._grpc_client:
            try:
                self._grpc_client.close()
            except Exception:
                pass
            self._grpc_client = None
            _session_dict.pop(self._session_id, None)

        # clean up
        if self._config_params["addr"] is None:
            try:
                if self._launcher:
                    self._launcher.stop()
            except Exception:
                pass
            self._pod_name_list = []

    def _close_interactive_instance(self, instance):
        """Close an interactive instance."""
        self._grpc_client.close_interactive_instance(instance.object_id)

    def _close_learning_instance(self, instance):
        """Close a learning instance."""
        self._grpc_client.close_learning_instance(instance.object_id)

    def __del__(self):
        # cleanly ignore all exceptions
        try:
            self.close()
        except Exception:  # pylint: disable=broad-except
            pass

    def _check_closed(self, msg=None):
        """Internal: raise a ValueError if session is closed"""
        if self.closed:
            raise ValueError(msg or "Operation on closed session.")

    # Context manager
    def __enter__(self):
        """Context management protocol.
        Returns self and register self as default session.
        """
        self._check_closed()
        self.as_default()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Unregister self from the default session,
        close the session and release the resources, ignore all exceptions in close().
        """
        try:
            self._unregister_default()
            self.close()
        except Exception:
            pass

    def as_default(self):
        """Obtain a context manager that make this object as default session.

        This method is used when a Session is constructed, which will immediately
        install self as a default session.

        Raises:
            ValueError: If default session exist in current context.

        Returns:
            A context manager using this session as the default session.
        """
        if not _default_session_stack.is_cleared():
            raise ValueError(
                "A default session is already active. You must explicitly call Session.close()."
            )
        # session context manager
        self._default_session = default_session(self)
        self._default_session.__enter__()

    def _unregister_default(self):
        """Remove self from the default session stack."""
        if self._default_session:
            self._default_session.__exit__(None, None, None)
            self._default_session = None

    def _wrapper(self, dag_node):
        if self.eager():
            return self.run(dag_node)
        return dag_node

    def run(self, fetches):
        """Run operations of `fetches`.
        Args:
            fetches: :class:`Operation`

        Raises:
            RuntimeError:
                Client disconnect to the service. Or run on a closed session.

            ValueError:
                If fetch is not an instance of :class:`Operation`. Or
                the fetch has been evaluated.

            InvalidArgumentError:
                Not recognized on output type.

        Returns:
            Different values for different output types of :class:`Operation`
        """
        return self.run_fetches(fetches)

    def run_fetches(self, fetches):
        """Run operations of `fetches` without the session lock."""
        if self._closed:
            raise RuntimeError("Attempted to use a closed Session.")
        if not self._grpc_client:
            raise RuntimeError("Session disconnected.")
        fetch_handler = _FetchHandler(self.dag, fetches)
        try:
            response = self._grpc_client.run(fetch_handler.targets)
        except FatalError:
            self.close()
            raise
        if not self.eager():
            # Unload operations that cannot be touched anymore
            dag_to_unload = fetch_handler.get_dag_for_unload()
            try:
                self._grpc_client.run(dag_to_unload)
            except FatalError:
                self.close()
                raise
        return fetch_handler.wrap_results(response)

    def _connect(self):
        if self._config_params["addr"] is not None:
            # try to connect to exist coordinator
            self._coordinator_endpoint = self._config_params["addr"]
        elif self._cluster_type == types_pb2.K8S:
            if isinstance(
                self._config_params["k8s_client_config"],
                kube_client.api_client.ApiClient,
            ):
                api_client = self._config_params["k8s_client_config"]
            else:
                try:
                    api_client = resolve_api_client(
                        self._config_params["k8s_client_config"]
                    )
                except kube_config.ConfigException as e:
                    raise RuntimeError(
                        "Kubernetes environment not found, you may want to"
                        ' launch session locally with param cluster_type="hosts"'
                    ) from e
            self._launcher = KubernetesClusterLauncher(
                api_client=api_client,
                **self._config_params,
            )
        elif (
            self._cluster_type == types_pb2.HOSTS
            and isinstance(self._config_params["hosts"], list)
            and len(self._config_params["hosts"]) != 0
            and self._config_params["num_workers"] > 0
        ):
            # launch coordinator with hosts
            self._launcher = HostsClusterLauncher(
                **self._config_params,
            )
        else:
            raise RuntimeError(
                f"Unrecognized cluster type {types_pb2.ClusterType.Name(self._cluster_type)}."
            )

        # launching graphscope service
        if self._launcher is not None:
            self._launcher.start()
            self._coordinator_endpoint = self._launcher.coordinator_endpoint

        # waiting service ready
        self._grpc_client = GRPCClient(
            self._launcher, self._coordinator_endpoint, self._config_params["reconnect"]
        )
        self._grpc_client.waiting_service_ready(
            timeout_seconds=self._config_params["timeout_seconds"],
        )

        # connect and fetch logs from rpc server
        try:
            (
                self._session_id,
                self._cluster_type,
                self._config_params["num_workers"],
                self._config_params["k8s_namespace"],
                self._engine_config,
                pod_name_list,
            ) = self._grpc_client.connect(
                cleanup_instance=not bool(self._config_params["addr"]),
                dangling_timeout_seconds=self._config_params[
                    "dangling_timeout_seconds"
                ],
            )
            self._pod_name_list = list(pod_name_list)

            # fetch logs
            if self._config_params["addr"] or self._cluster_type == types_pb2.K8S:
                self._grpc_client.fetch_logs()
            _session_dict[self._session_id] = self

            # Launch analytical engine right after session connected.
            # This may be changed to on demand launching in the future
            if not self._engine_config and not self._pod_name_list:
                (
                    self._engine_config,
                    pod_name_list,
                ) = self._grpc_client.create_analytical_instance()
                self._pod_name_list = list(pod_name_list)
        except Exception:
            self.close()
            raise

    def get_config(self):
        """Get configuration of the session."""
        return self._config_params

    def g(
        self,
        incoming_data=None,
        oid_type="int64",
        directed=True,
        generate_eid=True,
        vertex_map="global",
    ):
        return self._wrapper(
            GraphDAGNode(
                self, incoming_data, oid_type, directed, generate_eid, vertex_map
            )
        )

    def load_from(self, *args, **kwargs):
        """Load a graph within the session.
        See more information in :meth:`graphscope.load_from`.
        """
        with default_session(self):
            return graphscope.load_from(*args, **kwargs)

    def _run_on_local(self):
        self._config_params["port"] = None
        self._config_params["vineyard_socket"] = ""

    def gremlin(self, graph):
        """Get an interactive engine handler to execute gremlin queries.

        It will return an instance of :class:`graphscope.interactive.query.InteractiveQuery`,

        .. code:: python

            >>> # close and recreate InteractiveQuery.
            >>> interactive_query = sess.gremlin(g)
            >>> interactive_query.close()
            >>> interactive_query = sess.gremlin(g)

        Args:
            graph (:class:`graphscope.framework.graph.GraphDAGNode`):
                The graph to create interactive instance.

        Raises:
            InvalidArgumentError:
                - :code:`graph` is not a property graph.

        Returns:
            :class:`graphscope.interactive.query.InteractiveQuery`:
                InteractiveQuery to execute gremlin queries.
        """
        if self._session_id != graph.session_id:
            raise RuntimeError(
                "Failed to create interactive engine on the graph with different session: {0} vs {1}".format(
                    self._session_id, graph.session_id
                )
            )

        if not graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
            raise InvalidArgumentError("The graph should be a property graph.")

        if not isinstance(graph, Graph):  # Is a GraphDAGNode
            graph = self.run(graph)

        object_id = graph.vineyard_id
        schema_path = graph.schema_path
        endpoint = self._grpc_client.create_interactive_instance(object_id, schema_path)
        interactive_query = InteractiveQuery(graph, endpoint)
        self._interactive_instance_dict[object_id] = interactive_query
        graph._attach_interactive_instance(interactive_query)
        return interactive_query

    def learning(self, graph, nodes=None, edges=None, gen_labels=None):
        """Start a graph learning engine.

        Note that this method has been deprecated, using `graphlearn` replace.
        """
        warnings.warn(
            "The method 'learning' has been deprecated, using graphlearn replace."
        )
        return self.graphlearn(graph, nodes, edges, gen_labels)

    def graphlearn(self, graph, nodes=None, edges=None, gen_labels=None):
        """Start a graph learning engine.

        Args:
            graph (:class:`graphscope.framework.graph.GraphDAGNode`):
                The graph to create learning instance.
            nodes (list, optional): list of node types that will be used for GNN
                training, the element of list can be `"node_label"` or
                `(node_label, features)`. If the element of the list is a tuple and
                contains selected feature list, it would use the selected
                feature list for training. Default is None which use all type of
                nodes and for the GNN training.
            edges (list, optional): list of edge types that will be used for GNN
                training. We use `(src_label, edge_label, dst_label)`
                to specify one edge type. Default is None which use all type of
                edges for GNN training.
            gen_labels (list, optional): Alias node and edge labels and extract
                train/validation/test dataset from original graph for supervised
                GNN training. The detail is explained in the examples below.

        Examples
        --------
        >>> # Assume the input graph contains one label node `paper` and one edge label `link`.
        >>> features = ["weight", "name"] # use properties "weight" and "name" as features
        >>> lg = sess.graphlearn(
                graph,
                nodes=[("paper", features)])  # use "paper" node and features for training
                edges=[("paper", "links", "paper")]  # use the `paper->links->papers` edge type for training
                gen_labels=[
                    # split "paper" nodes into 100 pieces, and uses random 75 pieces (75%) as training dataset
                    ("train", "paper", 100, (0, 75)),
                    # split "paper" nodes into 100 pieces, and uses random 10 pieces (10%) as validation dataset
                    ("val", "paper", 100, (75, 85)),
                    # split "paper" nodes into 100 pieces, and uses random 15 pieces (15%) as test dataset
                    ("test", "paper", 100, (85, 100)),
                ]
            )
        Note that the training, validation and test datasets are not overlapping. And for unsupervised learning:
        >>> lg = sess.graphlearn(
                graph,
                nodes=[("paper", features)])  # use "paper" node and features for training
                edges=[("paper", "links", "paper")]  # use the `paper->links->papers` edge type for training
                gen_labels=[
                    # split "paper" nodes into 100 pieces, and uses all pieces as training dataset
                    ("train", "paper", 100, (0, 100)),
                ]
            )
        """
        if self._session_id != graph.session_id:
            raise RuntimeError(
                "Failed to create learning engine on the graph with different session: {0} vs {1}".format(
                    self._session_id, graph.session_id
                )
            )

        if not graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
            raise InvalidArgumentError("The graph should be a property graph.")

        from graphscope.learning.graph import Graph as LearningGraph
        from graphscope.learning.graph import get_gl_handle

        handle = get_gl_handle(
            graph.schema,
            graph.vineyard_id,
            self._pod_name_list,
            self._engine_config,
            graph.fragments,
        )
        config = LearningGraph.preprocess_args(handle, nodes, edges, gen_labels)
        config = base64.b64encode(json.dumps(config).encode("utf-8")).decode("utf-8")
        handle, config, endpoints = self._grpc_client.create_learning_instance(
            graph.vineyard_id, handle, config
        )

        handle = json.loads(base64.b64decode(handle).decode("utf-8"))
        handle["server"] = ",".join(endpoints)
        handle["client_count"] = 1

        # construct learning graph
        g = LearningGraph(graph, handle, config, graph.vineyard_id)
        self._learning_instance_dict[graph.vineyard_id] = g
        graph._attach_learning_instance(g)
        return g

    def nx(self):
        if not self.eager():
            raise RuntimeError(
                "Networkx module need the session to be eager mode. "
                "Current session is lazy mode."
            )
        if self._nx:
            return self._nx
        import importlib.util

        spec = importlib.util.find_spec("graphscope.nx")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        graph = type("Graph", (mod.Graph.__base__,), dict(mod.Graph.__dict__))
        digraph = type("DiGraph", (mod.DiGraph.__base__,), dict(mod.DiGraph.__dict__))
        setattr(graph, "_session", self)
        setattr(digraph, "_session", self)
        setattr(mod, "Graph", graph)
        setattr(mod, "DiGraph", digraph)
        self._nx = mod
        return self._nx

    def add_lib(self, resource_name):
        """
        add the specified resource to the k8s cluster from client machine.
        """
        logger.info("client: adding lib %s", resource_name)
        if not os.path.isfile(resource_name):
            raise RuntimeError("Resource {} can not be found".format(resource_name))
        # pack into a gar file
        garfile = InMemoryZip()
        resource_reader = open(resource_name, "rb")
        bytes_ = resource_reader.read()
        if len(bytes_) <= 0:
            raise RuntimeError("Expect a non-empty file.")
        # the uploaded file may be placed in the same directory
        garfile.append(resource_name.split("/")[-1], bytes_)
        self._grpc_client.add_lib(garfile.read_bytes().getvalue())


session = Session


def set_option(**kwargs):
    """Set the value of specified options.

    Find params detail in :class:`graphscope.Session`

    Available options:
        - num_workers
        - log_level
        - show_log
        - vineyard_shared_mem
        - k8s_namespace
        - k8s_service_type
        - k8s_gs_image
        - k8s_etcd_image
        - k8s_image_pull_policy
        - k8s_image_pull_secrets
        - k8s_coordinator_cpu
        - k8s_coordinator_mem
        - k8s_vineyard_daemonset
        - k8s_vineyard_cpu
        - k8s_vineyard_mem
        - k8s_engine_cpu
        - k8s_engine_mem
        - k8s_mars_worker_cpu
        - k8s_mars_worker_mem
        - k8s_mars_scheduler_cpu
        - k8s_mars_scheduler_mem
        - with_mars
        - with_analytical
        - with_analytical_java
        - with_interactive
        - with_learning
        - k8s_volumes
        - k8s_waiting_for_delete
        - timeout_seconds
        - dataset_download_retries

    Args:
        kwargs: dict
            kv pair of GraphScope config you want to set.

    Raises:
        ValueError: If no such option exists.

    Returns: None
    """
    # check exists
    for k, v in kwargs.items():
        if not hasattr(gs_config, k):
            raise ValueError(f"No such option {k} exists.")

    for k, v in kwargs.items():
        setattr(gs_config, k, v)

    GSLogger.update()


def get_option(key):
    """Get the value of specified option.

    Find params detail in :class:`graphscope.Session`

    Available options:
        - num_workers
        - log_level
        - show_log
        - vineyard_shared_mem
        - k8s_namespace
        - k8s_service_type
        - k8s_gs_image
        - k8s_etcd_image
        - k8s_image_pull_policy
        - k8s_image_pull_secrets
        - k8s_coordinator_cpu
        - k8s_coordinator_mem
        - k8s_vineyard_daemonset
        - k8s_vineyard_cpu
        - k8s_vineyard_mem
        - k8s_engine_cpu
        - k8s_engine_mem
        - k8s_mars_worker_cpu
        - k8s_mars_worker_mem
        - k8s_mars_scheduler_cpu
        - k8s_mars_scheduler_mem
        - with_mars
        - with_analytical
        - with_analytical_java
        - with_interactive
        - with_learning
        - k8s_volumes
        - k8s_waiting_for_delete
        - timeout_seconds
        - dataset_download_retries

    Args:
        key: str
            Key of GraphScope config you want to get.

    Raises:
        ValueError: If no such option exists.

    Returns: result: the value of the option
    """
    if hasattr(gs_config, key):
        return getattr(gs_config, key)
    raise ValueError("No such option {} exists.".format(key))


def default_session(session):
    """Python's :code:`with` handler for defining a default session.

    This function provides a means of registering a session for handling
    and code that need a default session calls.

    The :code:`with` keyword to specify that code invocations within
    the scope of a block should be executed by a particular session.

    Args:
        session: :class:`Session`
            The session to be installed as the default session.

    Returns:
        A context manager for the default session.
    """
    return _default_session_stack.get_controller(session)


def has_default_session():
    """True if default session exists in current context."""
    return not _default_session_stack.empty()


def get_default_session():
    """Returns the default session for the current context.

    Note that a new session will be created if there is no
    default session in current context.

    Returns:
        The default :class:`graphscope.Session`.
    """
    return _default_session_stack.get_default()


def get_session_by_id(handle):
    """Return the session by handle."""
    if handle not in _session_dict:
        raise ValueError("Session {} not exists.".format(handle))
    return _session_dict.get(handle)


class _DefaultSessionStack(object):
    """A stack of objects for providing implicit defaults."""

    def __init__(self):
        super().__init__()
        self.stack = []

    def get_default(self):
        if not self.stack:
            logger.info("Creating default session ...")
            sess = session(cluster_type="hosts", num_workers=1)
            sess.as_default()
        return self.stack[-1]

    def empty(self):
        return len(self.stack) == 0

    def reset(self):
        self.stack = []

    def is_cleared(self):
        return not self.stack

    @contextlib.contextmanager
    def get_controller(self, default):
        """A context manager for manipulating a default stack."""
        self.stack.append(default)
        try:
            yield default
        finally:
            # stack may be empty if reset() was called
            if self.stack:
                self.stack.remove(default)


_default_session_stack = _DefaultSessionStack()  # pylint: disable=protected-access


def g(
    incoming_data=None,
    oid_type="int64",
    directed=True,
    generate_eid=True,
    vertex_map="global",
):
    """Construct a GraphScope graph object on the default session.

    It will launch and set a session to default when there is no default session found.

    See params detail in :class:`graphscope.framework.graph.GraphDAGNode`

    Returns:
        :class:`graphscope.framework.graph.GraphDAGNode`: Evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> g = graphscope.g()

        >>> import graphscope
        >>> sess = graphscope.session()
        >>> sess.as_default()
        >>> g = graphscope.g() # creating graph on the session "sess"
    """
    return get_default_session().g(
        incoming_data, oid_type, directed, generate_eid, vertex_map
    )


def gremlin(graph):
    """Create an interactive engine and get the handler to execute the gremlin queries.

    See params detail in :meth:`graphscope.Session.gremlin`

    Returns:
        :class:`graphscope.interactive.query.InteractiveQueryDAGNode`:
            InteractiveQuery to execute gremlin queries, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> g = graphscope.g()
        >>> interactive_query = graphscope.gremlin()
    """
    assert graph is not None, "graph cannot be None"
    assert (
        graph._session is not None
    ), "The graph object is invalid"  # pylint: disable=protected-access
    return graph._session.gremlin(graph)  # pylint: disable=protected-access


def graphlearn(graph, nodes=None, edges=None, gen_labels=None):
    """Create a graph learning engine.

    See params detail in :meth:`graphscope.Session.graphlearn`

    Returns:
        :class:`graphscope.learning.GraphDAGNode`:
            An instance of learning graph that could be feed to the learning engine, evaluated in eager node.

    Example:

    .. code:: python

        >>> import graphscope
        >>> g = graphscope.g()
        >>> lg = graphscope.learning(g)
    """
    assert graph is not None, "graph cannot be None"
    assert (
        graph._session is not None
    ), "The graph object is invalid"  # pylint: disable=protected-access
    return graph._session.graphlearn(
        graph, nodes, edges, gen_labels
    )  # pylint: disable=protected-access
