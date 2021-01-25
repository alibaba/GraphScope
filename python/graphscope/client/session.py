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
import copy
import json
import logging
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import warnings
from queue import Empty as EmptyQueue

try:
    from kubernetes import config as kube_config
except ImportError:
    kube_config = None

import graphscope
from graphscope.client.rpc import GRPCClient
from graphscope.client.utils import GSLogger
from graphscope.client.utils import set_defaults
from graphscope.config import GSConfig as gs_config
from graphscope.deploy.kubernetes.cluster import KubernetesCluster
from graphscope.framework.errors import ConnectionError
from graphscope.framework.errors import FatalError
from graphscope.framework.errors import GRPCError
from graphscope.framework.errors import InteractiveEngineInternalError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import K8sError
from graphscope.framework.errors import LearningEngineInternalError
from graphscope.framework.errors import check_argument
from graphscope.framework.operation import Operation
from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

try:
    import gscoordinator

    COORDINATOR_HOME = os.path.abspath(os.path.join(gscoordinator.__file__, "..", ".."))
except ModuleNotFoundError:
    # If gscoordinator is not installed, try to locate it by relative path,
    # which is strong related with the directory structure of GraphScope
    COORDINATOR_HOME = os.path.abspath(
        os.path.join(__file__, "..", "..", "..", "..", "coordinator")
    )

DEFAULT_CONFIG_FILE = os.environ.get("GS_CONFIG_PATH", "")

_session_dict = {}

logger = logging.getLogger("graphscope")


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
        >>> s = gs.session()
        >>> g = s.load_from('xxx')
        >>> r = s.sssp(g, 4)
        >>> s.close()

        >>> # or use a session as default
        >>> s = gs.session().as_default()
        >>> g = gs.load_from('xxx')
        >>> r = gs.sssp(g, 4)
        >>> s.close()

    We support setup a service cluster and create a RPC session in following ways:

    - GraphScope graph computation service run in cluster managed by kubernetes.

        >>> s = graphscope.session()

        Also, :class:`Session` provides several keyword params for users to define the cluster.
        You may use the param :code:`k8s_gs_image` to specify the image for all engine pod, and
        param :code:`k8s_engine_cpu` or :code:`k8s_engine_mem` to specify the resources. More,
        you can find all params detail in :meth:`__init__` method.

        >>> s = graphscope.session(
        ...         k8s_gs_image="registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest",
        ...         k8s_vineyard_cpu=0.1,
        ...         k8s_vineyard_mem="256Mi",
        ...         k8s_vineyard_shared_mem="4Gi",
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
        num_workers=gs_config.num_workers,
        k8s_namespace=gs_config.k8s_namespace,
        k8s_service_type=gs_config.k8s_service_type,
        k8s_gs_image=gs_config.k8s_gs_image,
        k8s_etcd_image=gs_config.k8s_etcd_image,
        k8s_gie_graph_manager_image=gs_config.k8s_gie_graph_manager_image,
        k8s_zookeeper_image=gs_config.k8s_zookeeper_image,
        k8s_image_pull_policy=gs_config.k8s_image_pull_policy,
        k8s_image_pull_secrets=gs_config.k8s_image_pull_secrets,
        k8s_coordinator_cpu=gs_config.k8s_coordinator_cpu,
        k8s_coordinator_mem=gs_config.k8s_coordinator_mem,
        k8s_etcd_cpu=gs_config.k8s_etcd_cpu,
        k8s_etcd_mem=gs_config.k8s_etcd_mem,
        k8s_zookeeper_cpu=gs_config.k8s_zookeeper_cpu,
        k8s_zookeeper_mem=gs_config.k8s_zookeeper_mem,
        k8s_gie_graph_manager_cpu=gs_config.k8s_gie_graph_manager_cpu,
        k8s_gie_graph_manager_mem=gs_config.k8s_gie_graph_manager_mem,
        k8s_vineyard_cpu=gs_config.k8s_vineyard_cpu,
        k8s_vineyard_mem=gs_config.k8s_vineyard_mem,
        k8s_vineyard_shared_mem=gs_config.k8s_vineyard_shared_mem,
        k8s_engine_cpu=gs_config.k8s_engine_cpu,
        k8s_engine_mem=gs_config.k8s_engine_mem,
        k8s_volumes=gs_config.k8s_volumes,
        k8s_waiting_for_delete=gs_config.k8s_waiting_for_delete,
        timeout_seconds=gs_config.timeout_seconds,
        **kw
    ):
        """Construct a new GraphScope session.

        Args:
            config (dict or str, optional): The configuration dict or file about how to launch the GraphScope instance.
                For str, it will identify it as a path and read the configuration file to build a
                session if file exist. If not specified, the global default configuration
                :code:`DEFAULT_CONFIG_FILE` will be used, which get value of GS_CONFIG_PATH
                in environment. Note that it will overwrite explicit parameters. Defaults to None.

            num_workers (int, optional): The number of workers to launch GraphScope engine. Defaults to 2.

            k8s_namespace (str, optional): Contains the namespace to create all resource inside.
                If param missing, it will try to read namespace from kubernetes context, or
                a random namespace will be created and deleted if namespace not exist.
                Defaults to None.

            k8s_service_type (str, optional): Type determines how the GraphScope service is exposed.
                Valid options are NodePort, and LoadBalancer. Defaults to NodePort.

            k8s_gs_image (str, optional): The GraphScope engine's image.

            k8s_etcd_image (str, optional): The image of etcd, which used by vineyard.

            k8s_image_pull_policy (str, optional): Kubernetes image pull policy. Defaults to "IfNotPresent".

            k8s_image_pull_secrets (list[str], optional): A list of secret name used to authorize pull image.

            k8s_gie_graph_manager_image (str, optional): The GraphScope interactive engine's graph manager image.

            k8s_zookeeper_image (str, optional): The image of zookeeper, which used by GIE graph manager.

            k8s_vineyard_cpu (float, optional): Minimum number of CPU cores request for vineyard container. Defaults to 0.5.

            k8s_vineyard_mem (str, optional): Minimum number of memory request for vineyard container. Defaults to '512Mi'.

            k8s_vineyard_shared_mem (str, optional): Init size of vineyard shared memory. Defaults to '4Gi'.

            k8s_engine_cpu (float, optional): Minimum number of CPU cores request for engine container. Defaults to 0.5.

            k8s_engine_mem (str, optional): Minimum number of memory request for engine container. Defaults to '4Gi'.

            k8s_coordinator_cpu (float, optional): Minimum number of CPU cores request for coordinator pod. Defaults to 1.0.

            k8s_coordinator_mem (str, optional): Minimum number of memory request for coordinator pod. Defaults to '4Gi'.

            k8s_etcd_cpu (float, optional): Minimum number of CPU cores request for etcd pod. Defaults to 0.5.

            k8s_etcd_mem (str, optional): Minimum number of memory request for etcd pod. Defaults to '128Mi'.

            k8s_zookeeper_cpu (float, optional):
                Minimum number of CPU cores request for zookeeper container. Defaults to 0.5.

            k8s_zookeeper_mem (str, optional):
                Minimum number of memory request for zookeeper container. Defaults to '128Mi'.

            k8s_gie_graph_manager_cpu (float, optional):
                Minimum number of CPU cores request for graphmanager container. Defaults to 1.0.

            k8s_gie_graph_manager_mem (str, optional):
                Minimum number of memory request for graphmanager container. Defaults to '4Gi'.

            k8s_volumes (dict, optional): A dict of k8s volume which represents a directory containing data, accessible to the
                containers in a pod. Defaults to {}. Only 'hostPath' supported yet. For example, we can mount host path with:

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
                Also, after seconds of client disconnect, coordinator will clean up this graphscope instance.
                Defaults to 600.

            k8s_waiting_for_delete (bool, optional): Waiting for service delete or not. Defaults to False.

            **kw (dict, optional): Other optional parameters will be put to :code:`**kw`.
                - k8s_minikube_vm_driver (bool, optional):
                    If your kubernetes cluster deployed on inner virtual machine
                    (such as minikube with param --vm-driver is not None), you can specify
                    :code:`k8s_minikube_vm_driver` is :code:`True`.

                - k8s_client_config (dict, optional):
                    Provide configurable parameters for connecting to remote k8s,
                    which strongly relies on the `kube_config.new_client_from_config` function.
                    eg: {"config_file": "~/.kube/config", "context": None, "persist_config": True}
                    config_file: Name of the kube-config file.
                    context: set the active context. If is set to None, current_context from config file will be used.
                    persist_config: If True, config file will be updated when changed(e.g GCP token refresh).

                - log_level: Deprecated.
                    Move this param as a global configuration. Set via `graphscope.set_option(log_level='DEBUG')`

                - show_log: Deprecated.
                    Move this param as a global configuration.Set via `graphscope.set_option(show_log=True)`

        Raises:
            TypeError: If the given argument combination is invalid and cannot be used to create
                a GraphScope session.
        """
        num_workers = int(num_workers)
        self._config_params = {}
        self._accessable_params = (
            "num_workers",
            "k8s_namespace",
            "k8s_service_type",
            "k8s_gs_image",
            "k8s_etcd_image",
            "k8s_image_pull_policy",
            "k8s_image_pull_secrets",
            "k8s_gie_graph_manager_image",
            "k8s_zookeeper_image",
            "k8s_coordinator_cpu",
            "k8s_coordinator_mem",
            "k8s_etcd_cpu",
            "k8s_etcd_mem",
            "k8s_zookeeper_cpu",
            "k8s_zookeeper_mem",
            "k8s_gie_graph_manager_cpu",
            "k8s_gie_graph_manager_mem",
            "k8s_vineyard_cpu",
            "k8s_vineyard_mem",
            "k8s_vineyard_shared_mem",
            "k8s_engine_cpu",
            "k8s_engine_mem",
            "k8s_volumes",
            "k8s_waiting_for_delete",
            "timeout_seconds",
        )
        saved_locals = locals()
        for param in self._accessable_params:
            self._config_params[param] = saved_locals[param]

        # parse config, which should be a path to config file, or dict
        # config has highest priority
        if isinstance(config, dict):
            self._config_params.update(config)
        elif isinstance(config, str):
            self._load_config(config)
        elif DEFAULT_CONFIG_FILE:
            self._load_config(DEFAULT_CONFIG_FILE)

        # update other optional params
        self._config_params.update(kw)

        self._config_params["addr"] = kw.pop("addr", None)

        # Reserved keyword for local testing.
        run_on_local = kw.pop("run_on_local", False)
        if not run_on_local:
            self._config_params["enable_k8s"] = True
        else:
            self._run_on_local()

        # deprecated params handle
        if "show_log" in kw:
            warnings.warn(
                "The `show_log` parameter has been deprecated and has no effect, "
                "please use `graphscope.set_option(show_log=%s)` instead."
                % kw.pop("show_log", None),
                category=DeprecationWarning,
            )
        if "log_level" in kw:
            warnings.warn(
                "The `log_level` parameter has been deprecated and has no effect, "
                "please use `graphscope.set_option(log_level=%r)` instead."
                % kw.pop("show_log", None),
                category=DeprecationWarning,
            )

        # deploy minikube on virtual machine
        self._config_params["k8s_minikube_vm_driver"] = kw.pop(
            "k8s_minikube_vm_driver", False
        )

        # update k8s_client_config params
        self._config_params["k8s_client_config"] = kw.pop("k8s_client_config", {})

        # There should be no more custom keyword arguments.
        if kw:
            raise ValueError("Not recognized value: ", list(kw.keys()))

        logger.info(
            "Initializing graphscope session with parameters: %s", self._config_params
        )

        self._closed = False

        # set _session_type in self._connect()
        self._session_type = types_pb2.INVALID_SESSION

        # need clean up when session exit
        self._proc = None
        self._endpoint = None

        self._k8s_cluster = None
        self._heartbeat_sending_thread = None

        self._grpc_client = None
        self._session_id = None  # unique identifier across sessions
        # engine config:
        #
        #   {
        #       "experiment": "ON/OFF",
        #       "vineyard_socket": "...",
        #       "vineyard_rpc_endpoint": "..."
        #   }
        self._engine_config = None

        # interactive instance related graph map
        self._interactive_instance_dict = {}
        # learning engine related graph map
        self._learning_instance_dict = {}

        self._default_session = None

        atexit.register(self.close)
        # create and connect session
        self._proc, self._endpoint = self._connect()

        self._disconnected = False

        # heartbeat
        self._heartbeat_interval_seconds = 5
        self._heartbeat_sending_thread = threading.Thread(
            target=self._send_heartbeat, args=()
        )
        self._heartbeat_sending_thread.daemon = True
        self._heartbeat_sending_thread.start()

    def __repr__(self):
        return str(self.info)

    def __str__(self):
        return repr(self)

    @property
    def session_id(self):
        return self._session_id

    def _load_config(self, path):
        config_path = os.path.expandvars(os.path.expanduser(path))
        with open(config_path, "r") as f:
            data = json.load(f)
            self._config_params.update(data)

    @property
    def info(self):
        """Show all resources info associated with session in json format."""
        info = {}
        if self._closed:
            info["status"] = "closed"
        elif self._grpc_client is None or self._disconnected:
            info["status"] = "disconnected"
        else:
            info["status"] = "active"

        if self._session_type == types_pb2.K8S:
            info["type"] = "k8s"
            info["engine_hosts"] = ",".join(self._pod_name_list)
            info["namespace"] = self._config_params["k8s_namespace"]
        else:
            info["type"] = "hosts"
            if self._config_params["addr"] is not None:
                info["engine_hosts"] = self._engine_config["engine_hosts"]
            else:
                info["engine_hosts"] = ",".join(self._config_params["hosts"])

        info["num_workers"] = self._config_params["num_workers"]
        info["coordinator_endpoint"] = self._endpoint
        info["engine_config"] = self._engine_config
        return info

    def _send_heartbeat(self):
        while not self._closed:
            if self._grpc_client:
                try:
                    self._grpc_client.send_heartbeat()
                except GRPCError as exc:
                    logger.warning(exc)
                    self._disconnected = True
                else:
                    self._disconnected = False
            time.sleep(self._heartbeat_interval_seconds)

    def close(self):
        """Closes this session.

        This method frees all resources associated with the session.
        """
        if self._closed:
            return
        self._closed = True
        self._endpoint = None

        self._deregister_default()

        if self._heartbeat_sending_thread:
            self._heartbeat_sending_thread.join(
                timeout=self._heartbeat_interval_seconds
            )
            self._heartbeat_sending_thread = None

        self._disconnected = True

        # close all interactive instances
        for instance in self._interactive_instance_dict.values():
            try:
                if instance is not None:
                    instance.close()
            except InteractiveEngineInternalError:
                pass
        self._interactive_instance_dict.clear()

        # close all learning instances
        for instance in self._learning_instance_dict.values():
            try:
                if instance is not None:
                    instance.close()
            except LearningEngineInternalError:
                pass
        self._learning_instance_dict.clear()

        if self._grpc_client:
            self._grpc_client.close()
            self._grpc_client = None
            _session_dict.pop(self._session_id, None)

        # clean up
        if self._proc is not None:
            # coordinator's GRPCServer.wait_for_termination works for SIGINT (Ctrl-C)
            self._proc.send_signal(signal.SIGINT)
            self._proc.wait()
            self._proc = None

        if self._config_params["enable_k8s"]:
            if self._k8s_cluster:
                self._k8s_cluster.stop()
                self._pod_name_list = []

    def _close_interactive_instance(self, instance):
        """Close a interactive instance."""
        if self._grpc_client:
            self._grpc_client.close_interactive_engine(instance.object_id)
            self._interactive_instance_dict[instance.object_id] = None

    def _close_learning_instance(self, instance):
        """Close a learning instance."""
        if self._grpc_client:
            self._grpc_client.close_learning_engine(instance.object_id)
            self._learning_instance_dict[instance.object_id] = None

    def __del__(self):
        # cleanly ignore all exceptions
        try:
            self.close()
        except Exception:  # pylint: disable=broad-except
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

    def _deregister_default(self):
        """Remove self from the default session stack."""
        if self._default_session:
            self._default_session.__exit__(None, None, None)
            self._default_session = None

    def run(self, fetch):
        """Run operations of `fetch`.

        Args:
            fetch: :class:`Operation`

        Raises:
            RuntimeError:
                Client disconnect to the service. Or run on a closed session.

            ValueError:
                If fetch is not a instance of :class:`Operation`. Or
                the fetch has been evaluated.

            InvalidArgumentError:
                Not recognized on output type.

        Returns:
            Different values for different output types of :class:`Operation`
        """

        # prepare names to run and fetch
        if hasattr(fetch, "op"):
            fetch = fetch.op
        if not isinstance(fetch, Operation):
            raise ValueError("Expect a `Operation`")
        if fetch.output is not None:
            raise ValueError("The op <%s> are evaluated duplicated." % fetch.key)

        # convert to list to be compatible with rpc client method signature
        fetch_ops = [fetch]

        dag = op_def_pb2.DagDef()
        for op in fetch_ops:
            dag.op.extend([copy.deepcopy(op.as_op_def())])

        if self._closed:
            raise RuntimeError("Attempted to use a closed Session.")

        if not self._grpc_client:
            raise RuntimeError("Session disconnected.")

        # execute the query
        try:
            response = self._grpc_client.run(dag)
        except FatalError:
            self.close()
            raise
        check_argument(
            len(fetch_ops) == 1, "Cannot execute multiple ops at the same time"
        )
        return self._parse_value(fetch_ops[0], response)

    def _parse_value(self, op, response: message_pb2.RunStepResponse):
        # attach an output to op, indicating the op is already run.
        op.set_output(response.metrics)

        # if loads a arrow property graph, will return {'object_id': xxxx}
        if op.output_types == types_pb2.GRAPH:
            return response.graph_def
        if op.output_types == types_pb2.APP:
            return response.result.decode("utf-8")
        if op.output_types in (
            types_pb2.RESULTS,
            types_pb2.VINEYARD_TENSOR,
            types_pb2.VINEYARD_DATAFRAME,
        ):
            return response.result.decode("utf-8")
        if op.output_types in (types_pb2.TENSOR, types_pb2.DATAFRAME):
            return response.result
        else:
            raise InvalidArgumentError(
                "Not recognized output type: %s", op.output_types
            )

    def _connect(self):
        if self._config_params["addr"] is not None:
            # try connect to exist coordinator
            self._session_type = types_pb2.HOSTS
            proc, endpoint = None, self._config_params["addr"]
        elif self._config_params["enable_k8s"]:
            if (
                self._config_params["k8s_etcd_image"] is None
                or self._config_params["k8s_gs_image"] is None
            ):
                raise K8sError("None image found.")
            api_client = kube_config.new_client_from_config(
                **self._config_params["k8s_client_config"]
            )
            proc = None
            self._session_type = types_pb2.K8S
            self._k8s_cluster = KubernetesCluster(
                api_client=api_client,
                namespace=self._config_params["k8s_namespace"],
                service_type=self._config_params["k8s_service_type"],
                minikube_vm_driver=self._config_params["k8s_minikube_vm_driver"],
                num_workers=self._config_params["num_workers"],
                gs_image=self._config_params["k8s_gs_image"],
                etcd_image=self._config_params["k8s_etcd_image"],
                gie_graph_manager_image=self._config_params[
                    "k8s_gie_graph_manager_image"
                ],
                zookeeper_image=self._config_params["k8s_zookeeper_image"],
                image_pull_policy=self._config_params["k8s_image_pull_policy"],
                image_pull_secrets=self._config_params["k8s_image_pull_secrets"],
                vineyard_cpu=self._config_params["k8s_vineyard_cpu"],
                vineyard_mem=self._config_params["k8s_vineyard_mem"],
                vineyard_shared_mem=self._config_params["k8s_vineyard_shared_mem"],
                etcd_cpu=self._config_params["k8s_etcd_cpu"],
                etcd_mem=self._config_params["k8s_etcd_mem"],
                zookeeper_cpu=self._config_params["k8s_zookeeper_cpu"],
                zookeeper_mem=self._config_params["k8s_zookeeper_mem"],
                gie_graph_manager_cpu=self._config_params["k8s_gie_graph_manager_cpu"],
                gie_graph_manager_mem=self._config_params["k8s_gie_graph_manager_mem"],
                engine_cpu=self._config_params["k8s_engine_cpu"],
                engine_mem=self._config_params["k8s_engine_mem"],
                coordinator_cpu=float(self._config_params["k8s_coordinator_cpu"]),
                coordinator_mem=self._config_params["k8s_coordinator_mem"],
                volumes=self._config_params["k8s_volumes"],
                waiting_for_delete=self._config_params["k8s_waiting_for_delete"],
                timeout_seconds=self._config_params["timeout_seconds"],
            )
            endpoint = self._k8s_cluster.start()
            if self._config_params["k8s_namespace"] is None:
                self._config_params["k8s_namespace"] = self._k8s_cluster.get_namespace()
        elif (
            isinstance(self._config_params["hosts"], list)
            and len(self._config_params["hosts"]) != 0
            and self._config_params["num_workers"] > 0
        ):
            # lanuch coordinator with hosts
            proc, endpoint = _launch_coordinator_on_local(self._config_params)
            self._session_type = types_pb2.HOSTS
        else:
            raise RuntimeError("Session initialize failed.")

        # waiting service ready
        self._grpc_client = GRPCClient(endpoint)
        self._grpc_client.waiting_service_ready(
            timeout_seconds=self._config_params["timeout_seconds"],
            enable_k8s=self._config_params["enable_k8s"],
        )

        # connect to rpc server
        try:
            (
                self._session_id,
                self._engine_config,
                self._pod_name_list,
            ) = self._grpc_client.connect()
            _session_dict[self._session_id] = self
        except Exception:
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                except:  # noqa: E722
                    pass
            raise

        if self._config_params["enable_k8s"]:
            # minikube service
            self._k8s_cluster.check_and_set_vineyard_rpc_endpoint(self._engine_config)

        return proc, endpoint

    def get_config(self):
        """Get configuration of the session."""
        return self._config_params

    def load_from(self, *args, **kwargs):
        """Load a graph within the session.
        See more information in :meth:`graphscope.load_from`.
        """
        with default_session(self):
            return graphscope.load_from(*args, **kwargs)

    def _run_on_local(self):
        self._config_params["hosts"] = ["localhost"]
        self._config_params["port"] = None
        self._config_params["vineyard_socket"] = ""
        self._config_params["enable_k8s"] = False

    def _get_gl_handle(self, graph):
        """Dump a handler for GraphLearn for interaction.

        Fields in :code:`schema` are:

        + the name of node type or edge type
        + whether the graph is weighted graph
        + whether the graph is labeled graph
        + the number of int attributes
        + the number of float attributes
        + the number of string attributes

        An example of the graph handle:

        .. code:: python

            {
                "server": "127.0.0.1:8888,127.0.0.1:8889",
                "client_count": 1,
                "vineyard_socket": "/var/run/vineyard.sock",
                "vineyard_id": 13278328736,
                "node_schema": [
                    "user:false:false:10:0:0",
                    "item:true:false:0:0:5"
                ],
                "edge_schema": [
                    "user:click:item:true:false:0:0:0",
                    "user:buy:item:true:true:0:0:0",
                    "item:similar:item:false:false:10:0:0"
                ],
                "node_attribute_types": {
                    "person": {
                        "age": "i",
                        "name": "s",
                    },
                },
                "edge_attribute_types": {
                    "knows": {
                        "weight": "f",
                    },
                },
            }

        The handle can be decoded using:

        .. code:: python

           base64.b64decode(handle.encode('ascii')).decode('ascii')

        Note that the ports are selected from a range :code:`(8000, 9000)`.

        Args:
            graph (:class:`Graph`): A Property Graph.
            client_number (int): Number of client.

        Returns:
            str: Base64 encoded handle

        Raises:
            InvalidArgumentError: If the graph is not loaded, or graph_type isn't
                `ARROW_PROPERTY`.
        """

        if not graph.loaded():
            raise InvalidArgumentError("The graph has already been unloaded")
        if not graph.graph_type == types_pb2.ARROW_PROPERTY:
            raise InvalidArgumentError("The graph should be a property graph.")

        def group_property_types(props):
            weighted, labeled, i, f, s, attr_types = "false", "false", 0, 0, 0, {}
            for field_name, field_type in props.items():
                if field_type in [types_pb2.STRING]:
                    s += 1
                    attr_types[field_name] = "s"
                elif field_type in (types_pb2.FLOAT, types_pb2.DOUBLE):
                    f += 1
                    attr_types[field_name] = "f"
                else:
                    i += 1
                    attr_types[field_name] = "i"
                if field_name == "weight":
                    weighted = "true"
                elif field_name == "label":
                    labeled = "true"
            return weighted, labeled, i, f, s, attr_types

        node_schema, node_attribute_types = [], dict()
        for index, label in enumerate(graph.schema.vertex_labels):
            weighted, labeled, i, f, s, attr_types = group_property_types(
                graph.schema.vertex_properties[index]
            )
            node_schema.append(
                "{}:{}:{}:{}:{}:{}".format(label, weighted, labeled, i, f, s)
            )
            node_attribute_types[label] = attr_types

        edge_schema, edge_attribute_types = [], dict()
        for index, label in enumerate(graph.schema.edge_labels):
            weighted, labeled, i, f, s, attr_types = group_property_types(
                graph.schema.edge_properties[index]
            )
            for rel in graph.schema.edge_relationships[index]:
                edge_schema.append(
                    "{}:{}:{}:{}:{}:{}:{}:{}".format(
                        rel[0], label, rel[1], weighted, labeled, i, f, s
                    )
                )
            edge_attribute_types[label] = attr_types

        handle = {
            "hosts": self.info["engine_hosts"],
            "client_count": 1,
            "vineyard_id": graph.vineyard_id,
            "vineyard_socket": self._engine_config["vineyard_socket"],
            "node_schema": node_schema,
            "edge_schema": edge_schema,
            "node_attribute_types": node_attribute_types,
            "edge_attribute_types": edge_attribute_types,
        }
        handle_json_string = json.dumps(handle)
        return base64.b64encode(handle_json_string.encode("utf-8")).decode("utf-8")

    def gremlin(self, graph):
        """Get a interactive engine handler to execute gremlin queries.

        Args:
            graph: :class:`Graph`

        Raises:
            InvalidArgumentError: :code:`graph` is not a property graph or unloaded.

        Returns:
            :class:`InteractiveQuery`
        """
        if (
            graph.vineyard_id in self._interactive_instance_dict
            and self._interactive_instance_dict[graph.vineyard_id] is not None
        ):
            return self._interactive_instance_dict[graph.vineyard_id]

        if not graph.loaded():
            raise InvalidArgumentError("The graph has already been unloaded")
        if not graph.graph_type == types_pb2.ARROW_PROPERTY:
            raise InvalidArgumentError("The graph should be a property graph.")

        from graphscope.interactive.query import InteractiveQuery

        response = self._grpc_client.create_interactive_engine(
            object_id=graph.vineyard_id,
            schema_path=graph.schema_path,
            gremlin_server_cpu=gs_config.k8s_gie_gremlin_server_cpu,
            gremlin_server_mem=gs_config.k8s_gie_gremlin_server_mem,
        )
        interactive_query = InteractiveQuery(
            graphscope_session=self,
            object_id=graph.vineyard_id,
            front_ip=response.frontend_host,
            front_port=response.frontend_port,
        )
        self._interactive_instance_dict[graph.vineyard_id] = interactive_query
        graph.attach_interactive_instance(interactive_query)
        return interactive_query

    def learning(self, graph, nodes=None, edges=None, gen_labels=None):
        """Start a graph learning engine.

        Args:
            nodes (list): The node types that will be used for gnn training.
            edges (list): The edge types that will be used for gnn training.
            gen_labels (list): Extra node and edge labels on original graph for gnn training.

        Returns:
            `graphscope.learning.Graph`: An instance of `graphscope.learning.Graph`
                that could be feed to the learning engine.
        """
        if (
            graph.vineyard_id in self._learning_instance_dict
            and self._learning_instance_dict[graph.vineyard_id] is not None
        ):
            return self._learning_instance_dict[graph.vineyard_id]

        if sys.platform != "linux" and sys.platform != "linux2":
            raise RuntimeError(
                "The learning engine currently supports Linux only, doesn't support %s"
                % sys.platform
            )

        if not graph.loaded():
            raise InvalidArgumentError("The graph has already been unloaded")
        if not graph.graph_type == types_pb2.ARROW_PROPERTY:
            raise InvalidArgumentError("The graph should be a property graph.")

        from graphscope.learning.graph import Graph as LearningGraph

        handle = self._get_gl_handle(graph)
        config = LearningGraph.preprocess_args(handle, nodes, edges, gen_labels)
        config = base64.b64encode(json.dumps(config).encode("utf-8")).decode("utf-8")
        endpoints = self._grpc_client.create_learning_engine(
            graph.vineyard_id, handle, config
        )

        handle = json.loads(base64.b64decode(handle.encode("utf-8")).decode("utf-8"))
        handle["server"] = endpoints
        handle["client_count"] = 1

        learning_graph = LearningGraph(handle, config, graph.vineyard_id, self)
        self._learning_instance_dict[graph.vineyard_id] = learning_graph
        graph.attach_learning_instance(learning_graph)
        return learning_graph


session = Session


def _is_port_in_use(port):
    """Check whether port of localhost is used

    Returns: bool
      True if port already in used.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def _launch_coordinator_on_local(config_params):
    """Launch coordinator locally using specific configuration.

    Args:
        config_params (dict): Specific configurations,
          in which we will look at several specific keys:
            port: Port used to launch coordinator, use random port if None.
            num_workers: Workers number.
            hosts: Hosts name of workers.
            timeout_seconds: Wait until reached timeout.
            vineyard_socket: Vineyard socket path. Use default path if None.
    Returns:
        process: instance of Popen object.
        endpoint (str): The endpoint to connect to coordinator.
    """
    port = config_params["port"]
    if port is None:
        # use random port
        port = random.randint(60801, 63801)
        while _is_port_in_use(port):
            port = random.randint(60801, 63801)
    else:
        # check port conflict
        if _is_port_in_use(port):
            raise ConnectionError("Port already used.")

    cmd = [
        sys.executable,
        "-m",
        "gscoordinator",
        "--num_workers",
        "{}".format(str(config_params["num_workers"])),
        "--hosts",
        "{}".format(",".join(config_params["hosts"])),
        "--log_level",
        "{}".format(gs_config.log_level),
        "--timeout_seconds",
        "{}".format(config_params["timeout_seconds"]),
        "--port",
        "{}".format(str(port)),
    ]

    if config_params["vineyard_socket"]:
        cmd.extend(["--vineyard_socket", "{}".format(config_params["vineyard_socket"])])

    logger.info("Client is initializing coordinator.")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "TRUE"
    process = subprocess.Popen(
        cmd,
        cwd=COORDINATOR_HOME,
        universal_newlines=True,
        encoding="utf-8",
        stdin=subprocess.DEVNULL,
        stdout=sys.stdout if gs_config.show_log else subprocess.DEVNULL,
        stderr=sys.stderr if gs_config.show_log else subprocess.DEVNULL,
        bufsize=1,
        env=env,
    )

    return process, "localhost:%s" % port


def set_option(**kwargs):
    """Set the value of specified options.

    Available options:
        - num_workers
        - log_level
        - show_log
        - k8s_namespace
        - k8s_service_type
        - k8s_gs_image
        - k8s_etcd_image
        - k8s_gie_graph_manager_image
        - k8s_zookeeper_image
        - k8s_image_pull_policy
        - k8s_image_pull_secrets
        - k8s_coordinator_cpu
        - k8s_coordinator_mem
        - k8s_vineyard_cpu
        - k8s_vineyard_mem
        - k8s_vineyard_shared_mem
        - k8s_engine_cpu
        - k8s_engine_mem
        - k8s_waiting_for_delete
        - timeout_seconds

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
            raise ValueError("No such option {} exists.".format(k))

    for k, v in kwargs.items():
        setattr(gs_config, k, v)

    GSLogger.update()


def get_option(key):
    """Get the value of specified option.

    Available options:
        - num_workers
        - log_level
        - show_log
        - k8s_namespace
        - k8s_service_type
        - k8s_gs_image
        - k8s_etcd_image
        - k8s_gie_graph_manager_image
        - k8s_zookeeper_image
        - k8s_image_pull_policy
        - k8s_image_pull_secrets
        - k8s_coordinator_cpu
        - k8s_coordinator_mem
        - k8s_vineyard_cpu
        - k8s_vineyard_mem
        - k8s_vineyard_shared_mem
        - k8s_engine_cpu
        - k8s_engine_mem
        - k8s_waiting_for_delete
        - timeout_seconds

    Args:
        key: str
            Key of GraphScope config you want to get.

    Raises:
        ValueError: If no such option exists.

    Returns: result: the value of the option
    """
    if hasattr(gs_config, key):
        return getattr(gs_config, key)
    else:
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


def get_default_session():
    """Returns the default session for the current context.

    Raises:
        RuntimeError: Default session is not exist.

    Returns:
        The default :class:`Session`.
    """
    return _default_session_stack.get_default()


def get_session_by_id(handle):
    """Return the session by handle."""
    if handle not in _session_dict:
        raise ValueError("Session not exists.")
    return _session_dict.get(handle)


class _DefaultSessionStack(object):
    """A stack of objects for providing implicit defaults."""

    def __init__(self):
        super().__init__()
        self.stack = []

    def get_default(self):
        if not self.stack:
            raise RuntimeError("No default session found.")
        return self.stack[-1]

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
