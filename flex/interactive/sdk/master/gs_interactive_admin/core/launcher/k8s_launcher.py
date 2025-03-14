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

from abc import ABCMeta
import os
from abc import abstractmethod
import logging

import random
import string
logger = logging.getLogger("interactive")


from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes import watch as kube_watch
from kubernetes.client import AppsV1Api
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException as K8SApiException
from kubernetes.config import ConfigException as K8SConfigException


from gs_interactive_admin.core.config import Config
from gs_interactive_admin.core.launcher.abstract_launcher import (
    InteractiveCluster,
    ILauncher,
)
from gs_interactive_admin.core.launcher.k8s_utils import resolve_api_client
from gs_interactive_admin.version import __version__


class InteractiveK8sCluster(InteractiveCluster):
    def __init__(self, graph_id: str, config: Config):
        super().__init__()

        self._started = False
        self._graph_id = graph_id
        self._namespace = config.k8s_launcher_config.namespace
        self._instance_prefix = config.k8s_launcher_config.instance_prefix
        self._instance_id = config.k8s_launcher_config.instance_id
        self._config_file = config.k8s_launcher_config.config_file
        self._default_replicas = config.k8s_launcher_config.default_replicas
        self._config = config
        self._initialized = True

        self._image_pull_policy = config.k8s_launcher_config.image_pull_policy
        self._image_tag = config.k8s_launcher_config.image_tag
        self._image_registry = config.k8s_launcher_config.image_registry
        self._repository = config.k8s_launcher_config.repository
        self._image_name = config.k8s_launcher_config.image_name
        self._default_container_name = config.k8s_launcher_config.default_container_name

        self._cpu_request = config.compute_engine.thread_num_per_worker
        self._cpu_limit = config.compute_engine.thread_num_per_worker
        self._memory_request = config.compute_engine.memory_per_worker
        self._memory_limit = config.compute_engine.memory_per_worker
        self._node_selectors = config.k8s_launcher_config.node_selectors
        self._workspace = config.workspace
        
        self._admin_port = config.http_service.admin_port
        self._query_port = config.http_service.query_port
        self._cypher_port = config.compiler.endpoint.bolt_connector.port
        
        self._service_type = config.k8s_launcher_config.service_type
        self._cluster_ip = config.k8s_launcher_config.cluster_ip

        # Some preprocessing
        if self._config_file is not None:
            self._config_file = os.environ.get("KUBECONFIG", "~/.kube/config")

        self._api_client = resolve_api_client(self._config_file)
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._apps_api = kube_client.AppsV1Api(self._api_client)

    @property
    def namespace(self):
        return self._namespace

    @property
    def image_full_name(self):
        return f"{self._image_registry}/{self._repository}/{self._image_name}:{self._image_tag}"

    @property
    def instance_id(self):
        return self._instance_id or f"{self._instance_prefix}-graph-{self._graph_id}"

    @property
    def engine_stateful_set_name(self):
        return f"{self.instance_id}-engine"

    @property
    def engine_service_name(self):
        return f"{self.engine_stateful_set_name}-headless"

    @property
    def node_selectors(self):
        return self._node_selectors
    
    @property
    def admin_port(self):
        return self._admin_port
    
    @property
    def query_port(self):
        return self._query_port
    
    @property
    def cypher_port(self):
        return self._cypher_port

    @property
    def pod_labels(self):
        return {
            "app.kubernetes.io/name": "graphscope-interactive",
            "app.kubernetes.io/instance": self.instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
            # "app.kubernetes.io/engine_selector": self.engine_stateful_set_name,
        }
        
    @property
    def service_type(self):
        return self._service_type
    
    @property
    def cluster_ip(self):
        return self._cluster_ip

    @property
    def engine_envs(self):
        envs = {
            # TODO: Add more envs
            "INTERACTIVE_WORKSPACE": self._workspace,
        }
        return [kube_client.V1EnvVar(name=k, value=v) for k, v in envs.items()]

    def start(self):
        """
        Start the cluster.
        """
        logger.info(
            f"Creating the interactive cluster with image {self.image_full_name}"
        )
        self._create_interactive_server_sts()
        self._create_interactive_service()
        
        # We just need to create the stateful set. No load balancer is needed.
        self._started = True
        return True

    def stop(self):
        """
        Stop the cluster. Exit the stateful set and pods
        """
        if not self._started:
            return True
        logger.info(
            f"Stopping the interactive cluster {self.instance_id}, namespace {self.namespace}, stateful set {self.engine_stateful_set_name}"
        )
        
        self._apps_api.delete_namespaced_stateful_set(
            name=self.engine_stateful_set_name,
            namespace=self.namespace,
            body=kube_client.V1DeleteOptions(grace_period_seconds=0),
        )
        

        self._core_api.delete_namespaced_service(
            name=self.engine_service_name,
            namespace=self.namespace,
            body=kube_client.V1DeleteOptions(grace_period_seconds=0),
        )
    
    def is_ready(self):
        """
        Check whether the cluster is ready.
        """
        stateful_set = self._apps_api.read_namespaced_stateful_set(
            name=self.engine_stateful_set_name, namespace=self.namespace
        )
        logger.info(f"Stateful set ready replicas: {stateful_set.status.ready_replicas}, desired replicas: {stateful_set.spec.replicas}")
        return stateful_set.status.ready_replicas == stateful_set.spec.replicas

    def wait_pods_ready(self, timeout: int = 600):
        """
        Wait for the pods to be ready.
        """
        w = kube_watch.Watch()
        try:
            for event in w.stream(
                self._apps_api.list_namespaced_stateful_set,
                namespace=self.namespace,
                label_selector=f"app.kubernetes.io/instance={self.instance_id}",
                timeout_seconds=timeout,
            ):
                logger.info(f"Event: {event}")
        except Exception as e:
            logger.error(f"Failed to watch the stateful set {self.engine_stateful_set_name}, error: {e}")
            return False
        finally:
            w.stop()
    
    
    def _create_interactive_service(self):
        """
        Create the service for the interactive servers, the service type is by default NodePort.
        """
        service_name = self.engine_service_name
        logger.info(f"Creating service {service_name}")
        service = kube_client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=kube_client.V1ObjectMeta(
                name=service_name,
                namespace=self.namespace,
                labels=self.pod_labels,
            ),
            spec=kube_client.V1ServiceSpec(
                type=self.service_type,
                selector=self.pod_labels,
                ports=[
                    kube_client.V1ServicePort(
                        name="admin-port", port=self.admin_port, target_port=self.admin_port
                    ),
                    kube_client.V1ServicePort(
                        name="query-port", port=self.query_port, target_port=self.query_port
                    ),
                    kube_client.V1ServicePort(
                        name="cypher-port", port=self.cypher_port, target_port=self.cypher_port
                    ),
                ],
            ),
        )
        if self.service_type == "ClusterIP" and self.cluster_ip is not None:
            service.spec.cluster_ip = self.cluster_ip
     
        resp = self._core_api.create_namespaced_service(
            namespace=self.namespace, body=service
        )
        logger.info(f"Service created. resp={resp}")

    def _create_interactive_server_sts(self):
        stateful_set = self._generate_engine_stateful_set()
        logger.info(f"Succeed to create stateful set {stateful_set.metadata}")

        logger.info(f"Creating namespaced stateful set {self.namespace}")
        resp = self._apps_api.create_namespaced_stateful_set(
            namespace=self.namespace, body=stateful_set
        )
        logger.info(f"Stateful set created. resp={resp}")

    def _generate_engine_stateful_set(self):
        stateful_set_name = self.engine_stateful_set_name
        volume_claim_template, dst_volume_mount = self._get_volume_pvc_spec()
        stateful_set_template_spec = self._get_engine_template_spec(dst_volume_mount)
        replicas = self._default_replicas
        service_name = self.engine_service_name
        logger.info(
            f"Creating stateful set {stateful_set_name} with {replicas} replicas"
        )
        spec = kube_client.V1StatefulSetSpec(
            selector=kube_client.V1LabelSelector(
                match_labels=self.pod_labels   
            ),
            service_name=service_name,
            replicas=replicas,
            template=stateful_set_template_spec,
            volume_claim_templates=[volume_claim_template],
        )
        logger.info(f"Stateful set spec: {spec}")
        return kube_client.V1StatefulSet(
            api_version="apps/v1",
            kind="StatefulSet",
            metadata=kube_client.V1ObjectMeta(
                name=stateful_set_name,
                namespace=self.namespace,
                labels=self.pod_labels,
            ),
            spec=spec,
        )

    def _get_engine_template_spec(self, dst_volume_mount):
        """
        Get the template spec for the engine.
        """
        container = self._get_container_spec(dst_volume_mount)
        pod_spec =  kube_client.V1PodTemplateSpec(
            metadata=kube_client.V1ObjectMeta(labels=self.pod_labels),
            spec=kube_client.V1PodSpec(
                containers=[container],
                restart_policy="Always",
                termination_grace_period_seconds=10,
                volumes=[dst_volume_mount],
            ),
        )
        if self.node_selectors is not None:
            pod_spec.node_selector = self.node_selectors
        return pod_spec

    def _get_container_spec(self, dst_volume_mount):
        return kube_client.V1Container(
            name=self._default_container_name,
            image=self.image_full_name,
            image_pull_policy=self._image_pull_policy,
            volume_mounts=[dst_volume_mount],
            env=self.engine_envs,
            args=["-w", "/tmp/interactive/workspace"],
            resources=kube_client.V1ResourceRequirements(
                requests={"cpu": self._cpu_request, "memory": self._memory_request},
                limits={"cpu": self._cpu_limit, "memory": self._memory_limit},
            ),
            readiness_probe=kube_client.V1Probe(
                http_get=kube_client.V1HTTPGetAction(
                    path="/v1/service/status",  # TODO: Implement on the server side
                    port=self.admin_port,
                ),
                initial_delay_seconds=5,
                period_seconds=10,
            ),
            liveness_probe=kube_client.V1Probe(
                http_get=kube_client.V1HTTPGetAction(
                    path="/v1/service/status",
                    port=self.admin_port,
                ),
                initial_delay_seconds=5,
                period_seconds=10,
            ),
            ports=[kube_client.V1ContainerPort(name="admin-port", container_port=self.admin_port), 
                   kube_client.V1ContainerPort(name="query-port", container_port=self.query_port), 
                   kube_client.V1ContainerPort(name="cypher-port", container_port=self.cypher_port)],
        )

    def _get_volume_pvc_spec(self):
        """
        Create the volume and pvc spec for the engine.
        """
        pvc = kube_client.V1PersistentVolumeClaim(
            api_version="v1",
            kind="PersistentVolumeClaim",
            metadata=kube_client.V1ObjectMeta(
                name=self._config.k8s_launcher_config.volume_claim_name
            ),
            spec=kube_client.V1PersistentVolumeClaimSpec(
                access_modes=[self._config.k8s_launcher_config.volume_access_mode],
                storage_class_name=self._config.k8s_launcher_config.volume_storage_class,
                resources=kube_client.V1ResourceRequirements(
                    requests={"storage": self._config.k8s_launcher_config.volume_size}
                ),
            ),
        )
        # mount to the container
        volume_mount = kube_client.V1VolumeMount(
            name=self._config.k8s_launcher_config.volume_claim_name,
            mount_path=self._config.k8s_launcher_config.volume_mount_path,
        )
        return pvc, volume_mount


class K8sLauncher(ILauncher):
    """
    The implementation for launching interactive engines in k8s.
    Note that the master could be outside the k8s cluster or inside the k8s cluster.
    Currently, we consider the master is outside the k8s cluster.

    No state should be stored in the launcher, because the launcher may be re-initialized.
    """

    def __init__(self, config: Config):
        """
        Initialize the launcher.
        """
        self._config = config
        self._config_file = self._config.k8s_launcher_config.config_file
        if self._config_file is not None:
            self._config_file = os.environ.get("KUBECONFIG", "~/.kube/config")

        self._api_client = resolve_api_client(self._config_file)
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._apps_api = kube_client.AppsV1Api(self._api_client)

    def launch_cluster(
        self,
        graph_id: str,
        config: Config,
        wait_service_ready=False,
        wait_service_ready_timeout=30,
    ) -> InteractiveCluster:
        """
        Launch a new interactive cluster, which contains a master pod and some standby pod.
        The started pods will serve for graph with the given graph_id.
        """
        # First check whether there is already a cluster for the graph.
        self._check_whether_cluster_exists(graph_id)

        cur_config = self._config
        # override the default config with the given config
        if config is not None:
            logger.info("Override the default config with the given config.")
            cur_config = config
        logger.debug(f"Launch a new cluster for graph {graph_id}, config: {cur_config}")

        # Generating the deployment config for the given graph.
        cluster = InteractiveK8sCluster(graph_id, cur_config)

        # Start the cluster
        cluster.start()

        # Wait for the cluster to be ready
        if wait_service_ready:
            if not cluster.wait_pods_ready(timeout=wait_service_ready_timeout):
                raise Exception(f"Failed to wait the pods to be ready for graph {graph_id}")

        return cluster

    def update_cluster(self, graph_id: str, config: Config) -> bool:
        pass

    def delete_cluster(self, graph_id: str) -> bool:
        pass

    def get_cluster_status(self, instance_id: str) -> str:
        """
        Get the status of the cluster, with the given instance_id(the statueful set name).
        """
        if instance_id is None:
            return None
        try:
            stateful_set = self._apps_api.read_namespaced_stateful_set(
                name=instance_id, namespace=self._config.k8s_launcher_config.namespace
            )
            return stateful_set.status
        except Exception as e:
            logger.warning(f"Failed to get the status of the cluster {instance_id}, error: {e}")
            return None

    def get_all_clusters(self) -> list:
        pass

    def _check_whether_cluster_exists(self, graph_id: str):
        pass
