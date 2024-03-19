#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import logging
import os
import time
from abc import ABCMeta, abstractmethod

import graphscope
from dateutil import tz
from graphscope.deploy.kubernetes.utils import get_service_endpoints, resolve_api_client
from gremlin_python.driver.client import Client
from kubernetes import client as kube_client
from kubernetes import config as kube_config

from gs_flex_coordinator.core.config import (
    CLUSTER_TYPE,
    CREATION_TIME,
    ENABLE_DNS,
    GROOT_GREMLIN_PORT,
    GROOT_GRPC_PORT,
    GROOT_PASSWORD,
    GROOT_USERNAME,
    INSTANCE_NAME,
    NAMESPACE,
    WORKSPACE,
)
from gs_flex_coordinator.core.scheduler import schedule
from gs_flex_coordinator.core.utils import (
    data_type_to_groot,
    encode_datetime,
    get_internal_ip,
)
from gs_flex_coordinator.version import __version__


class Graph(metaclass=ABCMeta):
    """Base class to derive GrootGraph"""

    def __init__(self):
        self._name = None
        self._type = None
        self._creation_time = None
        self._schema = None
        # {
        #   "gremlin_endpoint": "",
        #   "grpc_endpoint": "",
        #   "username: "",
        #   "password: """
        # }
        self._gremlin_interface = None
        self._directed = None

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def creation_time(self):
        return self._creation_time

    @property
    def schema(self):
        return self._schema

    @property
    def gremlin_interface(self):
        return self._gremlin_interface

    @property
    def directed(self):
        return self._directed

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "creation_time": self._creation_time,
            "schema": self._schema,
            "gremlin_interface": self._gremlin_interface,
            "directed": self._directed,
        }


class GrootGraph(Graph):
    """Class for GraphScope Store"""

    def __init__(self, name, version, creation_time, gremlin_endpoint, grpc_endpoint):
        super().__init__()
        self._conn = graphscope.conn(
            grpc_endpoint, gremlin_endpoint, GROOT_USERNAME, GROOT_PASSWORD
        )
        self._g = self._conn.g()

        self._name = name
        self._version = version
        self._type = "GrootGraph"
        self._creation_time = creation_time
        self._schema = self._g.schema().to_dict()
        self._gremlin_interface = {
            "gremlin_endpoint": gremlin_endpoint,
            "grpc_endpoint": grpc_endpoint,
            "username": GROOT_USERNAME,
            "password": GROOT_PASSWORD,
        }

        # kubernetes
        if CLUSTER_TYPE == "K8S":
            self._api_client = resolve_api_client()
            self._core_api = kube_client.CoreV1Api(self._api_client)

        # defaults true
        self._directed = True

        # update the endpoints when frontend node restart
        self._fetch_endpoints_job = (
            schedule.every(30)
            .seconds.do(self._fetch_endpoints_impl)
            .tag("fetch", "frontend endpoints")
        )

    def __del__(self):
        self._conn.close()

    @property
    def version(self):
        return self._version

    @property
    def schema(self):
        self._g.schema().update()
        self._schema = self._g.schema().to_dict()
        return self._schema

    @property
    def conn(self):
        return self._conn

    def _fetch_endpoints_impl(self):
        if CLUSTER_TYPE != "K8S":
            return

        try:
            if ENABLE_DNS:
                # frontend statefulset and service name
                name = "{0}-graphscope-store-frontend".format(INSTANCE_NAME)
                frontend_pod_name = "{0}-graphscope-store-frontend-0".format(
                    INSTANCE_NAME
                )
                pod = self._core_api.read_namespaced_pod(frontend_pod_name, NAMESPACE)
                endpoints = [
                    "{0}:{1}".format(pod.status.pod_ip, GROOT_GRPC_PORT),
                    "{0}:{1}".format(pod.status.pod_ip, GROOT_GREMLIN_PORT),
                ]
            else:
                endpoints = get_service_endpoints(
                    self._api_client, NAMESPACE, name, "NodePort"
                )
            gremlin_endpoint = "ws://{0}/gremlin".format(endpoints[1])
            grpc_endpoint = endpoints[0]
            conn = graphscope.conn(
                grpc_endpoint, gremlin_endpoint, GROOT_USERNAME, GROOT_PASSWORD
            )
            g = conn.g()
        except Exception as e:
            logging.warn(f"Failed to fetch frontend endpoints: {str(e)}")
        else:
            if (
                gremlin_endpoint != self._gremlin_interface["gremlin_endpoint"]
                or grpc_endpoint != self._gremlin_interface["grpc_endpoint"]
            ):
                self._conn = conn
                self._g = g
                self._schema = self._g.schema().to_dict()
                self._gremlin_interface = {
                    "gremlin_endpoint": gremlin_endpoint,
                    "grpc_endpoint": grpc_endpoint,
                    "username": GROOT_USERNAME,
                    "password": GROOT_PASSWORD,
                }
                logging.info(f"Update frontend endpoints: {str(endpoints)}")

    def get_vertex_primary_key(self, vertex_type: str) -> str:
        for v in self._schema["vertices"]:
            if vertex_type == v["label"]:
                for p in v["properties"]:
                    if p["is_primary_key"]:
                        return p["name"]
        raise RuntimeError(f"Vertex type {vertex_type} not exists")

    def import_schema(self, data: dict):
        schema = self._g.schema()
        schema.from_dict(data)
        schema.update()
        self._schema = self._g.schema().to_dict()

    def create_vertex_type(self, data: dict):
        schema = self._g.schema()
        vertex = schema.add_vertex_label(data["type_name"])
        for property in data["properties"]:
            if property["property_name"] in data["primary_keys"]:
                vertex.add_primary_key(
                    property["property_name"],
                    data_type_to_groot(property["property_type"]["primitive_type"]),
                )
            else:
                vertex.add_property(
                    property["property_name"],
                    data_type_to_groot(property["property_type"]["primitive_type"]),
                )
        schema.update()
        self._schema = self._g.schema().to_dict()

    def create_edge_type(self, data: dict):
        schema = self._g.schema()
        edge = (
            schema.add_edge_label(data["type_name"])
            .source(data["vertex_type_pair_relations"][0]["source_vertex"])
            .destination(data["vertex_type_pair_relations"][0]["destination_vertex"])
        )
        for property in data["properties"]:
            edge.add_property(
                property["property_name"],
                data_type_to_groot(property["property_type"]["primitive_type"]),
            )
        schema.update()
        self._schema = self._g.schema().to_dict()

    def delete_vertex_type(self, graph_name: str, vertex_type: str):
        schema = self._g.schema()
        for edge_schema in schema.to_dict()["edges"]:
            for relation in edge_schema["relations"]:
                if (
                    vertex_type == relation["src_label"]
                    or vertex_type == relation["dst_label"]
                ):
                    raise RuntimeError(
                        "Can not delete '{0}' type, cause exists in edge type '{1}'".format(
                            vertex_type, edge_schema["label"]
                        ),
                    )
        schema.drop(vertex_type)
        schema.update()
        self._schema = self._g.schema().to_dict()

    def delete_edge_type(
        self,
        graph_name: str,
        edge_type: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ):
        schema = self._g.schema()
        schema.drop(edge_type, source_vertex_type, destination_vertex_type)
        schema.drop(edge_type)
        schema.update()
        self._schema = self._g.schema().to_dict()


def get_groot_graph_from_local():
    host = get_internal_ip()
    grpc_endpoint = f"{host}:{GROOT_GRPC_PORT}"
    gremlin_endpoint = f"ws://{host}:{GROOT_GREMLIN_PORT}/gremlin"
    client = Client(
        gremlin_endpoint, "g", username=GROOT_USERNAME, password=GROOT_PASSWORD
    )
    # loop for waiting groot ready
    while True:
        try:
            client.submit(
                "g.with('evaluationTimeout', 5000).V().limit(1)"
            ).all().result()
        except Exception as e:  # noqa: B110
            pass
        else:
            break
        time.sleep(5)
    # groot graph
    return GrootGraph(
        name=INSTANCE_NAME,
        version=__version__,
        creation_time=encode_datetime(CREATION_TIME),
        gremlin_endpoint=gremlin_endpoint,
        grpc_endpoint=grpc_endpoint,
    )


def get_groot_graph_from_k8s():
    api_client = resolve_api_client()
    core_api = kube_client.CoreV1Api(api_client)
    app_api = kube_client.AppsV1Api(api_client)
    # frontend statefulset and service name
    name = "{0}-graphscope-store-frontend".format(INSTANCE_NAME)
    respoonse = app_api.read_namespaced_stateful_set(name, NAMESPACE)
    labels = response.metadata.labels
    # creation time
    creation_time = response.metadata.creation_timestamp.astimezone(
        tz.tzlocal()
    ).strftime("%Y/%m/%d %H:%M:%S")
    # service endpoints: [grpc_endpoint, gremlin_endpoint]
    if ENABLE_DNS:
        frontend_pod_name = "{0}-graphscope-store-frontend-0".format(INSTANCE_NAME)
        pod = core_api.read_namespaced_pod(frontend_pod_name, NAMESPACE)
        endpoints = [
            f"{pod.status.pod_ip}:{GROOT_GRPC_PORT}",
            f"{pod.status.pod_ip}:{GROOT_GREMLIN_PORT}",
        ]
    else:
        endpoints = get_service_endpoints(api_client, NAMESPACE, name, "NodePort")
    # groot graph
    return GrootGraph(
        name=INSTANCE_NAME,
        version=__version__,
        creation_time=creation_time,
        gremlin_endpoint="ws://{0}/gremlin".format(endpoints[1]),
        grpc_endpoint=endpoints[0],
    )


def get_groot_graph():
    """Groot service has been deployed and availabl"""
    if CLUSTER_TYPE == "HOSTS":
        return get_groot_graph_from_local()
    elif CLUSTER_TYPE == "K8S":
        return get_groot_graph_from_k8s()
    raise RuntimeError(f"Failed to get groot graph with cluster type {CLUSTER_TYPE}")
