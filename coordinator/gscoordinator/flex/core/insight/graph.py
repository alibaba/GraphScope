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

import datetime
import itertools
import logging

import graphscope
from dateutil import tz
from gremlin_python.driver.client import Client
from kubernetes import client as kube_client
from kubernetes import config as kube_config

from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import CREATION_TIME
from gscoordinator.flex.core.config import GROOT_GREMLIN_PORT
from gscoordinator.flex.core.config import GROOT_GRPC_PORT
from gscoordinator.flex.core.config import GROOT_PASSWORD
from gscoordinator.flex.core.config import GROOT_USERNAME
from gscoordinator.flex.core.config import INSTANCE_NAME
from gscoordinator.flex.core.config import NAMESPACE
from gscoordinator.flex.core.scheduler import schedule
from gscoordinator.flex.core.utils import data_type_to_groot
from gscoordinator.flex.core.utils import get_internal_ip
from gscoordinator.flex.core.utils import get_service_endpoints
from gscoordinator.flex.core.utils import resolve_api_client

logger = logging.getLogger("graphscope")


class GrootGraph(object):
    """Graph class for GraphScope store"""

    def __init__(self, name, creation_time, gremlin_endpoint, grpc_endpoint):
        self._id = "1"
        self._name = name

        # graph infos
        self._creation_time = creation_time
        self._schema_update_time = creation_time
        self._data_update_time = "null"

        self._conn = graphscope.conn(
            grpc_endpoint, gremlin_endpoint, GROOT_USERNAME, GROOT_PASSWORD
        )
        self._g = self._conn.g()
        self._schema = self._g.schema().to_dict()
        self._gremlin_interface = {
            "gremlin_endpoint": gremlin_endpoint,
            "grpc_endpoint": grpc_endpoint,
            "username": GROOT_USERNAME,
            "password": GROOT_PASSWORD,
        }
        # kubernetes
        if CLUSTER_TYPE == "KUBERNETES":
            self._api_client = resolve_api_client()
            self._core_api = kube_client.CoreV1Api(self._api_client)
        # update the endpoints when frontend node restart
        self._fetch_endpoints_job = (
            schedule.every(30)
            .seconds.do(self._fetch_endpoints_impl)
            .tag("fetch", "frontend endpoints")
        )

    def _fetch_endpoints_impl(self):
        if CLUSTER_TYPE != "KUBERNETES":
            return

        try:
            # frontend statefulset and service name
            frontend_pod_name = "{0}-graphscope-store-frontend-0".format(
                INSTANCE_NAME
            )
            pod = self._core_api.read_namespaced_pod(frontend_pod_name, NAMESPACE)
            endpoints = [
                "{0}:{1}".format(pod.status.pod_ip, GROOT_GRPC_PORT),
                "{0}:{1}".format(pod.status.pod_ip, GROOT_GREMLIN_PORT),
            ]
            gremlin_endpoint = "ws://{0}/gremlin".format(endpoints[1])
            grpc_endpoint = endpoints[0]
            conn = graphscope.conn(
                grpc_endpoint, gremlin_endpoint, GROOT_USERNAME, GROOT_PASSWORD
            )
            g = conn.g()
        except Exception as e:
            logger.warn(f"Failed to fetch frontend endpoints: {str(e)}")
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
                logger.info(f"Update frontend endpoints: {str(endpoints)}")

    def __del__(self):
        self._conn.close()

    @property
    def conn(self):
        return self._conn

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def gremlin_interface(self):
        return self._gremlin_interface

    @property
    def schema(self):
        self._g.schema().update()
        self._schema = self._g.schema().to_dict()
        return self._schema

    @property
    def data_update_time(self):
        return self._data_update_time

    @data_update_time.setter
    def data_update_time(self, value):
        self._data_update_time = value

    def to_dict(self):
        return {
            "id": self._id,
            "name": self._name,
            "creation_time": self._creation_time,
            "schema_update_time": self._schema_update_time,
            "data_update_time": self._data_update_time,
            "stored_procedures": [],
            "schema": self.schema,
        }

    def import_schema(self, data: dict):
        def _delete_none(_dict):
            """Delete None values recursively from all of the dictionaries"""
            for key, value in list(_dict.items()):
                if isinstance(value, dict):
                    _delete_none(value)
                elif value is None:
                    del _dict[key]
                elif isinstance(value, list):
                    for v_i in value:
                        if isinstance(v_i, dict):
                            _delete_none(v_i)
            return _dict

        data = _delete_none(data)
        for item in itertools.chain(data["vertex_types"], data["edge_types"]):
            if "properties" in item:
                for p in item["properties"]:
                    if (
                        "string" in p["property_type"]
                        and "long_text" in p["property_type"]["string"]
                    ):
                        p["property_type"]["string"]["long_text"] = None

        schema = self._g.schema()
        schema.from_dict(data)
        schema.update()
        self._schema = self._g.schema().to_dict()
        self._schema_update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    def create_vertex_type(self, data: dict):
        schema = self._g.schema()
        vertex = schema.add_vertex_label(data["type_name"])
        for property in data["properties"]:
            if property["property_name"] in data["primary_keys"]:
                vertex.add_primary_key(
                    property["property_name"],
                    data_type_to_groot(property["property_type"]),
                )
            else:
                vertex.add_property(
                    property["property_name"],
                    data_type_to_groot(property["property_type"]),
                )
        schema.update()
        self._schema = self._g.schema().to_dict()
        self._schema_update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    def delete_vertex_type_by_name(self, type_name: str):
        schema = self._g.schema()
        for edge_schema in schema.to_dict()["edge_types"]:
            for relation in edge_schema["vertex_type_pair_relations"]:
                if (
                    type_name == relation["source_vertex"]
                    or type_name == relation["destination_vertex"]
                ):
                    raise RuntimeError(
                        "Can not delete '{0}' type, cause exists in edge '{1}'".format(
                            type_name, edge_schema["type_name"]
                        ),
                    )
        schema.drop(type_name)
        schema.update()
        self._schema = self._g.schema().to_dict()
        self._schema_update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    def create_edge_type(self, data: dict):
        schema = self._g.schema()
        edge = (
            schema.add_edge_label(data["type_name"])
            .source(data["vertex_type_pair_relations"][0]["source_vertex"])
            .destination(data["vertex_type_pair_relations"][0]["destination_vertex"])
        )
        if "properties" in data:
            for property in data["properties"]:
                edge.add_property(
                    property["property_name"],
                    data_type_to_groot(property["property_type"]),
                )
        schema.update()
        self._schema = self._g.schema().to_dict()
        self._schema_update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    def delete_edge_type_by_name(
        self,
        type_name: str,
        source_vertex_type: str,
        destination_vertex_type: str,
    ):
        schema = self._g.schema()
        schema.drop(type_name, source_vertex_type, destination_vertex_type)
        schema.drop(type_name)
        schema.update()
        self._schema = self._g.schema().to_dict()
        self._schema_update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    def get_vertex_primary_key(self, type_name: str) -> str:
        for v in self._schema["vertex_types"]:
            if type_name == v["type_name"]:
                return v["primary_keys"][0]
        raise RuntimeError(f"Vertex type {type_name} not exists")

    def get_storage_usage(self) -> dict:
        return self._conn.get_store_state()


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
        except Exception:  # noqa: B110
            pass
        else:
            client.close()
            break
        time.sleep(5)
    # groot graph
    return GrootGraph(
        name=INSTANCE_NAME,
        creation_time=CREATION_TIME,
        gremlin_endpoint=gremlin_endpoint,
        grpc_endpoint=grpc_endpoint,
    )


def get_groot_graph_from_k8s():
    api_client = resolve_api_client()
    core_api = kube_client.CoreV1Api(api_client)
    app_api = kube_client.AppsV1Api(api_client)
    # frontend statefulset and service name
    name = "{0}-graphscope-store-frontend".format(INSTANCE_NAME)
    response = app_api.read_namespaced_stateful_set(name, NAMESPACE)
    # creation time
    creation_time = response.metadata.creation_timestamp.astimezone(
        tz.tzlocal()
    ).strftime("%Y/%m/%d %H:%M:%S")
    # service endpoints: [grpc_endpoint, gremlin_endpoint]
    frontend_pod_name = "{0}-graphscope-store-frontend-0".format(INSTANCE_NAME)
    pod = core_api.read_namespaced_pod(frontend_pod_name, NAMESPACE)
    endpoints = [
        f"{pod.status.pod_ip}:{GROOT_GRPC_PORT}",
        f"{pod.status.pod_ip}:{GROOT_GREMLIN_PORT}",
    ]
    # groot graph
    return GrootGraph(
        name=INSTANCE_NAME,
        creation_time=creation_time,
        gremlin_endpoint="ws://{0}/gremlin".format(endpoints[1]),
        grpc_endpoint=endpoints[0],
    )


def get_groot_graph():
    """Groot service has been deployed and available"""
    if CLUSTER_TYPE == "HOSTS":
        return get_groot_graph_from_local()
    elif CLUSTER_TYPE == "KUBERNETES":
        return get_groot_graph_from_k8s()
    raise RuntimeError(f"Failed to get groot graph with cluster type {CLUSTER_TYPE}")
