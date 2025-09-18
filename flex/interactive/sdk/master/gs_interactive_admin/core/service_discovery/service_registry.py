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
from abc import abstractmethod

import json
import logging
import etcd3

import etcd3.events
import etcd3.watch

from gs_interactive_admin import util
from gs_interactive_admin.core.metadata.kv_store import ETCDKeyValueStore
from etcd3.events import PutEvent
from etcd3.events import DeleteEvent
from gs_interactive_admin.core.config import Config
from gs_interactive_admin.util import META_SERVICE_KEY, MetaKeyHelper


from gs_interactive_admin.models.base_model import Model

logger = logging.getLogger("interactive")

ETCD_RETRY_TIMES = 3


def like_endpoint(ip_port: str):
    """Expect ip_port to be a ip or ip+port"""
    if ip_port is None:
        return False
    parts = ip_port.split(":")
    if len(parts) == 1:
        return True
    if len(parts) == 2:
        return parts[1].isdigit()
    return False


class IServiceRegistry(metaclass=ABCMeta):
    """
    An abstraction for service registry.
    """

    @abstractmethod
    def discover(self, graph_id, service_name):
        """
        Discover a service.
        """
        pass

    @abstractmethod
    def start(self):
        """
        start a service.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        stop a service.
        """
        pass


class ServiceInstance(Model):
    """
    A service instance.
    """

    def __init__(self, endpoint, metrics: str = None):
        self._endpoint = endpoint
        self._metrics = metrics

        self.openapi_types = {"endpoint": str, "metrics": str}

        self.attribute_map = {"endpoint": "endpoint", "metrics": "metrics"}

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def metrics(self):
        return self._metrics

    @metrics.setter
    def metrics(self, metrics: str):
        self._metrics = metrics

    @endpoint.setter
    def endpoint(self, endpoint):
        self._endpoint = endpoint

    @classmethod
    def from_dict(cls, dikt) -> "ServiceInstance":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The ServiceInstance of this ServiceInstance.  # noqa: E501
        :rtype: ServiceInstance
        """
        return util.deserialize_model(dikt, cls)

    def __eq__(self, other):
        if isinstance(other, ServiceInstance):
            return self._endpoint == other._endpoint and self._metrics == other._metrics
        return False

    def __hash__(self):
        return hash((self._endpoint, self._metrics))

    def __str__(self):
        return "ServiceInstance(%s:%s)" % (self._endpoint, self._metrics)

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {"endpoint": self._endpoint, "metrics": self._metrics}


class DiscoverResult(Model):
    """
    The result of the service discovery.
    ├────── instance_list
    │   │   ├── cypher
    │   │   │   ├── 11.12.13.14_7687
    │   │   │   └── 22.12.13.14_7687
    │   |   ├── gremlin
    │   │   │   ├── 11.12.13.14_12314
    │   │   │   └── 22.12.13.14_12314
    │   |	└─-- procedure
    │   |       ├── 11.12.13.14_10000
    │   |       └── 22.12.13.14_10000
    │   │
    |   └── primary
    """

    def __init__(self):
        self._instance_list = dict()
        self._primary: str = None

        self.openapi_types = {"instance_list": dict, "primary": str}

        self.attribute_map = {"instance_list": "instance_list", "primary": "primary"}

    @property
    def primary(self):
        return self._primary

    def set_primary(self, primary: str):
        logger.info(f"----Set primary: {primary}")
        self._primary = primary

    @property
    def instance_list(self):
        return self._instance_list

    @instance_list.setter
    def instance_list(self, instance_list: dict):
        self._instance_list = instance_list

    @classmethod
    def from_dict(cls, dikt) -> "DiscoverResult":
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DiscoverResult of this DiscoverResult.  # noqa: E501
        :rtype: DiscoverResult
        """
        return util.deserialize_model(dikt, cls)

    def add_instance(self, service_name: str, end_point, metrics: str):
        """Add a service instance to the instance list of the service name.

        Args:
            service_name (str): The name of the service.
            end_point (_type_): The endpoint of the service instance.
            metrics (str): The metrics of the service instance.
        """
        if service_name not in self._instance_list:
            self._instance_list[service_name] = set()
        self._instance_list[service_name].add(ServiceInstance(end_point, metrics))

    def get_instance_list(self):
        return self._instance_list

    def get_instance_list(self, service_name: str) -> list:
        if service_name not in self._instance_list:
            return []
        return list(self._instance_list[service_name])

    def find_service(self, service_name: str) -> bool:
        return service_name in self._instance_list

    def to_dict(self):
        ret = {}
        logger.info(f"-----to dict {self._primary}")
        if self._primary is not None:
            ret["primary"] = self._primary
        if self._instance_list is not None:
            ret["instance_list"] = {}
            for k, v in self._instance_list.items():
                ret["instance_list"][k] = list(v)
        return ret


class GlobalServiceDiscovery(Model):
    def __init__(self):
        """
        ├── graph_1
        │   ├── instance_list
        │   │   ├── cypher
        │   │   │   ├── 11.12.13.14_7687
        │   │   │   └── 22.12.13.14_7687
        │   |   ├── gremlin
        │   │   │   ├── 11.12.13.14_12314
        │   │   │   └── 22.12.13.14_12314
        │   |	└─-- procedure
        │   |       ├── 11.12.13.14_10000
        │   |       └── 22.12.13.14_10000
        │   │
        |   └── primary
        """
        self._map = {}
        """
        {
            graph_id: {
                primary: primary_ip
                instance_list: {
                    cypher: [
                        {
                        endpoint: xxxx
                        metrics {
                            snapshot: xxxx
                            }
                        },
                        {
                        endpoint: xxxx
                        metrics {
                            snapshot: xxxx
                            },
                        }]
                }
            }
        }
        """

    def add_discovery_instance(self, graph_id, service_name, endpoint, metrics):
        if graph_id not in self._map:
            self._map[graph_id] = DiscoverResult()
        self._map[graph_id].add_instance(service_name, endpoint, metrics)

    def set_primary_instance(self, graph_id: str, primary_ip: str):
        """Expect primary_ip to be a single string

        Args:
            graph_id (_type_): The unique identifier for the graph
            primary_ip (str): A single string
        """
        if graph_id not in self._map:
            self._map[graph_id] = DiscoverResult()
        logger.info(f"Set primary instance: {graph_id}, {primary_ip}")
        self._map[graph_id].set_primary(primary_ip)

    def get(self, graph_id, service_name) -> dict:
        """In raw storage, we store primary as a ip, when fetching, we should return it as a ServiceInstance

        Args:
            graph_id (_type_): _description_
            service_name (_type_): _description_

        Returns:
            dict: {
                primary: ServiceInstance,
                instance_list: [ServiceInstance]
            }
        """
        if graph_id not in self._map:
            return None
        if not self._map[graph_id].find_service(service_name):
            logger.error(f"Service not found: {service_name}")
            return None
        # TODO: FIX me
        instance_list = self._map[graph_id].get_instance_list(service_name)
        primary = self._map[graph_id].primary
        for i in range(len(instance_list)):
            if instance_list[i].endpoint.startswith(primary):
                primary = instance_list[i]
                return {
                    "graph_id": graph_id,
                    "service_registry": {
                        "service_name": service_name,
                        "primary": primary.to_dict(),
                        "instances": [x.to_dict() for x in instance_list],
                    },
                }
        logger.error(
            f"Primary instance not found in instance list: {primary}, {instance_list}"
        )
        return {
            "graph_id": graph_id,
            "service_registry": {
                "primary": None,
                "serivce_name": service_name,
                "instances": [x.to_dict() for x in instance_list],
            },
        }

    def list_all(self) -> list:
        """

        Returns:
            dict: {
                graph_id: {
                    service_name: {
                        primary: ServiceInstance,
                        instance_list: [ServiceInstance]
                    }
                }
            }
        """
        ori_dict = self.to_dict()
        ret = []
        for graph_id, registry_info in ori_dict.items():
            cur = {}
            cur["graph_id"] = graph_id
            cur["service_registry"] = {}
            primary_ip = registry_info.primary
            logger.info(f"Found primary ip {primary_ip} for {graph_id}")
            for service_name, instance_list in registry_info.instance_list.items():
                logger.info(
                    f"Found service {service_name} for {graph_id}, {instance_list}"
                )
                cur["service_registry"]["service_name"] = service_name
                if primary_ip:
                    for instance in instance_list:
                        if instance.endpoint.startswith(primary_ip):
                            cur["service_registry"]["primary"] = instance.to_dict()
                            logger.info(f"Found primary instance {instance} for {graph_id}, {service_name}")
                    # for instance in instance_list:
                    #     if instance.endpoint.startswith(primary_ip):
                    #         cur["service_registry"]["primary"] = instance.to_dict()
                    #         logger.info(f"Found primary instance {instance} for {graph_id}, {service_name}")
                cur["service_registry"]["instances"] = [x.to_dict() for x in instance_list]
            ret.append(cur)
        return ret

    def remove_discovery(self, graph_id, service_name):
        logger.info(f"Remove discovery: {graph_id}, {service_name}")
        if graph_id in self._map and service_name in self._map[graph_id]:
            del self._map[graph_id][service_name]

    def remove_primary_instance(self, graph_id, service_name):
        logger.info(
            f'Remove primary instance: {graph_id}, {self._map[graph_id]["primary"]}'
        )
        if graph_id in self._map:
            del self._map[graph_id]["primary"]

    def to_dict(self):
        return dict(self._map)


class EtcdServiceRegistry(IServiceRegistry):
    """
    Implement the service registry using etcd.
    """

    def __init__(
        self,
        etcd_host="localhost",
        etcd_port=2379,
        namespace="interactive",
        instance_name="default",
    ):
        logger.info("namespace: %s, instance_name: %s", namespace, instance_name)
        self._namespace = namespace
        self._instance_name = instance_name
        self._key_helper = MetaKeyHelper(
            namespace=namespace, instance_name=instance_name
        )
        self._etcd_kv_store = ETCDKeyValueStore.create(
            self._key_helper, etcd_host, etcd_port
        )
        self._etcd_kv_store.open()

        self._global_discovery = GlobalServiceDiscovery()
        self._cancel_watch_handler = None

    @property
    def namespace(self):
        return self._namespace

    @property
    def instance_name(self):
        return self._instance_name

    def __del__(self):
        self.stop()
        logger.info("EtcdServiceRegistry is closed.")

    def start(self):
        """
        Start watching the service registry, and will be kept updated with watch mechanism.
        Watch all changes in the service registry.
        """
        logger.info(
            "Start watching the service registry on %s",
            self._key_helper.service_prefix(),
        )

        def service_watch_call_back(event):
            """
            Handling all the watch events. Should handle the events in a sequential way.
            """
            logger.info("Got event: %s", event)
            if event is None:
                return
            if isinstance(event, PutEvent):
                logger.info("Put event: %s", event)
                self._handle_put_event(event)
            elif isinstance(event, DeleteEvent):
                logger.info("Delete event: %s", event)
                self._handle_delete_event(event)
            elif isinstance(event, etcd3.watch.WatchResponse):
                logger.info("Watch response: %s", event.events)
                for e in event.events:
                    if isinstance(e, PutEvent):
                        self._handle_put_event(e)
                    elif isinstance(e, DeleteEvent):
                        self._handle_delete_event(e)
            else:
                raise ValueError("Invalid event type: %s", event)

        # When we start, we need to first scan whether primary is set, then we watch
        # the service registry
        self._etcd_kv_store.get_with_prefix(META_SERVICE_KEY)
        for key, value in self._etcd_kv_store.get_with_prefix(META_SERVICE_KEY):
            logger.info(f"Get key: {key}, value: {value}")
            # The returned key is after prefix, append the prefix
            key = self._etcd_kv_store.root + "/" + key
            self._handle_put_event_impl(key, value)

        logger.info("Watch prefix: %s", self._key_helper.service_prefix())
        self._cancel_watch_handler = self._etcd_kv_store.add_watch_prefix_callback(
            self._key_helper.service_prefix(), service_watch_call_back
        )
        logger.info("Watch handler: %s", self._cancel_watch_handler)

    def stop(self):
        """
        Stop watching the service registry.
        """
        if self._cancel_watch_handler is not None:
            logger.info("Stop watching the service registry.")
            cancel_res = self._etcd_kv_store.cancel_watch(self._cancel_watch_handler)
            self._cancel_watch_handler = None
        if self._etcd_kv_store is not None:
            self._etcd_kv_store.close()
            self._etcd_kv_store = None

    def discover(self, graph_id: str, service_name: str) -> dict:
        """
        Manually discover the registered services for a graph for a given service.

        param graph_id: the unique graph id
        param service_name: the name of the service, e.g., gremlin, cypher, etc.

        return: True if the service is registered successfully, False otherwise
        """
        return self._global_discovery.get(graph_id, service_name)

    def list_all(self) -> dict:
        """
        List all services in the service registry.
        """
        return self._global_discovery.list_all()

    def _handle_put_event_impl(self, key, value):
        graph_id, service_name, ip_port = self._try_decode_key(key)
        if graph_id is None:
            return
        logger.info(
            "Put event: graph_id=%s, service_name=%s, endpoint=%s, value=%s",
            graph_id,
            service_name,
            ip_port,
            value,
        )
        # check whether ip_port is like ip or ip_port
        if ip_port is None:
            self._global_discovery.set_primary_instance(graph_id, value)
        else:
            self._global_discovery.add_discovery_instance(
                graph_id, service_name, ip_port, value
            )

    def _handle_put_event(self, event: PutEvent):
        """
        Handle the put event.
        """
        value = event.value.decode("utf-8")
        key = event.key.decode("utf-8")
        return self._handle_put_event_impl(key, value)

    def _handle_delete_event(self, event: DeleteEvent):
        """
        Handle the delete event.
        """
        key = event.key.decode("utf-8")
        graph_id, service_name, ip_port = self._try_decode_key(key)
        logger.info(
            "Delete event: graph_id=%s, service_name=%s, endpoint=%s",
            graph_id,
            service_name,
            ip_port,
        )
        if ip_port is None:
            self._global_discovery.remove_primary_instance(graph_id)
        else:
            self._global_discovery.remove_discovery(graph_id, service_name)

    def _try_decode_key(self, key):
        """
        Try to decode the key to get the graph_id and service_name.
        If the key is instance_list key, return the graph_id, service_name, and ip_port.
        If the key is primary key, return the graph_id and service_name and None.
        """
        _tuple = self._key_helper.decode_service_key(key)
        if _tuple is None:
            return None, None, None
        if len(_tuple) != 3:
            raise RuntimeError("Expect 3 parts, but got %d", len(_tuple))
        return _tuple


service_registry = None


def initialize_service_registry(config: Config):
    global service_registry
    if config.master.service_registry.type == "etcd":
        # get ip and port from http://ip:port
        endpoint = config.master.service_registry.endpoint
        endpoint = endpoint.startswith("http://") and endpoint[7:] or endpoint
        ip, port = endpoint.split(":")
        service_registry = EtcdServiceRegistry(
            ip,
            int(port),
            config.master.k8s_launcher_config.namespace,
            config.master.instance_name,
        )
    else:
        raise ValueError(
            "Invalid service registry type: %s", config.master.service_registry.type
        )
    service_registry.start()


def get_service_registry():
    global service_registry
    return service_registry
