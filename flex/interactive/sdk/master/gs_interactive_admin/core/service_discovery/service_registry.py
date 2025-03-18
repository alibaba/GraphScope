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

import etcd3.events
import etcd3.watch

from gs_interactive_admin import util
from gs_interactive_admin.core.metadata.kv_store import ETCDKeyValueStore
from etcd3.events import PutEvent
from etcd3.events import DeleteEvent
from gs_interactive_admin.core.config import Config

import etcd3

from gs_interactive_admin.models.base_model import Model

logger = logging.getLogger("interactive")

ETCD_SERVICE_KEY = "service"
ETCD_SERVICE_INSTANCE_LIST_KEY = "instance_list"
ETCD_SERVICE_INSTANCE_PRIMARY_KEY = "primary"
ETCD_RETRY_TIMES = 3


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


class EtcdKeyHelper(object):
    """
    A helper class to generate etcd keys. The root /service/namespace/instance_name is not shown in the following tree.
    ├── graph_1
    │   ├── cypher
    │   │   ├── instance_list
    │   │   │   ├── 11.12.13.14_7687
    │   │   │   └── 22.12.13.14_7687
    │   │   └── primary
    │   ├── gremlin
    │   │   ├── instance_list
    │   │   │   ├── 11.12.13.14_12314
    │   │   │   └── 22.12.13.14_12314
    │   │   └── primary
    │   └── procedure
    │       ├── instance_list
    │       │   ├── 11.12.13.14_10000
    │       │   └── 22.12.13.14_10000
    │       └── primary
    ├── graph_2
    │   ├── cypher
    │   │   ├── instance_list
    │   │   │   ├── 66.77.88.99_7687
    │   │   │   └── 77.77.88.99_7687
    │   │   └── primary
    │   ├── gremlin
    │   │   ├── instance_list
    │   │   │   ├── 66.77.88.99_12314
    │   │   │   └── 77.77.88.99_12314
    │   │   └── primary
    │   └── procedure
    │       ├── instance_list
    │       │   ├── 66.77.88.99_10000
    │       │   └── 77.77.88.99_10000
    │       └── primary
    └── metadata
        ├── graph_meta
        │   ├── graph_1
        │   └── graph_2
        ├── job_meta
        │   └── job_1
        └── plugin_meta
            └── plugin_1
    """

    def __init__(self, namespace="interactive", instance_name="default"):
        self._root = "/" + "/".join([namespace, instance_name, ETCD_SERVICE_KEY])

    def service_prefix(self):
        """
        Get the key for the service prefix.
        """
        return self._root

    def service_instance_list_prefix(self, graph_id: str, service_name: str):
        """
        Get the key for the service instance list.
        """
        return "/".join(
            [self._root, graph_id, service_name, ETCD_SERVICE_INSTANCE_LIST_KEY]
        )

    def service_primary_key(self, graph_id: str, service_name: str):
        """
        Get the key for the primary service instance.
        """
        return "/".join(
            [self._root, graph_id, service_name, ETCD_SERVICE_INSTANCE_PRIMARY_KEY]
        )

    def decode_service_key(self, key):
        """
        Decode the key to get the graph_id and service_name.
        expect key: /service/namespace/instance_name/graph_id/service_name/instance_list/ip_port
        """
        logger.info("Decode key: %s", key)
        if isinstance(key, bytes):
            key = key.decode("utf-8")
        if not key.startswith(self._root):
            return None
        split_list = key[len(self._root) :].strip("/").split("/")
        if len(split_list) == 4:
            return split_list[0], split_list[1], split_list[3]
        elif len(split_list) == 3:
            return split_list[0], split_list[1], None
        else:
            logger.error(
                "Fail to decode the key: %s, expect 3 or 4 parts, but got %d",
                key,
                len(split_list),
            )
            return None


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
    """

    def __init__(self):
        self._instance_list = set()
        self._primary_instance = None

        self.openapi_types = {"primary": ServiceInstance, "instance_list": set}

        self.attribute_map = {"primary": "primary", "instance_list": "instance_list"}

    @property
    def primary(self):
        return self._primary_instance

    @primary.setter
    def primary(self, primary_instance: ServiceInstance):
        self._primary_instance = primary_instance

    @property
    def instance_list(self):
        return self._instance_list

    @instance_list.setter
    def instance_list(self, instance_list: set):
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

    def add_instance(self, end_point, metrics: str):
        self._instance_list.add(ServiceInstance(end_point, metrics))

    def get_instance_list(self):
        return self._instance_list

    def get_primary_instance(self):
        return self._primary_instance

    def set_primary_instance(self, metrics: str):
        """
        expect metrics is json string, and endpoint is in the metrics.
        """
        try:
            logger.info("Set primary instance: %s", metrics)
            json_obj = json.loads(metrics)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid metrics: %s", metrics)
        if metrics is not None and "endpoint" in json_obj:
            self._primary_instance = ServiceInstance(json_obj["endpoint"], metrics)
        else:
            raise ValueError("Invalid metrics: %s", metrics)

    def is_valid(self):
        if self._primary_instance is None:
            return False
        if len(self._instance_list) == 0:
            return False
        if self._primary_instance not in self._instance_list:
            return False
        return True

    def to_dict(self):
        return {
            "primary": (
                self._primary_instance.to_dict()
                if self._primary_instance is not None
                else None
            ),
            "instance_list": [i.to_dict() for i in self._instance_list],
        }


class GlobalServiceDiscovery(Model):
    def __init__(self):
        self._map = {}  # graph_id -> service_name -> DiscoverResult

    def add_discovery_instance(self, graph_id, service_name, endpoint, metrics):
        if graph_id not in self._map:
            self._map[graph_id] = {}
        if service_name not in self._map[graph_id]:
            self._map[graph_id][service_name] = DiscoverResult()
        self._map[graph_id][service_name].add_instance(endpoint, metrics)

    def set_primary_instance(self, graph_id, service_name, metrics):
        if graph_id not in self._map:
            self._map[graph_id] = {}
        if service_name not in self._map[graph_id]:
            self._map[graph_id][service_name] = DiscoverResult()
        self._map[graph_id][service_name].set_primary_instance(metrics)

    def get(self, graph_id, service_name) -> DiscoverResult:
        if graph_id not in self._map:
            return None
        if service_name not in self._map[graph_id]:
            return None
        return self._map[graph_id][service_name]

    def remove_discovery(self, graph_id, service_name):
        if graph_id in self._map and service_name in self._map[graph_id]:
            del self._map[graph_id][service_name]

    def remove_primary_instance(self, graph_id, service_name):
        if graph_id in self._map and service_name in self._map[graph_id]:
            self._map[graph_id][service_name]._primary_instance = None

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
        self._etcd_kv_store = ETCDKeyValueStore(
            etcd_host, etcd_port, namespace, instance_name
        )
        self._etcd_kv_store.open()
        self._key_helper = EtcdKeyHelper(
            namespace=namespace, instance_name=instance_name
        )
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
        logger.info("Start watching the service registry on %s", ETCD_SERVICE_KEY)

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

        self._cancel_watch_handler = self._etcd_kv_store.add_watch_prefix_callback(
            ETCD_SERVICE_KEY, service_watch_call_back
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
        ret = self._global_discovery.get(graph_id, service_name)
        return ret.to_dict() if ret is not None else {}

    def list_all(self) -> dict:
        """
        List all services in the service registry.
        """
        ret = self._global_discovery
        return ret.to_dict() if ret is not None else {}

    def _handle_put_event(self, event: PutEvent):
        """
        Handle the put event.
        """
        value = event.value.decode("utf-8")
        graph_id, service_name, ip_port = self._try_decode_key(event.key)
        logger.info(
            "Put event: graph_id=%s, service_name=%s, endpoint=%s, value=%s",
            graph_id,
            service_name,
            ip_port,
            value,
        )
        if ip_port is None:
            self._global_discovery.set_primary_instance(graph_id, service_name, value)
        else:
            self._global_discovery.add_discovery_instance(
                graph_id, service_name, ip_port, value
            )

    def _handle_delete_event(self, event: DeleteEvent):
        """
        Handle the delete event.
        """
        graph_id, service_name, ip_port = self._try_decode_key(event.key)
        logger.info(
            "Delete event: graph_id=%s, service_name=%s, endpoint=%s",
            graph_id,
            service_name,
            ip_port,
        )
        if ip_port is None:
            self._global_discovery.remove_primary_instance(graph_id, service_name)
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
            raise RuntimeError("Got invalid key: %s", key)
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
            ip, int(port), config.master.k8s_launcher_config.namespace, config.master.instance_name
        )
    else:
        raise ValueError(
            "Invalid service registry type: %s", config.master.service_registry.type
        )
    service_registry.start()


def get_service_registry():
    global service_registry
    return service_registry
