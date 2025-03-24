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

import etcd3

import logging

logger = logging.getLogger("interactive")


class AbstractKeyValueStore(metaclass=ABCMeta):
    """
    An abstraction for key value store.
    """

    @abstractmethod
    def open(self):
        """
        Open a connection to the key value store.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the connection to the key value store.
        """
        pass

    @abstractmethod
    def insert(self, key, value) -> bool:
        """
        Create a key value pair.
        """
        pass

    @abstractmethod
    def insert_with_prefix(self, prefix, value) -> str:
        """
        Create a key value pair.
        """
        pass

    @abstractmethod
    def get(self, key) -> dict:
        """
        Get the value of a key.
        """
        pass

    @abstractmethod
    def get_with_prefix(self, prefix) -> list:
        """
        Get all the keys with a prefix.
        """
        pass

    @abstractmethod
    def delete(self, key) -> bool:
        """
        Delete a key value pair.
        """
        pass

    @abstractmethod
    def delete_with_prefix(self, prefix) -> bool:
        """
        Delete all the keys with a prefix.
        """
        pass

    @abstractmethod
    def update(self, key, value):
        """
        Update the value of a key.
        """
        pass

    @abstractmethod
    def update_with_func(self, key, func):
        """
        Update the value of a key using a function.
        """
        pass

    @abstractmethod
    def add_watch_prefix_callback(self, prefix, callback):
        """
        Add a watch on the keys with a prefix.
        """
        pass

    @abstractmethod
    def cancel_watch(self, watch_id):
        """
        Cancel a watch.
        """
        pass


class ETCDKeyValueStore(AbstractKeyValueStore):
    """
    An implementation of key value store using ETCD.
    """

    def __init__(self, host="localhost", port=2379, root="/interactive/default"):
        self._host = host
        self._port = port
        self._client = None
        self._root = root
        self.inc_id_dir = "/inc_id"

    @classmethod
    def create(
        cls, host: str, port: int, namespace="interactive", instance_name="default"
    ):
        return ETCDKeyValueStore(host, port, "/" + "/".join([namespace, instance_name]))

    @classmethod
    def create_from_endpoint(
        cls, endpoint: str, namespace="interactive", instance_name="default"
    ):
        """
        Initialize the key value store with the endpoint.
        param endpoint: The endpoint of the key value store, in format http://host:port
        """
        # parse the endpoint
        if not endpoint.startswith("http://"):
            raise ValueError("Invalid endpoint format.")
        endpoint = endpoint[7:]
        host, port = endpoint.split(":")
        return ETCDKeyValueStore(
            host, int(port), "/" + "/".join([namespace, instance_name])
        )

    def _get_full_key(self, keys: list):
        if isinstance(keys, str):
            return self._root + "/" + keys
        elif isinstance(keys, list):
            return self._root + "/" + "/".join(keys)
        else:
            raise ValueError("Invalid key type.")

    def _get_next_key(self, prefix: str):
        """
        Inside etcd client, we maintain a key to store the next key to be used.
        This operation is atomic.
        """
        full_key = self._get_full_key([self.inc_id_dir, prefix])
        self._client.put_if_not_exists(
            full_key, "1"
        )  # initialize the key if it does not exist.
        # compare_and swap
        max_retry = 10
        while max_retry > 0:
            cur_value = int(self._client.get(full_key)[0])
            if self._client.replace(full_key, str(cur_value), str(cur_value + 1)):
                return prefix + "/" + str(cur_value), cur_value
            max_retry -= 1
        raise RuntimeError("Failed to get next key.")

    def open(self):
        self._client = etcd3.client(host=self._host, port=self._port)

    def close(self):
        self._client.close()
        logger.info("ETCD connection closed.")

    def insert(self, key, value) -> str:
        logger.info(f"Inserting key {self._get_full_key(key)} with value {value}")
        print(f"Inserting key {self._get_full_key(key)} with value {value}")
        self._client.put(self._get_full_key(key), value)
        return key

    def insert_with_prefix(self, prefix, value) -> tuple:
        """
        Insert the value without giving a specific key, but a prefix. The key is generated automatically, in increasing order.
        """
        next_key, next_val = self._get_next_key(prefix)
        full_key = self._get_full_key(next_key)
        logger.info(f"Inserting key {full_key} with value {value}")
        self._client.put(full_key, value)
        return next_key, str(next_val)

    def get(self, key) -> str:
        logger.info("Getting key: " + self._get_full_key(key))
        ret = self._client.get(self._get_full_key(key))
        if not ret[0]:
            return None
        return ret[0].decode("utf-8")

    def get_with_prefix(self, prefix) -> list:
        logger.info("Getting keys with prefix: " + self._get_full_key(prefix))
        # return list(self._client.get_prefix(self._get_full_key(prefix)))
        ret = self._client.get_prefix(self._get_full_key(prefix))
        # for each key value pair in ret, use the pair[1].key and pair[0] to construct a list of tuples.
        # for pair[1].key substr with self._root
        return [
            (
                pair[1].key.decode("utf-8")[len(self._root) + 1 :],
                pair[0].decode("utf-8"),
            )
            for pair in ret
        ]

    def delete(self, key) -> bool:
        return self._client.delete(self._get_full_key(key))

    def delete_with_prefix(self, prefix) -> bool:
        return self._client.delete_prefix(self._get_full_key(prefix))

    def update(self, key, new_value):
        if not self._client.get(self._get_full_key(key)):
            raise ValueError("Key does not exist.")
        cur_value = self.get(key)
        return self._client.replace(self._get_full_key(key), cur_value, new_value)

    def update_with_func(self, key, func):
        if not self._client.get(self._get_full_key(key)):
            raise ValueError("Key does not exist.")
        cur_value = self.get(key)
        new_value = func(cur_value)
        return self._client.replace(self._get_full_key(key), cur_value, new_value)

    def add_watch_prefix_callback(self, prefix, callback):
        """
        Add a watch on the keys with a prefix.
        """
        logger.info("Adding watch on prefix: " + prefix)
        return self._client.add_watch_prefix_callback(prefix, callback)

    def cancel_watch(self, watch_id):
        """
        Cancel a watch.
        """
        logger.info("Cancelling watch with id: " + str(watch_id))
        return self._client.cancel_watch(watch_id)
