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

from gs_interactive_admin.core.metadata.kv_store import AbstractKeyValueStore
from abc import ABCMeta
from abc import abstractmethod
import logging

from gs_interactive_admin.core.config import Config
from gs_interactive_admin.core.metadata.kv_store import ETCDKeyValueStore
from gs_interactive_admin.util import MetaKeyHelper
from gs_interactive_admin.util import remove_nones
import yaml

logger = logging.getLogger("interactive")


class IMetadataStore(metaclass=ABCMeta):
    """
    The interface for the metadata store.
    """

    @abstractmethod
    def open(self):
        """
        Open the metadata store.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the metadata store.
        """
        pass

    @abstractmethod
    def create_graph_meta(self, graph_meta: dict):
        """
        Create the metadata for a graph.
        """
        pass

    @abstractmethod
    def get_graph_meta(self, graph_id: str):
        """
        Get the metadata for a graph.
        """
        pass
    
    @abstractmethod
    def get_graph_statistics(self, graph_id : str):
        """
        Get the statistics for a graph.
        """
        pass

    @abstractmethod
    def get_all_graph_meta(self):
        """
        Get the metadata for all graphs.
        """
        pass

    @abstractmethod
    def delete_graph_meta(self, graph_id: str):
        """
        Delete the metadata for a graph.
        """
        pass

    @abstractmethod
    def update_graph_meta(self, graph_id: str, graph_meta: dict):
        """
        Update the metadata for a graph.
        """
        pass
    
    @abstractmethod
    def update_graph_meta_with_func(self, graph_id: str, func):
        """
        Update the metadata for a graph with a function.
        """
        pass

    @abstractmethod
    def create_job_meta(self, job_meta: dict):
        """
        Create the metadata for a job.
        """
        pass

    @abstractmethod
    def get_job_meta(self, job_id: str):
        """
        Get the metadata for a job.
        """
        pass

    @abstractmethod
    def get_all_job_meta(self):
        """
        Get the metadata for all jobs.
        """
        pass

    @abstractmethod
    def delete_job_meta(self, job_id: str):
        """
        Delete the metadata for a job.
        """
        pass

    @abstractmethod
    def update_job_meta(self, job_id: str, job_meta: dict):
        """
        Update the metadata for a job.
        """
        pass

    @abstractmethod
    def create_plugin_meta(self, graph_id: str, plugin_meta: dict):
        """
        Create the metadata for a plugin.
        """
        pass

    @abstractmethod
    def get_plugin_meta(self, graph_id: str, plugin_id: str):
        """
        Get the metadata for a plugin.
        """
        pass

    @abstractmethod
    def get_all_plugin_meta(self, graph_id: str):
        """
        Get the metadata for all plugins.
        """
        pass

    @abstractmethod
    def delete_plugin_meta(self, graph_id: str, plugin_id: str):
        """
        Delete the metadata for a plugin.
        """
        pass

    @abstractmethod
    def update_plugin_meta(self, graph_id: str, plugin_id: str, plugin_meta: dict):
        """
        Update the metadata for a plugin.
        """
        pass

    @abstractmethod
    def delete_plugin_meta_by_graph_id(self, graph_id: str):
        """
        Delete the metadata for all plugins of a graph.
        """
        pass

    @abstractmethod
    def set_graph_status(self, graph_id: str, status: str):
        """
        Set the status for a graph.
        """
        pass


class DefaultMetadataStore(IMetadataStore):
    """
    The default implementation of the metadata store.
    """

    def __init__(
        self,
        kv_store_handle: AbstractKeyValueStore,
        namespace="interactive",
        instance_name="default",
    ):
        self._kv_store_handle = kv_store_handle
        self._meta_key_helper = MetaKeyHelper(namespace, instance_name)

    def open(self):
        self._kv_store_handle.open()

    def close(self):
        self._kv_store_handle.close()

    def __del__(self):
        self.close()

    def create_graph_meta(self, graph_meta: dict) -> str:
        graph_meta = remove_nones(graph_meta)
        logger.info(
            "Creating graph meta prefix %s, value : %s"
            % (self._meta_key_helper.graph_meta_prefix(), graph_meta)
        )
        full_key, key_id = self._kv_store_handle.insert_with_prefix(
            self._meta_key_helper.graph_meta_prefix(), str(graph_meta)
        )
        logger.info(
            "Created graph meta prefix %s, key_id : %s"
            % (self._meta_key_helper.graph_meta_prefix(), key_id)
        )
        # add id field back into the graph meta
        graph_meta["id"] = key_id
        self.update_graph_meta(key_id, str(graph_meta))
        return key_id

    def get_graph_meta(self, graph_id: str) -> dict:
        logger.info(
            "Getting graph meta prefix %s, graph_id %s"
            % (self._meta_key_helper.graph_meta_prefix(), graph_id)
        )
        meta_str =  self._kv_store_handle.get(
            "/".join([self._meta_key_helper.graph_meta_prefix(), graph_id])
        )
        if meta_str is None:
            logger.info("Graph meta not found for graph id %s" % graph_id)
            return None
        # convert the string to dict
        res = yaml.safe_load(meta_str)
        if "_schema" in res:
            res["schema"] = res["_schema"]
            del res["_schema"]
        # Remove all key-value pairs that value is None, recursively.
        res = remove_nones(res)
        logger.info("Remove none values from graph meta: %s" % res)
        return res
    
    def get_graph_statistics(self, graph_id : str) -> dict:
        """
        Get the statistic info for a graph.
        Args:
            graph_id (str): The unique identifier of the graph
        """
        logger.info(f"Getting graph statistics fo {graph_id}")
        return self._kv_store_handle.get(self._meta_key_helper.graph_statistics_key(graph_id))
        
    def get_graph_schema(self, graph_id: str) -> dict:
        meta = self.get_graph_meta(graph_id)
        return meta.get("schema", {})

    def get_all_graph_meta(self) -> list:
        logger.info(
            "Getting all graph meta prefix %s"
            % self._meta_key_helper.graph_meta_prefix()
        )
        return self._kv_store_handle.get_with_prefix(
            self._meta_key_helper.graph_meta_prefix()
        )

    def delete_graph_meta(self, graph_id: str) -> bool:
        logger.info(
            "Deleting graph meta prefix %s, id %s"
            % (self._meta_key_helper.graph_meta_prefix(), graph_id)
        )
        return self._kv_store_handle.delete(
            "/".join([self._meta_key_helper.graph_meta_prefix(), graph_id])
        )

    def update_graph_meta(self, graph_id: str, graph_meta: dict) -> bool:
        logger.info(
            "Updating graph meta prefix %s, id %s, value %s"
            % (self._meta_key_helper.graph_meta_prefix(), graph_id, graph_meta)
        )
        return self._kv_store_handle.update(
            "/".join([self._meta_key_helper.graph_meta_prefix(), graph_id]), graph_meta
        )
        
    def update_graph_meta_with_func(self, graph_id, func):
        logger.info(
            "Updating graph meta prefix %s, id %s with function"
            % (self._meta_key_helper.graph_meta_prefix(), graph_id)
        )
        return self._kv_store_handle.update_with_func(
            "/".join([self._meta_key_helper.graph_meta_prefix(), graph_id]), func
        )

    def create_job_meta(self, job_meta: dict) -> str:
        logger.info(
            "Creating job meta prefix %s, value %s"
            % (self._meta_key_helper.job_meta_prefix(), job_meta)
        )
        full_key, key_id = self._kv_store_handle.insert_with_prefix(
            self._meta_key_helper.job_meta_prefix(), job_meta
        )
        return key_id

    def get_job_meta(self, job_id: str) -> dict:
        logger.info(
            "Getting job meta prefix %s, id %s"
            % (self._meta_key_helper.job_meta_prefix(), job_id)
        )
        return self._kv_store_handle.get(
            "/".join([self._meta_key_helper.job_meta_prefix(), job_id])
        )

    def get_all_job_meta(self) -> list:
        return self._kv_store_handle.get_with_prefix(
            self._meta_key_helper.job_meta_prefix()
        )

    def delete_job_meta(self, job_id: str) -> bool:
        return self._kv_store_handle.delete(
            self._meta_key_helper.job_meta_prefix(), job_id
        )

    def update_job_meta(self, job_id: str, job_meta: dict) -> bool:
        return self._kv_store_handle.update(
            "/".join([self._meta_key_helper.job_meta_prefix(), job_id]), str(job_meta)
        )

    def create_plugin_meta(self, graph_id: str, plugin_meta: dict) -> str:
        logger.info(
            "Creating plugin meta prefix %s, value %s"
            % (self._meta_key_helper.plugin_meta_prefix(), plugin_meta)
        )
        full_key, key_id = self._kv_store_handle.insert_with_prefix(
            self._meta_key_helper.plugin_meta_prefix(), plugin_meta
        )
        return key_id

    def get_plugin_meta(self, graph_id: str, plugin_id: str) -> dict:
        logger.info(
            "Getting plugin meta prefix %s, id %s"
            % (self._meta_key_helper.plugin_meta_prefix(), plugin_id)
        )
        return self._kv_store_handle.get(
            "/".join([self._meta_key_helper.plugin_meta_prefix(), plugin_id])
        )

    def get_all_plugin_meta(self, graph_id: str) -> list:
        logger.info(
            "Getting all plugin meta prefix %s"
            % self._meta_key_helper.plugin_meta_prefix()
        )
        return self._kv_store_handle.get_with_prefix(
            self._meta_key_helper.plugin_meta_prefix()
        )

    def delete_plugin_meta(self, graph_id: str, plugin_id: str) -> bool:
        logger.info(
            "Deleting plugin meta prefix %s, id %s"
            % (self._meta_key_helper.plugin_meta_prefix(), plugin_id)
        )
        return self._kv_store_handle.delete(
            "/".join([self._meta_key_helper.plugin_meta_prefix(), plugin_id])
        )

    def update_plugin_meta(
        self, graph_id: str, plugin_id: str, plugin_meta: dict
    ) -> bool:
        logger.info(
            "Updating plugin meta prefix %s, id %s, value %s"
            % (self._meta_key_helper.plugin_meta_prefix(), plugin_id, plugin_meta)
        )
        return self._kv_store_handle.update(
            "/".join([self._meta_key_helper.plugin_meta_prefix(), plugin_id]),
            plugin_meta,
        )

    def delete_plugin_meta_by_graph_id(self, graph_id: str) -> bool:
        # get all plugins metas.
        logger.info("Deleting plugin meta by graph id %s" % graph_id)
        plugin_metas = self.get_all_plugin_meta(graph_id)
        for plugin_meta in plugin_metas:
            if plugin_meta["graph_id"] == graph_id:
                self.delete_plugin_meta(graph_id, plugin_meta["plugin_id"])
        return True

    def set_graph_status(self, graph_id, status):
        """
        Use a key-value pair to store the status of a graph.
        """
        return self._kv_store_handle.update(
            self._meta_key_helper.graph_status_key(graph_id), status
        )


metadata_store = None


def get_metadata_store():
    global metadata_store
    return metadata_store

test_graph_def = {
    "name": "modern_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"var_char": {"max_length": 16}}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            }
        ],
    },
}


def __make_default_graph_meta(metadata_store: IMetadataStore):
    if metadata_store.get_graph_meta("1") is None:
        key_id = metadata_store.create_graph_meta(test_graph_def)
        # Expect the key_id is 1
        if key_id != "1":
            raise ValueError("The key_id is not 1: %s" % key_id)
        logger.info("Created graph meta with key_id 1")
        #NOTE: The bulk_loading process will be automatically triggered when the engine pod are launched.

def init_metadata_store(config: Config):
    global metadata_store
    if config.compute_engine.metadata_store.uri.startswith("http://"):
        # we assume is etcd key-value store
        etcd_metadata_store = ETCDKeyValueStore.create_from_endpoint(
            config.compute_engine.metadata_store.uri,
            config.master.k8s_launcher_config.namespace,
            config.master.instance_name,
        )
        metadata_store = DefaultMetadataStore(etcd_metadata_store)
        metadata_store.open()
        
        # Check whether default graph's metadata exists, if not, create it.
        __make_default_graph_meta(metadata_store)
    else:
        raise ValueError(
            "Unsupported metadata store URI: %s"
            % config.compute_engine.metadata_store.uri
        )
