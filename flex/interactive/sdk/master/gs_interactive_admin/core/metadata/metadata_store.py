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


class DefaultMetadataStore(IMetadataStore):
    """
    The default implementation of the metadata store.
    """

    def __init__(self, kv_store_handle: AbstractKeyValueStore):
        self.kv_store_handle_ = kv_store_handle

    def open(self):
        self.kv_store_handle_.open()

    def close(self):
        self.kv_store_handle_.close()

    def create_graph_meta(self, graph_meta: dict) -> str:
        return self.kv_store_handle_.insert(graph_meta)

    def get_graph_meta(self, graph_id: str) -> dict:
        return self.kv_store_handle_.get(graph_id)

    def get_all_graph_meta(self) -> list:
        return self.kv_store_handle_.get_with_prefix("graph")

    def delete_graph_meta(self, graph_id: str) -> bool:
        return self.kv_store_handle_.delete(graph_id)

    def update_graph_meta(self, graph_id: str, graph_meta: dict) -> bool:
        return self.kv_store_handle_.update(graph_id, graph_meta)

    def create_job_meta(self, job_meta: dict) -> str:
        return self.kv_store_handle_.insert(job_meta)

    def get_job_meta(self, job_id: str) -> dict:
        return self.kv_store_handle_.get(job_id)

    def get_all_job_meta(self) -> list:
        return self.kv_store_handle_.get_with_prefix("job")

    def delete_job_meta(self, job_id: str) -> bool:
        return self.kv_store_handle_.delete(job_id)

    def update_job_meta(self, job_id: str, job_meta: dict) -> bool:
        return self.kv_store_handle_.update(job_id, job_meta)

    def create_plugin_meta(self, graph_id: str, plugin_meta: dict) -> str:
        return self.kv_store_handle_.insert(plugin_meta)

    def get_plugin_meta(self, graph_id: str, plugin_id: str) -> dict:
        return self.kv_store_handle_.get(plugin_id)

    def get_all_plugin_meta(self, graph_id: str) -> list:
        return self.kv_store_handle_.get_with_prefix("plugin")

    def delete_plugin_meta(self, graph_id: str, plugin_id: str) -> bool:
        return self.kv_store_handle_.delete(plugin_id)

    def update_plugin_meta(
        self, graph_id: str, plugin_id: str, plugin_meta: dict
    ) -> bool:
        return self.kv_store_handle_.update(plugin_id, plugin_meta)

    def delete_plugin_meta_by_graph_id(self, graph_id: str) -> bool:
        return self.kv_store_handle_.delete_with_prefix(graph_id)
