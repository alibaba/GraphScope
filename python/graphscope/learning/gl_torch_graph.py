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


class GLTorchGraph(object):
    def __init__(self, server_list):
        assert len(server_list) == 4
        self._master_addr, self._server_client_master_port = server_list[0].split(":")
        self._train_master_addr, self._train_loader_master_port = server_list[1].split(
            ":"
        )
        self._val_master_addr, self._val_loader_master_port = server_list[2].split(":")
        self._test_master_addr, self._test_loader_master_port = server_list[3].split(
            ":"
        )
        assert (
            self._master_addr
            == self._train_master_addr
            == self._val_master_addr
            == self._test_master_addr
        )

    @property
    def master_addr(self):
        return self._master_addr

    @property
    def server_client_master_port(self):
        return self._server_client_master_port

    @property
    def train_loader_master_port(self):
        return self._train_loader_master_port

    @property
    def val_loader_master_port(self):
        return self._val_loader_master_port

    @property
    def test_loader_master_port(self):
        return self._test_loader_master_port

    @staticmethod
    def check_edge(schema, edge):
        if not isinstance(edge, tuple) or len(edge) != 3:
            raise ValueError("Each edge should be a tuple of length 3")
        for vertex_label in [edge[0], edge[2]]:
            if vertex_label not in schema.vertex_labels:
                raise ValueError(f"Invalid edge label: {vertex_label}")
        if edge[1] not in schema.edge_labels:
            raise ValueError(f"Invalid edge label: {edge[1]}")

    @staticmethod
    def check_edges(schema, edges):
        for edge in edges:
            GLTorchGraph.check_edge(schema, edge)
            if edge in edges[edges.index(edge) + 1 :]:
                raise ValueError(f"Duplicated edge: {edge}")

    @staticmethod
    def check_features(feature_names, properties):
        data_type = None
        property_name = ""
        property_dict = {property.name: property for property in properties}
        for feature in feature_names:
            if feature not in property_dict:
                raise ValueError(f"Feature '{feature}' does not exist")
            property = property_dict[feature]
            if data_type is None:
                data_type = property.data_type
                property_name = property.name
            if data_type != property.data_type:
                raise ValueError(
                    f"Inconsistent DataType: '{data_type}' for {property_name} \
                        and '{property.data_type}' for {property.name}"
                )

    @staticmethod
    def check_node_features(schema, node_features):
        if node_features is None:
            return
        for label, feature_names in node_features.items():
            if label not in schema.vertex_labels:
                raise ValueError(f"Invalid vertex label: {label}")
            GLTorchGraph.check_features(
                feature_names, schema.get_vertex_properties(label)
            )

    @staticmethod
    def check_edge_features(schema, edge_features):
        if edge_features is None:
            return
        for edge, feature_names in edge_features.items():
            GLTorchGraph.check_edge(edge)
            GLTorchGraph.check_features(
                feature_names, schema.get_edge_properties(edge[1])
            )

    @staticmethod
    def check_node_labels(schema, node_labels):
        if node_labels is None:
            return
        for label, property_name in node_labels.items():
            if label not in schema.vertex_labels:
                raise ValueError(f"Invalid vertex label: {label}")
            vertex_property_names = [
                property.name for property in schema.get_vertex_properties(label)
            ]
            if property_name not in vertex_property_names:
                raise ValueError(
                    f"Invalid property name '{property_name}' for vertex label '{label}'"
                )

    @staticmethod
    def check_edge_weights(schema, edge_weights):
        if edge_weights is None:
            return
        for edge, property_name in edge_weights.items():
            GLTorchGraph.check_edge(edge)
            edge_property_names = [
                property.name for property in schema.get_edge_properties(edge[1])
            ]
            if property_name not in edge_property_names:
                raise ValueError(
                    f"Invalid property name '{property_name}' for edge '{edge}'"
                )

    @staticmethod
    def check_random_node_split(random_node_split):
        if random_node_split is None:
            return
        if not isinstance(random_node_split, dict):
            raise ValueError("Random node split should be a dictionary")
        if "num_val" not in random_node_split:
            raise ValueError("Missing 'num_val' in random node split")
        if "num_test" not in random_node_split:
            raise ValueError("Missing 'num_test' in random node split")
        if len(random_node_split) != 2:
            raise ValueError("Invalid parameters in random node split")

    @staticmethod
    def check_params(schema, config):
        GLTorchGraph.check_edges(schema, config.get("edges"))
        GLTorchGraph.check_node_features(schema, config.get("node_features"))
        GLTorchGraph.check_edge_features(schema, config.get("edge_features"))
        GLTorchGraph.check_node_labels(schema, config.get("node_labels"))
        GLTorchGraph.check_random_node_split(config.get("random_node_split"))
        GLTorchGraph.check_edge_weights(schema, config.get("edge_weights"))

    @staticmethod
    def transform_config(config):
        # transform config to a format that is compatible with json dumps and loads
        transformed_config = config.copy()
        transformed_config["edges"] = [
            [node for node in edge] for edge in config["edges"]
        ]
        if config["edge_weights"]:
            transformed_config["edge_weights"] = {
                config["edges"].index(edge): weights
                for edge, weights in config["edge_weights"].items()
            }
        if config["edge_features"]:
            transformed_config["edge_features"] = {
                config["edges"].index(edge): features
                for edge, features in config["edge_features"].items()
            }
        return transformed_config

    @staticmethod
    def reverse_transform_config(config):
        reversed_config = config.copy()
        reversed_config["edges"] = [tuple(edge) for edge in config["edges"]]
        if config["edge_weights"]:
            reversed_config["edge_weights"] = {
                reversed_config["edges"][int(index)]: weights
                for index, weights in config["edge_weights"].items()
            }
        if config["edge_features"]:
            reversed_config["edge_features"] = {
                reversed_config["edges"][int(index)]: features
                for index, features in config["edge_features"].items()
            }
        return reversed_config
