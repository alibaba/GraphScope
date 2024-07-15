#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

import os

import click
import yaml
from treelib import Node
from treelib import Tree

from graphscope.flex.rest import PrimitiveType
from graphscope.flex.rest import StringType


def read_yaml_file(path) -> dict:
    """Reads YAML file and returns as a python object."""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def write_yaml_file(data, path):
    """Writes python object to the YAML file."""
    with open(path, "w") as file:
        yaml.dump(data, file)


def is_valid_file_path(path) -> bool:
    """Check if the path exists and corresponds to a regular file."""
    return os.path.exists(path) and os.path.isfile(path)


def terminal_display(data):
    """Display tablular data in terminal.

    Args:
        data: two dimensional list of string type.
    """
    # Compute the maximum width for each column
    column_widths = [max(len(str(item)) for item in column) for column in zip(*data)]
    # Display the data with aligned columns
    for row in data:
        print(
            "  ".join(
                "{:<{}}".format(item, width) for item, width in zip(row, column_widths)
            )
        )


def info(message: str, bold=False, fg=None):
    click.secho(message, bold=bold, fg=fg)


def warning(message: str, bold=False):
    click.secho("[WARNING] ", nl=False, fg="yellow", bold=True)
    click.secho(message, bold=bold)


def succ(message: str, bold=False):
    click.secho("[SUCCESS] ", nl=False, fg="green", bold=True)
    click.secho(message, bold=bold)


def err(message: str, bold=False):
    click.secho("[FAILED] ", nl=False, fg="red", bold=True)
    click.secho(message, bold=bold)


class TreeDisplay(object):
    def __init__(self):
        self.tree = Tree()
        self.root_identifier = "GRAPH"
        self.tree.create_node(identifier=self.root_identifier)

    def parse_property_type(self, property):
        actual_instance = property.actual_instance
        if isinstance(actual_instance, PrimitiveType):
            return actual_instance.primitive_type
        elif isinstance(actual_instance, StringType):
            return "string"
        else:
            return str(actual_instance)

    def create_graph_node_for_groot(self, graph, recursive=True):
        # graph name must be unique
        self.tree.create_node(
            tag=f"identifier: {graph.name}",
            identifier=graph.name,
            parent=self.root_identifier,
        )
        if recursive:
            self.create_schema_node_for_groot(graph)

    def create_schema_node_for_groot(self, graph):
        schema_identifier = f"{graph.name}_schema"
        self.tree.create_node(
            tag="schema", identifier=schema_identifier, parent=graph.name
        )
        # vertex type
        self.tree.create_node(
            tag="vertex types",
            identifier=f"{schema_identifier}_vertex_types",
            parent=schema_identifier,
        )
        for vertex in graph.var_schema.vertices:
            self.tree.create_node(
                tag=f"{vertex.label}",
                identifier=f"{graph.name}_{vertex.label}",
                parent=f"{schema_identifier}_vertex_types",
            )
            # property
            for p in vertex.properties:
                identifier = f"{graph.name}_{vertex.label}_{p.name}"
                tag = "Property(name: {0}, type: {1}, is_primary_key: {2})".format(
                    p.name,
                    p.type,
                    p.is_primary_key,
                )
                self.tree.create_node(
                    tag=tag,
                    identifier=identifier,
                    parent=f"{graph.name}_{vertex.label}",
                )
        # edge type
        self.tree.create_node(
            tag="edge types",
            identifier=f"{schema_identifier}_edge_types",
            parent=schema_identifier,
        )
        for edge in graph.var_schema.edges:
            for relation in edge.relations:
                edge_label = "({0}) -[{1}]-> ({2})".format(
                    relation.src_label,
                    edge.label,
                    relation.dst_label,
                )
                self.tree.create_node(
                    tag=f"{edge_label}",
                    identifier=f"{graph.name}_{edge_label}",
                    parent=f"{schema_identifier}_edge_types",
                )
                # property
                for p in edge.properties:
                    identifier = f"{graph.name}_{edge_label}_{p.name}"
                    tag = "Property(name: {0}, type: {1}, is_primary_key: {2})".format(
                        p.name, p.type, p.is_primary_key
                    )
                    self.tree.create_node(
                        tag=tag,
                        identifier=identifier,
                        parent=f"{graph.name}_{edge_label}",
                    )

    def create_graph_node(self, graph, recursive=True):
        # graph name must be unique
        self.tree.create_node(
            tag=f"{graph.name}(identifier: {graph.id})",
            identifier=graph.id,
            parent=self.root_identifier,
        )
        if recursive:
            self.create_schema_node(graph)

    def create_schema_node(self, graph):
        schema_identifier = f"{graph.id}_schema"
        self.tree.create_node(
            tag="schema", identifier=schema_identifier, parent=graph.id
        )
        # vertex type
        vertex_type_identifier = f"{schema_identifier}_vertex_types"
        self.tree.create_node(
            tag="vertex types",
            identifier=vertex_type_identifier,
            parent=schema_identifier,
        )
        if graph.var_schema is not None and graph.var_schema.vertex_types is not None:
            for vertex in graph.var_schema.vertex_types:
                vertex_identifier = f"{vertex_type_identifier}_{vertex.type_name}"
                self.tree.create_node(
                    tag=f"{vertex.type_name}",
                    identifier=vertex_identifier,
                    parent=vertex_type_identifier,
                )
                # property
                if vertex.properties is not None:
                    for p in vertex.properties:
                        is_primary_key = (
                            True if p.property_name in vertex.primary_keys else False
                        )
                        property_identifier = f"{vertex_identifier}_{p.property_name}"
                        tag = "Property(name: {0}, type: {1}, is_primary_key: {2})".format(
                            p.property_name,
                            self.parse_property_type(p.property_type),
                            is_primary_key,
                        )
                        self.tree.create_node(
                            tag=tag,
                            identifier=property_identifier,
                            parent=vertex_identifier,
                        )
        # edge type
        edge_type_identifier = f"{schema_identifier}_edge_types"
        self.tree.create_node(
            tag="edge types",
            identifier=edge_type_identifier,
            parent=schema_identifier,
        )
        if graph.var_schema is not None and graph.var_schema.edge_types is not None:
            for edge in graph.var_schema.edge_types:
                for relation in edge.vertex_type_pair_relations:
                    edge_label = "({0}) -[{1}]-> ({2})".format(
                        relation.source_vertex,
                        edge.type_name,
                        relation.destination_vertex,
                    )
                    edge_identifier = f"{edge_type_identifier}_{edge_label}"
                    self.tree.create_node(
                        tag=f"{edge_label}",
                        identifier=edge_identifier,
                        parent=edge_type_identifier,
                    )
                    if edge.properties is not None:
                        for p in edge.properties:
                            is_primary_key = (
                                True if p.property_name in edge.primary_keys else False
                            )
                            property_identifier = f"{edge_identifier}_{p.property_name}"
                            tag = "Property(name: {0}, type: {1}, is_primary_key: {2})".format(
                                p.property_name,
                                self.parse_property_type(p.property_type),
                                is_primary_key,
                            )
                            self.tree.create_node(
                                tag=tag,
                                identifier=property_identifier,
                                parent=edge_identifier,
                            )

    def create_stored_procedure_node(self, graph, stored_procedures):
        stored_procedure_identifier = f"{graph.id}_stored_procedure"
        self.tree.create_node(
            tag="stored procedure",
            identifier=stored_procedure_identifier,
            parent=graph.id,
        )
        for p in stored_procedures:
            self.tree.create_node(
                tag="StoredProc(identifier: {0}, type: {1}, runnable: {2}, query: {3}, description: {4})".format(
                    p.id, p.type, p.runnable, p.query, p.description
                ),
                identifier=f"{stored_procedure_identifier}_{p.id}",
                parent=stored_procedure_identifier,
            )

    def create_job_node(self, graph, jobs):
        job_identifier = f"{graph.id}_job"
        self.tree.create_node(tag="job", identifier=job_identifier, parent=graph.id)
        for j in jobs:
            if j.detail["graph_id"] != graph.id:
                continue
            self.tree.create_node(
                tag="Job(identifier: {0}, type: {1}, status: {2}, start time: {3}, end time: {4})".format(
                    j.id, j.type, j.status, j.start_time, str(j.end_time)
                ),
                identifier=f"{job_identifier}_{j.id}",
                parent=job_identifier,
            )

    def create_datasource_node(self, graph, datasource):
        datasource_identifier = f"{graph.id}_datasource"
        self.tree.create_node(
            tag="data sources", identifier=datasource_identifier, parent=graph.id
        )
        # vertex mappings
        self.tree.create_node(
            tag="vertex mappings",
            identifier=f"{datasource_identifier}_vertex_mappings",
            parent=datasource_identifier,
        )
        for mapping in datasource.vertices_datasource:
            self.tree.create_node(
                tag=f"{mapping.type_name}(input: {mapping.location})",
                identifier=f"{datasource_identifier}_{mapping.type_name}",
                parent=f"{datasource_identifier}_vertex_mappings",
            )
            if mapping.property_mapping is not None:
                for index, property_name in mapping.property_mapping.items():
                    tag = "Property(name: {0}) -> DataSourceColumn(index: {1})".format(
                        property_name, index
                    )
                    identifier = (
                        f"{datasource_identifier}_{mapping.type_name}_{property_name}"
                    )
                    self.tree.create_node(
                        tag=tag,
                        identifier=identifier,
                        parent=f"{datasource_identifier}_{mapping.type_name}",
                    )
        # edge mappings
        self.tree.create_node(
            tag="edge mappings",
            identifier=f"{datasource_identifier}_edge_mappings",
            parent=datasource_identifier,
        )
        for mapping in datasource.edges_datasource:
            edge_label = "({0}) -[{1}]-> ({2})".format(
                mapping.source_vertex,
                mapping.type_name,
                mapping.destination_vertex,
            )
            self.tree.create_node(
                tag=f"{edge_label}(input: {mapping.location})",
                identifier=f"{datasource_identifier}_{edge_label}",
                parent=f"{datasource_identifier}_edge_mappings",
            )
            # source vertex mapping
            for (
                index,
                source_vertex_primary_key,
            ) in mapping.source_pk_column_map.items():
                self.tree.create_node(
                    tag="SourceVertexPrimaryKey(name: {0}) -> DataSourceColumn(index: {1})".format(
                        source_vertex_primary_key, index
                    ),
                    identifier="{0}_{1}_source_vertex_primary_key_{2}".format(
                        datasource_identifier,
                        edge_label,
                        source_vertex_primary_key,
                    ),
                    parent=f"{datasource_identifier}_{edge_label}",
                )
            # destination vertex mapping
            for (
                index,
                destination_vertex_primary_key,
            ) in mapping.destination_pk_column_map.items():
                self.tree.create_node(
                    tag="DestinationVertexPrimaryKey(name: {0}) -> DataSourceColumn(index: {1})".format(
                        destination_vertex_primary_key, index
                    ),
                    identifier="{0}_{1}_destination_vertex_primary_key_{2}".format(
                        datasource_identifier,
                        edge_label,
                        destination_vertex_primary_key,
                    ),
                    parent=f"{datasource_identifier}_{edge_label}",
                )
            # property mapping
            if mapping.property_mapping is not None:
                for index, property_name in mapping.property_mapping.items():
                    tag = "Property(name: {0}) -> DataSourceColumn(index: {1})".format(
                        property_name, index
                    )
                    identifier = f"{datasource_identifier}_{edge_label}_{property_name}"
                    self.tree.create_node(
                        tag=tag,
                        identifier=identifier,
                        parent=f"{datasource_identifier}_{edge_label}",
                    )

    def create_datasource_mapping_node(self, graph, datasource_mapping):
        datasource_identifier = f"{graph.id}_datasource"
        self.tree.create_node(
            tag="data sources", identifier=datasource_identifier, parent=graph.id
        )
        # vertex mapping
        vertex_mapping_identifier = f"{datasource_identifier}_vertex_mappings"
        self.tree.create_node(
            tag="vertex mappings",
            identifier=vertex_mapping_identifier,
            parent=datasource_identifier,
        )
        if datasource_mapping.vertex_mappings is not None:
            for mapping in datasource_mapping.vertex_mappings:
                specific_vertex_mapping_identifier = (
                    f"{vertex_mapping_identifier}_{mapping.type_name}"
                )
                self.tree.create_node(
                    tag=f"{mapping.type_name}(input: {mapping.inputs[0]})",
                    identifier=specific_vertex_mapping_identifier,
                    parent=vertex_mapping_identifier,
                )
                if mapping.column_mappings is not None:
                    for property_column_mapping in mapping.column_mappings:
                        tag = "Property(name: {0}) -> DataSourceColumn(index: {1}, name: {2})".format(
                            property_column_mapping.var_property,
                            property_column_mapping.column.index,
                            property_column_mapping.column.name,
                        )
                        p_mapping_identifier = f"{specific_vertex_mapping_identifier}_{property_column_mapping.var_property}"
                        self.tree.create_node(
                            tag=tag,
                            identifier=p_mapping_identifier,
                            parent=specific_vertex_mapping_identifier,
                        )
        # edge mapping
        edge_mapping_identifier = f"{datasource_identifier}_edge_mappings"
        self.tree.create_node(
            tag="edge mappings",
            identifier=edge_mapping_identifier,
            parent=datasource_identifier,
        )
        if datasource_mapping.edge_mappings is not None:
            for mapping in datasource_mapping.edge_mappings:
                edge_label = "({0}) -[{1}]-> ({2})".format(
                    mapping.type_triplet.source_vertex,
                    mapping.type_triplet.edge,
                    mapping.type_triplet.destination_vertex,
                )
                specific_edge_mapping_identifier = (
                    f"{edge_mapping_identifier}_{edge_label}"
                )
                self.tree.create_node(
                    tag=f"{edge_label}(input: {mapping.inputs[0]})",
                    identifier=specific_edge_mapping_identifier,
                    parent=edge_mapping_identifier,
                )
                # source vertex mapping
                if mapping.source_vertex_mappings is not None:
                    for source_vertex_column_mapping in mapping.source_vertex_mappings:
                        self.tree.create_node(
                            tag="SourceVertexPrimaryKey(name: {0}) -> DataSourceColumn(index: {1}, name: {2})".format(
                                source_vertex_column_mapping.var_property,
                                source_vertex_column_mapping.column.index,
                                source_vertex_column_mapping.column.name,
                            ),
                            identifier="{0}_source_vertex_primary_key_{1}".format(
                                specific_edge_mapping_identifier,
                                source_vertex_column_mapping.var_property,
                            ),
                            parent=specific_edge_mapping_identifier,
                        )
                # destination vertex mapping
                if mapping.destination_vertex_mappings is not None:
                    for (
                        destination_vertex_column_mapping
                    ) in mapping.destination_vertex_mappings:
                        self.tree.create_node(
                            tag="DestinationVertexPrimaryKey(name: {0}) -> DataSourceColumn(index: {1}, name: {2})".format(
                                destination_vertex_column_mapping.var_property,
                                destination_vertex_column_mapping.column.index,
                                destination_vertex_column_mapping.column.name,
                            ),
                            identifier="{0}_destination_vertex_primary_key_{1}".format(
                                specific_edge_mapping_identifier,
                                destination_vertex_column_mapping.var_property,
                            ),
                            parent=specific_edge_mapping_identifier,
                        )
                # property mapping
                if mapping.column_mappings is not None:
                    for property_column_mapping in mapping.column_mappings:
                        tag = "Property(name: {0}) -> DataSourceColumn(index: {1}, name: {2})".format(
                            property_column_mapping.var_property,
                            property_column_mapping.column.index,
                            property_column_mapping.column.name,
                        )
                        p_mapping_identifier = f"{specific_edge_mapping_identifier}_{property_column_mapping.var_property}"
                        self.tree.create_node(
                            tag=tag,
                            identifier=p_mapping_identifier,
                            parent=specific_edge_mapping_identifier,
                        )

    def show(self, graph_identifier=None, stdout=False, sorting=False):
        if graph_identifier is not None:
            # specific graph
            schema_tree = self.tree.subtree(f"{graph_identifier}_schema")
            click.secho(schema_tree.show(stdout=False, sorting=False))
            datasource_tree = self.tree.subtree(f"{graph_identifier}_datasource")
            click.secho(datasource_tree.show(stdout=False, sorting=False))
            stored_procedure_tree = self.tree.subtree(
                f"{graph_identifier}_stored_procedure"
            )
            click.secho(stored_procedure_tree.show(stdout=False, sorting=False))
        else:
            click.secho(self.tree.show(stdout=stdout, sorting=sorting))
