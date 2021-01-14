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

from itertools import chain
from typing import Any
from typing import Dict
from typing import Mapping
from typing import Sequence
from typing import Tuple
from typing import Union

import numpy as np
import pandas as pd

from graphscope.client.session import get_default_session
from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader
from graphscope.framework.vineyard_object import VineyardObject
from graphscope.proto import attr_value_pb2
from graphscope.proto import types_pb2

__all__ = ["load_from"]

LoaderVariants = Union[Loader, str, Sequence[np.ndarray], pd.DataFrame, VineyardObject]


class VertexLabel(object):
    """Holds meta informations about a single vertex label."""

    def __init__(
        self,
        label: str,
        loader: Any,
        properties: Sequence = None,
        vid: Union[str, int] = 0,
    ):
        self.label = label
        if isinstance(loader, Loader):
            self.loader = loader
        else:
            self.loader = Loader(loader)

        self.raw_properties = properties
        self.properties = []
        self.vid = vid

    def finish(self, id_type: str = "int64_t"):
        # Normalize properties
        # Add vid to property list
        self.add_property(str(self.vid), id_type)
        if self.raw_properties:
            self.add_properties(self.raw_properties)
        elif self.loader.deduced_properties:
            self.add_properties(self.loader.deduced_properties)
        self.loader.select_columns(
            self.properties, include_all=bool(not self.raw_properties)
        )
        self.loader.finish()

    def __str__(self) -> str:
        s = "\ntype: VertexLabel"
        s += "\nlabel: " + self.label
        s += "\nproperties: " + str(self.properties)
        s += "\nvid: " + str(self.vid)
        s += "\nloader: " + repr(self.loader)
        return s

    def __repr__(self) -> str:
        return self.__str__()

    def add_property(self, prop: str, dtype=None) -> None:
        """prop is a str, representing name. It can optionally have a type."""
        self.properties.append((prop, utils.unify_type(dtype)))

    def add_properties(self, properties: Sequence) -> None:
        for prop in properties:
            if isinstance(prop, str):
                self.add_property(prop)
            else:
                self.add_property(prop[0], prop[1])


class EdgeSubLabel(object):
    """Hold meta informations of a single relationship.
    i.e. src_label -> edge_label -> dst_label
    """

    def __init__(
        self,
        loader,
        properties=None,
        source=None,
        destination=None,
        load_strategy="both_out_in",
    ):
        if isinstance(loader, Loader):
            self.loader = loader
        else:
            self.loader = Loader(loader)

        self.raw_properties = properties
        self.properties = []
        self.source_vid = 0
        self.source_label = ""
        self.destination_vid = 1
        self.destination_label = ""
        self.load_strategy = ""

        if source is not None:
            self.set_source(source)
        if destination is not None:
            self.set_destination(destination)

        if (
            isinstance(self.source_vid, int) and isinstance(self.destination_vid, str)
        ) or (
            isinstance(self.source_vid, str) and isinstance(self.destination_vid, int)
        ):
            raise SyntaxError(
                "Source vid and destination vid must have same formats, both use name or both use index"
            )

        self.set_load_strategy(load_strategy)

    def finish(self, id_type: str):
        self.add_property(str(self.source_vid), id_type)
        self.add_property(str(self.destination_vid), id_type)
        if self.raw_properties:
            self.add_properties(self.raw_properties)
        elif self.loader.deduced_properties:
            self.add_properties(self.loader.deduced_properties)
        self.loader.select_columns(
            self.properties, include_all=bool(not self.raw_properties)
        )
        self.loader.finish()

    def __str__(self) -> str:
        s = "\ntype: EdgeSubLabel"
        s += "\nsource_label: " + self.source_label
        s += "\ndestination_label: " + self.destination_label
        s += "\nproperties: " + str(self.properties)
        s += "\nloader: " + repr(self.loader)
        return s

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def resolve_src_dst_value(value: Union[int, str, Tuple[Union[int, str], str]]):
        """Resolve the edge's source and destination.

        Args:
            value (Union[int, str, Tuple[Union[int, str], str]]):
            1. a int, represent vid id. a str, represent vid name
            2. a ([int/str], str). former represents vid, latter represents label

        Raises:
            SyntaxError: If the format is incorrect.
        """
        if isinstance(value, (int, str)):
            check_argument(
                isinstance(value, int)
                or (isinstance(value, str) and not value.isdecimal()),
                "Column name cannot be decimal",
            )
            return value, ""
        elif isinstance(value, Sequence):
            check_argument(len(value) == 2)
            check_argument(
                isinstance(value[0], int)
                or (isinstance(value[0], str) and not value[0].isdecimal()),
                "Column name cannot be decimal",
            )
            check_argument(isinstance(value[1], str), "Label must be str")
            return value[0], value[1]
        else:
            raise InvalidArgumentError(
                "Source / destination format incorrect. Expect vid or [vid, source_label]"
            )

    def set_source(self, source: Union[int, str, Tuple[Union[int, str], str]]):
        self.source_vid, self.source_label = self.resolve_src_dst_value(source)

    def set_destination(
        self, destination: Union[int, str, Tuple[Union[int, str], str]]
    ):
        self.destination_vid, self.destination_label = self.resolve_src_dst_value(
            destination
        )

    def set_load_strategy(self, strategy: str):
        check_argument(
            strategy in ("only_out", "only_in", "both_out_in"),
            "invalid load strategy: " + strategy,
        )
        self.load_strategy = strategy

    def add_property(self, prop: str, dtype=None) -> None:
        """prop is a str, representing name. It can optionally have a type."""
        self.properties.append((prop, utils.unify_type(dtype)))

    def add_properties(self, properties: Sequence) -> None:
        for prop in properties:
            if isinstance(prop, str):
                self.add_property(prop)
            else:
                self.add_property(prop[0], prop[1])

    def get_attr(self):
        attr_list = attr_value_pb2.NameAttrList()
        attr_list.name = "{}_{}".format(self.source_label, self.destination_label)
        attr_list.attr[types_pb2.SRC_LABEL].CopyFrom(utils.s_to_attr(self.source_label))
        attr_list.attr[types_pb2.DST_LABEL].CopyFrom(
            utils.s_to_attr(self.destination_label)
        )
        attr_list.attr[types_pb2.LOAD_STRATEGY].CopyFrom(
            utils.s_to_attr(self.load_strategy)
        )
        attr_list.attr[types_pb2.SRC_VID].CopyFrom(
            utils.s_to_attr(str(self.source_vid))
        )
        attr_list.attr[types_pb2.DST_VID].CopyFrom(
            utils.s_to_attr(str(self.destination_vid))
        )

        attr_list.attr[types_pb2.LOADER].CopyFrom(self.loader.get_attr())

        props = []
        for prop in self.properties[2:]:
            prop_attr = attr_value_pb2.NameAttrList()
            prop_attr.name = prop[0]
            prop_attr.attr[0].CopyFrom(utils.type_to_attr(prop[1]))
            props.append(prop_attr)
        attr_list.attr[types_pb2.PROPERTIES].list.func.extend(props)
        return attr_list


class EdgeLabel(object):
    """Hold meta informations of an edge label.
    An Edge label may be consist of a few `EdgeSubLabel`s.
    i.e. src_label1 -> edge_label -> dst_label1
         src_label2 -> edge_label -> dst_label2
         src_label3 -> edge_label -> dst_label3
    """

    def __init__(self, label: str):
        self.label = label

        self.sub_labels = []

        self._finished = False

    def __str__(self):
        s = "\ntype: EdgeLabel"
        s += "\nlabel: " + self.label
        s += "\nsub_labels: "
        for sub_label in self.sub_labels:
            s += "\n"
            s += str(sub_label)
        return s

    def __repr__(self):
        return self.__str__()

    def add_sub_label(self, sub_label):
        self.sub_labels.append(sub_label)

    def finish(self, id_type: str = "int64_t"):
        for sub_label in self.sub_labels:
            sub_label.finish(id_type)


def process_vertex(vertex: VertexLabel) -> attr_value_pb2.NameAttrList:
    attr_list = attr_value_pb2.NameAttrList()
    attr_list.name = "vertex"

    attr_list.attr[types_pb2.LABEL].CopyFrom(utils.s_to_attr(vertex.label))

    attr_list.attr[types_pb2.VID].CopyFrom(utils.s_to_attr(str(vertex.vid)))

    props = []
    for prop in vertex.properties[1:]:
        prop_attr = attr_value_pb2.NameAttrList()
        prop_attr.name = prop[0]
        prop_attr.attr[0].CopyFrom(utils.type_to_attr(prop[1]))
        props.append(prop_attr)
    attr_list.attr[types_pb2.PROPERTIES].list.func.extend(props)

    attr_list.attr[types_pb2.LOADER].CopyFrom(vertex.loader.get_attr())
    return attr_list


def process_edge(edge: EdgeLabel) -> attr_value_pb2.NameAttrList:
    attr_list = attr_value_pb2.NameAttrList()
    attr_list.name = "edge"

    attr_list.attr[types_pb2.LABEL].CopyFrom(utils.s_to_attr(edge.label))

    sub_label_attr = [sub_label.get_attr() for sub_label in edge.sub_labels]
    attr_list.attr[types_pb2.SUB_LABEL].list.func.extend(sub_label_attr)
    return attr_list


def _sanity_check(edges: Sequence[EdgeLabel], vertices: Sequence[VertexLabel]):
    vertex_labels = []
    for v in vertices:
        vertex_labels.append(v.label)
    if not vertex_labels:
        vertex_labels.append("_")

    for edge in edges:
        # Check source label and destination label
        check_argument(len(edge.sub_labels) != 0, "Edge label is malformed.")
        for sub_label in edge.sub_labels:
            if sub_label.source_label or sub_label.destination_label:
                if not (sub_label.source_label and sub_label.destination_label):
                    raise RuntimeError(
                        "source label and destination label must be both specified or either unspecified"
                    )

            # Handle default label. If edge doesn't specify label, then use default.
            if not sub_label.source_label and not sub_label.destination_label:
                check_argument(len(vertex_labels) == 1, "ambiguous vertex label")
                if len(vertex_labels) == 1:
                    sub_label.source_label = (
                        sub_label.destination_label
                    ) = vertex_labels[0]
            if vertices is not None and len(vertices) > 0:
                check_argument(
                    sub_label.source_label in vertex_labels,
                    "source label not found in vertex labels",
                )
                check_argument(
                    sub_label.destination_label in vertex_labels,
                    "destination label not found in vertex labels",
                )
            check_argument(
                sub_label.source_vid != sub_label.destination_vid,
                "source col and destination col cannot refer to the same col",
            )

    return edges, vertices


def _get_config(
    edges: Sequence[EdgeLabel],
    vertices: Sequence[VertexLabel],
    directed: bool,
    oid_type: str,
    generate_eid: bool,
) -> Dict:
    config = {}
    attr = attr_value_pb2.AttrValue()

    for label in chain(edges, vertices):
        label.finish(oid_type)

    for edge in edges:
        attr.list.func.extend([process_edge(edge)])

    attr.list.func.extend([process_vertex(vertex) for vertex in vertices])

    directed_attr = utils.b_to_attr(directed)
    generate_eid_attr = utils.b_to_attr(generate_eid)
    config[types_pb2.ARROW_PROPERTY_DEFINITION] = attr
    config[types_pb2.DIRECTED] = directed_attr
    config[types_pb2.OID_TYPE] = utils.s_to_attr(oid_type)
    config[types_pb2.GENERATE_EID] = generate_eid_attr
    # vid_type is fixed
    config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
    config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(False)
    return config


def normalize_parameter_edges(
    edges: Union[
        Mapping[str, Union[Sequence, LoaderVariants, Mapping]], Tuple, LoaderVariants
    ]
):
    """Normalize parameters user passed in. Since parameters are very flexible, we need to be
    careful about it.

    Args:
        edges (Union[ Mapping[str, Union[Sequence, LoaderVariants, Mapping]], Tuple, LoaderVariants ]):
            Edges definition.
    """

    def process_sub_label(items):
        if isinstance(items, (Loader, str, pd.DataFrame, VineyardObject)):
            return EdgeSubLabel(items, properties=None, source=None, destination=None)
        elif isinstance(items, Sequence):
            if all([isinstance(item, np.ndarray) for item in items]):
                return EdgeSubLabel(
                    loader=items, properties=None, source=None, destination=None
                )
            else:
                check_argument(len(items) < 6, "Too many arguments for a edge label")
                return EdgeSubLabel(*items)
        elif isinstance(items, Mapping):
            return EdgeSubLabel(**items)
        else:
            raise SyntaxError("Wrong format of e sub label: " + str(items))

    def process_label(label, items):
        e_label = EdgeLabel(label)
        if isinstance(items, (Loader, str, pd.DataFrame, VineyardObject)):
            e_label.add_sub_label(process_sub_label(items))
        elif isinstance(items, Sequence):
            if isinstance(
                items[0], (Loader, str, pd.DataFrame, VineyardObject, np.ndarray)
            ):
                e_label.add_sub_label(process_sub_label(items))
            else:
                for item in items:
                    e_label.add_sub_label(process_sub_label(item))
        elif isinstance(items, Mapping):
            e_label.add_sub_label(process_sub_label(items))
        else:
            raise SyntaxError("Wrong format of e label: " + str(items))
        return e_label

    e_labels = []
    if edges is None:
        raise ValueError("Edges should be None")
    if isinstance(edges, Mapping):
        for label, attr in edges.items():
            e_labels.append(process_label(label, attr))
    else:
        e_labels.append(process_label("_", edges))
    return e_labels


def normalize_parameter_vertices(
    vertices: Union[
        Mapping[str, Union[Sequence, LoaderVariants, Mapping]],
        Tuple,
        LoaderVariants,
        None,
    ]
):
    """Normalize parameters user passed in. Since parameters are very flexible, we need to be
    careful about it.

    Args:
        vertices (Union[ Mapping[str, Union[Sequence, LoaderVariants, Mapping]], Tuple, LoaderVariants, None, ]):
            Vertices definition.
    """

    def process_label(label, items):
        if isinstance(items, (Loader, str, pd.DataFrame, VineyardObject)):
            return VertexLabel(label=label, loader=items)
        elif isinstance(items, Sequence):
            if all([isinstance(item, np.ndarray) for item in items]):
                return VertexLabel(label=label, loader=items)
            else:
                check_argument(len(items) < 4, "Too many arguments for a vertex label")
                return VertexLabel(label, *items)
        elif isinstance(items, Mapping):
            return VertexLabel(label, **items)
        else:
            raise RuntimeError("Wrong format of v label: " + str(items))

    v_labels = []
    if vertices is None:
        return v_labels
    if isinstance(vertices, Mapping):
        for label, attr in vertices.items():
            v_labels.append(process_label(label, attr))
    else:
        v_labels.append(process_label("_", vertices))
    return v_labels


def load_from(
    edges: Union[
        Mapping[str, Union[LoaderVariants, Sequence, Mapping]], LoaderVariants, Sequence
    ],
    vertices: Union[
        Mapping[str, Union[LoaderVariants, Sequence, Mapping]],
        LoaderVariants,
        Sequence,
        None,
    ] = None,
    directed=True,
    oid_type="int64_t",
    generate_eid=True,
) -> Graph:
    """Load a Arrow property graph using a list of vertex/edge specifications.

    - Use Dict of tuples to setup a graph.
        We can use a dict to set vertex and edge configurations,
        which can be used to build graphs.

        Examples:

        .. code:: ipython

            g = graphscope_session.load_from(
                edges={
                    "group": [
                        (
                            "file:///home/admin/group.e",
                            ["group_id", "member_size"],
                            ("leader_student_id", "student"),
                            ("member_student_id", "student"),
                        ),
                        (
                            "file:///home/admin/group_for_teacher_student.e",
                            ["group_id", "group_name", "establish_date"],
                            ("teacher_in_charge_id", "teacher"),
                            ("member_student_id", "student"),
                        ),
                    ]
                },
                vertices={
                    "student": (
                        "file:///home/admin/student.v",
                        ["name", "lesson_nums", "avg_score"],
                        "student_id",
                    ),
                    "teacher": (
                        "file:///home/admin/teacher.v",
                        ["name", "salary", "age"],
                        "teacher_id",
                    ),
                },
            )

        'e' is the label of edges, and 'v' is the label for vertices, edges are stored in the 'both_in_out' format
        edges with label 'e' linking from 'v' to 'v'.

    - Use Dict of dict to setup a graph.
        We can also give each element inside the tuple a meaningful name,
        makes it more understandable.

        Examples:

        .. code:: ipython

            g = graphscope_session.load_from(
                edges={
                    "group": [
                        {
                            "loader": "file:///home/admin/group.e",
                            "properties": ["group_id", "member_size"],
                            "source": ("leader_student_id", "student"),
                            "destination": ("member_student_id", "student"),
                        },
                        {
                            "loader": "file:///home/admin/group_for_teacher_student.e",
                            "properties": ["group_id", "group_name", "establish_date"],
                            "source": ("teacher_in_charge_id", "teacher"),
                            "destination": ("member_student_id", "student"),
                        },
                    ]
                },
                vertices={
                    "student": {
                        "loader": "file:///home/admin/student.v",
                        "properties": ["name", "lesson_nums", "avg_score"],
                        "vid": "student_id",
                    },
                    "teacher": {
                        "loader": "file:///home/admin/teacher.v",
                        "properties": ["name", "salary", "age"],
                        "vid": "teacher_id",
                    },
                },
            )

    Args:
        edges: Edge configuration of the graph
        vertices (optional): Vertices configurations of the graph. Defaults to None.
            If None, we assume all edge's src_label and dst_label are deduced and unambiguous.
        directed (bool, optional): Indicate whether the graph
            should be treated as directed or undirected.
        oid_type (str, optional): ID type of graph. Can be "int64_t" or "string". Defaults to "int64_t".
        generate_eid (bool, optional): Whether to generate a unique edge id for each edge. Generated eid will be placed
            in third column. This feature is for cooperating with interactive engine.
            If you only need to work with analytical engine, set it to False. Defaults to False.
    """

    # Don't import the :code:`nx` in top-level statments to improve the
    # performance of :code:`import graphscope`.
    from graphscope.experimental import nx

    sess = get_default_session()
    if sess is None:
        raise ValueError("No default session found.")
    if isinstance(edges, (Graph, nx.Graph, VineyardObject)):
        return Graph(sess.session_id, edges)
    oid_type = utils.normalize_data_type_str(oid_type)
    e_labels = normalize_parameter_edges(edges)
    v_labels = normalize_parameter_vertices(vertices)
    e_labels, v_labels = _sanity_check(e_labels, v_labels)
    config = _get_config(e_labels, v_labels, directed, oid_type, generate_eid)
    op = dag_utils.create_graph(sess.session_id, types_pb2.ARROW_PROPERTY, attrs=config)
    graph_def = sess.run(op)
    graph = Graph(sess.session_id, graph_def)
    return graph


g = load_from
