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
import vineyard

from graphscope.framework import utils
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.loader import Loader
from graphscope.proto import attr_value_pb2
from graphscope.proto import types_pb2

VineyardObjectTypes = (vineyard.Object, vineyard.ObjectID, vineyard.ObjectName)

LoaderVariants = Union[
    Loader,
    str,
    Sequence[np.ndarray],
    pd.DataFrame,
    vineyard.Object,
    vineyard.ObjectID,
    vineyard.ObjectName,
]


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


def check_edge_validity(edges: Sequence[EdgeLabel], vertex_labels: Sequence[str]):
    for edge in edges:
        # Check source label and destination label
        check_argument(len(edge.sub_labels) != 0, "Edge label is malformed.")
        for sub_label in edge.sub_labels:
            if sub_label.source_label or sub_label.destination_label:
                if not (sub_label.source_label and sub_label.destination_label):
                    raise RuntimeError(
                        "source label and destination label must be both specified or either unspecified"
                    )

            check_argument(
                sub_label.source_vid != sub_label.destination_vid,
                "source col and destination col cannot refer to the same col",
            )
            # Handle default label. If edge doesn't specify label, then use default.
            if not sub_label.source_label and not sub_label.destination_label:
                check_argument(len(vertex_labels) <= 1, "ambiguous vertex label")
                if len(vertex_labels) == 1:
                    sub_label.source_label = vertex_labels[0]
                    sub_label.destination_label = vertex_labels[0]
                else:
                    sub_label.source_label = "_"
                    sub_label.destination_label = "_"
            elif vertex_labels:
                check_argument(
                    sub_label.source_label in vertex_labels,
                    "source label not found in vertex labels",
                )
                check_argument(
                    sub_label.destination_label in vertex_labels,
                    "destination label not found in vertex labels",
                )

    return edges


def assemble_op_config(
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
        if isinstance(items, (Loader, str, pd.DataFrame, *VineyardObjectTypes)):
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
        if isinstance(items, (Loader, str, pd.DataFrame, *VineyardObjectTypes)):
            e_label.add_sub_label(process_sub_label(items))
        elif isinstance(items, Sequence):
            if isinstance(
                items[0], (Loader, str, pd.DataFrame, *VineyardObjectTypes, np.ndarray)
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
        if isinstance(items, (Loader, str, pd.DataFrame, *VineyardObjectTypes)):
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
