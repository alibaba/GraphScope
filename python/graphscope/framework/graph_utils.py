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
from typing import Iterable
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
        vid_field: Union[str, int] = 0,
        session_id=None,
    ):
        self.label = label
        if isinstance(loader, Loader):
            self.loader = loader
        else:
            self.loader = Loader(loader)

        self.raw_properties = properties
        self.properties = []
        self.vid_field = vid_field
        self._session_id = session_id
        self._finished = False

    def finish(self, id_type: str = "int64_t"):
        # Normalize properties
        # Add vid to property list
        if self._finished:
            return
        self.add_property(str(self.vid_field), id_type)
        if self.raw_properties:
            self.add_properties(self.raw_properties)
        elif self.loader.deduced_properties:
            self.add_properties(self.loader.deduced_properties)
        self.loader.select_columns(
            self.properties, include_all=bool(self.raw_properties is None)
        )
        self.loader.finish(self._session_id)
        self._finished = True

    def __str__(self) -> str:
        s = "\ntype: VertexLabel"
        s += "\nlabel: " + self.label
        s += "\nproperties: " + str(self.properties)
        s += "\nvid: " + str(self.vid_field)
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
        src_label: str = "_",
        dst_label: str = "_",
        src_field: Union[str, int] = 0,
        dst_field: Union[str, int] = 1,
        load_strategy="both_out_in",
    ):
        if isinstance(loader, Loader):
            self.loader = loader
        else:
            self.loader = Loader(loader)

        self.raw_properties = properties
        self.properties = []
        self.src_label = src_label
        self.dst_label = dst_label
        self.src_field = src_field
        self.dst_field = dst_field

        self._finished = False

        check_argument(
            load_strategy in ("only_out", "only_in", "both_out_in"),
            "invalid load strategy: " + load_strategy,
        )
        self.load_strategy = load_strategy

        if (isinstance(self.src_field, int) and isinstance(self.dst_field, str)) or (
            isinstance(self.src_field, str) and isinstance(self.dst_field, int)
        ):
            print("src field", self.src_field, "dst_field", self.dst_field)
            raise SyntaxError(
                "Source vid and destination vid must have same formats, both use name or both use index"
            )

    def finish(self, id_type: str = "int64_t", session_id=None):
        if self._finished:
            return
        self.add_property(str(self.src_field), id_type)
        self.add_property(str(self.dst_field), id_type)
        if self.raw_properties:
            self.add_properties(self.raw_properties)
        elif self.loader.deduced_properties:
            self.add_properties(self.loader.deduced_properties)
        self.loader.select_columns(
            self.properties, include_all=bool(self.raw_properties is None)
        )
        self.loader.finish(session_id)
        self._finished = True

    def __str__(self) -> str:
        s = "\ntype: EdgeSubLabel"
        s += "\nsource_label: " + self.src_label
        s += "\ndestination_label: " + self.dst_label
        s += "\nproperties: " + str(self.properties)
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

    def get_attr(self):
        attr_list = attr_value_pb2.NameAttrList()
        attr_list.name = "{}_{}".format(self.src_label, self.dst_label)
        attr_list.attr[types_pb2.SRC_LABEL].CopyFrom(utils.s_to_attr(self.src_label))
        attr_list.attr[types_pb2.DST_LABEL].CopyFrom(utils.s_to_attr(self.dst_label))
        attr_list.attr[types_pb2.LOAD_STRATEGY].CopyFrom(
            utils.s_to_attr(self.load_strategy)
        )
        attr_list.attr[types_pb2.SRC_VID].CopyFrom(utils.s_to_attr(str(self.src_field)))
        attr_list.attr[types_pb2.DST_VID].CopyFrom(utils.s_to_attr(str(self.dst_field)))

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

    def __init__(self, label: str, session_id=None):
        self.label = label
        self.sub_labels = {}

        self._session_id = session_id

    def __str__(self):
        s = "\ntype: EdgeLabel"
        s += "\nlabel: " + self.label
        s += "\nsub_labels: "
        for sub_label in self.sub_labels.values():
            s += "\n"
            s += str(sub_label)
        return s

    def __repr__(self):
        return self.__str__()

    def add_sub_label(self, sub_label):
        src = sub_label.src_label
        dst = sub_label.dst_label
        if (src, dst) in self.sub_labels:
            raise ValueError(
                f"The relationship {src} -> {self.label} <- {dst} already existed in graph."
            )
        self.sub_labels[(src, dst)] = sub_label

    def finish(self, id_type: str = "int64_t"):
        for sub_label in self.sub_labels.values():
            sub_label.finish(id_type, self._session_id)


def process_vertex(vertex: VertexLabel) -> attr_value_pb2.NameAttrList:
    attr_list = attr_value_pb2.NameAttrList()
    attr_list.name = "vertex"

    attr_list.attr[types_pb2.LABEL].CopyFrom(utils.s_to_attr(vertex.label))

    attr_list.attr[types_pb2.VID].CopyFrom(utils.s_to_attr(str(vertex.vid_field)))

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

    sub_label_attr = [sub_label.get_attr() for sub_label in edge.sub_labels.values()]
    attr_list.attr[types_pb2.SUB_LABEL].list.func.extend(sub_label_attr)
    return attr_list


def assemble_op_config(
    vertices: Iterable[VertexLabel],
    edges: Iterable[EdgeLabel],
    oid_type: str,
    directed: bool,
    generate_eid: bool,
) -> Dict:
    attr = attr_value_pb2.AttrValue()

    for label in chain(vertices, edges):
        label.finish(oid_type)

    attr.list.func.extend([process_vertex(vertex) for vertex in vertices])
    attr.list.func.extend([process_edge(edge) for edge in edges])

    config = {}
    config[types_pb2.ARROW_PROPERTY_DEFINITION] = attr
    config[types_pb2.DIRECTED] = utils.b_to_attr(directed)
    config[types_pb2.OID_TYPE] = utils.s_to_attr(oid_type)
    config[types_pb2.GENERATE_EID] = utils.b_to_attr(generate_eid)
    # vid_type is fixed
    config[types_pb2.VID_TYPE] = utils.s_to_attr("uint64_t")
    config[types_pb2.IS_FROM_VINEYARD_ID] = utils.b_to_attr(False)
    return config


def _convert_array_to_deprecated_form(items):
    compat_items = []
    for i in range(len(items)):
        if i < 2:
            compat_items.append(items[i])
        elif i == 2:
            if isinstance(items[i], (int, str)) and isinstance(
                items[i + 1], (int, str)
            ):
                compat_items.append("_")
                compat_items.append("_")
                compat_items.append(items[i])
                compat_items.append(items[i + 1])
            else:
                assert len(items[i]) == 2 and len(items[i + 1]) == 2
                compat_items.append(items[i][1])
                compat_items.append(items[i + 1][1])
                compat_items.append(items[i][0])
                compat_items.append(items[i + 1][0])
        elif i == 3:
            pass
        else:
            compat_items.append(items[i])
    return compat_items


def _convert_dict_to_compat_form(items):
    if "source" in items:
        if isinstance(items["source"], (int, str)):
            items["src_label"] = "_"
            items["src_field"] = items["source"]
        else:
            assert len(items["source"]) == 2
            items["src_label"] = items["source"][1]
            items["src_field"] = items["source"][0]
        items.pop("source")
    if "destination" in items:
        if isinstance(items["destination"], (int, str)):
            items["dst_label"] = "_"
            items["dst_field"] = items["destination"]
        else:
            assert len(items["destination"]) == 2
            items["dst_label"] = items["destination"][1]
            items["dst_field"] = items["destination"][0]
        items.pop("destination")
    return items


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
            return EdgeSubLabel(items, None, "_", "_", 0, 1)
        elif isinstance(items, Sequence):
            if all([isinstance(item, np.ndarray) for item in items]):
                return EdgeSubLabel(items, None, "_", "_", 0, 1)
            else:
                check_argument(len(items) < 6, "Too many arguments for a edge label")
                compat_items = _convert_array_to_deprecated_form(items)
                return EdgeSubLabel(*compat_items)
        elif isinstance(items, Mapping):
            items = _convert_dict_to_compat_form(items)
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
            if "vid" in items:
                items["vid_field"] = items["vid"]
                items.pop("vid")
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
