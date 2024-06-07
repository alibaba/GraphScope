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

import hashlib
import itertools
import json
from collections import namedtuple
from typing import List

from graphscope.framework.utils import data_type_to_unified_type
from graphscope.framework.utils import unified_type_to_data_type
from graphscope.framework.utils import unify_type
from graphscope.proto import ddl_service_pb2
from graphscope.proto import graph_def_pb2


class Property:
    def __init__(
        self, name, data_type, is_primary_key=False, property_id=0, comment=""
    ):
        self.name: str = name
        self.data_type: int = data_type
        self.is_primary_key: bool = is_primary_key

        self.id: int = property_id
        self.comment = comment

        self.inner_id: int = 0
        self.default_value = None

    def as_property_def(self):
        pb = graph_def_pb2.PropertyDefPb()
        pb.name = self.name
        pb.data_type = self.data_type
        pb.pk = self.is_primary_key
        pb.comment = self.comment
        return pb

    @property
    def type(self):
        return self.data_type

    @classmethod
    def from_property_def(cls, pb):
        prop = cls(pb.name, pb.data_type, pb.pk, pb.id, pb.comment)
        prop.inner_id = pb.inner_id
        prop.default_value = pb.default_value
        return prop

    def __repr__(self) -> str:
        type_str = graph_def_pb2.DataTypePb.Name(self.data_type)
        return f"Property({self.id}, {self.name}, {type_str}, {self.is_primary_key}, {self.comment})"

    def __str__(self) -> str:
        return self.__repr__()

    def to_dict(self) -> dict:
        return {
            "property_name": self.name,
            "property_id": self.id,
            "property_type": data_type_to_unified_type(self.data_type),
            "is_primary_key": self.is_primary_key,
            "description": self.comment,
        }


Relation = namedtuple("Relation", "source destination")


class Label:
    __slots__ = [
        "_name",
        "_props",
        "_version_id",
        "_label_id",
        "_valid_props",
        "_prop_index",
        "_comment",
    ]

    def __init__(self, name, label_id=0):
        self._name: str = name
        self._props: List[Property] = []
        self._version_id: int = 0
        self._label_id: int = label_id

        self._valid_props: List[int] = []
        self._prop_index: dict[str, int] = {}
        self._comment: str = ""

    def add_property(self, name, data_type, is_primary_key=False, comment=""):
        self._prop_index[name] = len(self._props)
        if isinstance(data_type, str):
            data_type = unify_type(data_type)
        self._props.append(
            Property(name, data_type, is_primary_key, len(self._props), comment)
        )
        self._valid_props.append(1)
        return self

    def set_comment(self, comment):
        self._comment = comment
        return self

    @property
    def id(self) -> int:
        return self._label_id

    @property
    def label(self) -> str:
        return self._name

    @property
    def properties(self) -> List:
        return list(itertools.compress(self._props, self._valid_props))

    @property
    def comment(self):
        return self._comment

    def get_property_id(self, name):
        idx = self._prop_index[name]
        if not self._valid_props[idx]:
            raise ValueError(f"{name} not exist in properties")
        return idx

    def property_exists(self, name):
        return (name in self._prop_index) and (
            self._valid_props[self._prop_index[name]]
        )

    def __repr__(self) -> str:
        s = f"Label: {self.label}\nProperties: {', '.join([str(p) for p in self.properties])}\nComment: {self.comment}"
        return s

    def __str__(self) -> str:
        return self.__repr__()

    def to_dict(self) -> dict:
        properties = []
        primary_keys = []
        for p in self.properties:
            properties.append(p.to_dict())
            if p.is_primary_key:
                primary_keys.append(p.name)
        return {
            "type_name": self.label,
            "properties": properties,
            "primary_keys": primary_keys,
            "description": self.comment,
        }

    @property
    def type_enum(self):
        raise NotImplementedError()

    def as_type_def(self):
        pb = graph_def_pb2.TypeDefPb()
        pb.label = self._name
        pb.type_enum = self.type_enum
        pb.comment = self._comment
        for prop in self.properties:
            pb.props.append(prop.as_property_def())
        return pb

    @classmethod
    def from_type_def(cls, pb):
        label = cls(pb.label)
        label._label_id = pb.label_id.id
        label._version_id = pb.version_id
        label._comment = pb.comment
        for prop_pb in pb.props:
            label._props.append(Property.from_property_def(prop_pb))
            label._valid_props.append(1)
        return label


class VertexLabel(Label):
    __slots__ = []

    @property
    def type_enum(self):
        return graph_def_pb2.TypeEnumPb.VERTEX

    def add_primary_key(self, name, data_type, comment=""):
        return self.add_property(name, data_type, True, comment)


class EdgeLabel(Label):
    __slots__ = ["_relations"]

    def __init__(self, name, label_id=0):
        super().__init__(name, label_id)
        self._relations: List[Relation] = []

    @property
    def type_enum(self):
        return graph_def_pb2.TypeEnumPb.EDGE

    def source(self, label):
        self._relations.append(Relation(label, ""))
        return self

    def destination(self, label):
        assert (
            self._relations
        ), "Found empty relation, maybe you should use `source` first."
        assert not self._relations[-1].destination, "An destination is already exists."
        self._relations[-1] = self._relations[-1]._replace(destination=label)
        return self

    @property
    def relations(self) -> List[Relation]:
        return self._relations

    def __repr__(self) -> str:
        s = super().__repr__()
        if self._relations:
            s += f"Relations: {self.relations}"
        return s

    def to_dict(self) -> dict:
        # super dict
        sd = super().to_dict()
        relations = []
        for r in self._relations:
            relations.append(
                {"source_vertex": r.source, "destination_vertex": r.destination}
            )
        sd.update({"vertex_type_pair_relations": relations})
        return sd


class GraphSchema:
    """Hold schema of a graph.

    Attributes:
        oid_type (str): Original ID type
        vid_type (str): Internal ID representation
        vdata_type (str): Type of the data that holding by vertex (simple graph only)
        edata_type (str): Type of the data that holding by edge (simple graph only)
        vertex_labels (list): Label names of vertex
        edge_labels (list): Label names of edge
        edge_relationships (list(list(tuple))): Source label and destination label of each edge label
    """

    def __init__(self):
        self._conn = None

        self._oid_type = None
        self._vid_type = None
        # simple graph only
        self._vdata_type = graph_def_pb2.UNKNOWN
        self._edata_type = graph_def_pb2.UNKNOWN

        # list of entries
        self._vertex_labels: List[VertexLabel] = []
        self._edge_labels: List[EdgeLabel] = []

        self._vertex_labels_to_add: List[VertexLabel] = []
        self._edge_labels_to_add: List[EdgeLabel] = []
        self._vertex_labels_to_drop: List[VertexLabel] = []
        self._edge_labels_to_drop: List[EdgeLabel] = []
        self._vertex_labels_to_add_property: List[VertexLabel] = []
        self._edge_labels_to_add_property: List[VertexLabel] = []
        # 1 indicate valid, 0 indicate invalid.
        self._valid_vertices = []
        self._valid_edges = []

        self._v_label_index = {}
        self._e_label_index = {}

    def from_graph_def(self, graph_def):
        if graph_def.extension.Is(graph_def_pb2.VineyardInfoPb.DESCRIPTOR):
            return self._from_vineyard(graph_def)
        if graph_def.extension.Is(graph_def_pb2.MutableGraphInfoPb.DESCRIPTOR):
            return self._from_mutable_graph(graph_def)
        return self._from_store_service(graph_def)

    def _from_store_service(self, graph_def):
        """Decode information from proto message, generated by engine.

        Args:
            schema_def (`GraphSchemaDef`): Proto message defined in `proto/graph_def.proto`.

        Raises:
            ValueError: If the schema is not valid.
        """
        self.clear()
        id_to_label = {}
        for type_def_pb in graph_def.type_defs:
            id_to_label[type_def_pb.label_id.id] = type_def_pb.label
        edge_kinds = {}
        for kind in graph_def.edge_kinds:
            edge_label = id_to_label[kind.edge_label_id.id]
            if edge_label not in edge_kinds:
                edge_kinds[edge_label] = []
            edge_kinds[edge_label].append(
                (
                    kind.src_vertex_label,
                    kind.dst_vertex_label,
                )
            )
        for type_def_pb in graph_def.type_defs:
            if type_def_pb.type_enum == graph_def_pb2.VERTEX:
                self._v_label_index[type_def_pb.label] = len(self._vertex_labels)
                self._vertex_labels.append(VertexLabel.from_type_def(type_def_pb))
                self._valid_vertices.append(1)
            else:
                label = EdgeLabel.from_type_def(type_def_pb)
                if label.label in edge_kinds:
                    for src, dst in edge_kinds[label.label]:
                        label.source(src).destination(dst)
                self._e_label_index[type_def_pb.label] = len(self._edge_labels)
                self._edge_labels.append(label)
                self._valid_edges.append(1)
        return self

    def _from_vineyard(self, graph_def):
        self.clear()
        vy_info = graph_def_pb2.VineyardInfoPb()
        graph_def.extension.Unpack(vy_info)
        self._oid_type = vy_info.oid_type
        self._vid_type = vy_info.vid_type
        # simple graph schema.
        if vy_info.vdata_type:
            self._vdata_type = unify_type(vy_info.vdata_type)
        if vy_info.edata_type:
            self._edata_type = unify_type(vy_info.edata_type)

        # property graph schema
        if vy_info.property_schema_json:
            try:
                schema = json.loads(vy_info.property_schema_json)
                if schema:
                    for item in schema["types"]:

                        def add_common_attributes(entry, item):
                            for prop in item["propertyDefList"]:
                                entry.add_property(
                                    prop["name"], unify_type(prop["data_type"])
                                )
                            entry._valid_props = item["valid_properties"]

                        if item["type"] == "VERTEX":
                            entry = VertexLabel(item["label"], item["id"])
                            assert entry.id == len(self._vertex_labels)
                            add_common_attributes(entry, item)
                            self._vertex_labels.append(entry)
                            self._v_label_index[entry.label] = entry.id
                        elif item["type"] == "EDGE":
                            entry = EdgeLabel(item["label"], item["id"])
                            assert entry.id == len(self._edge_labels)
                            for rel in item["rawRelationShips"]:
                                entry.source(rel["srcVertexLabel"]).destination(
                                    rel["dstVertexLabel"]
                                )
                            add_common_attributes(entry, item)
                            self._edge_labels.append(entry)
                            self._e_label_index[entry.label] = entry.id
                    self._valid_vertices = schema["valid_vertices"]
                    self._valid_edges = schema["valid_edges"]
            except Exception as e:
                raise ValueError("Invalid property graph schema") from e
        return self

    def _from_mutable_graph(self, graph_def):
        graph_info = graph_def_pb2.MutableGraphInfoPb()
        graph_def.extension.Unpack(graph_info)
        # simple graph schema
        if graph_info.vdata_type:
            self._vdata_type = unify_type(graph_info.vdata_type)
        if graph_info.edata_type:
            self._edata_type = unify_type(graph_info.edata_type)

    def __repr__(self):
        s = ""
        if self._oid_type is not None:
            s += f"oid_type: {graph_def_pb2.DataTypePb.Name(self._oid_type)}\n"
        if self._oid_type is not None:
            s += f"vid_type: {graph_def_pb2.DataTypePb.Name(self._vid_type)}\n"
        if (
            self._vdata_type != graph_def_pb2.UNKNOWN
            and self._edata_type != graph_def_pb2.UNKNOWN
        ):
            s += f"vdata_type: {graph_def_pb2.DataTypePb.Name(self._vdata_type)}\n"
            s += f"edata_type: {graph_def_pb2.DataTypePb.Name(self._edata_type)}\n"
        for entry in self._valid_vertex_labels():
            s += f"type: VERTEX\n{str(entry)}\n"
        for entry in self._valid_edge_labels():
            s += f"type: EDGE\n{str(entry)}\n"
        return s

    def __str__(self):
        return self.__repr__()

    def to_dict(self) -> dict:
        vertices = []
        for entry in self._valid_vertex_labels():
            vertices.append(entry.to_dict())
        edges = []
        for entry in self._valid_edge_labels():
            edges.append(entry.to_dict())
        return {"vertex_types": vertices, "edge_types": edges}

    def from_dict(self, input: dict):
        if self._vertex_labels or self._edge_labels:
            raise RuntimeError("Cannot load schema from dict within a non-empty graph.")
        try:
            self.clear()
            vertices: list[dict] = input["vertex_types"]
            edges: list[dict] = input["edge_types"]
            for vertex in vertices:
                label = VertexLabel(vertex["type_name"])
                label.set_comment(vertex.get("description", ""))
                primary_keys = vertex.get("primary_keys", [])
                for prop in vertex["properties"]:
                    is_primary_key = prop["property_name"] in primary_keys
                    label = label.add_property(
                        prop["property_name"],
                        unified_type_to_data_type(prop["property_type"]),
                        is_primary_key,
                        prop["description"],
                    )
                self._vertex_labels_to_add.append(label)
            for edge in edges:
                label = EdgeLabel(edge["type_name"])
                label.set_comment(edge.get("description", ""))
                primary_keys = edge.get("primary_keys", [])
                for prop in edge["properties"]:
                    is_primary_key = prop["property_name"] in primary_keys
                    label = label.add_property(
                        prop["property_name"],
                        unified_type_to_data_type(prop["property_type"]),
                        is_primary_key,
                        prop["description"],
                    )
                for rel in edge["vertex_type_pair_relations"]:
                    label = label.source(rel["source_vertex"]).destination(
                        rel["destination_vertex"]
                    )
                self._edge_labels_to_add.append(label)
        except Exception as e:
            self.clear()
            raise RuntimeError("Construct schema from dict failed!") from e

    @property
    def oid_type(self):
        return self._oid_type

    @property
    def vid_type(self):
        return self._vid_type

    @property
    def vdata_type(self):
        # NB: simple graph only contain a single vertex property.
        return self._vdata_type

    @property
    def edata_type(self):
        # NB: simple graph only contain a single edge property.
        return self._edata_type

    def _valid_vertex_labels(self):
        return itertools.compress(self._vertex_labels, self._valid_vertices)

    def _valid_edge_labels(self):
        return itertools.compress(self._edge_labels, self._valid_edges)

    @property
    def vertex_labels(self):
        return [entry.label for entry in self._valid_vertex_labels()]

    @property
    def edge_labels(self):
        return [entry.label for entry in self._valid_edge_labels()]

    @property
    def edge_relationships(self):
        return [entry.relations for entry in self._valid_edge_labels()]

    def get_relationships(self, label):
        if label not in self._e_label_index:
            raise KeyError(f"{label} not exists.")
        label_id = self._e_label_index[label]
        if not self._valid_edges[label_id]:
            raise ValueError(f"{label} not exists.")
        return self._edge_labels[self._e_label_index[label]].relations

    @property
    def vertex_label_num(self):
        return sum(self._valid_vertices)

    @property
    def edge_label_num(self):
        return sum(self._valid_edges)

    def get_vertex_properties(self, label):
        return self._vertex_labels[self.get_vertex_label_id(label)].properties

    def get_edge_properties(self, label):
        return self._edge_labels[self.get_edge_label_id(label)].properties

    def vertex_properties_num(self, label):
        return len(self._vertex_labels[self.get_vertex_label_id(label)].properties)

    def edge_properties_num(self, label):
        return len(self._edge_labels[self.get_edge_label_id(label)].properties)

    def get_vertex_label_id(self, label):
        if label not in self._v_label_index:
            raise KeyError(f"{label} not exists.")
        idx = self._v_label_index[label]
        if not self._valid_vertices[idx]:
            raise ValueError(f"Vertex {label} not exists in graph")
        return idx

    def get_edge_label_id(self, label):
        if label not in self._e_label_index:
            raise KeyError(f"{label} not exists.")
        idx = self._e_label_index[label]
        if not self._valid_edges[idx]:
            raise ValueError(f"Edge {label} not exists in graph")
        return idx

    def get_vertex_property_id(self, label, prop):
        return self._vertex_labels[self.get_vertex_label_id(label)].get_property_id(
            prop
        )

    def vertex_property_exists(self, label, prop):
        return self._vertex_labels[self.get_vertex_label_id(label)].property_exists(
            prop
        )

    def get_edge_property_id(self, label, prop):
        return self._edge_labels[self.get_edge_label_id(label)].get_property_id(prop)

    def edge_property_exists(self, label, prop):
        return self._edge_labels[self.get_edge_label_id(label)].property_exists(prop)

    def clear(self):
        self._oid_type = None
        self._vid_type = None
        self._vdata_type = graph_def_pb2.UNKNOWN
        self._edata_type = graph_def_pb2.UNKNOWN

        self._vertex_labels.clear()
        self._edge_labels.clear()
        self._vertex_labels_to_add.clear()
        self._vertex_labels_to_drop.clear()
        self._edge_labels_to_add.clear()
        self._edge_labels_to_drop.clear()
        self._vertex_labels_to_add_property.clear()
        self._edge_labels_to_add_property.clear()
        self._valid_vertices.clear()
        self._valid_edges.clear()
        self._v_label_index.clear()
        self._e_label_index.clear()

    def signature(self):
        return hashlib.sha256("{}".format(self.__repr__()).encode("utf-8")).hexdigest()

    def add_vertex_label(self, label, vid_field=None, properties=None, comment=""):
        item = VertexLabel(label)
        item.set_comment(comment)
        if vid_field:
            item = item.add_primary_key(*vid_field)
        if properties:
            for prop in properties:
                item = item.add_property(*prop)
        self._vertex_labels_to_add.append(item)
        return self._vertex_labels_to_add[-1]

    def add_edge_label(
        self, label, src_label=None, dst_label=None, properties=None, comment=""
    ):
        item = EdgeLabel(label)
        item.set_comment(comment)
        if src_label:
            item = item.source(src_label)
        if dst_label:
            item = item.destination(dst_label)
        if properties:
            for prop in properties:
                item = item.add_property(*prop)
        self._edge_labels_to_add.append(item)
        return self._edge_labels_to_add[-1]

    def add_vertex_properties(self, label, properties=None):
        item = VertexLabel(label)
        if properties is not None:
            for prop in properties:
                item = item.add_property(*prop)
        self._vertex_labels_to_add_property.append(item)
        return self._vertex_labels_to_add_property[-1]

    def add_edge_properties(self, label, properties=None):
        item = EdgeLabel(label)
        if properties is not None:
            for prop in properties:
                item = item.add_property(*prop)
        self._edge_labels_to_add_property.append(item)
        return self._edge_labels_to_add_property[-1]

    def drop(self, label, src_label=None, dst_label=None):
        for item in self._vertex_labels:
            if label == item.label:
                if src_label is not None or dst_label is not None:
                    raise ValueError(
                        "Vertex label should not have source and destination."
                    )
                self._vertex_labels_to_drop.append(VertexLabel(label))
                return
        for item in self._edge_labels:
            if label == item.label:
                label_to_drop = EdgeLabel(label)
                if src_label is not None and dst_label is not None:
                    label_to_drop.source(src_label).destination(dst_label)
                self._edge_labels_to_drop.append(label_to_drop)
                return
        raise ValueError(f"Label {label} not found.")

    def drop_all(self):
        for item in self._edge_labels:
            for rel in item.relations:
                self._edge_labels_to_drop.append(
                    EdgeLabel(item.label)
                    .source(rel.source)
                    .destination(rel.destination)
                )
            self._edge_labels_to_drop.append(EdgeLabel(item.label))
        for item in self._vertex_labels:
            self._vertex_labels_to_drop.append(VertexLabel(item.label))

    def _prepare_batch_rpc(self):
        requests = ddl_service_pb2.BatchSubmitRequest()
        for item in self._vertex_labels_to_add:
            type_pb = item.as_type_def()
            requests.value.add().create_vertex_type_request.type_def.CopyFrom(type_pb)
        for item in self._edge_labels_to_add:
            type_pb = item.as_type_def()
            requests.value.add().create_edge_type_request.type_def.CopyFrom(type_pb)
            for rel in item.relations:
                assert rel.source and rel.destination, "Invalid relation"
                request = ddl_service_pb2.AddEdgeKindRequest()
                request.edge_label = item.label
                request.src_vertex_label = rel.source
                request.dst_vertex_label = rel.destination
                requests.value.add().add_edge_kind_request.CopyFrom(request)
        for item in self._edge_labels_to_drop:
            if item.relations:
                request = ddl_service_pb2.RemoveEdgeKindRequest()
                request.edge_label = item.label
                request.src_vertex_label = item.relations[0].source
                request.dst_vertex_label = item.relations[0].destination
                requests.value.add().remove_edge_kind_request.CopyFrom(request)
            else:
                requests.value.add().drop_edge_type_request.label = item.label
        for item in self._vertex_labels_to_add_property:
            type_pb = item.as_type_def()
            requests.value.add().add_vertex_type_properties_request.type_def.CopyFrom(
                type_pb
            )
        for item in self._edge_labels_to_add_property:
            type_pb = item.as_type_def()
            requests.value.add().add_edge_type_properties_request.type_def.CopyFrom(
                type_pb
            )
        for item in self._vertex_labels_to_drop:
            requests.value.add().drop_vertex_type_request.label = item.label
        return requests

    def update(self):
        requests = self._prepare_batch_rpc()
        self._vertex_labels_to_add.clear()
        self._edge_labels_to_add.clear()
        self._vertex_labels_to_drop.clear()
        self._edge_labels_to_drop.clear()
        self._vertex_labels_to_add_property.clear()
        self._edge_labels_to_add_property.clear()
        response = self._conn.submit(requests)
        self.from_graph_def(response.graph_def)
        return self

    def create_and_replace(self, vineyard_schema):
        pass
