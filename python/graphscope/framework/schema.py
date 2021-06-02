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

from collections import namedtuple
import grpc

from graphscope.proto import graph_def_pb2
from graphscope.proto import ddl_service_pb2
from graphscope.proto import ddl_service_pb2_grpc


def unify_type(t):
    # If type is None, we deduce type from source file.
    if t is None:
        return graph_def_pb2.DataTypePb.UNKNOWN
    if isinstance(t, str):
        t = t.lower()
        if t in ("b", "bool"):
            return graph_def_pb2.DataTypePb.BOOL
        elif t in ("c", "char"):
            return graph_def_pb2.DataTypePb.CHAR
        elif t in ("s", "short"):
            return graph_def_pb2.DataTypePb.SHORT
        elif t in ("i", "int", "int32", "int32_t"):
            return graph_def_pb2.DataTypePb.INT
        elif t in ("l", "long", "int64", "int64_t"):
            return graph_def_pb2.DataTypePb.LONG
        elif t in ("f", "float"):
            return graph_def_pb2.DataTypePb.FLOAT
        elif t in ("d", "double"):
            return graph_def_pb2.DataTypePb.DOUBLE
        elif t in ("str", "string", "std::string"):
            return graph_def_pb2.DataTypePb.STRING
        elif t == "bytes":
            return graph_def_pb2.DataTypePb.BYTES
        elif t == "int_list":
            return graph_def_pb2.DataTypePb.INT_LIST
        elif t == "long_list":
            return graph_def_pb2.DataTypePb.LONG_LIST
        elif t == "float_list":
            return graph_def_pb2.DataTypePb.FLOAT_LIST
        elif t == "double_list":
            return graph_def_pb2.DataTypePb.DOUBLE_LIST
    raise TypeError("Not supported type {}".format(t))


class Property:
    def __init__(self, name, data_type, is_primary_key=False):
        self.name: str = name
        self.data_type: int = data_type
        self.is_primary_key: bool = is_primary_key

        self.id: int = -1
        self.inner_id: int = -1
        self.default_value = None
        self.comment = ""

    def as_property_def(self):
        pb = graph_def_pb2.PropertyDefPb()
        pb.name = self.name
        pb.data_type = self.data_type
        pb.pk = self.is_primary_key
        return pb


Relation = namedtuple("Relation", "source destination")


class Label:
    __slots__ = ['name', 'properties', 'version_id', 'label_id']
    def __init__(self, name):
        self.name: str = name
        self.properties: list[Property] = []
        self.version_id: int = -1
        self.label_id: int = -1

    def add_property(self, name, type):
        self.properties.append(Property(name, unify_type(type), False))
        return self

    @property
    def type_enum(self):
        raise NotImplementedError()

    def as_type_def(self):
        pb = graph_def_pb2.TypeDefPb()
        pb.label = self.name
        pb.type_enum = self.type_enum
        for prop in self.properties:
            pb.props.append(prop.as_property_def())
        return pb

class VertexLabel(Label):
    __slots__ = []
    def __init__(self, name):
        super().__init__(name)

    @property
    def type_enum(self):
        return graph_def_pb2.TypeEnumPb.VERTEX

    def add_primary_key(self, name, type):
        self.properties.append(Property(name, unify_type(type), True))
        return self

class EdgeLabel(Label):
    __slots__ = ['relations']
    def __init__(self, name):
        super().__init__(name)
        self.relations: list[Relation] = []

    @property
    def type_enum(self):
        return graph_def_pb2.TypeEnumPb.EDGE

    def source(self, label):
        self.relations.append(Relation(label, ""))
        return self

    def destination(self, label):
        assert self.relations, "Empty relation"
        assert not self.relations[-1].destination
        self.relations[-1] = self.relations[-1]._replace(destination=label)
        return self

class Schema:
    def __init__(self):
        self.vertex_labels: list[VertexLabel] = []
        self.edge_labels: list[EdgeLabel] = []
        self.vertex_labels_to_drop: list[VertexLabel] = []
        self.edge_labels_to_drop: list[EdgeLabel] = []

    def add_vertex_label(self, label, vid_field=None, properties=None):
        item = VertexLabel(label)
        if vid_field:
            item = item.add_primary_key(*vid_field)
        if properties:
            for prop in properties:
                item = item.add_property(*prop)
        self.vertex_labels.append(item)
        return self.vertex_labels[-1]

    def add_edge_label(self, label, src_label=None, dst_label=None, properties=None):
        item = EdgeLabel(label)
        if src_label:
            item = item.source(src_label)
        if dst_label:
            item = item.destination(dst_label)
        if properties:
            for prop in properties:
                item = item.add_property(*prop)
        self.edge_labels.append(item)
        return self.edge_labels[-1]

    def drop(self, label, src_label=None, dst_label=None):
        for item in self.vertex_labels:
            if label == item.name:
                self.vertex_labels_to_drop.append(VertexLabel(label))
                return
        for item in self.edge_labels:
            if label == item.name:
                label_to_drop = EdgeLabel(label)
                if src_label and dst_label:
                    label_to_drop.source(src_label).destination(dst_label)
                self.edge_labels_to_drop.append(label)
                return
        raise ValueError(f"Label {label} not found.")

    def _prepare_batch_rpc(self):
        requests = ddl_service_pb2.BatchSubmitRequest()
        for item in self.vertex_labels:
            type_pb = item.as_type_def()
            requests.value.add().create_vertex_type_request.type_def.CopyFrom(type_pb)
        for item in self.edge_labels:
            type_pb = item.as_type_def()
            requests.value.add().create_edge_type_request.type_def.CopyFrom(type_pb)
            for rel in item.relations:
                assert rel.source and rel.destination, "Invalid relation "
                request = ddl_service_pb2.AddEdgeKindRequest()
                request.edge_label = item.name
                request.src_vertex_label = rel.source
                request.dst_vertex_label = rel.destination
                requests.value.add().add_edge_kind_request.CopyFrom(request)
        for item in self.vertex_labels_to_drop:
            requests.value.add().drop_vertex_type_request.label = item.name
        for item in self.edge_labels_to_drop:
            if item.relations:
                request = ddl_service_pb2.RemoveEdgeKindRequest()
                request.edge_label = item.name
                request.src_vertex_label = item.relations[0].source
                request.dst_vertex_label = item.relations[0].destination
                requests.value.add().drop_edge_kind_request.CopyFrom(request)
            else:
                requests.value.add().drop_edge_type_request.label = item.name
        return requests

    def update(self):
        requests = self._prepare_batch_rpc()
        return requests

    def create_and_replace(self, vineyard_schema):
        pass




if __name__ == "__main__":
    schema = Schema()
    schema.add_vertex_label("person").add_primary_key("id", "int").add_property("name", "str")
    schema.add_edge_label("knows").source("person").destination("person").add_property("date", "int")
    print(schema.update())
    endpoint = "host:port"
    channel = grpc.insecure_channel(endpoint)
    stub = ddl_service_pb2_grpc.ClientDdlStub(channel)
    stub.batchSubmit(schema.update())
