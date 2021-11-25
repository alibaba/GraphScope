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

from graphscope.proto import write_service_pb2


class VertexRecordKey:
    """Unique identifier of a vertex.
    The primary key may be a dict, the key is the property name,
    and the value is the data.
    """

    __slots__ = ["label", "primary_key"]

    def __init__(self, label, primary_key):
        self.label: str = label
        self.primary_key: dict = primary_key


class EdgeRecordKey:
    """Unique identifier of a edge.
    The `eid` is required in Update and Delete, which is a
    system generated unsigned integer. User need to get that eid
    by other means such as gremlin query.
    """

    __slots__ = ["label", "src_vertex_key", "dst_vertex_key", "eid"]

    def __init__(self, label, src_vertex_key, dst_vertex_key, eid=None):
        self.label: str = label
        self.src_vertex_key: VertexRecordKey = src_vertex_key
        self.dst_vertex_key: VertexRecordKey = dst_vertex_key
        self.eid: int = eid  # Only required in Update and Delete.


def to_vertex_record_key_pb(vertex_record_key: VertexRecordKey):
    pb = write_service_pb2.VertexRecordKeyPb()
    pb.label = vertex_record_key.label
    for key, value in vertex_record_key.primary_key.items():
        pb.pk_properties[key] = str(value)
    return pb


def to_edge_record_key_pb(edge_record_key: EdgeRecordKey):
    pb = write_service_pb2.EdgeRecordKeyPb()
    pb.label = edge_record_key.label
    pb.src_vertex_key.CopyFrom(to_vertex_record_key_pb(edge_record_key.src_vertex_key))
    pb.dst_vertex_key.CopyFrom(to_vertex_record_key_pb(edge_record_key.dst_vertex_key))
    if edge_record_key.eid is not None:
        pb.inner_id = edge_record_key.eid
    return pb


def to_data_record_pb(kind, key, properties):
    pb = write_service_pb2.DataRecordPb()
    if kind == "VERTEX":
        pb.vertex_record_key.CopyFrom(to_vertex_record_key_pb(key))
    elif kind == "EDGE":
        pb.edge_record_key.CopyFrom(to_edge_record_key_pb(key))
    else:
        raise TypeError(f"Not supported kind: {kind}")
    for key, value in properties.items():
        pb.properties[key] = str(value)
    return pb


def to_write_requests_pb(kind: str, inputs: list, write_type):
    """Wrap inputs to BatchWriteRequest.

    Args:
        kind (str): "VERTEX" or "EDGE"
        inputs (list): list of [pk, properties]
        ddl_type (write_service_pb2.WRITE_TYPE): the write type
    """
    request = write_service_pb2.BatchWriteRequest()
    for pk, properties in inputs:
        write_request_pb = request.write_requests.add()
        write_request_pb.write_type = write_type
        write_request_pb.data_record.CopyFrom(to_data_record_pb(kind, pk, properties))
    return request
