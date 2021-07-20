#!/usr/bin/env python3

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

""" Manage connections of the GraphScope store service.
"""

import grpc
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from graphscope.framework.graph_schema import GraphSchema
from graphscope.framework.record import VertexRecordKey
from graphscope.framework.record import EdgeRecordKey
from graphscope.proto import ddl_service_pb2
from graphscope.proto import write_service_pb2
from graphscope.proto import write_service_pb2_grpc
from graphscope.proto import ddl_service_pb2_grpc


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

class Graph:
  def __init__(self, graph_def, conn=None) -> None:
    self._schema = GraphSchema()
    self._schema.from_graph_def(graph_def)
    self._conn = conn
    self._schema._conn = conn
    
  def schema(self):
    return self._schema
  
  def insert_vertex(self, vertex: VertexRecordKey, properties: dict):
    return self.insert_vertices([vertex, properties])
  
  def insert_vertices(self, vertices: list):
    for vertex, properties in vertices:
      pb = to_vertex_record_key_pb(vertex)
    pass
    
  def update_vertex_properties(self, vertex: VertexRecordKey, properties: dict): 
    pass
    
  def delete_vertex(self, vertex_pk: VertexRecordKey):
    return self.delete_vertices([vertex_pk])
  
  def delete_vertices(self, vertex_pks: list):
    pass
    
  def insert_edge(self,  edge: EdgeRecordKey, properties: dict):
    return self.insert_edges([edge, properties])
  
  def insert_edges(self, edges=list):
    pass
    
  def update_edge_properties(self, edge: EdgeRecordKey, properties: dict):
    pass
    
  def delete_edge(self, edge: EdgeRecordKey):
    return self.delete_edges([edge])
  
  def delete_edges(self, edges: list):
    pass


class Connection:
  def __init__(self, addr, gremlin_endpoint=None) -> None:
    self._addr = addr
    self._gremlin_endpoint = gremlin_endpoint
    channel = grpc.insecure_channel(addr)
    self._stub = ddl_service_pb2_grpc.ClientDdlStub(channel)
    
  def submit(self, requests):
    return self._stub.batchSubmit(requests)
  
  def get_graph_def(self, requests):
    return self._stub.getGraphDef(requests)
  
  def g(self):
    request = ddl_service_pb2.GetGraphDefRequest()
    graph_def = self.get_graph_def(request).graph_def
    graph = Graph(graph_def, self)
    return graph
  
  def gremlin(self):
    graph_url = "ws://%s/gremlin" % self._gremlin_endpoint
    return traversal().withRemote(DriverRemoteConnection(graph_url, "g"))
  
  
def conn(addr, gremlin_endpoint=None):
  return Connection(addr, gremlin_endpoint)
