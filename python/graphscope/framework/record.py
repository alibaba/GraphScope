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

class VertexRecordKey:
  """Unique identifier of a vertex.
  The primary key may be a dict, the key is the property name,
  and the value is the data.
  """
  def __init__(self, label, primary_key):
    self.label: str = label
    self.primary_key: dict = primary_key


class EdgeRecordKey:
  """Unique identifier of a edge.
  The `eid` is required in Update and Delete, which is a
  system generated unsigned integer. User need to get that eid
  by other means such as gremlin query.
  """
  def __init__(self, label, src_vertex_key, dst_vertex_key, eid=None):
    self.label: str = label
    self.src_vertex_key: VertexRecordKey = src_vertex_key
    self.dst_vertex_key: VertexRecordKey = dst_vertex_key
    self.eid: int = eid  # Only required in Update and Delete.