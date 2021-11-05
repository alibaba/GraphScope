/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include "common/namespace.h"

namespace LGRAPH_NAMESPACE {

typedef uint64_t SnapshotId;
typedef uint32_t LabelId;
typedef uint64_t VertexId;
typedef uint64_t EdgeInnerId;
typedef uint32_t PropertyId;
typedef uint32_t SerialId;

enum EntityType : int32_t {
  VERTEX = 0,
  EDGE = 1,
};

enum DataType : int32_t {
  UNKNOWN = 0,
  BOOL = 1,
  CHAR = 2,
  SHORT = 3,
  INT = 4,
  LONG = 5,
  FLOAT = 6,
  DOUBLE = 7,
  STRING = 8,
  BYTES = 9,
  INT_LIST = 10,
  LONG_LIST = 11,
  FLOAT_LIST = 12,
  DOUBLE_LIST = 13,
  STRING_LIST = 14,
};

struct EdgeId {
  EdgeInnerId edge_inner_id;
  VertexId src_vertex_id;
  VertexId dst_vertex_id;

  static EdgeId From(EdgeInnerId inner_id, VertexId src_id, VertexId dst_id) {
    EdgeId e_id{};
    e_id.edge_inner_id = inner_id;
    e_id.src_vertex_id = src_id;
    e_id.dst_vertex_id = dst_id;
    return e_id;
  }
};

struct EdgeRelation {
  LabelId edge_label_id;
  LabelId src_vertex_label_id;
  LabelId dst_vertex_label_id;

  EdgeRelation() = default;

  EdgeRelation(LabelId e_label_id, LabelId src_label_id, LabelId dst_label_id)
      : edge_label_id(e_label_id), src_vertex_label_id(src_label_id), dst_vertex_label_id(dst_label_id) {}

  EdgeRelation(const EdgeRelation &) = default;
  EdgeRelation(EdgeRelation &&rhs) = default;
  EdgeRelation &operator=(const EdgeRelation &) = default;
  EdgeRelation &operator=(EdgeRelation &&rhs) = default;
};

struct StringSlice {
  void *data;
  size_t len;

  static StringSlice From(void *data_ptr, size_t length) {
    StringSlice ss{};
    ss.data = data_ptr;
    ss.len = length;
    return ss;
  }
};

const LabelId none_label_id = std::numeric_limits<LabelId>::max();
const EdgeRelation none_edge_relation = EdgeRelation{std::numeric_limits<LabelId>::max(),
                                                     std::numeric_limits<LabelId>::max(),
                                                     std::numeric_limits<LabelId>::max()};

typedef void *PartitionGraphHandle;
typedef void *PartitionSnapshotHandle;
typedef void *ErrorHandle;
typedef void *VertexHandle;
typedef void *VertexIterHandle;
typedef void *EdgeHandle;
typedef void *EdgeIterHandle;
typedef void *PropertyHandle;
typedef void *PropertyIterHandle;

}
