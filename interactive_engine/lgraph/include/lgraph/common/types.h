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
#pragma GCC diagnostic ignored "-Wreturn-type-c-linkage"

#include <cstddef>
#include <cstdint>
#include <limits>

#include "lgraph/common/namespace.h"

namespace LGRAPH_NAMESPACE {

typedef uint64_t SnapshotId;
typedef uint32_t LabelId;
typedef uint64_t VertexId;
typedef uint64_t EdgeInnerId;
typedef uint32_t PropertyId;
typedef uint32_t SerialId;
typedef int32_t BackupId;

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

enum OpType : int32_t {
  MARKER = 0,

  OVERWRITE_VERTEX = 1,
  UPDATE_VERTEX = 2,
  DELETE_VERTEX = 3,
  OVERWRITE_EDGE = 4,
  UPDATE_EDGE = 5,
  DELETE_EDGE = 6,

  CREATE_VERTEX_TYPE = 7,
  CREATE_EDGE_TYPE = 8,
  ADD_EDGE_KIND = 9,

  DROP_VERTEX_TYPE = 10,
  DROP_EDGE_TYPE = 11,
  REMOVE_EDGE_KIND = 12,

  PREPARE_DATA_LOAD = 13,
  COMMIT_DATA_LOAD = 14,
  };

struct EdgeId {
  EdgeInnerId edge_inner_id;
  VertexId src_vertex_id;
  VertexId dst_vertex_id;

  EdgeId(EdgeInnerId inner_id, VertexId src_id, VertexId dst_id)
      : edge_inner_id(inner_id), src_vertex_id(src_id), dst_vertex_id(dst_id) {}
  EdgeId(const EdgeId &) = default;
  EdgeId(EdgeId &&) = default;
  EdgeId &operator=(const EdgeId &) = default;
  EdgeId &operator=(EdgeId &&) = default;
};

struct EdgeRelation {
  LabelId edge_label_id;
  LabelId src_vertex_label_id;
  LabelId dst_vertex_label_id;

  EdgeRelation(LabelId e_label_id, LabelId src_label_id, LabelId dst_label_id)
      : edge_label_id(e_label_id), src_vertex_label_id(src_label_id), dst_vertex_label_id(dst_label_id) {}
  EdgeRelation(const EdgeRelation &) = default;
  EdgeRelation(EdgeRelation &&) = default;
  EdgeRelation &operator=(const EdgeRelation &) = default;
  EdgeRelation &operator=(EdgeRelation &&) = default;
};

struct StringSlice {
  void *data;
  size_t len;
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
