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

#include "db/common/namespace.h"

namespace DB_NAMESPACE {

typedef uint64_t SnapshotId;
typedef uint32_t LabelId;
typedef uint64_t VertexId;
typedef uint64_t EdgeInnerId;
typedef uint32_t PropertyId;
typedef uint32_t SerialId;

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

  static EdgeRelation From(LabelId e_label_id, LabelId src_label_id, LabelId dst_label_id) {
    EdgeRelation er{};
    er.edge_label_id = e_label_id;
    er.src_vertex_label_id = src_label_id;
    er.dst_vertex_label_id = dst_label_id;
    return er;
  }

  static EdgeRelation None() {
    return From(std::numeric_limits<LabelId>::max(),
                std::numeric_limits<LabelId>::max(),
                std::numeric_limits<LabelId>::max());
  }
 };

struct StringSlice {
  void* data;
  size_t len;

  static StringSlice From(void* data_ptr, size_t length) {
    StringSlice ss{};
    ss.data = data_ptr;
    ss.len = length;
    return ss;
  }
};

const LabelId none_label_id = std::numeric_limits<LabelId>::max();
const EdgeRelation none_edge_relation = EdgeRelation::None();

typedef void* PartitionGraphHandle;
typedef void* PartitionSnapshotHandle;
typedef void* ErrorHandle;
typedef void* VertexHandle;
typedef void* VertexIterHandle;
typedef void* EdgeHandle;
typedef void* EdgeIterHandle;
typedef void* PropertyHandle;
typedef void* PropertyIterHandle;

}  // namespace DB_NAMESPACE
