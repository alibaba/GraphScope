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
typedef size_t SerialId;

struct EdgeId {
  EdgeInnerId edge_inner_id;
  VertexId src_vertex_id;
  VertexId dst_vertex_id;

//  EdgeId(EdgeInnerId edge_inner_id, VertexId src_vertex_id, VertexId dst_vertex_id)
//    : edge_inner_id(edge_inner_id), src_vertex_id(src_vertex_id), dst_vertex_id(dst_vertex_id) {}
//
//  EdgeInnerId GetEdgeInnerId() const { return edge_inner_id; }
//  VertexId GetSrcVertexId() const { return src_vertex_id; }
//  VertexId GetDstVertexId() const { return dst_vertex_id; }
};

struct EdgeRelation {
  LabelId edge_label_id;
  LabelId src_vertex_label_id;
  LabelId dst_vertex_label_id;

//  EdgeRelation(LabelId edge_label_id, LabelId src_vertex_label_id, LabelId dst_vertex_label_id)
//      : edge_label_id(edge_label_id), src_vertex_label_id(src_vertex_label_id), dst_vertex_label_id(dst_vertex_label_id) {}
//
//  LabelId GetEdgeLabelId() const { return edge_label_id; }
//  LabelId GetSrcVertexLabelId() const { return src_vertex_label_id; }
//  LabelId GetDstVertexLabelId() const { return dst_vertex_label_id; }
};

enum DataType {
  Boolean = 0,
  Char = 1,
  Short = 2,
  Int = 3,
  Long = 4,
  Float = 5,
  Double = 6,
  String = 7,
  Bytes = 8,
  IntList = 9,
  LongList = 10,
  FloatList = 11,
  DoubleList = 12,
  StringList = 13,
};

struct StringSlice {
  void* data;
  size_t len;

//  StringSlice() : data(nullptr), len(0) {}
//  StringSlice(void* data, size_t len) : data(data), len(len) {}
//
//  const void* Data() const { return data; }
//  size_t Len() const { return len; }
};

const LabelId none_label_id = std::numeric_limits<LabelId>::max();
const EdgeRelation none_edge_relation{};

typedef void* SnapshotHandle;
typedef void* ErrorHandle;
typedef void* VertexHandle;
typedef void* VertexIterHandle;
typedef void* EdgeHandle;
typedef void* EdgeIterHandle;
typedef void* PropertyHandle;
typedef void* PropertyIterHandle;

}  // namespace DB_NAMESPACE
