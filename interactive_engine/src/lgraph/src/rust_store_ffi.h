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

#include <cstdint>

#include "db/common/namespace.h"
#include "db/common/types.h"

namespace DB_NAMESPACE {

namespace ffi {

extern "C" {
  /// Snapshot FFIs
  SnapshotHandle GetSnapshot();
  VertexHandle GetVertex(SnapshotHandle snapshot, VertexId vertex_id, LabelId label_id, ErrorHandle* error);
  EdgeHandle GetEdge(SnapshotHandle snapshot, EdgeId edge_id, const EdgeRelation& edge_relation, ErrorHandle* error);
  VertexIterHandle ScanVertex(SnapshotHandle snapshot, LabelId label_id, ErrorHandle* error);
  EdgeIterHandle ScanEdge(SnapshotHandle snapshot, const EdgeRelation& edge_relation, ErrorHandle* error);
  EdgeIterHandle GetOutEdges(SnapshotHandle snapshot, VertexId vertex_id, LabelId edge_label_id, ErrorHandle* error);
  EdgeIterHandle GetInEdges(SnapshotHandle snapshot, VertexId vertex_id, LabelId edge_label_id, ErrorHandle* error);
  size_t GetOutDegree(SnapshotHandle snapshot, VertexId vertex_id, const EdgeRelation& edge_relation, ErrorHandle* error);
  size_t GetInDegree(SnapshotHandle snapshot, VertexId vertex_id, const EdgeRelation& edge_relation, ErrorHandle* error);
  EdgeHandle GetKthOutEdge(SnapshotHandle snapshot, VertexId vertex_id, const EdgeRelation& edge_relation, SerialId k, ErrorHandle* error);
  EdgeHandle GetKthInEdge(SnapshotHandle snapshot, VertexId vertex_id, const EdgeRelation& edge_relation, SerialId k, ErrorHandle* error);
  SnapshotId GetSnapshotId(SnapshotHandle snapshot);

  /// Vertex FFIs
  VertexHandle VertexIteratorNext(VertexIterHandle vertex_iter, ErrorHandle* error);
  VertexId GetVertexId(VertexHandle vertex_hdl);
  LabelId GetVertexLabelId(VertexHandle vertex_hdl);
  PropertyHandle GetVertexProperty(VertexHandle vertex_hdl, PropertyId prop_id);
  PropertyIterHandle GetVertexPropertyIterator(VertexHandle vertex_hdl);

  /// Edge FFIs
  EdgeHandle EdgeIteratorNext(EdgeIterHandle edge_iter, ErrorHandle* error);
  EdgeId GetEdgeId(EdgeHandle edge_hdl);
  EdgeRelation GetEdgeRelation(EdgeHandle edge_hdl);
  PropertyHandle GetEdgeProperty(EdgeHandle edge_hdl, PropertyId prop_id);
  PropertyIterHandle GetEdgePropertyIterator(EdgeHandle edge_hdl);

  /// Property FFIs
  PropertyHandle PropertyIteratorNext(PropertyIterHandle prop_iter, ErrorHandle* error);
  PropertyId GetPropertyId(PropertyHandle prop_hdl);
  int32_t GetPropertyAsInt32(PropertyHandle prop_hdl, ErrorHandle* error);
  int64_t GetPropertyAsInt64(PropertyHandle prop_hdl, ErrorHandle* error);
  float GetPropertyAsFloat(PropertyHandle prop_hdl, ErrorHandle* error);
  double GetPropertyAsDouble(PropertyHandle prop_hdl, ErrorHandle* error);
  StringSlice GetPropertyAsString(PropertyHandle prop_hdl, ErrorHandle* error);

  /// Error FFIs
  void PrintError(ErrorHandle error_hdl);

  /// Release FFIs
  void ReleasePartitionSnapshotHandle(SnapshotHandle ptr);
  void ReleaseErrorHandle(ErrorHandle ptr);
  void ReleaseVertexHandle(VertexHandle ptr);
  void ReleaseVertexIteratorHandle(VertexIterHandle ptr);
  void ReleaseEdgeHandle(EdgeHandle ptr);
  void ReleaseEdgeIteratorHandle(EdgeIterHandle ptr);
  void ReleasePropertyHandle(PropertyHandle ptr);
  void ReleasePropertyIteratorHandle(PropertyIterHandle ptr);
}

}  // namespace ffi

}  // namespace DB_NAMESPACE