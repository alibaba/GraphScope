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

#include "lgraph/db/vertex.h"
#include "lgraph/db/edge.h"

namespace LGRAPH_NAMESPACE {
namespace db {

class Snapshot {
public:
  explicit Snapshot(PartitionSnapshotHandle handle);

  ~Snapshot();

  // Move Only!
  // Avoid copy construction and assignment.
  Snapshot(const Snapshot &) = delete;
  Snapshot &operator=(const Snapshot &) = delete;
  Snapshot(Snapshot &&ss) noexcept;
  Snapshot &operator=(Snapshot &&ss) noexcept;

  bool Valid() const { return handle_ != nullptr; }

  // Get vertex by vertex_id and label_id.
  // Default : label_id not specified.
  Result<Vertex, Error> GetVertex(VertexId vertex_id, LabelId label_id = none_label_id);

  // Get edge by edge_id and edge_relation.
  // Default : edge_relation not specified.
  Result<Edge, Error> GetEdge(EdgeId edge_id, const EdgeRelation &edge_relation = none_edge_relation);

  // Scan vertex by label_id.
  // Default : label_id not specified.
  Result<VertexIterator, Error> ScanVertex(LabelId label_id = none_label_id);

  // Scan edge by edge_relation.
  // Default : edge_relation not specified.
  Result<EdgeIterator, Error> ScanEdge(const EdgeRelation &edge_relation = none_edge_relation);

  // Get out/in edges of vertex_id by edge_label_id.
  // Default : edge_label_id not specified.
  Result<EdgeIterator, Error> GetOutEdges(VertexId vertex_id, LabelId edge_label_id = none_label_id);
  Result<EdgeIterator, Error> GetInEdges(VertexId vertex_id, LabelId edge_label_id = none_label_id);

  // Get out/in degree of vertex_id by edge_relation.
  // edge_relation must be specified.
  Result<size_t, Error> GetOutDegree(VertexId vertex_id, const EdgeRelation &edge_relation);
  Result<size_t, Error> GetInDegree(VertexId vertex_id, const EdgeRelation &edge_relation);

  // Get the kth out/in edge of vertex_id by edge_relation.
  // edge_relation must be specified.
  Result<Edge, Error> GetKthOutEdge(VertexId vertex_id, const EdgeRelation &edge_relation, SerialId k);
  Result<Edge, Error> GetKthInEdge(VertexId vertex_id, const EdgeRelation &edge_relation, SerialId k);

  // Get the snapshot id.
  SnapshotId GetSnapshotId();

private:
  PartitionSnapshotHandle handle_;

  Snapshot();
};

inline Snapshot::Snapshot() : handle_(nullptr) {}

inline Snapshot::Snapshot(Snapshot &&ss) noexcept: Snapshot() {
  *this = std::move(ss);
}

inline Snapshot &Snapshot::operator=(Snapshot &&ss) noexcept {
  if (this != &ss) {
    this->~Snapshot();
    handle_ = ss.handle_;
    ss.handle_ = nullptr;
  }
  return *this;
}

}
}
