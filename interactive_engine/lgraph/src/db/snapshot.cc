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

#include "lgraph/db/snapshot.h"
#include "lgraph/db/store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {
namespace db {

Snapshot::Snapshot(PartitionSnapshotHandle handle) : handle_(handle) {}

Snapshot::~Snapshot() {
  if (handle_ != nullptr) {
    ffi::ReleasePartitionSnapshotHandle(handle_);
  }
}

Result<Vertex, Error> Snapshot::GetVertex(VertexId vertex_id, LabelId label_id) {
  ErrorHandle err_hdl = nullptr;
  VertexHandle vertex_hdl = ffi::GetVertex(handle_, vertex_id, label_id, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Vertex, Error>(Ok(Vertex(vertex_hdl)));
  }
  return Result<Vertex, Error>(Err(Error(err_hdl)));
}

Result<Edge, Error> Snapshot::GetEdge(EdgeId edge_id, const EdgeRelation &edge_relation) {
  ErrorHandle err_hdl = nullptr;
  EdgeHandle edge_hdl = ffi::GetEdge(handle_, edge_id, edge_relation, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Edge, Error>(Ok(Edge(edge_hdl)));
  }
  return Result<Edge, Error>(Err(Error(err_hdl)));
}

Result<VertexIterator, Error> Snapshot::ScanVertex(LabelId label_id) {
  ErrorHandle err_hdl = nullptr;
  VertexIterHandle vertex_iter_hdl = ffi::ScanVertex(handle_, label_id, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<VertexIterator, Error>(Ok(VertexIterator(vertex_iter_hdl)));
  }
  return Result<VertexIterator, Error>(Err(Error(err_hdl)));
}

Result<EdgeIterator, Error> Snapshot::ScanEdge(const EdgeRelation &edge_relation) {
  ErrorHandle err_hdl = nullptr;
  EdgeIterHandle edge_iter_hdl = ffi::ScanEdge(handle_, edge_relation, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<EdgeIterator, Error>(Ok(EdgeIterator(edge_iter_hdl)));
  }
  return Result<EdgeIterator, Error>(Err(Error(err_hdl)));
}

Result<EdgeIterator, Error> Snapshot::GetOutEdges(VertexId vertex_id, LabelId edge_label_id) {
  ErrorHandle err_hdl = nullptr;
  EdgeIterHandle edge_iter_hdl = ffi::GetOutEdges(handle_, vertex_id, edge_label_id, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<EdgeIterator, Error>(Ok(EdgeIterator(edge_iter_hdl)));
  }
  return Result<EdgeIterator, Error>(Err(Error(err_hdl)));
}

Result<EdgeIterator, Error> Snapshot::GetInEdges(VertexId vertex_id, LabelId edge_label_id) {
  ErrorHandle err_hdl = nullptr;
  EdgeIterHandle edge_iter_hdl = ffi::GetInEdges(handle_, vertex_id, edge_label_id, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<EdgeIterator, Error>(Ok(EdgeIterator(edge_iter_hdl)));
  }
  return Result<EdgeIterator, Error>(Err(Error(err_hdl)));
}

Result<size_t, Error> Snapshot::GetOutDegree(VertexId vertex_id, const EdgeRelation &edge_relation) {
  ErrorHandle err_hdl = nullptr;
  size_t degree = ffi::GetOutDegree(handle_, vertex_id, edge_relation, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<size_t, Error>(Ok(degree));
  }
  return Result<size_t, Error>(Err(Error(err_hdl)));
}

Result<size_t, Error> Snapshot::GetInDegree(VertexId vertex_id, const EdgeRelation &edge_relation) {
  ErrorHandle err_hdl = nullptr;
  size_t degree = ffi::GetInDegree(handle_, vertex_id, edge_relation, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<size_t, Error>(Ok(degree));
  }
  return Result<size_t, Error>(Err(Error(err_hdl)));
}

Result<Edge, Error> Snapshot::GetKthOutEdge(VertexId vertex_id, const EdgeRelation &edge_relation, SerialId k) {
  ErrorHandle err_hdl = nullptr;
  EdgeHandle edge_hdl = ffi::GetKthOutEdge(handle_, vertex_id, edge_relation, k, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Edge, Error>(Ok(Edge(edge_hdl)));
  }
  return Result<Edge, Error>(Err(Error(err_hdl)));
}

Result<Edge, Error> Snapshot::GetKthInEdge(VertexId vertex_id, const EdgeRelation &edge_relation, SerialId k) {
  ErrorHandle err_hdl = nullptr;
  EdgeHandle edge_hdl = ffi::GetKthInEdge(handle_, vertex_id, edge_relation, k, &err_hdl);
  if (err_hdl == nullptr) {
    return Result<Edge, Error>(Ok(Edge(edge_hdl)));
  }
  return Result<Edge, Error>(Err(Error(err_hdl)));
}

SnapshotId Snapshot::GetSnapshotId() {
  return ffi::GetSnapshotId(handle_);
}

}
}
