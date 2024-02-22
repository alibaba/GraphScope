/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
#define GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_

#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"
#include "glog/logging.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

inline void serialize_field(grape::InArchive& arc, const Any& prop) {
  if (prop.type == PropertyType::Bool()) {
    arc << prop.value.b;
  } else if (prop.type == PropertyType::Int32()) {
    arc << prop.value.i;
  } else if (prop.type == PropertyType::UInt32()) {
    arc << prop.value.ui;
  } else if (prop.type == PropertyType::Date()) {
    arc << prop.value.d.milli_second;
  } else if (prop.type == PropertyType::Day()) {
    arc << prop.value.day.to_u32();
  } else if (prop.type == PropertyType::String()) {
    arc << prop.value.s;
  } else if (prop.type == PropertyType::Int64()) {
    arc << prop.value.l;
  } else if (prop.type == PropertyType::UInt64()) {
    arc << prop.value.ul;
  } else if (prop.type == PropertyType::Double()) {
    arc << prop.value.db;
  } else if (prop.type == PropertyType::Float()) {
    arc << prop.value.f;
  } else if (prop.type == PropertyType::Empty()) {
  } else {
    LOG(FATAL) << "Unexpected property type" << int(prop.type.type_enum);
  }
}

inline void deserialize_field(grape::OutArchive& arc, Any& prop) {
  if (prop.type == PropertyType::Bool()) {
    arc >> prop.value.b;
  } else if (prop.type == PropertyType::Int32()) {
    arc >> prop.value.i;
  } else if (prop.type == PropertyType::UInt32()) {
    arc >> prop.value.ui;
  } else if (prop.type == PropertyType::Date()) {
    int64_t date_val;
    arc >> date_val;
    prop.value.d.milli_second = date_val;
  } else if (prop.type == PropertyType::Day()) {
    uint32_t val;
    arc >> val;
    prop.value.day.from_u32(val);
  } else if (prop.type == PropertyType::String()) {
    arc >> prop.value.s;
  } else if (prop.type == PropertyType::Int64()) {
    arc >> prop.value.l;
  } else if (prop.type == PropertyType::UInt64()) {
    arc >> prop.value.ul;
  } else if (prop.type == PropertyType::Double()) {
    arc >> prop.value.db;
  } else if (prop.type == PropertyType::Float()) {
    arc >> prop.value.f;
  } else if (prop.type == PropertyType::Empty()) {
  } else {
    LOG(FATAL) << "Unexpected property type: "
               << static_cast<int>(prop.type.type_enum);
  }
}

inline label_t deserialize_oid(const MutablePropertyFragment& graph,
                               grape::OutArchive& arc, Any& oid) {
  label_t label;
  arc >> label;
  oid.type = std::get<0>(graph.schema_.get_vertex_primary_key(label).at(0));
  deserialize_field(arc, oid);
  return label;
}

class UpdateBatch {
 public:
  UpdateBatch() { arc_.Resize(sizeof(WalHeader)); }
  UpdateBatch(const UpdateBatch& other) = delete;

  ~UpdateBatch() {
    arc_.Clear();
    update_vertices_.clear();
    update_edges_.clear();
  }
  void clear() {
    arc_.Clear();
    update_vertices_.clear();
    update_edges_.clear();
    arc_.Resize(sizeof(WalHeader));
  }
  void AddVertex(label_t label, Any&& oid, std::vector<Any>&& props) {
    arc_ << static_cast<uint8_t>(0) << label;
    serialize_field(arc_, oid);
    for (auto& prop : props) {
      serialize_field(arc_, prop);
    }
    update_vertices_.emplace_back(label, std::move(oid), std::move(props));
  }

  void AddEdge(label_t src_label, Any&& src, label_t dst_label, Any&& dst,
               label_t edge_label, Any&& prop) {
    arc_ << static_cast<uint8_t>(1) << src_label;
    serialize_field(arc_, src);
    arc_ << dst_label;
    serialize_field(arc_, dst);
    arc_ << edge_label;
    serialize_field(arc_, prop);
    update_edges_.emplace_back(src_label, std::move(src), dst_label,
                               std::move(dst), edge_label, std::move(prop));
  }
  const std::vector<std::tuple<label_t, Any, std::vector<Any>>>&
  GetUpdateVertices() const {
    return update_vertices_;
  }
  const std::vector<std::tuple<label_t, Any, label_t, Any, label_t, Any>>&
  GetUpdateEdges() const {
    return update_edges_;
  }
  grape::InArchive& GetArc() { return arc_; }

 private:
  std::vector<std::tuple<label_t, Any, std::vector<Any>>> update_vertices_;
  std::vector<std::tuple<label_t, Any, label_t, Any, label_t, Any>>
      update_edges_;
  grape::InArchive arc_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_TRANSACTION_UTILS_H_
