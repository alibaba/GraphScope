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

#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

InsertTransaction::InsertTransaction(MutablePropertyFragment& graph,
                                     ArenaAllocator& alloc, WalWriter& logger,
                                     VersionManager& vm, timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

InsertTransaction::~InsertTransaction() { Abort(); }

bool InsertTransaction::AddVertex(label_t label, oid_t id,
                                  const std::vector<Any>& props) {
  size_t arc_size = arc_.GetSize();
  arc_ << static_cast<uint8_t>(0) << label << id;
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    arc_.Resize(arc_size);
    return false;
  }
  int col_num = props.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    auto& prop = props[col_i];
    if (prop.type != types[col_i]) {
      arc_.Resize(arc_size);
      return false;
    }
    serialize_field(arc_, prop);
  }
  added_vertices_.emplace(label, id);
  return true;
}

bool InsertTransaction::AddEdge(label_t src_label, oid_t src, label_t dst_label,
                                oid_t dst, label_t edge_label,
                                const Any& prop) {
  vid_t lid;
  if (!graph_.get_lid(src_label, src, lid)) {
    if (added_vertices_.find(std::make_pair(src_label, src)) ==
        added_vertices_.end()) {
      return false;
    }
  }
  if (!graph_.get_lid(dst_label, dst, lid)) {
    if (added_vertices_.find(std::make_pair(dst_label, dst)) ==
        added_vertices_.end()) {
      return false;
    }
  }
  const PropertyType& type =
      graph_.schema().get_edge_property(src_label, dst_label, edge_label);
  if (prop.type != type) {
    return false;
  }
  arc_ << static_cast<uint8_t>(1) << src_label << src << dst_label << dst
       << edge_label;
  serialize_field(arc_, prop);
  return true;
}

void InsertTransaction::Commit() {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return;
  }
  if (arc_.GetSize() == sizeof(WalHeader)) {
    vm_.release_insert_timestamp(timestamp_);
    clear();
    return;
  }
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 0;
  header->timestamp = timestamp_;

  logger_.append(arc_.GetBuffer(), arc_.GetSize());
  IngestWal(graph_, timestamp_, arc_.GetBuffer() + sizeof(WalHeader),
            header->length, alloc_);

  vm_.release_insert_timestamp(timestamp_);
  clear();
}

void InsertTransaction::Abort() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "aborting " << timestamp_ << "-th transaction (insert)";
    vm_.release_insert_timestamp(timestamp_);
    clear();
  }
}

timestamp_t InsertTransaction::timestamp() const { return timestamp_; }

void InsertTransaction::IngestWal(MutablePropertyFragment& graph,
                                  uint32_t timestamp, char* data, size_t length,
                                  ArenaAllocator& alloc) {
  grape::OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    uint8_t op_type;
    arc >> op_type;
    if (op_type == 0) {
      label_t label;
      oid_t id;

      arc >> label >> id;
      vid_t lid = graph.add_vertex(label, id);
      graph.get_vertex_table(label).ingest(lid, arc);
    } else if (op_type == 1) {
      label_t src_label, dst_label, edge_label;
      oid_t src, dst;
      vid_t src_lid, dst_lid;

      arc >> src_label >> src >> dst_label >> dst >> edge_label;

      CHECK(get_vertex_with_retries(graph, src_label, src, src_lid));
      CHECK(get_vertex_with_retries(graph, dst_label, dst, dst_lid));

      graph.IngestEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                       timestamp, arc, alloc);
    } else {
      LOG(FATAL) << "Unexpected op-" << static_cast<int>(op_type);
    }
  }
}

void InsertTransaction::clear() {
  arc_.Clear();
  arc_.Resize(sizeof(WalHeader));
  added_vertices_.clear();

  timestamp_ = std::numeric_limits<timestamp_t>::max();
}

#define likely(x) __builtin_expect(!!(x), 1)

bool InsertTransaction::get_vertex_with_retries(MutablePropertyFragment& graph,
                                                label_t label, oid_t oid,
                                                vid_t& lid) {
  if (likely(graph.get_lid(label, oid, lid))) {
    return true;
  }
  for (int i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(1000000));
    if (likely(graph.get_lid(label, oid, lid))) {
      return true;
    }
  }

  LOG(ERROR) << "get_vertex [" << oid << "] failed";
  return false;
}

#undef likely

}  // namespace gs
