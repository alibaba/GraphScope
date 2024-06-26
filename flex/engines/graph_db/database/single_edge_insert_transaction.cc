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

#include "grape/serialization/out_archive.h"

#include "flex/engines/graph_db/database/single_edge_insert_transaction.h"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {

SingleEdgeInsertTransaction::SingleEdgeInsertTransaction(
    MutablePropertyFragment& graph, Allocator& alloc, WalWriter& logger,
    VersionManager& vm, timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

SingleEdgeInsertTransaction::~SingleEdgeInsertTransaction() { Abort(); }

bool SingleEdgeInsertTransaction::AddEdge(label_t src_label, const Any& src,
                                          label_t dst_label, const Any& dst,
                                          label_t edge_label, const Any& prop) {
  if (!graph_.get_lid(src_label, src, src_vid_)) {
    std::string label_name = graph_.schema().get_vertex_label_name(src_label);
    LOG(ERROR) << "Source vertex " << label_name << "[" << src.to_string()
               << "] not found...";
    return false;
  }
  if (!graph_.get_lid(dst_label, dst, dst_vid_)) {
    std::string label_name = graph_.schema().get_vertex_label_name(dst_label);
    LOG(ERROR) << "Destination vertex " << label_name << "[" << dst.to_string()
               << "] not found...";
    return false;
  }
  if (prop.type != PropertyType::kRecord) {
    const PropertyType& type =
        graph_.schema().get_edge_property(src_label, dst_label, edge_label);
    if (prop.type != type) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " type not match, expected " << type << ", got "
                 << prop.type;
      return false;
    }
  } else {
    const auto& types =
        graph_.schema().get_edge_properties(src_label, dst_label, edge_label);
    if (prop.AsRecord().size() != types.size()) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " size not match, expected " << types.size() << ", got "
                 << prop.AsRecord().size();
      return false;
    }
    auto r = prop.AsRecord();
    for (size_t i = 0; i < r.size(); ++i) {
      if (r[i].type != types[i]) {
        std::string label_name =
            graph_.schema().get_edge_label_name(edge_label);
        LOG(ERROR) << "Edge property " << label_name
                   << " type not match, expected " << types[i] << ", got "
                   << r[i].type;
        return false;
      }
    }
  }
  src_label_ = src_label;
  dst_label_ = dst_label;
  edge_label_ = edge_label;
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
  serialize_field(arc_, prop);
  return true;
}

void SingleEdgeInsertTransaction::Abort() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    LOG(ERROR) << "aborting " << timestamp_
               << "-th transaction (single edge insert)";
    vm_.release_insert_timestamp(timestamp_);
    clear();
  }
}

timestamp_t SingleEdgeInsertTransaction::timestamp() const {
  return timestamp_;
}

void SingleEdgeInsertTransaction::Commit() {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return;
  }
  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 0;
  header->timestamp = timestamp_;
  logger_.append(arc_.GetBuffer(), arc_.GetSize());

  grape::OutArchive arc;
  {
    arc.SetSlice(arc_.GetBuffer() + sizeof(WalHeader),
                 arc_.GetSize() - sizeof(WalHeader));
    label_t op_type, label;
    Any temp;
    arc >> op_type;
    deserialize_oid(graph_, arc, temp);
    deserialize_oid(graph_, arc, temp);
    arc >> label;
  }
  graph_.IngestEdge(src_label_, src_vid_, dst_label_, dst_vid_, edge_label_,
                    timestamp_, arc, alloc_);
  vm_.release_insert_timestamp(timestamp_);
  clear();
}

void SingleEdgeInsertTransaction::clear() {
  arc_.Clear();
  arc_.Resize(sizeof(WalHeader));
  timestamp_ = std::numeric_limits<timestamp_t>::max();
}

}  // namespace gs
