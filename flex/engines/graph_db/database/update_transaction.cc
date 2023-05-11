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

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

UpdateTransaction::UpdateTransaction(MutablePropertyFragment& graph,
                                     ArenaAllocator& alloc, WalWriter& logger,
                                     VersionManager& vm, timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));

  vertex_label_num_ = graph_.schema().vertex_label_num();
  edge_label_num_ = graph_.schema().edge_label_num();

  added_vertices_.resize(vertex_label_num_);
  added_vertices_base_.resize(vertex_label_num_);
  vertex_nums_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    added_vertices_base_[i] = vertex_nums_[i] = graph_.vertex_num(i);
  }
  vertex_offsets_.resize(vertex_label_num_);
  extra_vertex_properties_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    const Table& table = graph_.get_vertex_table(i);
    extra_vertex_properties_[i].init(table.column_names(), table.column_types(),
                                     {}, 4096);
  }

  size_t csr_num = 2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_;
  added_edges_.resize(csr_num);
  updated_edge_data_.resize(csr_num);
}

UpdateTransaction::~UpdateTransaction() { release(); }

timestamp_t UpdateTransaction::timestamp() const { return timestamp_; }

void UpdateTransaction::Commit() {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return;
  }
  if (op_num_ == 0) {
    release();
    return;
  }

  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 1;
  header->timestamp = timestamp_;
  logger_.append(arc_.GetBuffer(), arc_.GetSize());

  applyVerticesUpdates();
  applyEdgesUpdates();
  release();
}

void UpdateTransaction::Abort() { release(); }

bool UpdateTransaction::AddVertex(label_t label, oid_t oid,
                                  const std::vector<Property>& props) {
  vid_t id;
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    LOG(ERROR) << "Wrong number of fields when adding vertex, expected "
               << types.size() << ", got " << props.size();
    return false;
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i]) {
      LOG(ERROR) << "Property type of field " << col_i << " is wrong, expected "
                 << types[col_i] << ", got " << props[col_i].type();
      return false;
    }
  }
  if (!oid_to_lid(label, oid, id)) {
    added_vertices_[label]._add(oid);
    id = vertex_nums_[label]++;
  }

  vid_t row_num = vertex_offsets_[label].size();
  vertex_offsets_[label].emplace(id, row_num);
  grape::InArchive arc;
  for (auto& prop : props) {
    serialize_field(arc, prop);
  }
  grape::OutArchive oarc;
  oarc.SetSlice(arc.GetBuffer(), arc.GetSize());
  extra_vertex_properties_[label].ingest(row_num, oarc);

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(0) << label << oid;
  arc_.AddBytes(arc.GetBuffer(), arc.GetSize());
  return true;
}

bool UpdateTransaction::AddEdge(label_t src_label, oid_t src, label_t dst_label,
                                oid_t dst, label_t edge_label,
                                const Property& value) {
  vid_t src_lid, dst_lid;
  if (!oid_to_lid(src_label, src, src_lid)) {
    std::string label_name = graph_.schema().get_vertex_label_name(src_label);
    LOG(ERROR) << "Source vertex " << label_name << "[" << src
               << "] not found...";
    return false;
  }
  if (!oid_to_lid(dst_label, dst, dst_lid)) {
    std::string label_name = graph_.schema().get_vertex_label_name(dst_label);
    LOG(ERROR) << "Destination vertex " << label_name << "[" << dst
               << "] not found...";
    return false;
  }
  PropertyType type =
      graph_.schema().get_edge_property(src_label, dst_label, edge_label);
  if (type != value.type()) {
    std::string label_name = graph_.schema().get_edge_label_name(edge_label);
    LOG(ERROR) << "Edge property " << label_name << " type not match, expected "
               << type << ", got " << value.type();
    return false;
  }
  size_t in_csr_index = get_in_csr_index(src_label, dst_label, edge_label);
  size_t out_csr_index = get_out_csr_index(src_label, dst_label, edge_label);
  added_edges_[in_csr_index][dst_lid].push_back(src_lid);
  updated_edge_data_[in_csr_index][dst_lid].emplace(src_lid, value);

  added_edges_[out_csr_index][src_lid].push_back(dst_lid);
  updated_edge_data_[out_csr_index][src_lid].emplace(dst_lid, value);

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(1) << src_label << src << dst_label << dst
       << edge_label;
  serialize_field(arc_, value);

  return true;
}

UpdateTransaction::vertex_iterator::vertex_iterator(label_t label, vid_t cur,
                                                    vid_t& num,
                                                    UpdateTransaction* txn)
    : label_(label), cur_(cur), num_(num), txn_(txn) {}
UpdateTransaction::vertex_iterator::~vertex_iterator() = default;
bool UpdateTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void UpdateTransaction::vertex_iterator::Next() { ++cur_; }
void UpdateTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

oid_t UpdateTransaction::vertex_iterator::GetId() const {
  return txn_->lid_to_oid(label_, cur_);
}

vid_t UpdateTransaction::vertex_iterator::GetIndex() const { return cur_; }

Property UpdateTransaction::vertex_iterator::GetField(int col_id) const {
  return txn_->GetVertexField(label_, cur_, col_id);
}

bool UpdateTransaction::vertex_iterator::SetField(int col_id,
                                                  const Property& value) {
  return txn_->SetVertexField(label_, cur_, col_id, value);
}

UpdateTransaction::edge_iterator::edge_iterator(
    bool dir, label_t label, vid_t v, label_t neighbor_label,
    label_t edge_label, const vid_t* aeb, const vid_t* aee,
    std::shared_ptr<GenericNbrIteratorMut<vid_t>> init_iter,
    UpdateTransaction* txn)
    : dir_(dir),
      label_(label),
      v_(v),
      neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      added_edges_cur_(aeb),
      added_edges_end_(aee),
      init_iter_(std::move(init_iter)),
      txn_(txn) {}
UpdateTransaction::edge_iterator::~edge_iterator() = default;

Property UpdateTransaction::edge_iterator::GetData() const {
  if (init_iter_->IsValid()) {
    vid_t cur = init_iter_->GetNeighbor();
    Property ret;
    if (txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                 edge_label_, ret)) {
      return ret;
    } else {
      return init_iter_->GetData();
    }
  } else {
    vid_t cur = *added_edges_cur_;
    Property ret;
    CHECK(txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                   edge_label_, ret));
    return ret;
  }
}

void UpdateTransaction::edge_iterator::SetData(const Property& value) {
  if (init_iter_->IsValid()) {
    vid_t cur = init_iter_->GetNeighbor();
    txn_->SetEdgeData(dir_, label_, v_, neighbor_label_, cur, edge_label_,
                      value);
  } else {
    vid_t cur = *added_edges_cur_;
    txn_->SetEdgeData(dir_, label_, v_, neighbor_label_, cur, edge_label_,
                      value);
  }
}

bool UpdateTransaction::edge_iterator::IsValid() const {
  return init_iter_->IsValid() || (added_edges_cur_ != added_edges_end_);
}

void UpdateTransaction::edge_iterator::Next() {
  if (init_iter_->IsValid()) {
    init_iter_->Next();
  } else {
    ++added_edges_cur_;
  }
}

vid_t UpdateTransaction::edge_iterator::GetNeighbor() const {
  if (init_iter_->IsValid()) {
    return init_iter_->GetNeighbor();
  } else {
    return *added_edges_cur_;
  }
}

label_t UpdateTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t UpdateTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

UpdateTransaction::vertex_iterator UpdateTransaction::GetVertexIterator(
    label_t label) {
  return {label, 0, vertex_nums_[label], this};
}

UpdateTransaction::edge_iterator UpdateTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) {
  size_t csr_index = get_out_csr_index(label, neighnor_label, edge_label);
  const vid_t* begin = nullptr;
  const vid_t* end = nullptr;
  auto iter = added_edges_[csr_index].find(u);
  if (iter != added_edges_[csr_index].end()) {
    begin = iter->second.data();
    end = begin + iter->second.size();
  }
  return {true,
          label,
          u,
          neighnor_label,
          edge_label,
          begin,
          end,
          graph_.get_outgoing_edges_mut(label, u, neighnor_label, edge_label,
                                        timestamp_),
          this};
}

UpdateTransaction::edge_iterator UpdateTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) {
  size_t csr_index = get_in_csr_index(label, neighnor_label, edge_label);
  const vid_t* begin = nullptr;
  const vid_t* end = nullptr;
  auto iter = added_edges_[csr_index].find(u);
  if (iter != added_edges_[csr_index].end()) {
    begin = iter->second.data();
    end = begin + iter->second.size();
  }
  return {false,
          label,
          u,
          neighnor_label,
          edge_label,
          begin,
          end,
          graph_.get_incoming_edges_mut(label, u, neighnor_label, edge_label,
                                        timestamp_),
          this};
}

Property UpdateTransaction::GetVertexField(label_t label, vid_t lid,
                                           int col_id) const {
  auto& vertex_offset = vertex_offsets_[label];
  auto iter = vertex_offset.find(lid);
  if (iter == vertex_offset.end()) {
    return graph_.get_vertex_table(label).get_column_by_id(col_id)->get(lid);
  } else {
    return extra_vertex_properties_[label].get_column_by_id(col_id)->get(
        iter->second);
  }
}

bool UpdateTransaction::SetVertexField(label_t label, vid_t lid, int col_id,
                                       const Property& value) {
  auto& vertex_offset = vertex_offsets_[label];
  auto iter = vertex_offset.find(lid);
  auto& extra_table = extra_vertex_properties_[label];
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id] != value.type()) {
    return false;
  }
  if (iter == vertex_offset.end()) {
    auto& table = graph_.get_vertex_table(label);
    if (table.col_num() <= static_cast<size_t>(col_id)) {
      return false;
    }
    if (graph_.vertex_num(label) <= lid) {
      return false;
    }
    vid_t new_offset = vertex_offset.size();
    vertex_offset.emplace(lid, new_offset);
    size_t col_num = table.col_num();
    for (size_t i = 0; i < col_num; ++i) {
      extra_table.get_column_by_id(i)->set(new_offset,
                                           table.get_column_by_id(i)->get(lid));
    }
    extra_table.get_column_by_id(col_id)->set(new_offset, value);
  } else {
    if (extra_table.col_num() <= static_cast<size_t>(col_id)) {
      return false;
    }
    extra_table.get_column_by_id(col_id)->set(iter->second, value);
  }

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(2) << label << lid_to_oid(label, lid) << col_id;
  serialize_field(arc_, value);
  return true;
}

void UpdateTransaction::SetEdgeData(bool dir, label_t label, vid_t v,
                                    label_t neighbor_label, vid_t nbr,
                                    label_t edge_label, const Property& value) {
  size_t csr_index = dir ? get_out_csr_index(label, neighbor_label, edge_label)
                         : get_in_csr_index(label, neighbor_label, edge_label);
  if (value.type() == PropertyType::kStringView) {
    size_t loc = sv_vec_.size();
    sv_vec_.emplace_back(value.get_value<std::string_view>());
    Property dup_value;
    dup_value.set_value<std::string>(sv_vec_[loc]);
    updated_edge_data_[csr_index][v].emplace(nbr, dup_value);
  } else {
    updated_edge_data_[csr_index][v].emplace(nbr, value);
  }

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(3) << static_cast<uint8_t>(dir ? 1 : 0) << label
       << lid_to_oid(label, v) << neighbor_label
       << lid_to_oid(neighbor_label, nbr) << edge_label;
  serialize_field(arc_, value);
}

bool UpdateTransaction::GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                                           label_t neighbor_label, vid_t nbr,
                                           label_t edge_label,
                                           Property& ret) const {
  size_t csr_index = dir ? get_out_csr_index(label, neighbor_label, edge_label)
                         : get_in_csr_index(label, neighbor_label, edge_label);
  auto map_iter = updated_edge_data_[csr_index].find(v);
  if (map_iter == updated_edge_data_[csr_index].end()) {
    return false;
  } else {
    auto& updates = map_iter->second;
    auto iter = updates.find(nbr);
    if (iter == updates.end()) {
      return false;
    } else {
      ret = iter->second;
      return true;
    }
  }
}

void UpdateTransaction::IngestWal(MutablePropertyFragment& graph,
                                  uint32_t timestamp, char* data, size_t length,
                                  ArenaAllocator& alloc) {
  std::vector<IdIndexer<oid_t, vid_t>> added_vertices;
  std::vector<vid_t> added_vertices_base;
  std::vector<vid_t> vertex_nums;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> vertex_offsets;
  std::vector<Table> extra_vertex_properties;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges;
  std::vector<ska::flat_hash_map<vid_t, ska::flat_hash_map<vid_t, Property>>>
      updated_edge_data;

  size_t vertex_label_num = graph.schema().vertex_label_num();
  size_t edge_label_num = graph.schema().edge_label_num();

  added_vertices.resize(vertex_label_num);
  added_vertices_base.resize(vertex_label_num);
  vertex_nums.resize(vertex_label_num);
  for (size_t i = 0; i < vertex_label_num; ++i) {
    added_vertices_base[i] = vertex_nums[i] = graph.vertex_num(i);
  }
  vertex_offsets.resize(vertex_label_num);
  extra_vertex_properties.resize(vertex_label_num);
  for (size_t i = 0; i < vertex_label_num; ++i) {
    const Table& table = graph.get_vertex_table(i);
    extra_vertex_properties[i].init(table.column_names(), table.column_types(),
                                    {}, 4096);
  }

  size_t csr_num = 2 * vertex_label_num * vertex_label_num * edge_label_num;
  added_edges.resize(csr_num);
  updated_edge_data.resize(csr_num);

  grape::OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    uint8_t op_type;
    arc >> op_type;
    if (op_type == 0) {
      label_t label;
      oid_t oid;

      arc >> label >> oid;
      vid_t vid;
      if (!graph.get_lid(label, oid, vid)) {
        vid = graph.add_vertex(label, oid);
      }
      graph.get_vertex_table(label).ingest(vid, arc);
    } else if (op_type == 1) {
      label_t src_label, dst_label, edge_label;
      oid_t src, dst;
      vid_t src_vid, dst_vid;

      arc >> src_label >> src >> dst_label >> dst >> edge_label;
      CHECK(graph.get_lid(src_label, src, src_vid));
      CHECK(graph.get_lid(dst_label, dst, dst_vid));
      graph.IngestEdge(src_label, src_vid, dst_label, dst_vid, edge_label,
                       timestamp, arc, alloc);
    } else if (op_type == 2) {
      label_t label;
      oid_t oid;
      int col_id;

      arc >> label >> oid >> col_id;
      vid_t vid;
      CHECK(graph.get_lid(label, oid, vid));
      graph.get_vertex_table(label).get_column_by_id(col_id)->ingest(vid, arc);
    } else if (op_type == 3) {
      uint8_t dir;
      label_t label, neighbor_label, edge_label;
      oid_t v, nbr;
      vid_t v_lid, nbr_lid;

      arc >> dir >> label >> v >> neighbor_label >> nbr >> edge_label;
      CHECK(graph.get_lid(label, v, v_lid));
      CHECK(graph.get_lid(neighbor_label, nbr, nbr_lid));

      std::shared_ptr<GenericNbrIteratorMut<vid_t>> edge_iter(nullptr);
      if (dir == 0) {
        edge_iter = graph.get_incoming_edges_mut(label, v_lid, neighbor_label,
                                                 edge_label, timestamp);
      } else {
        CHECK_EQ(dir, 1);
        edge_iter = graph.get_outgoing_edges_mut(label, v_lid, neighbor_label,
                                                 edge_label, timestamp);
      }
      Property value;
      value.set_type(graph.schema().get_edge_property(
          dir == 0 ? neighbor_label : label, dir == 0 ? label : neighbor_label,
          label));
      while (edge_iter->IsValid()) {
        if (edge_iter->GetNeighbor() == nbr_lid) {
          deserialize_field(arc, value);
          edge_iter->SetData(value);
        }
        edge_iter->Next();
      }
    } else {
      LOG(FATAL) << "unexpected op_type";
    }
  }
}

size_t UpdateTransaction::get_in_csr_index(label_t src_label, label_t dst_label,
                                           label_t edge_label) const {
  return src_label * vertex_label_num_ * edge_label_num_ +
         dst_label * edge_label_num_ + edge_label;
}

size_t UpdateTransaction::get_out_csr_index(label_t src_label,
                                            label_t dst_label,
                                            label_t edge_label) const {
  return src_label * vertex_label_num_ * edge_label_num_ +
         dst_label * edge_label_num_ + edge_label +
         vertex_label_num_ * vertex_label_num_ * edge_label_num_;
}

bool UpdateTransaction::oid_to_lid(label_t label, oid_t oid, vid_t& lid) const {
  if (graph_.get_lid(label, oid, lid)) {
    return true;
  } else {
    if (added_vertices_[label].get_index(oid, lid)) {
      lid += added_vertices_base_[label];
      return true;
    }
  }
  return false;
}

oid_t UpdateTransaction::lid_to_oid(label_t label, vid_t lid) const {
  if (graph_.vertex_num(label) > lid) {
    return graph_.get_oid(label, lid);
  } else {
    oid_t ret;
    CHECK(
        added_vertices_[label].get_key(lid - added_vertices_base_[label], ret));
    return ret;
  }
}

void UpdateTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = std::numeric_limits<timestamp_t>::max();

    op_num_ = 0;

    added_vertices_.clear();
    added_vertices_base_.clear();
    vertex_offsets_.clear();
    extra_vertex_properties_.clear();
    added_edges_.clear();
    updated_edge_data_.clear();
  }
}

void UpdateTransaction::applyVerticesUpdates() {
  for (label_t label = 0; label < vertex_label_num_; ++label) {
    std::vector<std::pair<vid_t, oid_t>> added_vertices;
    vid_t added_vertices_num = added_vertices_[label].size();
    for (vid_t v = 0; v < added_vertices_num; ++v) {
      vid_t lid = v + added_vertices_base_[label];
      oid_t oid;
      CHECK(added_vertices_[label].get_key(v, oid));
      added_vertices.emplace_back(lid, oid);
    }
    std::sort(added_vertices.begin(), added_vertices.end(),
              [](const std::pair<vid_t, oid_t>& lhs,
                 const std::pair<vid_t, oid_t>& rhs) {
                return lhs.first < rhs.first;
              });

    auto& table = extra_vertex_properties_[label];
    auto& vertex_offset = vertex_offsets_[label];
    for (auto& pair : added_vertices) {
      vid_t offset = vertex_offset.at(pair.first);
      vid_t lid = graph_.add_vertex(label, pair.second);
      graph_.get_vertex_table(label).insert(lid, table.get_row(offset));
      CHECK_EQ(lid, pair.first);
      vertex_offset.erase(pair.first);
    }

    for (auto& pair : vertex_offset) {
      vid_t lid = pair.first;
      vid_t offset = pair.second;
      graph_.get_vertex_table(label).insert(lid, table.get_row(offset));
    }

    CHECK_EQ(graph_.vertex_num(label), vertex_nums_[label]);
  }

  added_vertices_.clear();
  vertex_nums_.clear();
  vertex_offsets_.clear();
  extra_vertex_properties_.clear();
}

void UpdateTransaction::applyEdgesUpdates() {
  for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
    for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
      for (label_t edge_label = 0; edge_label < edge_label_num_; ++edge_label) {
        size_t oe_csr_index =
            get_out_csr_index(src_label, dst_label, edge_label);
        for (auto& pair : updated_edge_data_[oe_csr_index]) {
          auto& updates = pair.second;
          if (updates.empty()) {
            continue;
          }
          std::shared_ptr<GenericNbrIteratorMut<vid_t>> edge_iter =
              graph_.get_outgoing_edges_mut(src_label, pair.first, dst_label,
                                            edge_label, timestamp_);
          while (edge_iter->IsValid()) {
            auto iter = updates.find(edge_iter->GetNeighbor());
            if (iter != updates.end()) {
              edge_iter->SetData(iter->second);
            }
            edge_iter->Next();
          }
        }

        for (auto& pair : added_edges_[oe_csr_index]) {
          vid_t v = pair.first;
          auto& add_list = pair.second;
          if (add_list.empty()) {
            continue;
          }
          auto& edge_data = updated_edge_data_[oe_csr_index].at(v);
          for (auto u : add_list) {
            auto value = edge_data.at(u);
            graph_.PutEdge(src_label, v, dst_label, u, edge_label, timestamp_,
                           value, alloc_);
          }
        }
      }
    }
  }

  for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
    for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
      for (label_t edge_label = 0; edge_label < edge_label_num_; ++edge_label) {
        size_t ie_csr_index =
            get_in_csr_index(src_label, dst_label, edge_label);
        for (auto& pair : updated_edge_data_[ie_csr_index]) {
          auto& updates = pair.second;
          if (updates.empty()) {
            continue;
          }
          std::shared_ptr<GenericNbrIteratorMut<vid_t>> edge_iter =
              graph_.get_incoming_edges_mut(dst_label, pair.first, src_label,
                                            edge_label, timestamp_);
          while (edge_iter->IsValid()) {
            auto iter = updates.find(edge_iter->GetNeighbor());
            if (iter != updates.end()) {
              edge_iter->SetData(iter->second);
            }
            edge_iter->Next();
          }
        }
      }
    }
  }
  added_edges_.clear();
  updated_edge_data_.clear();
}

}  // namespace gs
