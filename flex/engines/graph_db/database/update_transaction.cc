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

#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/file_names.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

UpdateTransaction::UpdateTransaction(MutablePropertyFragment& graph,
                                     Allocator& alloc,
                                     const std::string& work_dir,
                                     WalWriter& logger, VersionManager& vm,
                                     timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));

  vertex_label_num_ = graph_.schema().vertex_label_num();
  edge_label_num_ = graph_.schema().edge_label_num();
  for (label_t idx = 0; idx < vertex_label_num_; ++idx) {
    if (graph_.lf_indexers_[idx].get_type() == PropertyType::kInt64) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<int64_t, vid_t>>());
    } else if (graph_.lf_indexers_[idx].get_type() == PropertyType::kUInt64) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<uint64_t, vid_t>>());
    } else if (graph_.lf_indexers_[idx].get_type() == PropertyType::kInt32) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<int32_t, vid_t>>());
    } else if (graph_.lf_indexers_[idx].get_type() == PropertyType::kUInt32) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<uint32_t, vid_t>>());
    } else if (graph_.lf_indexers_[idx].get_type() == PropertyType::kString) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<std::string_view, vid_t>>());
    } else {
      LOG(FATAL) << "Only int64 and string_view types for pk are supported..";
    }
  }

  added_vertices_base_.resize(vertex_label_num_);
  vertex_nums_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    added_vertices_base_[i] = vertex_nums_[i] = graph_.vertex_num(i);
  }
  vertex_offsets_.resize(vertex_label_num_);
  extra_vertex_properties_.resize(vertex_label_num_);
  std::string txn_work_dir = update_txn_dir(work_dir, 0);
  if (std::filesystem::exists(txn_work_dir)) {
    std::filesystem::remove_all(txn_work_dir);
  }
  std::filesystem::create_directories(txn_work_dir);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    const Table& table = graph_.get_vertex_table(i);
    std::string v_label = graph_.schema().get_vertex_label_name(i);
    std::string table_prefix = vertex_table_prefix(v_label);
    extra_vertex_properties_[i].init(table_prefix, txn_work_dir,
                                     table.column_names(), table.column_types(),
                                     {});
    extra_vertex_properties_[i].resize(4096);
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

bool UpdateTransaction::AddVertex(label_t label, const Any& oid,
                                  const std::vector<Any>& props) {
  vid_t id;
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return false;
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type != types[col_i]) {
      if (types[col_i] == PropertyType::kStringMap &&
          props[col_i].type == PropertyType::kString) {
        continue;
      }
      return false;
    }
  }
  if (!oid_to_lid(label, oid, id)) {
    added_vertices_[label]->_add(oid);
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
  arc_ << static_cast<uint8_t>(0) << label;
  serialize_field(arc_, oid);
  arc_.AddBytes(arc.GetBuffer(), arc.GetSize());
  return true;
}

static size_t get_offset(const std::shared_ptr<CsrConstEdgeIterBase>& base,
                         vid_t target) {
  size_t offset = 0;
  while (base != nullptr && base->is_valid()) {
    if (base->get_neighbor() == target) {
      return offset;
    }
    offset++;
    base->next();
  }
  return std::numeric_limits<size_t>::max();
}

bool UpdateTransaction::AddEdge(label_t src_label, const Any& src,
                                label_t dst_label, const Any& dst,
                                label_t edge_label, const Any& value) {
  vid_t src_lid, dst_lid;
  static constexpr size_t sentinel = std::numeric_limits<size_t>::max();
  size_t offset_out = sentinel, offset_in = sentinel;
  if (graph_.get_lid(src_label, src, src_lid) &&
      graph_.get_lid(dst_label, dst, dst_lid)) {
    const auto& oe =
        graph_.get_outgoing_edges(src_label, src_lid, dst_label, edge_label);
    offset_out = get_offset(oe, dst_lid);
    const auto& ie =
        graph_.get_incoming_edges(dst_label, dst_lid, src_label, edge_label);
    offset_in = get_offset(ie, src_lid);
  } else {
    if (!oid_to_lid(src_label, src, src_lid) ||
        !oid_to_lid(dst_label, dst, dst_lid)) {
      return false;
    }
  }
  PropertyType type =
      graph_.schema().get_edge_property(src_label, dst_label, edge_label);
  if (type != value.type) {
    return false;
  }

  size_t in_csr_index = get_in_csr_index(src_label, dst_label, edge_label);
  size_t out_csr_index = get_out_csr_index(src_label, dst_label, edge_label);
  if (offset_in == sentinel) {
    added_edges_[in_csr_index][dst_lid].push_back(src_lid);
  }
  updated_edge_data_[in_csr_index][dst_lid].emplace(
      src_lid, std::pair<Any, size_t>{value, offset_in});
  if (offset_out == sentinel) {
    added_edges_[out_csr_index][src_lid].push_back(dst_lid);
  }
  updated_edge_data_[out_csr_index][src_lid].emplace(
      dst_lid, std::pair<Any, size_t>{value, offset_out});

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(1) << src_label;
  serialize_field(arc_, src);
  arc_ << dst_label;
  serialize_field(arc_, dst);
  arc_ << edge_label;
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

Any UpdateTransaction::vertex_iterator::GetId() const {
  return txn_->lid_to_oid(label_, cur_);
}

vid_t UpdateTransaction::vertex_iterator::GetIndex() const { return cur_; }

Any UpdateTransaction::vertex_iterator::GetField(int col_id) const {
  return txn_->GetVertexField(label_, cur_, col_id);
}

bool UpdateTransaction::vertex_iterator::SetField(int col_id,
                                                  const Any& value) {
  return txn_->SetVertexField(label_, cur_, col_id, value);
}

UpdateTransaction::edge_iterator::edge_iterator(
    bool dir, label_t label, vid_t v, label_t neighbor_label,
    label_t edge_label, const vid_t* aeb, const vid_t* aee,
    std::shared_ptr<CsrConstEdgeIterBase> init_iter, UpdateTransaction* txn)
    : dir_(dir),
      label_(label),
      v_(v),
      neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      added_edges_cur_(aeb),
      added_edges_end_(aee),
      init_iter_(std::move(init_iter)),
      txn_(txn),
      offset_(0) {}
UpdateTransaction::edge_iterator::~edge_iterator() = default;

Any UpdateTransaction::edge_iterator::GetData() const {
  if (init_iter_->is_valid()) {
    vid_t cur = init_iter_->get_neighbor();
    Any ret;
    if (txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                 edge_label_, ret)) {
      return ret;
    } else {
      return init_iter_->get_data();
    }
  } else {
    vid_t cur = *added_edges_cur_;
    Any ret;
    CHECK(txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                   edge_label_, ret));
    return ret;
  }
}

void UpdateTransaction::edge_iterator::SetData(const Any& value) {
  if (init_iter_->is_valid()) {
    vid_t cur = init_iter_->get_neighbor();
    txn_->set_edge_data_with_offset(dir_, label_, v_, neighbor_label_, cur,
                                    edge_label_, value, offset_);
  } else {
    vid_t cur = *added_edges_cur_;
    txn_->set_edge_data_with_offset(dir_, label_, v_, neighbor_label_, cur,
                                    edge_label_, value,
                                    std::numeric_limits<size_t>::max());
  }
}

bool UpdateTransaction::edge_iterator::IsValid() const {
  return init_iter_->is_valid() || (added_edges_cur_ != added_edges_end_);
}

void UpdateTransaction::edge_iterator::Next() {
  if (init_iter_->is_valid()) {
    init_iter_->next();
    if (init_iter_->is_valid()) {
      ++offset_;
    } else {
      offset_ = std::numeric_limits<size_t>::max();
    }
  } else {
    offset_ = std::numeric_limits<size_t>::max();
    ++added_edges_cur_;
  }
}

void UpdateTransaction::edge_iterator::Forward(size_t offset) {
  if (offset < init_iter_->size()) {
    *init_iter_ += offset;
    offset_ += offset;
  } else {
    size_t len = init_iter_->size();
    *init_iter_ += offset;
    offset -= len;
    added_edges_cur_ += offset;
    offset_ = std::numeric_limits<size_t>::max();
  }
}

vid_t UpdateTransaction::edge_iterator::GetNeighbor() const {
  if (init_iter_->is_valid()) {
    return init_iter_->get_neighbor();
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
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  size_t csr_index = get_out_csr_index(label, neighbor_label, edge_label);
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
          neighbor_label,
          edge_label,
          begin,
          end,
          graph_.get_outgoing_edges(label, u, neighbor_label, edge_label),
          this};
}

UpdateTransaction::edge_iterator UpdateTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  size_t csr_index = get_in_csr_index(label, neighbor_label, edge_label);
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
          neighbor_label,
          edge_label,
          begin,
          end,
          graph_.get_incoming_edges(label, u, neighbor_label, edge_label),
          this};
}

Any UpdateTransaction::GetVertexField(label_t label, vid_t lid,
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
                                       const Any& value) {
  auto& vertex_offset = vertex_offsets_[label];
  auto iter = vertex_offset.find(lid);
  auto& extra_table = extra_vertex_properties_[label];
  const std::vector<PropertyType>& types =
      graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id] != value.type) {
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
      extra_table.get_column_by_id(i)->set_any(
          new_offset, table.get_column_by_id(i)->get(lid));
    }
    extra_table.get_column_by_id(col_id)->set_any(new_offset, value);
  } else {
    if (extra_table.col_num() <= static_cast<size_t>(col_id)) {
      return false;
    }
    extra_table.get_column_by_id(col_id)->set_any(iter->second, value);
  }

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(2) << label;
  serialize_field(arc_, lid_to_oid(label, lid));
  arc_ << col_id;
  serialize_field(arc_, value);
  return true;
}

void UpdateTransaction::SetEdgeData(bool dir, label_t label, vid_t v,
                                    label_t neighbor_label, vid_t nbr,
                                    label_t edge_label, const Any& value) {
  const auto& edges =
      dir ? graph_.get_outgoing_edges(label, v, neighbor_label, edge_label)
          : graph_.get_incoming_edges(label, v, neighbor_label, edge_label);
  size_t offset = get_offset(edges, nbr);
  set_edge_data_with_offset(dir, label, v, neighbor_label, nbr, edge_label,
                            value, offset);
}

void UpdateTransaction::set_edge_data_with_offset(
    bool dir, label_t label, vid_t v, label_t neighbor_label, vid_t nbr,
    label_t edge_label, const Any& value, size_t offset) {
  size_t csr_index = dir ? get_out_csr_index(label, neighbor_label, edge_label)
                         : get_in_csr_index(neighbor_label, label, edge_label);
  if (value.type == PropertyType::kString) {
    size_t loc = sv_vec_.size();
    sv_vec_.emplace_back(std::string(value.value.s));
    Any dup_value;
    dup_value.set_string(sv_vec_[loc]);
    updated_edge_data_[csr_index][v].emplace(
        nbr, std::pair<Any, size_t>{dup_value, offset});
  } else {
    updated_edge_data_[csr_index][v].emplace(
        nbr, std::pair<Any, size_t>{value, offset});
  }

  op_num_ += 1;
  arc_ << static_cast<uint8_t>(3) << static_cast<uint8_t>(dir ? 1 : 0) << label;
  serialize_field(arc_, lid_to_oid(label, v));
  arc_ << neighbor_label;
  serialize_field(arc_, lid_to_oid(neighbor_label, nbr));
  arc_ << edge_label;
  serialize_field(arc_, value);
}

bool UpdateTransaction::GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                                           label_t neighbor_label, vid_t nbr,
                                           label_t edge_label, Any& ret) const {
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
      ret = iter->second.first;
      return true;
    }
  }
}

void UpdateTransaction::IngestWal(MutablePropertyFragment& graph,
                                  const std::string& work_dir,
                                  uint32_t timestamp, char* data, size_t length,
                                  Allocator& alloc) {
  std::vector<std::shared_ptr<IdIndexerBase<vid_t>>> added_vertices;
  std::vector<vid_t> added_vertices_base;
  std::vector<vid_t> vertex_nums;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> vertex_offsets;
  std::vector<Table> extra_vertex_properties;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges;
  std::vector<ska::flat_hash_map<vid_t, ska::flat_hash_map<vid_t, Any>>>
      updated_edge_data;

  size_t vertex_label_num = graph.schema().vertex_label_num();
  size_t edge_label_num = graph.schema().edge_label_num();

  for (label_t idx = 0; idx < vertex_label_num; ++idx) {
    if (graph.lf_indexers_[idx].get_type() == PropertyType::kInt64) {
      added_vertices.emplace_back(
          std::make_shared<IdIndexer<int64_t, vid_t>>());
    } else if (graph.lf_indexers_[idx].get_type() == PropertyType::kUInt64) {
      added_vertices.emplace_back(
          std::make_shared<IdIndexer<uint64_t, vid_t>>());
    } else if (graph.lf_indexers_[idx].get_type() == PropertyType::kInt32) {
      added_vertices.emplace_back(
          std::make_shared<IdIndexer<int32_t, vid_t>>());
    } else if (graph.lf_indexers_[idx].get_type() == PropertyType::kUInt32) {
      added_vertices.emplace_back(
          std::make_shared<IdIndexer<uint32_t, vid_t>>());
    } else if (graph.lf_indexers_[idx].get_type() == PropertyType::kString) {
      added_vertices.emplace_back(
          std::make_shared<IdIndexer<std::string_view, vid_t>>());
    } else {
      LOG(FATAL) << "Only int64, uint64, int32, uint32 and string_view types "
                    "for pk are supported..";
    }
  }

  added_vertices_base.resize(vertex_label_num);
  vertex_nums.resize(vertex_label_num);
  for (size_t i = 0; i < vertex_label_num; ++i) {
    added_vertices_base[i] = vertex_nums[i] = graph.vertex_num(i);
  }
  vertex_offsets.resize(vertex_label_num);
  extra_vertex_properties.resize(vertex_label_num);
  std::string txn_work_dir = update_txn_dir(work_dir, timestamp);
  std::filesystem::create_directories(txn_work_dir);
  for (size_t i = 0; i < vertex_label_num; ++i) {
    const Table& table = graph.get_vertex_table(i);
    std::string v_label = graph.schema().get_vertex_label_name(i);
    std::string table_prefix = vertex_table_prefix(v_label);
    extra_vertex_properties[i].init(
        table_prefix, work_dir, table.column_names(), table.column_types(), {});
    extra_vertex_properties[i].resize(4096);
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
      Any oid;
      label = deserialize_oid(graph, arc, oid);
      vid_t vid;
      if (!graph.get_lid(label, oid, vid)) {
        vid = graph.add_vertex(label, oid);
      }
      graph.get_vertex_table(label).ingest(vid, arc);
    } else if (op_type == 1) {
      label_t src_label, dst_label, edge_label;
      Any src, dst;
      vid_t src_vid, dst_vid;
      src_label = deserialize_oid(graph, arc, src);
      dst_label = deserialize_oid(graph, arc, dst);
      arc >> edge_label;
      CHECK(graph.get_lid(src_label, src, src_vid));
      CHECK(graph.get_lid(dst_label, dst, dst_vid));
      graph.IngestEdge(src_label, src_vid, dst_label, dst_vid, edge_label,
                       timestamp, arc, alloc);
    } else if (op_type == 2) {
      label_t label;
      Any oid;
      int col_id;
      label = deserialize_oid(graph, arc, oid);
      arc >> col_id;
      vid_t vid;
      CHECK(graph.get_lid(label, oid, vid));
      graph.get_vertex_table(label).get_column_by_id(col_id)->ingest(vid, arc);
    } else if (op_type == 3) {
      uint8_t dir;
      label_t label, neighbor_label, edge_label;
      Any v, nbr;
      vid_t v_lid, nbr_lid;
      arc >> dir;
      label = deserialize_oid(graph, arc, v);
      neighbor_label = deserialize_oid(graph, arc, nbr);
      arc >> edge_label;
      CHECK(graph.get_lid(label, v, v_lid));
      CHECK(graph.get_lid(neighbor_label, nbr, nbr_lid));

      std::shared_ptr<CsrEdgeIterBase> edge_iter(nullptr);
      if (dir == 0) {
        edge_iter = graph.get_incoming_edges_mut(label, v_lid, neighbor_label,
                                                 edge_label);
      } else {
        CHECK_EQ(dir, 1);
        edge_iter = graph.get_outgoing_edges_mut(label, v_lid, neighbor_label,
                                                 edge_label);
      }
      Any value;
      value.type = graph.schema().get_edge_property(
          dir == 0 ? neighbor_label : label, dir == 0 ? label : neighbor_label,
          label);
      while (edge_iter->is_valid()) {
        if (edge_iter->get_neighbor() == nbr_lid) {
          deserialize_field(arc, value);
          edge_iter->set_data(value, timestamp);
        }
        edge_iter->next();
      }
    } else {
      LOG(FATAL) << "unexpected op_type " << static_cast<int>(op_type) << "..";
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

bool UpdateTransaction::oid_to_lid(label_t label, const Any& oid,
                                   vid_t& lid) const {
  if (graph_.get_lid(label, oid, lid)) {
    return true;
  } else {
    if (added_vertices_[label]->get_index(oid, lid)) {
      lid += added_vertices_base_[label];
      return true;
    }
  }
  return false;
}

Any UpdateTransaction::lid_to_oid(label_t label, vid_t lid) const {
  if (graph_.vertex_num(label) > lid) {
    return graph_.get_oid(label, lid);
  } else {
    Any ret;
    CHECK(added_vertices_[label]->get_key(lid - added_vertices_base_[label],
                                          ret));
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

void UpdateTransaction::batch_commit(UpdateBatch& batch) {
  if (timestamp_ == std::numeric_limits<timestamp_t>::max()) {
    return;
  }
  const auto& updateVertices = batch.GetUpdateVertices();
  for (auto& [label, oid, props] : updateVertices) {
    vid_t lid;

    if (graph_.get_lid(label, oid, lid)) {
      graph_.get_vertex_table(label).insert(lid, props);
    } else {
      lid = graph_.add_vertex(label, oid);
      graph_.get_vertex_table(label).insert(lid, props);
    }
  }
  const auto& updateEdges = batch.GetUpdateEdges();

  for (auto& [src_label, src, dst_label, dst, edge_label, prop] : updateEdges) {
    vid_t src_lid, dst_lid;
    bool src_flag = graph_.get_lid(src_label, src, src_lid);
    bool dst_flag = graph_.get_lid(dst_label, dst, dst_lid);

    if (src_flag && dst_flag) {
      graph_.UpdateEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                        timestamp_, prop, alloc_);
    }
  }
  auto& arc = batch.GetArc();
  auto* header = reinterpret_cast<WalHeader*>(arc.GetBuffer());
  if (arc.GetSize() != sizeof(WalHeader)) {
    header->length = arc.GetSize() - sizeof(WalHeader);
    header->type = 1;
    header->timestamp = timestamp_;
    logger_.append(arc.GetBuffer(), arc.GetSize());
  }

  release();
}

void UpdateTransaction::applyVerticesUpdates() {
  for (label_t label = 0; label < vertex_label_num_; ++label) {
    std::vector<std::pair<vid_t, Any>> added_vertices;
    vid_t added_vertices_num = added_vertices_[label]->size();
    for (vid_t v = 0; v < added_vertices_num; ++v) {
      vid_t lid = v + added_vertices_base_[label];
      Any oid;
      CHECK(added_vertices_[label]->get_key(v, oid));
      added_vertices.emplace_back(lid, oid);
    }
    std::sort(
        added_vertices.begin(), added_vertices.end(),
        [](const std::pair<vid_t, Any>& lhs, const std::pair<vid_t, Any>& rhs) {
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

          std::shared_ptr<CsrEdgeIterBase> edge_iter =
              graph_.get_outgoing_edges_mut(src_label, pair.first, dst_label,
                                            edge_label);
          for (auto& edge : updates) {
            if (edge.second.second != std::numeric_limits<size_t>::max()) {
              auto& iter = *edge_iter;
              iter += edge.second.second;
              if (iter.is_valid() && iter.get_neighbor() == edge.first) {
                iter.set_data(edge.second.first, timestamp_);
              } else if (iter.is_valid() && iter.get_neighbor() != edge.first) {
                LOG(FATAL) << "Inconsistent neighbor id:" << iter.get_neighbor()
                           << " " << edge.first << "\n";
              } else {
                LOG(FATAL) << "Illegal offset: " << edge.first << " "
                           << edge.second.second << "\n";
              }
            }
          }
        }

        for (auto& pair : added_edges_[oe_csr_index]) {
          vid_t v = pair.first;
          auto& add_list = pair.second;

          if (add_list.empty()) {
            continue;
          }
          std::sort(add_list.begin(), add_list.end());
          auto& edge_data = updated_edge_data_[oe_csr_index].at(v);
          for (size_t idx = 0; idx < add_list.size(); ++idx) {
            if (idx && add_list[idx] == add_list[idx - 1])
              continue;
            auto u = add_list[idx];
            auto value = edge_data.at(u).first;
            grape::InArchive iarc;
            serialize_field(iarc, value);
            grape::OutArchive oarc(std::move(iarc));
            graph_.IngestEdge(src_label, v, dst_label, u, edge_label,
                              timestamp_, oarc, alloc_);
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
          std::shared_ptr<CsrEdgeIterBase> edge_iter =
              graph_.get_incoming_edges_mut(dst_label, pair.first, src_label,
                                            edge_label);
          for (auto& edge : updates) {
            if (edge.second.second != std::numeric_limits<size_t>::max()) {
              auto& iter = *edge_iter;
              iter += edge.second.second;
              if (iter.is_valid() && iter.get_neighbor() == edge.first) {
                iter.set_data(edge.second.first, timestamp_);
              } else if (iter.is_valid() && iter.get_neighbor() != edge.first) {
                LOG(FATAL) << "Inconsistent neighbor id:" << iter.get_neighbor()
                           << " " << edge.first << "\n";
              } else {
                LOG(FATAL) << "Illegal offset: " << edge.first << " "
                           << edge.second.second << "\n";
              }
            }
          }
        }
      }
    }
  }
  added_edges_.clear();
  updated_edge_data_.clear();
}

}  // namespace gs
