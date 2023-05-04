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

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

ReadTransaction::ReadTransaction(const MutablePropertyFragment& graph,
                                 VersionManager& vm, timestamp_t timestamp)
    : graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

void ReadTransaction::Commit() { release(); }

void ReadTransaction::Abort() { release(); }

ReadTransaction::vertex_iterator::vertex_iterator(
    label_t label, vid_t cur, vid_t num, const MutablePropertyFragment& graph)
    : label_(label), cur_(cur), num_(num), graph_(graph) {}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() { ++cur_; }
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

oid_t ReadTransaction::vertex_iterator::GetId() const {
  return graph_.get_oid(label_, cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

Property ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}

int ReadTransaction::vertex_iterator::FieldNum() const {
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<GenericNbrIterator<vid_t>> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

Property ReadTransaction::edge_iterator::GetData() const {
  return iter_->GetGenericData();
}

bool ReadTransaction::edge_iterator::IsValid() const {
  return iter_->IsValid();
}

void ReadTransaction::edge_iterator::Next() { iter_->Next(); }

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  return iter_->GetNeighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {label, 0, graph_.vertex_num(label), graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(label_t label,
                                                             oid_t id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, lid, graph_.vertex_num(label), graph_};
  } else {
    return {label, graph_.vertex_num(label), graph_.vertex_num(label), graph_};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, oid_t id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label);
}

oid_t ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index);
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighnor_label, edge_label, timestamp_)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighnor_label, edge_label, timestamp_)};
}

const Schema& ReadTransaction::schema() const { return graph_.schema(); }

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

}  // namespace gs
