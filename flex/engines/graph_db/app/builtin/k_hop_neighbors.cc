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
#include "flex/engines/graph_db/app/builtin/k_hop_neighbors.h"

namespace gs {

results::CollectiveResults KNeighbors::Query(const GraphDBSession& sess,
                                             std::string label_name,
                                             int64_t vertex_id, int32_t k) {
  auto txn = sess.GetReadTransaction();
  const Schema& schema_ = txn.schema();

  if (k <= 0) {
    LOG(ERROR) << "k must be greater than 0.";
    return {};
  }
  if (!schema_.has_vertex_label(label_name)) {
    // output.put_string_view("The requested label doesn't exits.");
    LOG(ERROR) << "The requested label doesn't exits.";
    return {};
  }
  label_t vertex_label_ = schema_.get_vertex_label_id(label_name);
  struct pair_hash {
    std::size_t operator()(const std::pair<label_t, vid_t>& p) const {
      auto hash1 = std::hash<label_t>{}(p.first);
      auto hash2 = std::hash<vid_t>{}(p.second);
      return hash1 ^ (hash2 << 1);
    }
  };
  std::unordered_set<std::pair<label_t, vid_t>, pair_hash> k_neighbors;

  int vertex_size_ = (int) schema_.vertex_label_num();
  int edge_size_ = (int) schema_.edge_label_num();

  std::vector<vid_t> nei_index_;
  std::vector<label_t> nei_label_;
  std::vector<vid_t> next_nei_indexs_;
  std::vector<label_t> next_nei_label_;

  nei_label_.push_back(vertex_label_);
  vid_t vertex_index{};
  if (!txn.GetVertexIndex(vertex_label_, vertex_id, vertex_index)) {
    LOG(ERROR) << "Vertex not found.";
    return {};
  }
  nei_index_.push_back(vertex_index);
  // get k hop neighbors
  while (!nei_index_.empty() && k > 0) {
    for (long unsigned int i = 0; i < nei_index_.size(); i++) {
      for (int j = 0; j < vertex_size_; j++) {
        for (int k = 0; k < edge_size_; k++) {
          if (schema_.has_edge_label(label_t(nei_label_[i]), label_t(j),
                                     label_t(k))) {
            auto outedges = txn.GetOutEdgeIterator(
                nei_label_[i], nei_index_[i], j,
                k);  // 1.self_label 2.self_index 3.edge_label 4.nei_label
            while (outedges.IsValid()) {
              auto neighbor = outedges.GetNeighbor();
              if (k_neighbors.find(std::make_pair(j, neighbor)) ==
                  k_neighbors.end()) {
                next_nei_label_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                k_neighbors.insert(std::make_pair(j, neighbor));
              }
              outedges.Next();
            }
          }
          if (schema_.has_edge_label(label_t(j), label_t(nei_label_[i]),
                                     label_t(k))) {
            auto inedges = txn.GetInEdgeIterator(
                nei_label_[i], nei_index_[i], j,
                k);  // 1.self_label 2.self_index 3.edge_label 4.nei_label
            while (inedges.IsValid()) {
              auto neighbor = inedges.GetNeighbor();
              if (k_neighbors.find(std::make_pair(j, neighbor)) ==
                  k_neighbors.end()) {
                next_nei_label_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                k_neighbors.insert(std::make_pair(j, neighbor));
              }
              inedges.Next();
            }
          }
        }
      }
    }
    nei_index_ = next_nei_indexs_;
    nei_label_ = next_nei_label_;
    next_nei_label_.clear();
    next_nei_indexs_.clear();
    k--;
  }
  results::CollectiveResults results;
  for (auto vertex_ : k_neighbors) {
    std::string label_name_ = schema_.get_vertex_label_name(vertex_.first);
    auto result = results.add_results();
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_str(label_name_);
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_i64(txn.GetVertexId(vertex_.first, vertex_.second).AsInt64());
  }

  txn.Commit();
  return results;
}

AppWrapper KNeighborsFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new KNeighbors(), NULL);
}
}  // namespace gs
