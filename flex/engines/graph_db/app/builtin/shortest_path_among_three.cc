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
#include "flex/engines/graph_db/app/builtin/shortest_path_among_three.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/engines/graph_db/runtime/common/types.h"

namespace gs {

void sink_shortest_path(const ReadTransaction& tx,
                        results::CollectiveResults& results,
                        const std::vector<std::pair<label_t, vid_t>>& nodes,
                        const std::vector<label_t>& edge_labels) {
  for (size_t i = 0; i < nodes.size(); ++i) {
    LOG(INFO) << "sink_shortest_path: " << static_cast<int>(nodes[i].first)
              << " " << nodes[i].second;
  }
  for (size_t i = 0; i < edge_labels.size(); ++i) {
    LOG(INFO) << "sink_shortest_path: " << static_cast<int>(edge_labels[i]);
  }
  Schema schema_ = tx.schema();
  std::string result_path;
  auto path = results.add_results()
                  ->mutable_record()
                  ->add_columns()
                  ->mutable_entry()
                  ->mutable_element()
                  ->mutable_graph_path();
  CHECK(nodes.size() == edge_labels.size() + 1);
  for (size_t i = 0; i < nodes.size(); ++i) {
    auto vertex_in_path = path->add_path();
    auto node = vertex_in_path->mutable_vertex();
    node->mutable_label()->set_id(nodes[i].first);
    node->set_id(
        runtime::encode_unique_vertex_id(nodes[i].first, nodes[i].second));
    if (i < edge_labels.size()) {
      auto edge_in_path = path->add_path();
      auto edge = edge_in_path->mutable_edge();
      edge->mutable_src_label()->set_id(nodes[i].first);
      edge->mutable_dst_label()->set_id(nodes[i + 1].first);
      edge->mutable_label()->set_id(edge_labels[i]);
      edge->set_id(runtime::encode_unique_edge_id(
          edge_labels[i], nodes[i].second, nodes[i + 1].second));
      edge->set_src_id(
          runtime::encode_unique_vertex_id(nodes[i].first, nodes[i].second));
      edge->set_dst_id(runtime::encode_unique_vertex_id(nodes[i + 1].first,
                                                        nodes[i + 1].second));
    }
  }
}

results::CollectiveResults ShortestPathAmongThree::Query(
    const GraphDBSession& sess, std::string label_name1, std::string oid1_str,
    std::string label_name2, std::string oid2_str, std::string label_name3,
    std::string oid3_str) {
  ReadTransaction txn = sess.GetReadTransaction();
  const Schema& schema_ = txn.schema();

  if (!schema_.has_vertex_label(label_name1) ||
      !schema_.has_vertex_label(label_name2) ||
      !schema_.has_vertex_label(label_name3)) {
    LOG(ERROR) << "The requested label doesn't exits.";
    return {};
  }
  label_t label_v1 = schema_.get_vertex_label_id(label_name1);
  auto oid1 = ConvertStringToAny(
      oid1_str, std::get<0>(schema_.get_vertex_primary_key(label_v1)[0]));
  if (oid1.type == PropertyType::Empty()) {
    LOG(ERROR) << "Invalid oid1.";
    return {};
  }
  label_t label_v2 = schema_.get_vertex_label_id(label_name2);
  auto oid2 = ConvertStringToAny(
      oid2_str, std::get<0>(schema_.get_vertex_primary_key(label_v2)[0]));
  if (oid2.type == PropertyType::Empty()) {
    LOG(ERROR) << "Invalid oid2.";
    return {};
  }
  label_t label_v3 = schema_.get_vertex_label_id(label_name3);
  auto oid3 = ConvertStringToAny(
      oid3_str, std::get<0>(schema_.get_vertex_primary_key(label_v3)[0]));
  if (oid3.type == PropertyType::Empty()) {
    LOG(ERROR) << "Invalid oid3.";
    return {};
  }
  vid_t index_v1{};
  vid_t index_v2{};
  vid_t index_v3{};
  if (!txn.GetVertexIndex(label_v1, oid1, index_v1) ||
      !txn.GetVertexIndex(label_v2, oid2, index_v2) ||
      !txn.GetVertexIndex(label_v3, oid3, index_v3)) {
    LOG(ERROR) << "Vertex not found.";
    return {};
  }
  // get the three shortest paths
  std::vector<std::pair<label_t, vid_t>> v1v2result_, v2v3result_, v1v3result_;
  std::vector<label_t> v1v2edge_labels_, v2v3edge_labels_, v1v3edge_labels_;

  bool find_flag = true;
  if (!ShortestPath(txn, label_v1, index_v1, label_v2, index_v2, v1v2result_,
                    v1v2edge_labels_)) {
    find_flag = false;
  }
  if (find_flag && !ShortestPath(txn, label_v2, index_v2, label_v3, index_v3,
                                 v2v3result_, v2v3edge_labels_)) {
    find_flag = false;
  }
  if (find_flag && !ShortestPath(txn, label_v1, index_v1, label_v3, index_v3,
                                 v1v3result_, v1v3edge_labels_)) {
    find_flag = false;
  }
  results::CollectiveResults results;

  if (find_flag) {
    sink_shortest_path(txn, results, v1v2result_, v1v2edge_labels_);
    sink_shortest_path(txn, results, v2v3result_, v2v3edge_labels_);
    sink_shortest_path(txn, results, v1v3result_, v1v3edge_labels_);
  }
  LOG(INFO) << "results: " << results.DebugString();

  txn.Commit();
  return results;
}

bool ShortestPathAmongThree::ShortestPath(
    const gs::ReadTransaction& txn, label_t v1_l, vid_t v1_index, label_t v2_l,
    vid_t v2_index, std::vector<std::pair<label_t, vid_t>>& result_,
    std::vector<label_t>& edge_labels) {
  Schema schema_ = txn.schema();
  label_t vertex_size_ = (int) schema_.vertex_label_num();
  label_t edge_size_ = (int) schema_.edge_label_num();
  struct pair_hash {
    std::size_t operator()(const std::pair<label_t, vid_t>& p) const {
      auto hash1 = std::hash<label_t>{}(p.first);
      auto hash2 = std::hash<vid_t>{}(p.second);
      return hash1 ^ (hash2 << 1);
    }
  };

  std::unordered_map<std::pair<label_t, vid_t>,
                     std::tuple<label_t, vid_t, label_t>, pair_hash>
      parent;
  std::vector<label_t> nei_label_;
  std::vector<vid_t> nei_index_;

  parent[std::make_pair(v1_l, v1_index)] = std::make_tuple(
      (label_t) UINT8_MAX, (vid_t) UINT32_MAX, (label_t) UINT8_MAX);
  nei_label_.push_back(v1_l);
  nei_index_.push_back(v1_index);

  std::vector<label_t> next_nei_labels_;
  std::vector<vid_t> next_nei_indexs_;
  bool find = false;
  while (!nei_label_.empty() && !find) {
    for (long unsigned int i = 0; i < nei_index_.size(); i++) {
      for (label_t j = 0; j < vertex_size_; j++) {
        for (label_t k = 0; k < edge_size_; k++) {
          if (schema_.has_edge_label(label_t(nei_label_[i]), label_t(j),
                                     label_t(k))) {
            auto outedges = txn.GetOutEdgeIterator(
                nei_label_[i], nei_index_[i], j,
                k);  // 1.self_label 2.self_index 3.edge_label 4.nei_label
            while (outedges.IsValid()) {
              auto neighbor = outedges.GetNeighbor();
              if (parent.find(std::make_pair(j, neighbor)) == parent.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                parent[std::make_pair(j, neighbor)] =
                    std::make_tuple(nei_label_[i], nei_index_[i], k);
                if (std::make_pair(j, neighbor) ==
                    std::make_pair(v2_l, v2_index)) {
                  find = true;
                  break;
                }
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
              if (parent.find(std::make_pair(j, neighbor)) == parent.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                parent[std::make_pair(j, neighbor)] =
                    std::make_tuple(nei_label_[i], nei_index_[i], k);
                if (std::make_pair(j, neighbor) ==
                    std::make_pair(v2_l, v2_index)) {
                  find = true;
                  break;
                }
              }
              inedges.Next();
            }
          }
        }
        if (find)
          break;
      }
      if (find)
        break;
    }
    if (find)
      break;
    nei_label_ = next_nei_labels_;
    nei_index_ = next_nei_indexs_;
    next_nei_labels_.clear();
    next_nei_indexs_.clear();
  }
  if (find) {
    std::pair<label_t, vid_t> vertex_key = std::make_pair(v2_l, v2_index);
    std::tuple<label_t, vid_t, label_t> vertex_value;
    while (std::get<0>(vertex_key) != UINT8_MAX) {
      result_.push_back(vertex_key);
      vertex_value = parent[vertex_key];
      vertex_key =
          std::make_pair(std::get<0>(vertex_value), std::get<1>(vertex_value));
      if (std::get<2>(vertex_value) != UINT8_MAX) {
        edge_labels.push_back(std::get<2>(vertex_value));
      }
    }
    std::reverse(result_.begin(), result_.end());
    return true;
  } else {
    return false;
  }
}

AppWrapper ShortestPathAmongThreeFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new ShortestPathAmongThree(), NULL);
}
}  // namespace gs
