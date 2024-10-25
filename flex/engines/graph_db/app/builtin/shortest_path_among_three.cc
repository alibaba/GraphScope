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

namespace gs {

results::CollectiveResults ShortestPathAmongThree::Query(
    const GraphDBSession& sess, std::string label_name1, int64_t oid1,
    std::string label_name2, int64_t oid2, std::string label_name3,
    int64_t oid3) {
  ReadTransaction txn = sess.GetReadTransaction();
  const Schema& schema_ = txn.schema();

  if (!schema_.has_vertex_label(label_name1) ||
      !schema_.has_vertex_label(label_name2) ||
      !schema_.has_vertex_label(label_name3)) {
    LOG(ERROR) << "The requested label doesn't exits.";
    return {};
  }
  label_t label_v1 = schema_.get_vertex_label_id(label_name1);
  label_t label_v2 = schema_.get_vertex_label_id(label_name2);
  label_t label_v3 = schema_.get_vertex_label_id(label_name3);
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
  std::vector<std::pair<label_t, vid_t>> v1v2result_;
  std::vector<std::pair<label_t, vid_t>> v2v3result_;
  std::vector<std::pair<label_t, vid_t>> v1v3result_;

  bool find_flag = true;
  if (!ShortestPath(txn, label_v1, index_v1, label_v2, index_v2, v1v2result_)) {
    find_flag = false;
  }
  if (find_flag &&
      !ShortestPath(txn, label_v2, index_v2, label_v3, index_v3, v2v3result_)) {
    find_flag = false;
  }
  if (find_flag &&
      !ShortestPath(txn, label_v1, index_v1, label_v3, index_v3, v1v3result_)) {
    find_flag = false;
  }
  std::string result_path = "";
  if (find_flag) {
    // connect the two shortest paths among three
    std::vector<std::pair<label_t, vid_t>> TSP =
        ConnectPath(v1v2result_, v2v3result_, v1v3result_);

    // construct return result
    for (auto it = TSP.begin(); it != TSP.end(); ++it) {
      if (std::next(it) != TSP.end()) {
        result_path +=
            "(" + schema_.get_vertex_label_name(it->first) + "," +
            std::to_string(txn.GetVertexId(it->first, it->second).AsInt64()) +
            ")" + "--";
      } else {
        result_path +=
            "(" + schema_.get_vertex_label_name(it->first) + "," +
            std::to_string(txn.GetVertexId(it->first, it->second).AsInt64()) +
            ")";
      }
    }
  } else {
    result_path = "no path find!";
  }

  // create result string
  results::CollectiveResults results;
  auto result = results.add_results();
  result->mutable_record()
      ->add_columns()
      ->mutable_entry()
      ->mutable_element()
      ->mutable_object()
      ->set_str(result_path);

  txn.Commit();
  return results;
}

bool ShortestPathAmongThree::ShortestPath(
    const gs::ReadTransaction& txn, label_t v1_l, vid_t v1_index, label_t v2_l,
    vid_t v2_index, std::vector<std::pair<label_t, vid_t>>& result_) {
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

  std::unordered_map<std::pair<label_t, vid_t>, std::pair<label_t, vid_t>,
                     pair_hash>
      parent;
  std::vector<label_t> nei_label_;
  std::vector<vid_t> nei_index_;

  parent[std::make_pair(v1_l, v1_index)] =
      std::make_pair((label_t) UINT8_MAX, (vid_t) UINT32_MAX);
  nei_label_.push_back(v1_l);
  nei_index_.push_back(v1_index);
  std::unordered_set<std::pair<label_t, vid_t>, pair_hash> visit;
  visit.insert(std::make_pair(v1_l, v1_index));

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
              if (visit.find(std::make_pair(j, neighbor)) == visit.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                visit.insert(std::make_pair(j, neighbor));
                parent[std::make_pair(j, neighbor)] =
                    std::make_pair(nei_label_[i], nei_index_[i]);
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
              if (visit.find(std::make_pair(j, neighbor)) == visit.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                visit.insert(std::make_pair(j, neighbor));
                parent[std::make_pair(j, neighbor)] =
                    std::make_pair(nei_label_[i], nei_index_[i]);
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
    std::pair<label_t, vid_t> vertex_v = std::make_pair(v2_l, v2_index);
    while (vertex_v !=
           std::make_pair((label_t) UINT8_MAX, (vid_t) UINT32_MAX)) {
      result_.push_back(vertex_v);
      vertex_v = parent[vertex_v];
    }
    std::reverse(result_.begin(), result_.end());
    return true;
  } else {
    return false;
  }
}

std::vector<std::pair<label_t, vid_t>> ShortestPathAmongThree::ConnectPath(
    const std::vector<std::pair<label_t, vid_t>>& path1,
    const std::vector<std::pair<label_t, vid_t>>& path2,
    const std::vector<std::pair<label_t, vid_t>>& path3) {
  std::vector<std::pair<label_t, vid_t>> TSP;
  size_t v1v2size = path1.size();
  size_t v2v3size = path2.size();
  size_t v1v3size = path3.size();
  if (v1v2size <= v2v3size && v1v3size <= v2v3size) {
    for (size_t i = v1v2size; i > 0; i--) {
      TSP.push_back(path1[i - 1]);
    }
    for (size_t i = 1; i < v1v3size; i++) {
      TSP.push_back(path3[i]);
    }
  } else if (v1v2size <= v1v3size && v2v3size <= v1v3size) {
    for (size_t i = 0; i < v1v2size; i++) {
      TSP.push_back(path1[i]);
    }
    for (size_t i = 1; i < v2v3size; i++) {
      TSP.push_back(path2[i]);
    }
  } else if (v2v3size <= v1v2size && v1v3size <= v1v2size) {
    for (size_t i = 0; i < v2v3size; i++) {
      TSP.push_back(path2[i]);
    }
    for (size_t i = v1v3size - 1; i > 0; i--) {
      TSP.push_back(path3[i - 1]);
    }
  }
  return TSP;
}
AppWrapper ShortestPathAmongThreeFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new ShortestPathAmongThree(), NULL);
}
}  // namespace gs
