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

bool ShortestPathAmongThree::DoQuery(GraphDBSession& sess, Decoder& input,
                                     Encoder& output) {
  ReadTransaction txn = sess.GetReadTransaction();
  if (input.empty()) {
    return false;
  }
  Schema schema_ = txn.schema();
  std::string label_name1{input.get_string()};
  int64_t vid1 = input.get_long();
  std::string label_name2{input.get_string()};
  int64_t vid2 = input.get_long();
  std::string label_name3{input.get_string()};
  int64_t vid3 = input.get_long();

  if (!schema_.has_vertex_label(label_name1) ||
      !schema_.has_vertex_label(label_name2) ||
      !schema_.has_vertex_label(label_name3)) {
    output.put_string_view("The requested label doesn't exits.");
    return false;
  }
  label_t label_v1 = schema_.get_vertex_label_id(label_name1);
  label_t label_v2 = schema_.get_vertex_label_id(label_name2);
  label_t label_v3 = schema_.get_vertex_label_id(label_name3);
  vid_t index_v1{};
  vid_t index_v2{};
  vid_t index_v3{};
  if (!txn.GetVertexIndex(label_v1, (int64_t) vid1, index_v1) ||
      !txn.GetVertexIndex(label_v2, (int64_t) vid2, index_v2) ||
      !txn.GetVertexIndex(label_v3, (int64_t) vid3, index_v3)) {
    output.put_string_view("get index fail.");
    return false;
  }

  std::vector<int64_t> v1v2result_;
  std::vector<int64_t> v2v3result_;
  std::vector<int64_t> v1v3result_;

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
    // connect two shortest paths
    std::vector<int64_t> TSP =
        ConnectPath(v1v2result_, v2v3result_, v1v3result_);
    for (auto it = TSP.begin(); it != TSP.end(); ++it) {
      if (std::next(it) != TSP.end()) {
        result_path += std::to_string(*it) + "--";
      } else {
        result_path += std::to_string(*it);
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

  output.put_string_view(results.SerializeAsString());
  txn.Commit();
  return true;
}

bool ShortestPathAmongThree::ShortestPath(const gs::ReadTransaction& txn,
                                          label_t v1_l, vid_t v1_index,
                                          label_t v2_l, vid_t v2_index,
                                          std::vector<int64_t>& result_) {
  Schema schema_ = txn.schema();
  int vertex_size_ = (int) schema_.vertex_label_num();
  int edge_size_ = (int) schema_.edge_label_num();

  std::unordered_map<int64_t, int64_t> parent;
  std::vector<label_t> nei_label_;
  std::vector<vid_t> nei_index_;
  int64_t v1_id = txn.GetVertexId(v1_l, v1_index).AsInt64();
  int64_t v2_id = txn.GetVertexId(v2_l, v2_index).AsInt64();

  parent[v1_id] = -1;
  nei_label_.push_back(v1_l);
  nei_index_.push_back(v1_index);
  std::unordered_set<int64_t> visit;
  visit.insert(v1_id);

  std::vector<label_t> next_nei_labels_;
  std::vector<vid_t> next_nei_indexs_;
  bool find = false;
  while (!nei_label_.empty() && !find) {
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
              int64_t v_id = txn.GetVertexId(j, neighbor).AsInt64();
              if (visit.find(v_id) == visit.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                visit.insert(v_id);
                parent[v_id] =
                    txn.GetVertexId(nei_label_[i], nei_index_[i]).AsInt64();
                if (v_id == v2_id) {
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
              int64_t v_id = txn.GetVertexId(j, neighbor).AsInt64();
              if (visit.find(v_id) == visit.end()) {
                next_nei_labels_.push_back(j);
                next_nei_indexs_.push_back(neighbor);
                visit.insert(v_id);
                parent[v_id] =
                    txn.GetVertexId(nei_label_[i], nei_index_[i]).AsInt64();
                if (v_id == v2_id) {
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
    int64_t v = v2_id;
    while (v != -1) {
      result_.push_back(v);
      v = parent[v];
    }
    std::reverse(result_.begin(), result_.end());
    return true;
  } else {
    return false;
  }
}

std::vector<int64_t> ShortestPathAmongThree::ConnectPath(
    const std::vector<int64_t>& path1, const std::vector<int64_t>& path2,
    const std::vector<int64_t>& path3) {
  std::vector<int64_t> TSP;
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
