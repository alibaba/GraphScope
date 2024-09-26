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
#include "flex/engines/graph_db/app/builtin/pagerank.h"

namespace gs {

bool PageRank::DoQuery(GraphDBSession& sess, Decoder& input, Encoder& output) {
  auto txn = sess.GetReadTransaction();
  if (input.empty()) {
    output.put_string_view(
        "Arguments required(vertex_label, edge_label, damping_factor_, max_iterations_, epsilon_)\n \
    for example:(\"person\", \"knows\", 0.85, 100, 0.000001)");
    return false;
  }

  std::string vertex_label{input.get_string()};
  std::string edge_label{input.get_string()};

  damping_factor_ = input.get_double();
  max_iterations_ = input.get_int();
  epsilon_ = input.get_double();

  if (!sess.schema().has_vertex_label(vertex_label)) {
    output.put_string_view("The requested vertex label doesn't exits.");
    return false;
  }
  if (!sess.schema().has_edge_label(vertex_label, vertex_label, edge_label)) {
    output.put_string_view("The requested edge label doesn't exits.");
    return false;
  }
  if (damping_factor_ < 0 || damping_factor_ >= 1) {
    output.put_string_view(
        "The value of the damping_factor_ is between 0 and 1.");
    return false;
  }
  if (max_iterations_ <= 0) {
    output.put_string_view("max_iterations_ must be greater than 0.");
    return false;
  }
  if (epsilon_ < 0 || epsilon_ >= 1) {
    output.put_string_view("The value of the epsilon_ is between 0 and 1.");
    return false;
  }

  vertex_label_id_ = sess.schema().get_vertex_label_id(vertex_label);
  edge_label_id_ = sess.schema().get_edge_label_id(edge_label);

  auto num_vertices = txn.GetVertexNum(vertex_label_id_);

  std::unordered_map<vid_t, double> pagerank;
  std::unordered_map<vid_t, double> new_pagerank;

  auto vertex_iter = txn.GetVertexIterator(vertex_label_id_);

  while (vertex_iter.IsValid()) {
    vid_t vid = vertex_iter.GetIndex();
    pagerank[vid] = 1.0 / num_vertices;
    new_pagerank[vid] = 0.0;
    vertex_iter.Next();
  }

  std::unordered_map<vid_t, double> outdegree;

  for (int iter = 0; iter < max_iterations_; ++iter) {
    for (auto& kv : new_pagerank) {
      kv.second = 0.0;
    }

    auto vertex_iter = txn.GetVertexIterator(vertex_label_id_);
    while (vertex_iter.IsValid()) {
      vid_t v = vertex_iter.GetIndex();

      double sum = 0.0;
      auto edges = txn.GetInEdgeIterator(vertex_label_id_, v, vertex_label_id_,
                                         edge_label_id_);
      while (edges.IsValid()) {
        auto neighbor = edges.GetNeighbor();
        if (outdegree[neighbor] == 0) {
          auto out_edges = txn.GetOutEdgeIterator(
              vertex_label_id_, neighbor, vertex_label_id_, edge_label_id_);
          while (out_edges.IsValid()) {
            outdegree[neighbor]++;
            out_edges.Next();
          }
        }
        sum += pagerank[neighbor] / outdegree[neighbor];
        edges.Next();
      }

      new_pagerank[v] =
          damping_factor_ * sum + (1.0 - damping_factor_) / num_vertices;
      vertex_iter.Next();
    }

    double diff = 0.0;
    for (const auto& kv : pagerank) {
      diff += std::abs(new_pagerank[kv.first] - kv.second);
    }

    if (diff < epsilon_) {
      break;
    }

    std::swap(pagerank, new_pagerank);
  }

  results::CollectiveResults results;

  for (auto kv : pagerank) {
    auto id = txn.GetVertexId(vertex_label_id_, kv.first).to_string();
    std::string res_string =
        "vertex: " + id + ", pagerank: " + std::to_string(kv.second);
    results.add_results()
        ->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_str(res_string);
  }

  output.put_string_view(results.SerializeAsString());

  txn.Commit();
  return true;
}

AppWrapper PageRankFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new PageRank(), NULL);
}
}  // namespace gs
