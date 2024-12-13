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

results::CollectiveResults PageRank::Query(const GraphDBSession& sess,
                                           std::string vertex_label,
                                           std::string edge_label,
                                           double damping_factor,
                                           int max_iterations, double epsilon) {
  auto txn = sess.GetReadTransaction();

  if (!sess.schema().has_vertex_label(vertex_label)) {
    LOG(ERROR) << "The requested vertex label doesn't exits.";
    return {};
  }
  if (!sess.schema().has_edge_label(vertex_label, vertex_label, edge_label)) {
    LOG(ERROR) << "The requested edge label doesn't exits.";
    return {};
  }
  if (damping_factor < 0 || damping_factor >= 1) {
    LOG(ERROR) << "The value of the damping factor is between 0 and 1.";
    return {};
  }
  if (max_iterations <= 0) {
    LOG(ERROR) << "The value of the max iterations must be greater than 0.";
    return {};
  }
  if (epsilon < 0 || epsilon >= 1) {
    LOG(ERROR) << "The value of the epsilon is between 0 and 1.";
    return {};
  }

  auto vertex_label_id = sess.schema().get_vertex_label_id(vertex_label);
  auto edge_label_id = sess.schema().get_edge_label_id(edge_label);

  auto num_vertices = txn.GetVertexNum(vertex_label_id);

  std::unordered_map<vid_t, double> pagerank;
  std::unordered_map<vid_t, double> new_pagerank;

  auto vertex_iter = txn.GetVertexIterator(vertex_label_id);

  while (vertex_iter.IsValid()) {
    vid_t vid = vertex_iter.GetIndex();
    pagerank[vid] = 1.0 / num_vertices;
    new_pagerank[vid] = 0.0;
    vertex_iter.Next();
  }

  std::unordered_map<vid_t, double> outdegree;

  for (int iter = 0; iter < max_iterations; ++iter) {
    for (auto& kv : new_pagerank) {
      kv.second = 0.0;
    }

    auto vertex_iter = txn.GetVertexIterator(vertex_label_id);
    while (vertex_iter.IsValid()) {
      vid_t v = vertex_iter.GetIndex();

      double sum = 0.0;
      auto edges = txn.GetInEdgeIterator(vertex_label_id, v, vertex_label_id,
                                         edge_label_id);
      while (edges.IsValid()) {
        auto neighbor = edges.GetNeighbor();
        if (outdegree[neighbor] == 0) {
          auto out_edges = txn.GetOutEdgeIterator(
              vertex_label_id, neighbor, vertex_label_id, edge_label_id);
          while (out_edges.IsValid()) {
            outdegree[neighbor]++;
            out_edges.Next();
          }
        }
        sum += pagerank[neighbor] / outdegree[neighbor];
        edges.Next();
      }

      new_pagerank[v] =
          damping_factor * sum + (1.0 - damping_factor) / num_vertices;
      vertex_iter.Next();
    }

    double diff = 0.0;
    for (const auto& kv : pagerank) {
      diff += std::abs(new_pagerank[kv.first] - kv.second);
    }

    if (diff < epsilon) {
      break;
    }

    std::swap(pagerank, new_pagerank);
  }

  results::CollectiveResults results;

  for (auto kv : pagerank) {
    int64_t oid_ = txn.GetVertexId(vertex_label_id, kv.first).AsInt64();
    auto result = results.add_results();
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_str(vertex_label);
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_i64(oid_);
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_f64(kv.second);
  }

  txn.Commit();
  return results;
}

AppWrapper PageRankFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new PageRank(), NULL);
}
}  // namespace gs
