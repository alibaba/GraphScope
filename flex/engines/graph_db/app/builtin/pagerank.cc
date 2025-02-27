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

void write_result(const ReadTransaction& txn,
                  results::CollectiveResults& results,
                  const label_t& vertex_label,
                  const std::vector<double>& pagerank) {
  auto vertex_label_name = txn.schema().get_vertex_label_name(vertex_label);
  for (size_t j = 0; j < pagerank.size(); ++j) {
    int64_t oid_ = txn.GetVertexId(vertex_label, j).AsInt64();
    auto result = results.add_results();
    result->mutable_record()
        ->add_columns()
        ->mutable_entry()
        ->mutable_element()
        ->mutable_object()
        ->set_str(vertex_label_name);
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
        ->set_f64(pagerank[j]);
  }
}

results::CollectiveResults PageRank::Query(const GraphDBSession& sess,
                                           std::string src_vertex_label,
                                           std::string dst_vertex_label,
                                           std::string edge_label,
                                           double damping_factor,
                                           int max_iterations, double epsilon) {
  auto txn = sess.GetReadTransaction();

  if (!sess.schema().has_vertex_label(src_vertex_label)) {
    LOG(ERROR) << "The requested src vertex label doesn't exits.";
    return {};
  }
  if (!sess.schema().has_vertex_label(dst_vertex_label)) {
    LOG(ERROR) << "The requested dst vertex label doesn't exits.";
    return {};
  }
  if (!sess.schema().has_edge_label(src_vertex_label, dst_vertex_label,
                                    edge_label)) {
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

  auto src_vertex_label_id =
      sess.schema().get_vertex_label_id(src_vertex_label);
  auto dst_vertex_label_id =
      sess.schema().get_vertex_label_id(dst_vertex_label);
  auto edge_label_id = sess.schema().get_edge_label_id(edge_label);

  auto num_src_vertices = txn.GetVertexNum(src_vertex_label_id);
  auto num_dst_vertices = txn.GetVertexNum(dst_vertex_label_id);
  auto num_vertices = src_vertex_label_id == dst_vertex_label_id
                          ? num_src_vertices
                          : num_src_vertices + num_dst_vertices;

  std::vector<std::vector<double>> pagerank;
  std::vector<std::vector<double>> new_pagerank;
  std::vector<std::vector<int32_t>> outdegree;

  bool dst_to_src = src_vertex_label_id != dst_vertex_label_id &&
                    txn.schema().exist(dst_vertex_label_id, src_vertex_label_id,
                                       edge_label_id);

  pagerank.emplace_back(std::vector<double>(num_src_vertices, 0.0));
  new_pagerank.emplace_back(std::vector<double>(num_src_vertices, 0.0));
  outdegree.emplace_back(std::vector<int32_t>(num_src_vertices, 0));
  if (dst_to_src) {
    pagerank.emplace_back(std::vector<double>(num_dst_vertices, 0.0));
    new_pagerank.emplace_back(std::vector<double>(num_dst_vertices, 0.0));
    outdegree.emplace_back(std::vector<int32_t>(num_dst_vertices, 0));
  }

  auto src_vertex_iter = txn.GetVertexIterator(src_vertex_label_id);

  while (src_vertex_iter.IsValid()) {
    vid_t vid = src_vertex_iter.GetIndex();
    pagerank[0][vid] = 1.0 / num_vertices;
    new_pagerank[0][vid] = 0.0;
    src_vertex_iter.Next();
    outdegree[0][vid] = txn.GetOutDegree(src_vertex_label_id, vid,
                                         dst_vertex_label_id, edge_label_id);
  }
  if (dst_to_src) {
    auto dst_vertex_iter = txn.GetVertexIterator(dst_vertex_label_id);
    while (dst_vertex_iter.IsValid()) {
      vid_t vid = dst_vertex_iter.GetIndex();
      pagerank[1][vid] = 1.0 / num_vertices;
      new_pagerank[1][vid] = 0.0;
      dst_vertex_iter.Next();
      outdegree[1][vid] = txn.GetOutDegree(
          dst_vertex_label_id, src_vertex_label_id, vid, edge_label_id);
    }
  }

  for (int iter = 0; iter < max_iterations; ++iter) {
    new_pagerank[0].assign(num_src_vertices, 0.0);
    if (dst_to_src) {
      new_pagerank[1].assign(num_dst_vertices, 0.0);
    }

    auto src_vertex_iter = txn.GetVertexIterator(src_vertex_label_id);
    while (src_vertex_iter.IsValid()) {
      vid_t v = src_vertex_iter.GetIndex();

      double sum = 0.0;
      {
        auto edges = txn.GetInEdgeIterator(dst_vertex_label_id, v,
                                           src_vertex_label_id, edge_label_id);
        while (edges.IsValid()) {
          LOG(INFO) << "got edge, from " << edges.GetNeighbor() << " to " << v
                    << " label: " << std::to_string(dst_vertex_label_id) << " "
                    << std::to_string(src_vertex_label_id) << " "
                    << std::to_string(edge_label_id);
          auto neighbor = edges.GetNeighbor();
          sum += pagerank[0][neighbor] / outdegree[0][neighbor];
          edges.Next();
        }
      }

      new_pagerank[0][v] =
          damping_factor * sum + (1.0 - damping_factor) / num_vertices;
      src_vertex_iter.Next();
    }

    if (dst_to_src) {
      auto dst_vertex_iter = txn.GetVertexIterator(dst_vertex_label_id);
      while (dst_vertex_iter.IsValid()) {
        vid_t v = dst_vertex_iter.GetIndex();

        double sum = 0.0;
        {
          auto edges = txn.GetInEdgeIterator(
              src_vertex_label_id, v, dst_vertex_label_id, edge_label_id);
          while (edges.IsValid()) {
            LOG(INFO) << "got edge, from " << edges.GetNeighbor() << " to " << v
                      << " label: " << std::to_string(src_vertex_label_id)
                      << " " << std::to_string(dst_vertex_label_id) << " "
                      << std::to_string(edge_label_id);
            auto neighbor = edges.GetNeighbor();
            sum += pagerank[1][neighbor] / outdegree[1][neighbor];
            edges.Next();
          }
        }

        new_pagerank[1][v] =
            damping_factor * sum + (1.0 - damping_factor) / num_vertices;
        dst_vertex_iter.Next();
      }
    }

    double diff = 0.0;
    for (size_t j = 0; j < new_pagerank[0].size(); ++j) {
      diff += std::abs(new_pagerank[0][j] - pagerank[0][j]);
    }
    if (dst_to_src) {
      for (size_t j = 0; j < new_pagerank[1].size(); ++j) {
        diff += std::abs(new_pagerank[1][j] - pagerank[1][j]);
      }
    }

    if (diff < epsilon) {
      break;
    }

    std::swap(pagerank, new_pagerank);
  }

  results::CollectiveResults results;

  write_result(txn, results, src_vertex_label_id, pagerank[0]);
  if (dst_to_src) {
    write_result(txn, results, dst_vertex_label_id, pagerank[1]);
  }

  txn.Commit();
  return results;
}

AppWrapper PageRankFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new PageRank(), NULL);
}
}  // namespace gs
