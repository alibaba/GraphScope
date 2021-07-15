/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_H_

#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/degree/degree_assortativity_coefficient_context.h"
#include "apps/assortativity/utils.h"
#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the degree assortativity for graph.
 *  Assortativity measures the similarity of connections in the graph with
 * respect to the node degree.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class DegreeAssortativity
    : public AppBase<FRAG_T, DegreeAssortativityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(DegreeAssortativity<FRAG_T>,
                         DegreeAssortativityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    ctx.merge_stage = false;
    // vid
    auto inner_vertices = frag.InnerVertices();
    // v of type: Vertex
    for (auto v : inner_vertices) {
      vid_t vid = frag.Vertex2Gid(v);
      VLOG(0) << "peval: " << frag.fid() << ": " << vid << std::endl;
      ProcessVertex(v, frag, ctx, messages);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      vertex_t v;
      int source_degree;
      while (messages.GetMessage<fragment_t, int>(frag, v, source_degree)) {
        VLOG(0) << "inceval: " << frag.fid() << ": " << frag.Vertex2Gid(v)
                << std::endl;
        int target_degree =
            GetDegreeByType(frag, v, ctx.target_degree_type_, ctx.directed);
        DegreeMixingCount(source_degree, target_degree, ctx);
      }
      ctx.merge_stage = true;
      if (frag.fid() != 0) {
        messages.SendToFragment(0, ctx.degree_mixing_map);
      }
      messages.ForceContinue();
    } else {
      // merge in work 0
      if (frag.fid() == 0) {
        // {{source_degree, target_degree}, num}
        // std::unordered_map<std::pair<int, int>, int, pair_hash> msg;
        std::unordered_map<int, std::unordered_map<int, int>> msg;
        while (messages.GetMessage(msg)) {
          for (auto& pair1 : msg) {
            for (auto& pair2 : pair1.second) {
              // update max degree
              if (ctx.max_degree < pair1.first) {
                ctx.max_degree = pair1.first;
              }
              if (ctx.max_degree < pair2.first) {
                ctx.max_degree = pair2.first;
              }
              // merge
              if (ctx.degree_mixing_map.count(pair1.first) == 0 ||
                  ctx.degree_mixing_map[pair1.first].count(pair2.first) == 0) {
                ctx.degree_mixing_map[pair1.first][pair2.first] = pair2.second;
              } else {
                ctx.degree_mixing_map[pair1.first][pair2.first] += pair2.second;
              }
            }
          }
        }
        std::vector<std::vector<double>> degree_mixing_matrix(
            ctx.max_degree + 1, std::vector<double>(ctx.max_degree + 1, 0.0));
        // get degree mixing matrix
        GetDegreeMixingMatrix(ctx, degree_mixing_matrix);
        ctx.degree_assortativity = ProcessMatrix(degree_mixing_matrix);

        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(ctx.degree_assortativity);
        VLOG(0) << "degree assortatity: " << ctx.degree_assortativity
                << std::endl;
      }
    }
  }

  void ProcessVertex(const vertex_t& v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    int source_degree, target_degree;
    source_degree =
        GetDegreeByType(frag, v, ctx.source_degree_type_, ctx.directed);
    // update max degree
    if (ctx.max_degree < source_degree) {
      ctx.max_degree = source_degree;
    }
    // bool need_sync = false;
    // get all neighbors of vertex v
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t neighbor = e.get_neighbor();
      VLOG(0) << frag.Vertex2Gid(v) << "-->" << frag.Vertex2Gid(neighbor)
              << std::endl;
      if (frag.IsOuterVertex(neighbor)) {
        messages.SyncStateOnOuterVertex(frag, neighbor, source_degree);
        // need_sync = true;
      } else {
        target_degree = GetDegreeByType(frag, neighbor, ctx.target_degree_type_,
                                        ctx.directed);
        DegreeMixingCount(source_degree, target_degree, ctx);
      }
    }
    // if (need_sync) {
    //   VLOG(0) << "source vertex: " << frag.Vertex2Gid(v) << std::endl;
    //   messages.SendMsgThroughOEdges<fragment_t, int>(frag, v, source_degree);
    // }
  }
  double ProcessMatrix(std::vector<std::vector<double>>& degree_mixing_matrix) {
    int n = degree_mixing_matrix.size();
    std::vector<double> a;
    // sum of column
    for (auto& row : degree_mixing_matrix) {
      a.emplace_back(accumulate(row.begin(), row.end(), 0.0));
    }
    std::vector<double> b;
    // sum of row
    for (int i = 0; i < n; i++) {
      double sum = 0.0;
      for (int j = 0; j < n; j++) {
        sum += degree_mixing_matrix[j][i];
      }
      b.emplace_back(sum);
    }
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        sum += i * j * (degree_mixing_matrix[i][j] - a[i] * b[j]);
      }
    }
    double vara = Variance(a);
    double varb = Variance(b);
    return sum / (vara * varb);
  }

  double Variance(std::vector<double>& vec) {
    double sum1 = 0.0, sum2 = 0.0;
    int n = vec.size();
    for (int i = 0; i < n; i++) {
      sum1 += i * i * vec[i];
      sum2 += i * vec[i];
    }
    return sqrt(sum1 - sum2 * sum2);
  }
  int GetDegreeByType(const fragment_t& frag, const vertex_t& vertex,
                      DegreeType type, bool directed) {
    // For GraphScope, the inDegree of the undirected graph may be 0
    if (!directed) {
      return frag.GetLocalOutDegree(vertex);
    }
    if (type == DegreeType::IN) {
      return frag.GetLocalInDegree(vertex);
    }
    return frag.GetLocalOutDegree(vertex);
  }
  void DegreeMixingCount(int source_degree, int target_degree, context_t& ctx) {
    if (ctx.degree_mixing_map.count(source_degree) == 0 ||
        ctx.degree_mixing_map[source_degree].count(target_degree) == 0) {
      ctx.degree_mixing_map[source_degree][target_degree] = 1;
    } else {
      ctx.degree_mixing_map[source_degree][target_degree] += 1;
    }
  }
  void GetDegreeMixingMatrix(
      context_t& ctx, std::vector<std::vector<double>>& degree_mixing_matrix) {
    int total_edge_num = 0;
    for (auto& pair1 : ctx.degree_mixing_map) {
      for (auto& pair2 : pair1.second) {
        total_edge_num += pair2.second;
      }
    }
    int n = degree_mixing_matrix.size();
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        if (ctx.degree_mixing_map.count(i) != 0 &&
            ctx.degree_mixing_map[i].count(j) != 0) {
          degree_mixing_matrix[i][j] =
              ctx.degree_mixing_map[i][j] / static_cast<double>(total_edge_num);
        }
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_H_
