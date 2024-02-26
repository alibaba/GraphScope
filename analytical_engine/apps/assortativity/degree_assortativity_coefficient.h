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

Author: Ning Xin
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_ASSORTATIVITY_COEFFICIENT_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_ASSORTATIVITY_COEFFICIENT_H_

#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/degree_assortativity_coefficient_context.h"
#include "apps/assortativity/utils.h"
#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
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
  using edata_t = typename fragment_t::edata_t;
  using degree_t = double;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      processVertex(v, frag, ctx, messages);
    }
    // if workers_num = 1, force continue
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      vertex_t v;
      degree_t source_degree;
      while (messages.GetMessage(frag, v, source_degree)) {
        degree_t target_degree =
            getDegreeByType(frag, v, ctx.target_degree_type_, ctx);
        degreeMixingCount(source_degree, target_degree, ctx);
      }
      ctx.merge_stage = true;
      if (frag.fid() != 0) {
        messages.SendToFragment(0, ctx.degree_mixing_map);
      }
      messages.ForceContinue();
    } else {
      // merge in work 0
      if (frag.fid() == 0) {
        // {{source_degree, {target_degree, num}}
        std::unordered_map<degree_t, std::unordered_map<degree_t, int>> msg;
        while (messages.GetMessage(msg)) {
          for (auto& pair1 : msg) {
            for (auto& pair2 : pair1.second) {
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
        std::vector<std::vector<degree_t>> degree_mixing_matrix;
        std::unordered_map<int, degree_t> map;
        // get degree mixing matrix
        getDegreeMixingMatrix(ctx, degree_mixing_matrix, map);
        ctx.degree_assortativity = ProcessMatrix(degree_mixing_matrix, map);

        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(ctx.degree_assortativity);
        VLOG(10) << "degree assortatity: " << ctx.degree_assortativity
                 << std::endl;
      }
    }
  }

 private:
  /**
   * @brief traverse the outgoing neighbors of vertex v and update the
   * degree-degree pairs.
   *
   * @param v
   * @param frag
   * @param ctx
   * @param messages
   */
  void processVertex(const vertex_t& v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    degree_t source_degree, target_degree;
    source_degree = getDegreeByType(frag, v, ctx.source_degree_type_, ctx);
    // get all neighbors of vertex v
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t neighbor = e.get_neighbor();
      if (frag.IsOuterVertex(neighbor)) {
        messages.SyncStateOnOuterVertex(frag, neighbor, source_degree);
      } else {
        target_degree =
            getDegreeByType(frag, neighbor, ctx.target_degree_type_, ctx);
        degreeMixingCount(source_degree, target_degree, ctx);
      }
    }
  }

  /**
   * @brief get the degree of vertex
   *
   * @param frag
   * @param vertex
   * @param type IN or OUT
   * @param ctx
   */
  degree_t getDegreeByType(const fragment_t& frag, const vertex_t& vertex,
                           DegreeType type, context_t& ctx) {
    bool directed = ctx.directed;
    bool weighted = ctx.weighted;
    degree_t res;
    if (weighted) {  // process weighted graph
      if (!directed || type == DegreeType::OUT) {
        auto oes = frag.GetOutgoingAdjList(vertex);
        res = computeWeightedDegree(oes);
      } else {
        auto oes = frag.GetIncomingAdjList(vertex);
        res = computeWeightedDegree(oes);
      }
    } else {  // process unweighted graph
      // For GraphScope, the inDegree of the undirected graph may be 0
      if (!directed) {
        res = static_cast<degree_t>(frag.GetLocalOutDegree(vertex));
      } else if (type == DegreeType::IN) {
        res = static_cast<degree_t>(frag.GetLocalInDegree(vertex));
      } else {
        res = static_cast<degree_t>(frag.GetLocalOutDegree(vertex));
      }
    }
    return res;
  }

  /**
   * @brief traverse the adjList to compute weighted degree.
   *
   * @param adjList
   * @tparam T
   *
   */
  template <typename T>
  degree_t computeWeightedDegree(T adjList) {
    degree_t res = 0;
    for (auto& e : adjList) {
      degree_t data = 0;
      vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
          [&](auto& e, auto& edata) {
            edata = static_cast<degree_t>(e.get_data());
          })(e, data);
      res += data;
    }
    return res;
  }

  /**
   * @brief count the degree-degree pair.
   *
   * @param source_degree
   * @param target_degree
   * @param ctx
   *
   */
  void degreeMixingCount(degree_t source_degree, degree_t target_degree,
                         context_t& ctx) {
    if (ctx.degree_mixing_map.count(source_degree) == 0 ||
        ctx.degree_mixing_map[source_degree].count(target_degree) == 0) {
      ctx.degree_mixing_map[source_degree][target_degree] = 1;
    } else {
      ctx.degree_mixing_map[source_degree][target_degree] += 1;
    }
  }

  /**
   * @brief convert degree mixing map to degree mixing matrix
   *
   * @param ctx
   * @param degree_mixing_matrix
   * @param map index-degree map
   */
  void getDegreeMixingMatrix(
      context_t& ctx, std::vector<std::vector<degree_t>>& degree_mixing_matrix,
      std::unordered_map<int, degree_t>& map) {
    int norm = 0, count = 0;
    std::unordered_map<degree_t, int> index_map;
    for (auto& pair1 : ctx.degree_mixing_map) {
      for (auto& pair2 : pair1.second) {
        if (index_map.count(pair1.first) == 0) {
          index_map[pair1.first] = count;
          map[count] = pair1.first;
          count++;
        }
        if (index_map.count(pair2.first) == 0) {
          index_map[pair2.first] = count;
          map[count] = pair2.first;
          count++;
        }
        norm += pair2.second;
      }
    }
    int n = map.size();
    degree_mixing_matrix =
        std::vector<std::vector<degree_t>>(n, std::vector<degree_t>(n, 0));
    for (auto& pair1 : ctx.degree_mixing_map) {
      for (auto& pair2 : pair1.second) {
        int row = index_map[pair1.first];
        int column = index_map[pair2.first];
        degree_mixing_matrix[row][column] =
            pair2.second / static_cast<degree_t>(norm);
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_ASSORTATIVITY_COEFFICIENT_H_
