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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_H_

#include <limits>
#include <map>
#include <queue>
#include <stack>
#include <tuple>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
#include "core/worker/default_worker.h"

#include "apps/centrality/betweenness/betweenness_centrality_generic_context.h"

namespace gs {

/**
 * @brief Compute the betweenness centrality for vertices. The betweenness
 * centrality for a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BetweennessCentralityGeneric
    : public grape::ParallelAppBase<
          FRAG_T, BetweennessCentralityGenericContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(BetweennessCentralityGeneric<FRAG_T>,
                          BetweennessCentralityGenericContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();
    auto outer_vertices = frag.OuterVertices();
    // compute pair dependency
    ForEach(inner_vertices,
            [&frag, &ctx, &vertices, this](int tid, vertex_t v) {
              ctx.pair_dependency[v].Init(vertices, 0);
              if (std::is_same<edata_t, grape::EmptyType>::value) {
                // unweighted graph, use bfs.
                this->bfs(frag, v, ctx);
              } else {
                // weighted graph, use dijkstra.
                this->dijkstra(frag, v, ctx);
              }
            });

    // accumulate inner pair dependency
    ForEach(inner_vertices,
            [&frag, &ctx, &inner_vertices, this](int tid, vertex_t v) {
              for (auto& u : inner_vertices) {
                ctx.centrality[v] += ctx.norm * ctx.pair_dependency[u][v];
              }
            });

    // exchange pair dependency
    ForEach(outer_vertices, [&frag, &ctx, &inner_vertices, this, &messages](
                                int tid, vertex_t v) {
      double msg = 0.0;
      for (auto& u : inner_vertices) {
        msg += ctx.norm * ctx.pair_dependency[u][v];
      }
      messages.Channels()[tid].SyncStateOnOuterVertex(frag, v, msg);
    });
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    // accumulate outer pair dependency
    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag,
        [&ctx](int tid, vertex_t v, double msg) { ctx.centrality[v] += msg; });
  }

 private:
  void dijkstra(const fragment_t& frag, vertex_t& s, context_t& ctx) {
    auto vertices = frag.Vertices();
    std::priority_queue<std::pair<double, vertex_t>> heap;
    std::stack<vertex_t> S;
    typename FRAG_T::template vertex_array_t<std::vector<vertex_t>> P(vertices,
                                                                      {});
    typename FRAG_T::template vertex_array_t<bool> D(vertices, false);
    typename FRAG_T::template vertex_array_t<double> sigma(vertices, 0.0);
    typename FRAG_T::template vertex_array_t<double> delta(vertices, 0.0);
    typename FRAG_T::template vertex_array_t<double> seen(
        vertices, std::numeric_limits<double>::max());
    seen[s] = 0.0;
    sigma[s] = 1.0;
    heap.emplace(0, s);

    double distu, distv, ndistv;
    vertex_t v, u;
    while (!heap.empty()) {
      u = heap.top().second;
      distu = -heap.top().first;
      heap.pop();

      if (D[u]) {
        continue;
      }
      D[u] = true;
      S.push(u);

      auto es = frag.GetOutgoingAdjList(u);
      for (auto& e : es) {
        v = e.get_neighbor();
        distv = seen[v];
        double edata = 1.0;
        vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
            [&](auto& e, auto& data) {
              data = static_cast<double>(e.get_data());
            })(e, edata);
        ndistv = distu + edata;
        if (!D[v] && distv > ndistv) {
          seen[v] = ndistv;
          heap.emplace(-ndistv, v);
          sigma[v] = 0.0;
          P[v].clear();
        }
        if (ndistv == seen[v]) {
          sigma[v] += sigma[u];
          P[v].emplace_back(u);
        }
      }
    }
    if (ctx.endpoints)
      ctx.pair_dependency[s][s] += S.size() - 1;
    while (!S.empty()) {
      auto w = S.top();
      S.pop();
      double coeff = (1 + delta[w]) / sigma[w];
      for (auto& v : P[w]) {
        delta[v] += sigma[v] * coeff;
      }
      if (w != s)
        ctx.pair_dependency[s][w] += delta[w] + ctx.endpoints;
    }
  }

  void bfs(const fragment_t& frag, vertex_t& s, context_t& ctx) {
    auto vertices = frag.Vertices();
    std::queue<vertex_t> que;
    std::stack<vertex_t> S;
    typename FRAG_T::template vertex_array_t<std::vector<vertex_t>> P(vertices,
                                                                      {});
    typename FRAG_T::template vertex_array_t<bool> D(vertices, false);
    typename FRAG_T::template vertex_array_t<double> sigma(vertices, 0.0);
    typename FRAG_T::template vertex_array_t<double> delta(vertices, 0.0);
    typename FRAG_T::template vertex_array_t<double> seen(
        vertices, std::numeric_limits<double>::max());
    seen[s] = 0.0;
    sigma[s] = 1.0;
    D[s] = true;

    que.push(s);
    while (!que.empty()) {
      vertex_t u = que.front();
      que.pop();
      S.push(u);

      // set depth for neighbors with current depth + 1
      auto new_depth = seen[u] + 1;
      auto oes = frag.GetOutgoingAdjList(u);
      for (auto& e : oes) {
        vertex_t v = e.get_neighbor();
        if (!D[v]) {
          que.push(v);
          seen[v] = new_depth;
          D[v] = true;
        }
        if (seen[v] == new_depth) {
          sigma[v] += sigma[u];
          P[v].emplace_back(u);
        }
      }
    }

    if (ctx.endpoints)
      ctx.pair_dependency[s][s] += S.size() - 1;
    while (!S.empty()) {
      auto w = S.top();
      S.pop();
      double coeff = (1 + delta[w]) / sigma[w];
      for (auto& v : P[w]) {
        delta[v] += sigma[v] * coeff;
      }
      if (w != s)
        ctx.pair_dependency[s][w] += delta[w] + ctx.endpoints;
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_BETWEENNESS_BETWEENNESS_CENTRALITY_GENERIC_H_
