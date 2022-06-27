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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_H_

#include <limits>
#include <map>
#include <queue>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/centrality/closeness/closeness_centrality_context.h"

#include "core/utils/trait_utils.h"

namespace gs {

/**
 * @brief Compute the closeness centrality of vertices.
 * Closeness centrality 1 of a node u is the reciprocal of the average shortest
 * path distance to u over all n-1 reachable nodes.
 * */
template <typename FRAG_T>
class ClosenessCentrality
    : public grape::ParallelAppBase<FRAG_T, ClosenessCentralityContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(ClosenessCentrality<FRAG_T>,
                          ClosenessCentralityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto vertices = frag.Vertices();
    ctx.length.resize(thread_num());
    for (auto& unit : ctx.length) {
      unit.Init(vertices);
    }

    ForEach(inner_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      ctx.length[tid].SetValue(std::numeric_limits<double>::max());
      this->reversedDijkstraLength(frag, v, ctx, tid);
      this->compute(frag, v, ctx, tid);
    });
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    return;
  }

 private:
  // sequential single source Dijkstra length algorithm.
  void reversedDijkstraLength(const fragment_t& frag, vertex_t& s,
                              context_t& ctx, int tid) {
    {
      auto vertices = frag.Vertices();
      std::priority_queue<std::pair<double, vertex_t>> heap;
      typename FRAG_T::template vertex_array_t<bool> modified(vertices, false);
      ctx.length[tid][s] = 0.0;
      heap.emplace(0, s);

      double distu, distv, ndistv;
      vertex_t v, u;
      while (!heap.empty()) {
        u = heap.top().second;
        distu = -heap.top().first;
        heap.pop();

        if (modified[u]) {
          continue;
        }
        modified[u] = true;

        auto es = frag.directed() ? frag.GetIncomingAdjList(u)
                                  : frag.GetOutgoingAdjList(u);
        for (auto& e : es) {
          v = e.get_neighbor();
          distv = ctx.length[tid][v];
          double edata = 1.0;
          vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
              [&](auto& e, auto& data) {
                data = static_cast<double>(e.get_data());
              })(e, edata);
          ndistv = distu + edata;
          if (distv > ndistv) {
            ctx.length[tid][v] = ndistv;
            heap.emplace(-ndistv, v);
          }
        }
      }
    }
  }

  void compute(const fragment_t& frag, vertex_t& u, context_t& ctx, int tid) {
    double tot_sp = 0.0;
    int connected_nodes_num = 0;
    int total_node_num = 0;
    auto vertices = frag.Vertices();
    double closeness_centrality = 0.0;
    for (auto& v : vertices) {
      if (ctx.length[tid][v] < std::numeric_limits<double>::max()) {
        tot_sp += ctx.length[tid][v];
        ++connected_nodes_num;
      }
      ++total_node_num;
    }
    if (tot_sp > 0 && total_node_num > 1) {
      closeness_centrality = (connected_nodes_num - 1.0) / tot_sp;
      if (ctx.wf_improve) {
        closeness_centrality *=
            ((connected_nodes_num - 1.0) / (total_node_num - 1));
      }
    }
    ctx.centrality[u] = closeness_centrality;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_CLOSENESS_CLOSENESS_CENTRALITY_H_
