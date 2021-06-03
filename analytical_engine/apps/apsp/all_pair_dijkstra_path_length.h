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

#ifndef ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIR_DIJKSTRA_PATH_LENGTH_H_
#define ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIR_DIJKSTRA_PATH_LENGTH_H_

#ifdef NETWORKX

#include <limits>
#include <map>
#include <queue>
#include <tuple>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/apsp/all_pair_dijkstra_path_length_context.h"
#include "core/utils/app_utils.h"

namespace gs {

/**
 * @brief Compute the average shortest path length in a *connected* graph.
 * Average shortest path length is average of all sssp length of (source = v,
 * target = u), where v, u is any vertex in graph. Note that this algorithm is
 * time consuming.
 * */
template <typename FRAG_T>
class AllPairDijkstraPathLength
    : public grape::ParallelAppBase<FRAG_T,
                                    AllPairDijkstraPathLengthContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(AllPairDijkstraPathLength<FRAG_T>,
                          AllPairDijkstraPathLengthContext<FRAG_T>, FRAG_T)
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
    ForEach(
        inner_vertices, [&frag, &ctx, &vertices, this](int tid, vertex_t v) {
          ctx.length[v].Init(vertices, std::numeric_limits<double>::max());
          this->dijkstraLength(frag, v, ctx);
          ctx.ret[v] = folly::dynamic::array;
          for (auto& dst : vertices) {
            if (ctx.length[v][dst] < std::numeric_limits<double>::max()) {
              ctx.ret[v].push_back(
                  folly::dynamic::array(frag.GetId(dst), ctx.length[v][dst]));
            }
          }
          ctx.length[v].Clear();
        });
    ctx.length.Clear();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    return;
  }

 private:
  // sequential Dijkstra length algorithm for SSSP.
  void dijkstraLength(const fragment_t& frag, vertex_t& s, context_t& ctx) {
    {
      auto vertices = frag.Vertices();
      std::priority_queue<std::pair<double, vertex_t>> heap;
      typename FRAG_T::template vertex_array_t<bool> modified(vertices, false);
      ctx.length[s][s] = 0.0;
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

        auto es = frag.GetOutgoingAdjList(u);
        for (auto& e : es) {
          v = e.get_neighbor();
          distv = ctx.length[s][v];
          double edata = 1.0;
          static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
              [&](auto& e, auto& data) {
                data = static_cast<double>(e.get_data());
              })(e, edata);
          ndistv = distu + edata;
          if (distv > ndistv) {
            ctx.length[s][v] = ndistv;
            heap.emplace(-ndistv, v);
          }
        }
      }
    }
  }
};

}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_APPS_APSP_ALL_PAIR_DIJKSTRA_PATH_LENGTH_H_
