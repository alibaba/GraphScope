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

#ifndef EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_AUTO_H_
#define EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_AUTO_H_

#include <queue>
#include <utility>

#include <grape/grape.h>

#include "sssp/sssp_auto_context.h"

namespace grape {

/**
 * @brief SSSP application, determines the length of the shortest paths from a
 * given source vertex to all other vertices in graphs, which can work
 * on both directed and undirected graph.
 *
 * This is the auto-parallel version inherits from AutoAppBase. In this version,
 * users plug sequential algorithms for PEval and IncEval, and libgrape-lite
 * automatically parallelizes them in the distributed setting. Users are not
 * aware of messages.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SSSPAuto : public AutoAppBase<FRAG_T, SSSPAutoContext<FRAG_T>> {
 public:
  // specialize the templated worker.
  INSTALL_AUTO_WORKER(SSSPAuto<FRAG_T>, SSSPAutoContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

 private:
  // sequential Dijkstra algorithm for SSSP.
  void Dijkstra(const fragment_t& frag, context_t& ctx,
                std::priority_queue<std::pair<double, vertex_t>>& heap) {
    {
      auto inner_vertices = frag.InnerVertices();
      VertexArray<typename FRAG_T::inner_vertices_t, bool> modified(
          inner_vertices, false);

      double distu, distv, ndistv;
      vertex_t v, u;

      while (!heap.empty()) {
        u = heap.top().second;
        distu = -heap.top().first;
        heap.pop();

        // since we are going to relax from u by the
        // current partial_result of u, we reset its
        // updated state back to false
        ctx.partial_result.Reset(u);

        if (modified[u]) {
          continue;
        }
        modified[u] = true;

        auto es = frag.GetOutgoingAdjList(u);
        for (auto& e : es) {
          v = e.get_neighbor();
          distv = ctx.partial_result[v];
          ndistv = distu + e.get_data();
          if (distv > ndistv) {
            ctx.partial_result.SetValue(v, ndistv);
            if (frag.IsInnerVertex(v)) {
              heap.emplace(-ndistv, v);
            }
          }
        }
      }
    }
  }

 public:
  /**
   * @brief Parital evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   */
  void PEval(const fragment_t& frag, context_t& ctx) {
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

    std::priority_queue<std::pair<double, vertex_t>> heap;

    if (native_source) {
      ctx.partial_result.SetValue(source, 0.0);
      heap.emplace(0, source);
    }

    Dijkstra(frag, ctx, heap);
  }

  /**
   * @brief Incremental evaluation for SSSP.
   *
   * @param frag
   * @param ctx
   */
  void IncEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();

    std::priority_queue<std::pair<double, vertex_t>> heap;

    for (auto& v : inner_vertices) {
      if (ctx.partial_result.IsUpdated(v)) {
        heap.emplace(-ctx.partial_result.GetValue(v), v);
      }
    }

    Dijkstra(frag, ctx, heap);
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_SSSP_SSSP_AUTO_H_
