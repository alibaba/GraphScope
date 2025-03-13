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

#ifndef EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_H_
#define EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_H_

#include <queue>
#include <utility>
#include <vector>

#include <grape/grape.h>

#include "wcc/wcc_auto_context.h"

namespace grape {

/**
 * @brief WCCAuto application, determines the weakly connected component each
 * vertex belongs to, which only works on both undirected graph.
 *
 * This is the auto-parallel version inherit AutoAppBase. In this version, users
 * plug sequential algorithms for PEval and IncEval, libgrape-lite parallelizes
 * them in the distributed setting. Users are not aware of messages.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class WCCAuto : public AutoAppBase<FRAG_T, WCCAutoContext<FRAG_T>> {
 public:
  INSTALL_AUTO_WORKER(WCCAuto<FRAG_T>, WCCAutoContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    typename FRAG_T::template inner_vertex_array_t<bool> visited(inner_vertices,
                                                                 false);
    std::vector<vertex_t> outers;
    typename FRAG_T::template outer_vertex_array_t<bool> outer_visited(
        outer_vertices, false);

    vid_t comp_id = 0;

    vertex_t u, v;
    for (const auto& x : inner_vertices) {
      if (visited[x]) {
        continue;
      }
      visited[x] = true;
      std::queue<vertex_t> que;
      que.push(x);

#ifdef WCC_USE_GID
      auto min_id = frag.GetInnerVertexGid(x);
#else
      auto min_id = frag.GetInnerVertexId(x);
#endif

      for (; !que.empty(); que.pop()) {
        u = que.front();
        ctx.local_comp_id[u] = comp_id;
        auto es = frag.GetOutgoingAdjList(u);
        auto e = es.begin();
        while (e != es.end()) {
          v = e->get_neighbor();
          ++e;
          if (frag.IsInnerVertex(v)) {
            if (!visited[v]) {
#ifdef WCC_USE_GID
              min_id = std::min(min_id, frag.GetInnerVertexGid(v));
#else
              min_id = std::min(min_id, frag.GetInnerVertexId(v));
#endif
              que.push(v);
              visited[v] = true;
            }
          } else {
            if (!outer_visited[v]) {
#ifdef WCC_USE_GID
              min_id = std::min(min_id, frag.GetOuterVertexGid(v));
#else
              min_id = std::min(min_id, frag.GetOuterVertexId(v));
#endif
              outers.push_back(v);
              outer_visited[v] = true;
            }
          }
        }
        auto es2 = frag.GetIncomingAdjList(u);
        e = es2.begin();
        while (e != es2.end()) {
          v = e->get_neighbor();
          ++e;
          if (frag.IsInnerVertex(v)) {
            if (!visited[v]) {
#ifdef WCC_USE_GID
              min_id = std::min(min_id, frag.GetInnerVertexGid(v));
#else
              min_id = std::min(min_id, frag.GetInnerVertexId(v));
#endif
              que.push(v);
              visited[v] = true;
            }
          } else {
            if (!outer_visited[v]) {
#ifdef WCC_USE_GID
              min_id = std::min(min_id, frag.GetOuterVertexGid(v));
#else
              min_id = std::min(min_id, frag.GetOuterVertexId(v));
#endif
              outers.push_back(v);
              outer_visited[v] = true;
            }
          }
        }
      }
      comp_id++;
      ctx.global_comp_id.push_back(min_id);

      for (auto& p : outers) {
        ctx.global_cluster_id.SetValue(
            p, std::min(min_id, ctx.global_cluster_id.GetValue(p)));
        outer_visited[p] = false;
      }
      ctx.outer_vertices.emplace_back(std::move(outers));
    }

    for (auto& v : inner_vertices) {
      ctx.global_cluster_id.SetValue(v,
                                     ctx.global_comp_id[ctx.local_comp_id[v]]);
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();

    std::vector<bool> updated(ctx.outer_vertices.size(), false);

    for (auto& v : inner_vertices) {
      if (ctx.global_cluster_id.IsUpdated(v)) {
        auto tag = ctx.global_cluster_id.GetValue(v);
        vid_t comp_id = ctx.local_comp_id[v];
        if (ctx.global_comp_id[comp_id] > tag) {
          ctx.global_comp_id[comp_id] = tag;
          updated[comp_id] = true;
        }
      }
    }

    for (vid_t comp_id = 0; comp_id < ctx.outer_vertices.size(); comp_id++) {
      if (updated[comp_id]) {
        auto tag = ctx.global_comp_id[comp_id];

        for (auto v : ctx.outer_vertices[comp_id]) {
          ctx.global_cluster_id.SetValue(
              v, std::min(ctx.global_cluster_id.GetValue(v), tag));
        }
      }
    }

    for (auto& v : inner_vertices) {
      ctx.global_cluster_id.SetValue(v,
                                     ctx.global_comp_id[ctx.local_comp_id[v]]);
    }
  }
};

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_WCC_WCC_AUTO_H_
