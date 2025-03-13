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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_H_

#include <grape/grape.h>

#include "lcc/lcc_auto_context.h"

namespace grape {

/**
 * @brief An implementation of LCC (Local CLustering Coefficient), without
 *        using explicit message-passing APIs, which only works on
 *        undirected graphs.
 *
 * This is the auto-parallel version inherit AutoAppBase. In this version, users
 * plug sequential algorithms for PEval and IncEval, libgrape-lite parallelizes
 * them in the distributed setting. Users are not aware of messages.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class LCCAuto : public AutoAppBase<FRAG_T, LCCAutoContext<FRAG_T>> {
 public:
  INSTALL_AUTO_WORKER(LCCAuto<FRAG_T>, LCCAutoContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();

    ctx.stage = 0;

    for (auto v : inner_vertices) {
      ctx.global_degree.SetValue(v, frag.GetLocalOutDegree(v));
    }
    // At the end of PEval, the message_manager will automatically sync
    // state with other works.
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    if (ctx.stage == 0) {
      ctx.stage = 1;

      for (auto v : inner_vertices) {
        auto v_gid = frag.GetInnerVertexGid(v);
        auto& nbr_vec = ctx.complete_neighbor[v];
        {
          auto es = frag.GetOutgoingAdjList(v);
          for (auto& e : es) {
            auto u = e.get_neighbor();
            auto u_gid = frag.Vertex2Gid(u);
            if (ctx.global_degree[u] < ctx.global_degree[v] ||
                (ctx.global_degree[u] == ctx.global_degree[v] &&
                 v_gid > u_gid)) {
              nbr_vec.push_back(u_gid);
            }
          }
        }
        ctx.complete_neighbor.SetUpdated(v);
      }
    } else if (ctx.stage == 1) {
      ctx.stage = 2;

      typename FRAG_T::template vertex_array_t<bool> v0_nbr_set(vertices,
                                                                false);
      for (auto v : inner_vertices) {
        auto& v0_nbr_vec = ctx.complete_neighbor[v];
        for (auto u_gid : v0_nbr_vec) {
          vertex_t u;
          if (frag.Gid2Vertex(u_gid, u)) {
            v0_nbr_set[u] = true;
          }
        }
        for (auto u_gid : v0_nbr_vec) {
          vertex_t u;
          if (frag.Gid2Vertex(u_gid, u)) {
            auto& v1_nbr_vec = ctx.complete_neighbor[u];
            for (auto w_gid : v1_nbr_vec) {
              vertex_t w;
              if (frag.Gid2Vertex(w_gid, w)) {
                if (v0_nbr_set[w]) {
                  ++ctx.tricnt[u];
                  ++ctx.tricnt[v];
                  ++ctx.tricnt[w];
                }
              }
            }
          }
        }
        for (auto u_gid : v0_nbr_vec) {
          vertex_t u;
          if (frag.Gid2Vertex(u_gid, u)) {
            v0_nbr_set[u] = false;
          }
        }
      }

      for (auto v : outer_vertices) {
        if (ctx.tricnt[v] != 0) {
          ctx.tricnt.SetUpdated(v);
        }
      }
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      auto& global_degree = ctx.global_degree;
      auto& tricnt = ctx.tricnt;
      auto& ctx_data = ctx.data();

      for (auto v : inner_vertices) {
        if (global_degree[v] == 0 || global_degree[v] == 1) {
          ctx_data[v] = 0;
        } else {
          double re = 2.0 * (tricnt[v]) /
                      (static_cast<int64_t>(global_degree[v]) *
                       (static_cast<int64_t>(global_degree[v]) - 1));
          ctx_data[v] = re;
        }
      }
    }
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_AUTO_H_
