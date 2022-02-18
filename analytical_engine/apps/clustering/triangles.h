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

#ifndef ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_H_
#define ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_H_

#include <vector>

#include "grape/grape.h"

#include "clustering/triangles_context.h"

namespace gs {
/**
 * @brief Compute the number of triangles in the graph
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class Triangles
    : public grape::ParallelAppBase<FRAG_T, TrianglesContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(Triangles<FRAG_T>, TrianglesContext<FRAG_T>, FRAG_T);
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    messages.InitChannels(thread_num());
    ctx.stage = 0;
    ForEach(inner_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      ctx.global_degree[v] = frag.GetLocalOutDegree(v);
      messages.SendMsgThroughOEdges<fragment_t, int>(frag, v,
                                                     ctx.global_degree[v], tid);
    });
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    if (ctx.stage == 0) {
      ctx.stage = 1;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, int msg) { ctx.global_degree[u] = msg; });

      ForEach(inner_vertices, [&frag, &ctx, &messages](int tid, vertex_t v) {
        vid_t u_gid, v_gid;
        auto& nbr_vec = ctx.complete_neighbor[v];
        int degree = ctx.global_degree[v];
        nbr_vec.reserve(degree);
        auto es = frag.GetOutgoingAdjList(v);
        std::vector<vid_t> msg_vec;
        msg_vec.reserve(degree);
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.global_degree[u] < ctx.global_degree[v]) {
            nbr_vec.push_back(u);
            msg_vec.push_back(frag.Vertex2Gid(u));
          } else if (ctx.global_degree[u] == ctx.global_degree[v]) {
            u_gid = frag.Vertex2Gid(u);
            v_gid = frag.GetInnerVertexGid(v);
            if (v_gid > u_gid) {
              nbr_vec.push_back(u);
              msg_vec.push_back(u_gid);
            }
          }
        }
        messages.SendMsgThroughOEdges<fragment_t, std::vector<vid_t>>(
            frag, v, msg_vec, tid);
      });
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages.ParallelProcess<fragment_t, std::vector<vid_t>>(
          thread_num(), frag,
          [&frag, &ctx](int tid, vertex_t u, const std::vector<vid_t>& msg) {
            auto& nbr_vec = ctx.complete_neighbor[u];
            for (auto gid : msg) {
              vertex_t v;
              if (frag.Gid2Vertex(gid, v)) {
                nbr_vec.push_back(v);
              }
            }
          });

      std::vector<grape::DenseVertexSet<typename FRAG_T::vertices_t>>
          vertexsets(thread_num());

      ForEach(
          inner_vertices,
          [&vertexsets, &frag](int tid) {
            auto& ns = vertexsets[tid];
            ns.Init(frag.Vertices());
          },
          [&vertexsets, &ctx](int tid, vertex_t v) {
            auto& v0_nbr_set = vertexsets[tid];
            auto& v0_nbr_vec = ctx.complete_neighbor[v];
            for (auto u : v0_nbr_vec) {
              v0_nbr_set.Insert(u);
            }
            for (auto u : v0_nbr_vec) {
              auto& v1_nbr_vec = ctx.complete_neighbor[u];
              for (auto w : v1_nbr_vec) {
                if (v0_nbr_set.Exist(w)) {
                  grape::atomic_add(ctx.tricnt[u], 1);
                  grape::atomic_add(ctx.tricnt[v], 1);
                  grape::atomic_add(ctx.tricnt[w], 1);
                }
              }
            }
            for (auto u : v0_nbr_vec) {
              v0_nbr_set.Erase(u);
            }
          },
          [](int tid) {});

      ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
        if (ctx.tricnt[v] != 0) {
          messages.SyncStateOnOuterVertex<fragment_t, int>(frag, v,
                                                           ctx.tricnt[v], tid);
        }
      });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, int deg) {
            grape::atomic_add(ctx.tricnt[u], deg);
          });
    } else {
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag, [](int tid, vertex_t u, int) {});
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CLUSTERING_TRIANGLES_H_
