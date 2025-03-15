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

#ifndef EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_BFS_H_
#define EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_BFS_H_

#include <grape/grape.h>

#include "bc/bc_context.h"

namespace grape {

/**
 * @brief An implementation of BFS.
 *
 * In this version, the Breadth-First Search (BFS) serves as a preprocessing
 * step for the centrality calculation. It not only computes the distances from
 * each node to the starting point but also calculates the number of shortest
 * paths from the starting point.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class StagedBCBFS : public ParallelAppBase<FRAG_T, BCContext<FRAG_T>,
                                           ParallelMessageManagerOpt>,
                    public ParallelEngine {
  INSTALL_PARALLEL_OPT_WORKER(StagedBCBFS<FRAG_T>, BCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    ctx.current_depth = 1;

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

    auto inner_vertices = frag.InnerVertices();

    // init double buffer which contains updated vertices using bitmap
    ctx.curr_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.next_inner_updated.Init(inner_vertices, GetThreadPool());

    ctx.path_num.Init(frag.Vertices(), 0);
    ctx.partial_result.Init(frag.Vertices(),
                            std::numeric_limits<depth_type>::max());

    if (frag.fnum() == 1) {
      if (!native_source) {
        return;
      }
      {
        ctx.partial_result[source] = 0;
        ctx.path_num[source] = 1;
        auto oes = frag.GetOutgoingAdjList(source);
        for (auto& e : oes) {
          auto u = e.get_neighbor();
          if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
            ctx.partial_result[u] = 1;
            ctx.path_num[u] = 1;
            ctx.curr_inner_updated.Insert(u);
          }
        }
        while (!ctx.curr_inner_updated.Empty()) {
          depth_type next_depth = ctx.current_depth + 1;
          ForEach(ctx.curr_inner_updated,
                  [&frag, &ctx, next_depth](int tid, vertex_t v) {
                    auto oes = frag.GetOutgoingAdjList(v);
                    double pn = ctx.path_num[v];
                    for (auto& e : oes) {
                      auto u = e.get_neighbor();
                      if (ctx.partial_result[u] ==
                          std::numeric_limits<depth_type>::max()) {
                        atomic_add(ctx.path_num[u], pn);
                        ctx.partial_result[u] = next_depth;
                        ctx.next_inner_updated.Insert(u);
                      } else if (ctx.partial_result[u] == next_depth) {
                        atomic_add(ctx.path_num[u], pn);
                      }
                    }
                  });
          ctx.curr_inner_updated.Swap(ctx.next_inner_updated);
          ctx.next_inner_updated.ParallelClear(GetThreadPool());
          ctx.current_depth = next_depth;
        }
      }
    } else {
      auto outer_vertices = frag.OuterVertices();
      ctx.outer_updated.Init(outer_vertices, GetThreadPool());

      messages.InitChannels(thread_num());
      auto& channel_0 = messages.Channels()[0];

      // run first round BFS, update unreached vertices
      if (native_source) {
        ctx.partial_result[source] = 0;
        ctx.path_num[source] = 1;
        auto oes = frag.GetOutgoingAdjList(source);
        for (auto& e : oes) {
          auto u = e.get_neighbor();
          if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
            ctx.partial_result[u] = 1;
            ctx.path_num[u] = 1;
            if (frag.IsOuterVertex(u)) {
              channel_0.template SyncStateOnOuterVertex<fragment_t, double>(
                  frag, u, 1);
            } else {
              ctx.curr_inner_updated.Insert(u);
            }
          }
        }
      }

      messages.ForceContinue();
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    auto& channels = messages.Channels();

    depth_type next_depth = ctx.current_depth + 1;
    ctx.next_inner_updated.ParallelClear(GetThreadPool());

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t v, double pn) {
          if (ctx.partial_result[v] == std::numeric_limits<depth_type>::max()) {
            ctx.partial_result[v] = ctx.current_depth;
            atomic_add(ctx.path_num[v], pn);
            ctx.curr_inner_updated.Insert(v);
          } else if (ctx.partial_result[v] == ctx.current_depth) {
            atomic_add(ctx.path_num[v], pn);
          }
        });

    // sync messages to other workers
    ForEach(ctx.curr_inner_updated, [next_depth, &frag, &ctx, &channels](
                                        int tid, vertex_t v) {
      auto oes = frag.GetOutgoingAdjList(v);
      double pn = ctx.path_num[v];
      for (auto& e : oes) {
        auto u = e.get_neighbor();
        if (ctx.partial_result[u] == std::numeric_limits<depth_type>::max()) {
          atomic_add(ctx.path_num[u], pn);
          ctx.partial_result[u] = next_depth;
          if (frag.IsOuterVertex(u)) {
            ctx.outer_updated.Insert(u);
          } else {
            ctx.next_inner_updated.Insert(u);
          }
        } else if (ctx.partial_result[u] == next_depth) {
          atomic_add(ctx.path_num[u], pn);
        }
      }
    });

    ForEach(ctx.outer_updated, [&frag, &ctx, &channels](int tid, vertex_t v) {
      channels[tid].template SyncStateOnOuterVertex<fragment_t, double>(
          frag, v, ctx.path_num[v]);
    });
    ctx.outer_updated.ParallelClear(GetThreadPool());

    ctx.current_depth = next_depth;
    if (!ctx.next_inner_updated.Empty()) {
      messages.ForceContinue();
    }

    ctx.next_inner_updated.Swap(ctx.curr_inner_updated);
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_BFS_H_
