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

#ifndef EXAMPLES_ANALYTICAL_APPS_BC_BC_H_
#define EXAMPLES_ANALYTICAL_APPS_BC_BC_H_

#include <grape/grape.h>

#include "bc/bc_context.h"

namespace grape {

/**
 * @brief An implementation of BC(betweeness centrality), which can work
 * on undirected graph.
 *
 * In this version Breadth-First Search (BFS) and centrality calculations will
 * be executed sequentially. During the first phase, global synchronization is
 * required in each round to determine whether the BFS has converged. When the
 * number of rounds is substantial, the overhead introduced by synchronization
 * becomes notably significant.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class BC : public ParallelAppBase<FRAG_T, BCContext<FRAG_T>,
                                  ParallelMessageManagerOpt>,
           public ParallelEngine,
           public Communicator {
  INSTALL_PARALLEL_OPT_WORKER(BC<FRAG_T>, BCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    messages.InitChannels(thread_num());

    ctx.current_depth = 1;

    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    // init double buffer which contains updated vertices using bitmap
    ctx.curr_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.next_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.outer_updated.Init(outer_vertices, GetThreadPool());

    ctx.path_num.Init(frag.Vertices(), 0);
    ctx.partial_result.Init(frag.Vertices(),
                            std::numeric_limits<depth_type>::max());

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
            channel_0.template SyncStateOnOuterVertex<fragment_t, double>(frag,
                                                                          u, 1);
          } else {
            ctx.curr_inner_updated.Insert(u);
          }
        }
      }
    }

    messages.ForceContinue();
    ctx.stage = 0;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;

    auto& channels = messages.Channels();

    if (ctx.stage == 0) {
      depth_type next_depth = ctx.current_depth + 1;
      ctx.next_inner_updated.ParallelClear(GetThreadPool());

      // process received messages and update depth
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), frag, [&ctx](int tid, vertex_t v, double pn) {
            if (ctx.partial_result[v] ==
                std::numeric_limits<depth_type>::max()) {
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

      int status = 0;
      if (!ctx.outer_updated.Empty()) {
        status = 1;

        ForEach(
            ctx.outer_updated, [&frag, &ctx, &channels](int tid, vertex_t v) {
              channels[tid].template SyncStateOnOuterVertex<fragment_t, double>(
                  frag, v, ctx.path_num[v]);
            });
        ctx.outer_updated.ParallelClear(GetThreadPool());
      } else if (!ctx.next_inner_updated.Empty()) {
        status = 1;
      }
      ctx.current_depth = next_depth;
      ctx.next_inner_updated.Swap(ctx.curr_inner_updated);

      int global_status = 0;
      Sum<int>(status, global_status);

      if (global_status == 0) {
        depth_type curr_depth = ctx.current_depth - ctx.stage;
        auto inner_vertices = frag.InnerVertices();

        ForEach(inner_vertices, [&ctx, curr_depth, &frag](int tid, vertex_t v) {
          if (ctx.partial_result[v] == curr_depth) {
            float accum = static_cast<float>(1) / ctx.path_num[v];
            auto es = frag.GetOutgoingAdjList(v);
            for (auto& e : es) {
              auto u = e.get_neighbor();
              if (frag.IsInnerVertex(u)) {
                if (ctx.partial_result[u] == curr_depth - 1) {
                  atomic_add(ctx.centrality_value[u], accum);
                }
              } else {
                atomic_add(ctx.centrality_value[u], accum);
                ctx.outer_updated.Insert(u);
              }
            }
          }
        });

        ForEach(ctx.outer_updated,
                [&frag, &ctx, &channels](int tid, vertex_t v) {
                  channels[tid].SyncStateOnOuterVertex<fragment_t, float>(
                      frag, v, ctx.centrality_value[v]);
                  ctx.centrality_value[v] = 0;
                });
        ctx.outer_updated.ParallelClear(GetThreadPool());
        ctx.stage = 1;
      }

      messages.ForceContinue();
    } else {
      depth_type curr_depth = ctx.current_depth - ctx.stage;

      messages.ParallelProcess<fragment_t, float>(
          thread_num(), frag,
          [&ctx, curr_depth](int tid, vertex_t v, float bc) {
            if (ctx.partial_result[v] == curr_depth) {
              atomic_add(ctx.centrality_value[v], bc);
            }
          });
      if (curr_depth > 0) {
        auto inner_vertices = frag.InnerVertices();

        ForEach(inner_vertices, [&ctx, curr_depth, &frag](int tid, vertex_t v) {
          if (ctx.partial_result[v] == curr_depth) {
            ctx.centrality_value[v] *= ctx.path_num[v];
            float accum = static_cast<float>(1) / ctx.path_num[v] *
                          (1.0 + ctx.centrality_value[v]);
            auto es = frag.GetOutgoingAdjList(v);
            for (auto& e : es) {
              auto u = e.get_neighbor();
              if (frag.IsInnerVertex(u)) {
                if (ctx.partial_result[u] == curr_depth - 1) {
                  atomic_add(ctx.centrality_value[u], accum);
                }
              } else {
                atomic_add(ctx.centrality_value[u], accum);
                ctx.outer_updated.Insert(u);
              }
            }
          }
        });

        ForEach(ctx.outer_updated,
                [&frag, &ctx, &channels](int tid, vertex_t v) {
                  channels[tid].SyncStateOnOuterVertex<fragment_t, float>(
                      frag, v, ctx.centrality_value[v]);
                  ctx.centrality_value[v] = 0;
                });
        ctx.outer_updated.ParallelClear(GetThreadPool());
        messages.ForceContinue();
      }

      ++ctx.stage;
    }
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BC_BC_H_
