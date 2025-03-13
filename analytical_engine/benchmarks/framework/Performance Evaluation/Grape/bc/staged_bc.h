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

#ifndef EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_H_
#define EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_H_

#include <grape/grape.h>

#include "bc/bc_context.h"

namespace grape {

/**
 * @brief An implementation of BC(betweeness centrality), which can work
 * on undirected graph.
 *
 * This version of Betweenness Centrality (BC) requires the prior execution of
 * StagedBCBFS, whose convergence is controlled by the message manager,
 * eliminating the need for additional blocking global synchronization.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class StagedBC : public ParallelAppBase<FRAG_T, BCContext<FRAG_T>,
                                        ParallelMessageManagerOpt>,
                 public ParallelEngine {
  INSTALL_PARALLEL_OPT_WORKER(StagedBC<FRAG_T>, BCContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;
    auto inner_vertices = frag.InnerVertices();

    if (frag.fnum() == 1) {
      depth_type curr_depth = ctx.current_depth - 1;
      ForEach(inner_vertices, [&frag, &ctx, curr_depth](int tid, vertex_t v) {
        if (ctx.partial_result[v] == curr_depth) {
          float accum = static_cast<float>(1) / ctx.path_num[v];
          auto es = frag.GetOutgoingAdjList(v);
          for (auto& e : es) {
            auto u = e.get_neighbor();
            if (ctx.partial_result[u] == curr_depth - 1) {
              atomic_add(ctx.centrality_value[u], accum);
            }
          }
        }
      });
      --curr_depth;
      while (curr_depth > 0) {
        ForEach(inner_vertices, [&frag, &ctx, curr_depth](int tid, vertex_t v) {
          if (ctx.partial_result[v] == curr_depth) {
            ctx.centrality_value[v] *= ctx.path_num[v];
            float accum = static_cast<float>(1) / ctx.path_num[v] *
                          (1.0 + ctx.centrality_value[v]);
            auto es = frag.GetOutgoingAdjList(v);
            for (auto& e : es) {
              auto u = e.get_neighbor();
              if (ctx.partial_result[u] == curr_depth - 1) {
                atomic_add(ctx.centrality_value[u], accum);
              }
            }
          }
        });
        --curr_depth;
      }
    } else {
      messages.InitChannels(thread_num(), 32768, 32768);
      auto& channels = messages.Channels();

      ForEach(inner_vertices, [&frag, &ctx, &channels](int tid, vertex_t v) {
        channels[tid].SendMsgThroughOEdges<fragment_t, depth_type>(
            frag, v, ctx.partial_result[v]);
      });

      messages.ForceContinue();
      ctx.stage = 0;
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using depth_type = typename context_t::depth_type;
    auto& channels = messages.Channels();
    if (ctx.stage == 0) {
      ctx.stage = 1;
      messages.ParallelProcess<fragment_t, depth_type>(
          thread_num(), frag, [&ctx](int tid, vertex_t v, depth_type msg) {
            ctx.partial_result[v] = msg;
          });
      depth_type curr_depth = ctx.current_depth - ctx.stage;
      auto inner_vertices = frag.InnerVertices();

      ForEach(inner_vertices, [&frag, &ctx, curr_depth](int tid, vertex_t v) {
        if (ctx.partial_result[v] == curr_depth) {
          float accum = static_cast<float>(1) / ctx.path_num[v];
          auto es = frag.GetOutgoingAdjList(v);
          for (auto& e : es) {
            auto u = e.get_neighbor();
            if (ctx.partial_result[u] == curr_depth - 1) {
              atomic_add(ctx.centrality_value[u], accum);
              if (frag.IsOuterVertex(u)) {
                ctx.outer_updated.Insert(u);
              }
            }
          }
        }
      });

      ForEach(ctx.outer_updated, [&frag, &ctx, &channels](int tid, vertex_t v) {
        channels[tid].SyncStateOnOuterVertex<fragment_t, float>(
            frag, v, ctx.centrality_value[v]);
      });
      ctx.outer_updated.ParallelClear(GetThreadPool());

      if (curr_depth > 0) {
        messages.ForceContinue();
      }
    } else {
      depth_type cur_depth = ctx.current_depth - ctx.stage;
      messages.ParallelProcess<fragment_t, float>(
          thread_num(), frag,
          [&ctx, cur_depth](int tid, vertex_t v, float msg) {
            CHECK_EQ(ctx.partial_result[v], cur_depth);
            atomic_add(ctx.centrality_value[v], msg);
          });

      if (cur_depth > 0) {
        ForEach(frag.InnerVertices(),
                [&frag, &ctx, cur_depth](int tid, vertex_t v) {
                  if (ctx.partial_result[v] == cur_depth) {
                    ctx.centrality_value[v] *= ctx.path_num[v];
                    float accum = static_cast<float>(1) / ctx.path_num[v] *
                                  (1.0 + ctx.centrality_value[v]);
                    auto es = frag.GetOutgoingAdjList(v);
                    for (auto& e : es) {
                      auto u = e.get_neighbor();
                      if (ctx.partial_result[u] == cur_depth - 1) {
                        atomic_add(ctx.centrality_value[u], accum);
                        if (frag.IsOuterVertex(u)) {
                          ctx.outer_updated.Insert(u);
                        }
                      }
                    }
                  }
                });

        ForEach(ctx.outer_updated,
                [&frag, &ctx, &channels, cur_depth](int tid, vertex_t v) {
                  CHECK(ctx.partial_result[v] == cur_depth - 1);
                  channels[tid].SyncStateOnOuterVertex<fragment_t, float>(
                      frag, v, ctx.centrality_value[v]);
                });
        ctx.outer_updated.ParallelClear(GetThreadPool());

        messages.ForceContinue();
      }
    }
    ++ctx.stage;
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_BC_STAGED_BC_H_
