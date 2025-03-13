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

#ifndef EXAMPLES_ANALYTICAL_APPS_CORE_DECOMPOSITION_CORE_DECOMPOSITION_H_
#define EXAMPLES_ANALYTICAL_APPS_CORE_DECOMPOSITION_CORE_DECOMPOSITION_H_

#include <grape/grape.h>

#include "core_decomposition/core_decomposition_context.h"

namespace grape {

/**
 * @brief An implementation of core-decomposition, which can work on undirected
 * graph.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class CoreDecomposition
    : public ParallelAppBase<FRAG_T, CoreDecompositionContext<FRAG_T>,
                             ParallelMessageManagerOpt>,
      public ParallelEngine,
      public Communicator {
  INSTALL_PARALLEL_OPT_WORKER(CoreDecomposition<FRAG_T>,
                              CoreDecompositionContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    ctx.level = 1;
    ctx.curr_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.next_inner_updated.Init(inner_vertices, GetThreadPool());
    ctx.reduced_degrees.Init(frag.Vertices(), 0);

    std::atomic<size_t> empty_vertices(0);
    ForEach(inner_vertices,
            [&frag, &ctx, &empty_vertices](int tid, vertex_t v) {
              ctx.partial_result[v] = frag.GetLocalOutDegree(v);
              if (ctx.partial_result[v] == 0) {
                ++empty_vertices;
              } else if (ctx.partial_result[v] == 1) {
                ctx.curr_inner_updated.Insert(v);
              }
            });
    ctx.remaining = frag.GetInnerVerticesNum() - empty_vertices;
    if (frag.fnum() == 1) {
      while (ctx.remaining != 0) {
        while (!ctx.curr_inner_updated.Empty()) {
          ctx.remaining -=
              ctx.curr_inner_updated.ParallelCount(GetThreadPool());
          ForEach(ctx.curr_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
            for (auto& e : frag.GetOutgoingAdjList(v)) {
              auto u = e.get_neighbor();
              atomic_add(ctx.reduced_degrees[u], 1);
              ctx.next_inner_updated.Insert(u);
            }
          });
          ctx.curr_inner_updated.ParallelClear(GetThreadPool());

          ForEach(ctx.next_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
            if (ctx.partial_result[v] > ctx.level) {
              int new_core = ctx.partial_result[v] - ctx.reduced_degrees[v];
              ctx.reduced_degrees[v] = 0;
              if (new_core <= ctx.level) {
                ctx.partial_result[v] = ctx.level;
                ctx.curr_inner_updated.Insert(v);
              } else {
                ctx.partial_result[v] = new_core;
              }
            }
          });
          ctx.next_inner_updated.ParallelClear(GetThreadPool());
        }
        ctx.level++;
        ForEach(inner_vertices, [&ctx](int tid, vertex_t v) {
          if (ctx.partial_result[v] == ctx.level) {
            ctx.curr_inner_updated.Insert(v);
          }
        });
      }
    } else {
      messages.InitChannels(thread_num());
      ctx.outer_updated.Init(frag.OuterVertices(), GetThreadPool());

      while (!ctx.curr_inner_updated.Empty()) {
        ctx.remaining -= ctx.curr_inner_updated.ParallelCount(GetThreadPool());
        ForEach(ctx.curr_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
          for (auto& e : frag.GetOutgoingAdjList(v)) {
            auto u = e.get_neighbor();
            atomic_add(ctx.reduced_degrees[u], 1);
            if (frag.IsOuterVertex(u)) {
              ctx.outer_updated.Insert(u);
            } else {
              ctx.next_inner_updated.Insert(u);
            }
          }
        });
        ctx.curr_inner_updated.ParallelClear(GetThreadPool());

        ForEach(ctx.next_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
          if (ctx.partial_result[v] > ctx.level) {
            int new_core = ctx.partial_result[v] - ctx.reduced_degrees[v];
            ctx.reduced_degrees[v] = 0;
            if (new_core <= ctx.level) {
              ctx.partial_result[v] = ctx.level;
              ctx.curr_inner_updated.Insert(v);
            } else {
              ctx.partial_result[v] = new_core;
            }
          }
        });
        ctx.next_inner_updated.ParallelClear(GetThreadPool());
      }

      auto& channels = messages.Channels();
      ForEach(ctx.outer_updated, [&frag, &ctx, &channels](int tid, vertex_t v) {
        channels[tid].SyncStateOnOuterVertex<fragment_t, int>(
            frag, v, ctx.reduced_degrees[v]);
        ctx.reduced_degrees[v] = 0;
      });
      ctx.outer_updated.ParallelClear(GetThreadPool());
      messages.ForceContinue();
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    messages.ParallelProcess<fragment_t, int>(
        thread_num(), frag, [&ctx](int tid, vertex_t v, int msg) {
          atomic_add(ctx.reduced_degrees[v], msg);
          ctx.next_inner_updated.Insert(v);
        });

    ForEach(ctx.next_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
      if (ctx.partial_result[v] > ctx.level) {
        int new_core = ctx.partial_result[v] - ctx.reduced_degrees[v];
        ctx.reduced_degrees[v] = 0;
        if (new_core <= ctx.level) {
          ctx.partial_result[v] = ctx.level;
          ctx.curr_inner_updated.Insert(v);
        } else {
          ctx.partial_result[v] = new_core;
        }
      }
    });
    ctx.next_inner_updated.ParallelClear(GetThreadPool());

    int self_state;
    if (ctx.curr_inner_updated.Empty()) {
      // vote to finish this level
      self_state = 0;
    } else {
      // this level is not finished
      self_state = 1;
    }
    int total_state;
    Sum(self_state, total_state);

    if (total_state == 0) {
      if (ctx.remaining == 0) {
        self_state = 0;
      } else {
        self_state = 1;
      }

      Sum(self_state, total_state);
      if (total_state == 0) {
        size_t total_k = 0;
        for (auto v : frag.InnerVertices()) {
          total_k += ctx.partial_result[v];
        }
        size_t global_total = 0;
        Sum(total_k, global_total);
        if (frag.fid() == 0) {
          LOG(INFO) << "Total k: " << global_total;
        }
        return;
      } else {
        ctx.level++;

        ForEach(frag.InnerVertices(), [&ctx](int tid, vertex_t v) {
          if (ctx.partial_result[v] == ctx.level) {
            ctx.curr_inner_updated.Insert(v);
          }
        });
      }
    }

    while (!ctx.curr_inner_updated.Empty()) {
      ctx.remaining -= ctx.curr_inner_updated.ParallelCount(GetThreadPool());
      ForEach(ctx.curr_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
        for (auto& e : frag.GetOutgoingAdjList(v)) {
          auto u = e.get_neighbor();
          atomic_add(ctx.reduced_degrees[u], 1);
          if (frag.IsOuterVertex(u)) {
            ctx.outer_updated.Insert(u);
          } else {
            ctx.next_inner_updated.Insert(u);
          }
        }
      });
      ctx.curr_inner_updated.ParallelClear(GetThreadPool());

      ForEach(ctx.next_inner_updated, [&frag, &ctx](int tid, vertex_t v) {
        if (ctx.partial_result[v] > ctx.level) {
          int new_core = ctx.partial_result[v] - ctx.reduced_degrees[v];
          ctx.reduced_degrees[v] = 0;
          if (new_core <= ctx.level) {
            ctx.partial_result[v] = ctx.level;
            ctx.curr_inner_updated.Insert(v);
          } else {
            ctx.partial_result[v] = new_core;
          }
        }
      });
      ctx.next_inner_updated.ParallelClear(GetThreadPool());
    }

    auto& channels = messages.Channels();
    ForEach(ctx.outer_updated, [&frag, &ctx, &channels](int tid, vertex_t v) {
      channels[tid].SyncStateOnOuterVertex<fragment_t, int>(
          frag, v, ctx.reduced_degrees[v]);
      ctx.reduced_degrees[v] = 0;
    });
    ctx.outer_updated.ParallelClear(GetThreadPool());

    messages.ForceContinue();
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_CORE_DECOMPOSITION_CORE_DECOMPOSITION_H_
