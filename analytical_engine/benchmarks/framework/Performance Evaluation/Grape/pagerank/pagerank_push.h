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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_H_

#include <grape/grape.h>

#include "pagerank/pagerank_push_context.h"

namespace grape {

template <typename FRAG_T>
class PageRankPush
    : public ParallelAppBase<FRAG_T, PageRankPushContext<FRAG_T>>,
      public ParallelEngine,
      public Communicator {
 public:
  INSTALL_PARALLEL_WORKER(PageRankPush<FRAG_T>, PageRankPushContext<FRAG_T>,
                          FRAG_T)

  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kSyncOnOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  PageRankPush() = default;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    if (ctx.max_round <= 0) {
      return;
    }

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    auto inner_vertices = frag.InnerVertices();

    ctx.step = 0;
    ctx.graph_vnum = frag.GetTotalVerticesNum();
    vid_t dangling_vnum = 0;
    double p = 1.0 / ctx.graph_vnum;

    std::vector<vid_t> dangling_vnum_tid(thread_num(), 0);
    ForEach(inner_vertices,
            [&ctx, &frag, p, &dangling_vnum_tid](int tid, vertex_t u) {
              int EdgeNum = frag.GetLocalOutDegree(u);
              ctx.degree[u] = EdgeNum;
              if (EdgeNum > 0) {
                ctx.result[u] = p;
                double msg = p / EdgeNum;
                for (auto& e : frag.GetOutgoingAdjList(u)) {
                  atomic_add(ctx.next_result[e.get_neighbor()], msg);
                }
              } else {
                ++dangling_vnum_tid[tid];
                ctx.result[u] = p;
              }
            });

    for (auto vn : dangling_vnum_tid) {
      dangling_vnum += vn;
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    Sum(dangling_vnum, ctx.total_dangling_vnum);
    ctx.dangling_sum = p * ctx.total_dangling_vnum;

    auto outer_vertices = frag.OuterVertices();
    ForEach(outer_vertices, [&ctx, &messages, &frag](int tid, vertex_t u) {
      messages.SyncStateOnOuterVertex<fragment_t, double>(
          frag, u, ctx.next_result[u], tid);
      ctx.next_result[u] = 0;
    });

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
#ifdef PROFILING
    ctx.preprocess_time -= GetCurrentTime();
#endif
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;

    double base = (1.0 - ctx.delta) / ctx.graph_vnum +
                  ctx.delta * ctx.dangling_sum / ctx.graph_vnum;
    ctx.dangling_sum = base * ctx.total_dangling_vnum;

    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, double msg) {
          atomic_add(ctx.next_result[u], msg);
        });

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    if (ctx.step == ctx.max_round) {
      ForEach(inner_vertices, [&ctx, base](int tid, vertex_t v) {
        ctx.result[v] = base + ctx.delta * ctx.next_result[v];
      });
#ifdef PROFILING
      ctx.exec_time += GetCurrentTime();
#endif
    } else {
      ForEach(inner_vertices, [&ctx, base](int tid, vertex_t v) {
        ctx.result[v] = base + ctx.delta * ctx.next_result[v];
        ctx.next_result[v] = 0;
      });

      ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
        int EdgeNum = ctx.degree[u];
        if (EdgeNum > 0) {
          double msg = ctx.result[u] / EdgeNum;
          for (auto& e : frag.GetOutgoingAdjList(u)) {
            atomic_add(ctx.next_result[e.get_neighbor()], msg);
          }
        }
      });
#ifdef PROFILING
      ctx.exec_time += GetCurrentTime();
      ctx.postprocess_time -= GetCurrentTime();
#endif

      auto outer_vertices = frag.OuterVertices();
      ForEach(outer_vertices, [&ctx, &messages, &frag](int tid, vertex_t u) {
        messages.SyncStateOnOuterVertex<fragment_t, double>(
            frag, u, ctx.next_result[u], tid);
        ctx.next_result[u] = 0;
      });

      messages.ForceContinue();
#ifdef PROFILING
      ctx.postprocess_time += GetCurrentTime();
#endif
    }
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_H_
