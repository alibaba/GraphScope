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
#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_PARALLEL_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_PARALLEL_H_

#include <grape/grape.h>

#include "pagerank/pagerank_local_parallel_context.h"

namespace grape {

/**
 * @brief An implementation of PageRankParallel, which can work
 * on both directed and undirected graphs.
 *
 * @tparam FRAG_T
 */

template <typename FRAG_T>
class PageRankLocalParallel
    : public ParallelAppBase<FRAG_T, PageRankLocalParallelContext<FRAG_T>>,
      public ParallelEngine {
 public:
  using vertex_t = typename FRAG_T::vertex_t;

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr bool need_split_edges = true;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(PageRankLocalParallel<FRAG_T>,
                          PageRankLocalParallelContext<FRAG_T>, FRAG_T)
  PageRankLocalParallel() = default;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif
    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);

    ctx.step = 0;
    ForEach(inner_vertices, [&ctx, &frag, &messages](int tid, vertex_t u) {
      int EdgeNum = frag.GetOutgoingAdjList(u).Size();
      if (EdgeNum > 0) {
        ctx.result[u] = 1.0 / EdgeNum;
        messages.SendMsgThroughOEdges<fragment_t, double>(frag, u,
                                                          ctx.result[u], tid);
      } else {
        ctx.result[u] = 1.0;
      }
    });
    if (ctx.step < ctx.max_round) {
      messages.ForceContinue();
    }
#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
#endif
  }
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif
    ++ctx.step;

    if (ctx.step <= ctx.max_round) {
      ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
        double cur = 0;
        auto es = frag.GetIncomingInnerVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = cur;
      });
    }
#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.preprocess_time -= GetCurrentTime();
#endif

    {
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, double msg) { ctx.result[u] = msg; });
    }

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif
    if (ctx.step < ctx.max_round) {
      ForEach(inner_vertices, [&ctx, &frag, &messages](int tid, vertex_t u) {
        double cur = ctx.next_result[u];
        auto es = frag.GetIncomingOuterVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
        messages.SendMsgThroughOEdges<fragment_t, double>(
            frag, u, ctx.next_result[u], tid);
      });
      messages.ForceContinue();
    } else if (ctx.step == ctx.max_round) {
      ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
        double cur = ctx.next_result[u];
        auto es = frag.GetIncomingOuterVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
      });
    } else {
      auto& degree = ctx.degree;
      auto& result = ctx.result;

      for (auto v : inner_vertices) {
        if (degree[v] != 0) {
          result[v] *= degree[v];
        }
      }
      return;
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif
    ctx.result.Swap(ctx.next_result);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
};

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_PARALLEL_H_
