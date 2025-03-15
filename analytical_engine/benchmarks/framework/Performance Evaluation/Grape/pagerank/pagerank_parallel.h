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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PARALLEL_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PARALLEL_H_

#include <grape/grape.h>

#include "pagerank/pagerank_parallel_context.h"

namespace grape {

/**
 * @brief An implementation of PageRank, the version in LDBC, which can work
 * on both directed and undirected graphs.
 *
 * This version of PageRank inherits ParallelAppBase. Messages can be sent in
 * parallel with the evaluation process. This strategy improves performance by
 * overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */

template <typename FRAG_T>
class PageRankParallel
    : public ParallelAppBase<FRAG_T, PageRankParallelContext<FRAG_T>>,
      public Communicator,
      public ParallelEngine {
 public:
  using vertex_t = typename FRAG_T::vertex_t;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr bool need_split_edges = true;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(PageRankParallel<FRAG_T>,
                          PageRankParallelContext<FRAG_T>, FRAG_T)

  PageRankParallel() {}
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    size_t graph_vnum = frag.GetTotalVerticesNum();
    messages.InitChannels(thread_num());

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.step = 0;
    double p = 1.0 / graph_vnum;

    // assign initial ranks
    ForEach(inner_vertices, [&ctx, &frag, p, &messages](int tid, vertex_t u) {
      int EdgeNum = frag.GetOutgoingAdjList(u).Size();
      ctx.degree[u] = EdgeNum;
      if (EdgeNum > 0) {
        ctx.result[u] = p / EdgeNum;
        messages.SendMsgThroughOEdges<fragment_t, double>(frag, u,
                                                          ctx.result[u], tid);
      } else {
        ctx.result[u] = p;
      }
    });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    for (auto u : inner_vertices) {
      if (ctx.degree[u] == 0) {
        ++ctx.dangling_vnum;
      }
    }

    double dangling_sum = p * static_cast<double>(ctx.dangling_vnum);

    Sum(dangling_sum, ctx.dangling_sum);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    double dangling_sum = ctx.dangling_sum;

    size_t graph_vnum = frag.GetTotalVerticesNum();

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
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
    ctx.exec_time -= GetCurrentTime();
#endif

    double base =
        (1.0 - ctx.delta) / graph_vnum + ctx.delta * dangling_sum / graph_vnum;

    // pull ranks from neighbors
    ForEach(inner_vertices, [&ctx, base, &frag](int tid, vertex_t u) {
      if (ctx.degree[u] == 0) {
        ctx.next_result[u] = base;
      } else {
        double cur = 0;
        auto es = frag.GetIncomingInnerVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = cur;
      }
    });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.preprocess_time -= GetCurrentTime();
#endif

    // process received ranks sent by other workers
    {
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, const double& msg) {
            ctx.result[u] = msg;
          });
    }

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    // compute new ranks and send messages
    if (ctx.step != ctx.max_round) {
      ForEach(inner_vertices,
              [&ctx, base, &frag, &messages](int tid, vertex_t u) {
                if (ctx.degree[u] != 0) {
                  double cur = ctx.next_result[u];
                  auto es = frag.GetIncomingOuterVertexAdjList(u);
                  for (auto& e : es) {
                    cur += ctx.result[e.get_neighbor()];
                  }
                  cur = (ctx.delta * cur + base) / ctx.degree[u];
                  ctx.next_result[u] = cur;
                  messages.SendMsgThroughOEdges<fragment_t, double>(
                      frag, u, ctx.next_result[u], tid);
                }
              });
    } else {
      ForEach(inner_vertices, [&ctx, base, &frag](int tid, vertex_t u) {
        if (ctx.degree[u] != 0) {
          double cur = ctx.next_result[u];
          auto es = frag.GetIncomingOuterVertexAdjList(u);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          cur = (ctx.delta * cur + base) / ctx.degree[u];
          ctx.next_result[u] = cur;
        }
      });
    }

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    ctx.result.Swap(ctx.next_result);

    double new_dangling = base * static_cast<double>(ctx.dangling_vnum);

    Sum(new_dangling, ctx.dangling_sum);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
    messages.ForceContinue();
  }
};

}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PARALLEL_H_
