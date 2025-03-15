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
#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_H_

#include <grape/grape.h>

#include "pagerank/pagerank_local_context.h"

namespace grape {

/**
 * @brief An implementation of PageRankLocal, which can work
 * on both directed and undirected graphs.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankLocal
    : public BatchShuffleAppBase<FRAG_T, PageRankLocalContext<FRAG_T>>,
      public ParallelEngine {
 public:
  INSTALL_BATCH_SHUFFLE_WORKER(PageRankLocal<FRAG_T>,
                               PageRankLocalContext<FRAG_T>, FRAG_T)

  using vertex_t = typename FRAG_T::vertex_t;

  static constexpr bool need_split_edges = true;
  static constexpr bool need_split_edges_by_fragment = true;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  PageRankLocal() = default;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.step = 0;
    ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
      int EdgeNum = frag.GetLocalOutDegree(u);
      ctx.result[u] = EdgeNum > 0 ? 1.0 / EdgeNum : 1.0;
    });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    messages.SyncInnerVertices<fragment_t, double>(frag, ctx.result,
                                                   thread_num());
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;
    bool last_step = (ctx.step == ctx.max_round);

    if (ctx.avg_degree > 10) {
#ifdef PROFILING
      ctx.exec_time -= GetCurrentTime();
#endif
      if (frag.fnum() > 1) {
        ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
          double cur = 0;
          auto es = frag.GetOutgoingInnerVertexAdjList(u);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          ctx.next_result[u] = cur;
        });
      } else {
        ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
          double cur = 0;
          auto es = frag.GetOutgoingInnerVertexAdjList(u);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
        });
      }
#ifdef PROFILING
      ctx.exec_time += GetCurrentTime();
#endif

      for (fid_t i = 2; i < frag.fnum(); ++i) {
#ifdef PROFILING
        ctx.preprocess_time -= GetCurrentTime();
#endif
        fid_t src_fid = messages.UpdatePartialOuterVertices();
#ifdef PROFILING
        ctx.preprocess_time += GetCurrentTime();
        ctx.exec_time -= GetCurrentTime();
#endif
        ForEach(inner_vertices, [src_fid, &frag, &ctx](int tid, vertex_t u) {
          double cur = ctx.next_result[u];
          auto es = frag.GetOutgoingAdjList(u, src_fid);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          ctx.next_result[u] = cur;
        });
#ifdef PROFILING
        ctx.exec_time += GetCurrentTime();
#endif
      }
      if (frag.fnum() > 1) {
#ifdef PROFILING
        ctx.preprocess_time -= GetCurrentTime();
#endif
        fid_t src_fid = messages.UpdatePartialOuterVertices();
#ifdef PROFILING
        ctx.preprocess_time += GetCurrentTime();
        ctx.exec_time -= GetCurrentTime();
#endif
        if (last_step) {
          ForEach(inner_vertices, [src_fid, &frag, &ctx](int tid, vertex_t u) {
            double cur = ctx.next_result[u];
            auto es = frag.GetOutgoingAdjList(u, src_fid);
            for (auto& e : es) {
              cur += ctx.result[e.get_neighbor()];
            }
            ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
          });
        } else {
          ForEach(inner_vertices, [src_fid, &frag, &ctx](int tid, vertex_t u) {
            double cur = ctx.next_result[u];
            auto es = frag.GetOutgoingAdjList(u, src_fid);
            for (auto& e : es) {
              cur += ctx.result[e.get_neighbor()];
            }
            ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
            int en = frag.GetLocalOutDegree(u);
            ctx.next_result[u] =
                en > 0 ? ctx.next_result[u] / en : ctx.next_result[u];
          });
        }
#ifdef PROFILING
        ctx.exec_time += GetCurrentTime();
#endif
      }
    } else {
#ifdef PROFILING
      ctx.exec_time -= GetCurrentTime();
#endif
      ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
        double cur = 0;
        auto es = frag.GetOutgoingInnerVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = 1 - ctx.delta + ctx.delta * cur;
      });
#ifdef PROFILING
      ctx.exec_time += GetCurrentTime();
#endif
      for (fid_t i = 1; i < frag.fnum(); ++i) {
#ifdef PROFILING
        ctx.preprocess_time -= GetCurrentTime();
#endif
        fid_t src_fid = messages.UpdatePartialOuterVertices();
#ifdef PROFILING
        ctx.preprocess_time += GetCurrentTime();
        ctx.exec_time -= GetCurrentTime();
#endif
        ForEach(frag.OuterVertices(src_fid),
                [&frag, &ctx](int tid, vertex_t u) {
                  double cur = ctx.result[u] * ctx.delta;
                  auto es = frag.GetIncomingAdjList(u);
                  for (auto& e : es) {
                    atomic_add(ctx.next_result[e.get_neighbor()], cur);
                  }
                });
#ifdef PROFILING
        ctx.exec_time += GetCurrentTime();
#endif
      }

      if (!last_step) {
        ForEach(inner_vertices, [&frag, &ctx](int tid, vertex_t u) {
          int en = frag.GetLocalOutDegree(u);
          if (en > 0) {
            ctx.next_result[u] /= en;
          }
        });
      }
    }
#ifdef PROFILING
    ctx.postprocess_time -= GetCurrentTime();
#endif
    if (!last_step) {
      messages.SyncInnerVertices<fragment_t, double>(frag, ctx.next_result,
                                                     thread_num());
    }

    ctx.result.Swap(ctx.next_result);
#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_LOCAL_H_
