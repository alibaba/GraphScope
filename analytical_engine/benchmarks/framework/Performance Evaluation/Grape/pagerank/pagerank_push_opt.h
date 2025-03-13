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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_H_

#include <grape/grape.h>

#include "pagerank/pagerank_push_opt_context.h"

namespace grape {

/**
 * @brief An implementation of PageRank, which can work
 * on undirected graphs.
 *
 * This version of PageRank inherits BatchShuffleAppBase.
 * Messages are generated in batches and received in-place.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankPushOpt
    : public ParallelAppBase<FRAG_T, PageRankPushOptContext<FRAG_T>,
                             ParallelMessageManagerOpt>,
      public ParallelEngine,
      public Communicator {
 public:
  INSTALL_PARALLEL_OPT_WORKER(PageRankPushOpt<FRAG_T>,
                              PageRankPushOptContext<FRAG_T>, FRAG_T)

  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;

  static constexpr bool need_split_edges = true;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  PageRankPushOpt() = default;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    if (ctx.max_round <= 0) {
      return;
    }
    messages.InitChannels(thread_num(), 98304, 98304);

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    ctx.step = 0;
    ctx.graph_vnum = frag.GetTotalVerticesNum();
    vid_t dangling_vnum = 0;
    double p = 1.0 / ctx.graph_vnum;

    std::vector<vid_t> dangling_vnum_tid(thread_num(), 0);
    ForEach(inner_vertices,
            [&ctx, &frag, p, &dangling_vnum_tid](int tid, vertex_t u) {
              int EdgeNum = frag.GetLocalOutDegree(u);
              if (EdgeNum > 0) {
                ctx.result[u] = p / EdgeNum;
              } else {
                ++dangling_vnum_tid[tid];
                ctx.result[u] = p;
              }
            });

    for (auto vn : dangling_vnum_tid) {
      dangling_vnum += vn;
    }

    Sum(dangling_vnum, ctx.total_dangling_vnum);
    ctx.dangling_sum = p * ctx.total_dangling_vnum;

    auto& channels = messages.Channels();
    ForEach(outer_vertices, [&ctx, &frag, &channels](int tid, vertex_t u) {
      auto es = frag.GetIncomingAdjList(u);
      double msg = 0.0;
      for (auto& e : es) {
        msg += ctx.result[e.get_neighbor()];
      }
      channels[tid].SyncStateOnOuterVertex<fragment_t, double>(frag, u, msg);
    });
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();
    ++ctx.step;

    double base = (1.0 - ctx.delta) / ctx.graph_vnum +
                  ctx.delta * ctx.dangling_sum / ctx.graph_vnum;
    ctx.dangling_sum = base * ctx.total_dangling_vnum;

    ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
      double cur = 0;
      auto es = frag.GetOutgoingInnerVertexAdjList(u);
      for (auto& e : es) {
        cur += ctx.result[e.get_neighbor()];
      }
      ctx.next_result[u] = cur;
    });

    messages.ParallelProcess<fragment_t, double>(
        thread_num(), frag, [&ctx](int tid, vertex_t u, double msg) {
          atomic_add(ctx.next_result[u], msg);
        });

    if (ctx.step != ctx.max_round) {
      ForEach(inner_vertices, [&ctx, &frag, base](int tid, vertex_t u) {
        int en = frag.GetLocalOutDegree(u);
        ctx.result[u] =
            en > 0 ? (ctx.delta * ctx.next_result[u] + base) / en : base;
        ctx.next_result[u] = 0;
      });

      auto& channels = messages.Channels();
      ForEach(outer_vertices, [&ctx, &frag, &channels](int tid, vertex_t u) {
        auto es = frag.GetIncomingAdjList(u);
        double msg = 0.0;
        for (auto& e : es) {
          msg += ctx.result[e.get_neighbor()];
        }
        channels[tid].SyncStateOnOuterVertex<fragment_t, double>(frag, u, msg);
      });
      messages.ForceContinue();
    } else {
      ForEach(inner_vertices, [&ctx, &frag, base](int tid, vertex_t u) {
        int en = frag.GetLocalOutDegree(u);
        ctx.result[u] = en > 0 ? (ctx.delta * ctx.next_result[u] + base) : base;
      });
    }
  }

  void EstimateMessageSize(const fragment_t& frag, size_t& send_size,
                           size_t& recv_size) {
    send_size = frag.GetOuterVerticesNum();
    send_size *= (sizeof(vertex_t) + sizeof(double));
    recv_size = frag.GetInnerVerticesNum();
    recv_size *= ((sizeof(vertex_t) + sizeof(double)) * (frag.fnum() - 1));
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_PUSH_OPT_H_
