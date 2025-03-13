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
#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_DIRECTED_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_DIRECTED_H_

#include <grape/grape.h>

#include "pagerank/pagerank_context.h"

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
class PageRankDirected
    : public BatchShuffleAppBase<FRAG_T, PageRankContext<FRAG_T>>,
      public ParallelEngine,
      public Communicator {
 public:
  INSTALL_BATCH_SHUFFLE_WORKER(PageRankDirected<FRAG_T>,
                               PageRankContext<FRAG_T>, FRAG_T)

  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;

  static constexpr bool need_split_edges = true;
  static constexpr bool need_split_edges_by_fragment = true;
  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  PageRankDirected() = default;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    if (ctx.max_round <= 0) {
      return;
    }

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

    messages.SyncInnerVertices<fragment_t, double>(frag, ctx.result,
                                                   thread_num());
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;

    double base = (1.0 - ctx.delta) / ctx.graph_vnum +
                  ctx.delta * ctx.dangling_sum / ctx.graph_vnum;

    std::vector<double> dangling_sums(thread_num(), 0);
    if (ctx.avg_degree > 10 && frag.fnum() > 1) {
      // If fragment is dense and there are multiple fragments, receiving
      // messages is overlapped with computation. Receiving and computing
      // procedures are be splitted into multiple rounds. In each round,
      // messages from a fragment are received and then processed.
      ForEach(inner_vertices, [&ctx, &frag](int tid, vertex_t u) {
        double cur = 0;
        auto es = frag.GetIncomingInnerVertexAdjList(u);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        ctx.next_result[u] = cur;
      });

      for (fid_t i = 2; i < frag.fnum(); ++i) {
        fid_t src_fid = messages.UpdatePartialOuterVertices();
        ForEach(inner_vertices, [src_fid, &frag, &ctx](int tid, vertex_t u) {
          double cur = ctx.next_result[u];
          auto es = frag.GetIncomingAdjList(u, src_fid);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          ctx.next_result[u] = cur;
        });
      }

      fid_t src_fid = messages.UpdatePartialOuterVertices();
      ForEach(inner_vertices, [src_fid, &frag, &ctx, base, &dangling_sums](
                                  int tid, vertex_t u) {
        double cur = ctx.next_result[u];
        auto es = frag.GetIncomingAdjList(u, src_fid);
        for (auto& e : es) {
          cur += ctx.result[e.get_neighbor()];
        }
        int en = frag.GetLocalOutDegree(u);
        cur = ctx.delta * cur + base;
        if (en == 0) {
          dangling_sums[tid] += cur;
          ctx.next_result[u] = cur;
        } else {
          ctx.next_result[u] = cur / en;
        }
      });
    } else {
      // If the fragment is sparse or there is only one fragment, one round of
      // iterating inner vertices is prefered.
      messages.UpdateOuterVertices();
      ForEach(inner_vertices,
              [&ctx, &frag, base, &dangling_sums](int tid, vertex_t u) {
                double cur = 0;
                auto es = frag.GetIncomingAdjList(u);
                for (auto& e : es) {
                  cur += ctx.result[e.get_neighbor()];
                }
                int en = frag.GetLocalOutDegree(u);
                cur = ctx.delta * cur + base;
                if (en == 0) {
                  dangling_sums[tid] += cur;
                  ctx.next_result[u] = cur;
                } else {
                  ctx.next_result[u] = cur / en;
                }
              });
    }

    ctx.result.Swap(ctx.next_result);

    if (ctx.step != ctx.max_round) {
      double frag_dangling_sum = 0;
      for (auto vn : dangling_sums) {
        frag_dangling_sum += vn;
      }
      Sum(frag_dangling_sum, ctx.dangling_sum);

      messages.SyncInnerVertices<fragment_t, double>(frag, ctx.result,
                                                     thread_num());
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
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_DIRECTED_H_
