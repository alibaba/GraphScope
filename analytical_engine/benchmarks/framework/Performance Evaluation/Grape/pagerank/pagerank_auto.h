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

#ifndef EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_AUTO_H_
#define EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_AUTO_H_

#include <grape/grape.h>

#include "pagerank/pagerank_auto_context.h"

namespace grape {
/**
 * @brief An implementation of PageRank without using explicit message-passing
 * APIs, the version in LDBC, which can work on both directed and undirected
 * graphs.
 *
 * This is the auto-parallel version inherited from AutoAppBase. In this
 * version, users plug sequential algorithms for PEval and IncEval, and
 * libgrape-lite automatically parallelizes them in the distributed setting.
 * Users are not aware of messages.
 *
 *  @tparam FRAG_T
 */
template <typename FRAG_T>
class PageRankAuto : public AutoAppBase<FRAG_T, PageRankAutoContext<FRAG_T>>,
                     public Communicator {
 public:
  INSTALL_AUTO_WORKER(PageRankAuto<FRAG_T>, PageRankAutoContext<FRAG_T>, FRAG_T)
  using vertex_t = typename FRAG_T::vertex_t;

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();

    size_t graph_vnum = frag.GetTotalVerticesNum();

    ctx.step = 0;
    double dangling_sum = 0;
    double p = 1.0 / graph_vnum;

    // assign initial ranks
    for (auto& u : inner_vertices) {
      int EdgesNum = frag.GetOutgoingAdjList(u).Size();
      ctx.degree[u] = EdgesNum;

      if (EdgesNum > 0) {
        ctx.results.SetValue(u, p / EdgesNum);
      } else {
        ctx.results.SetValue(u, p);
        dangling_sum += p;
      }
    }

    // aggregate dangling sum
    Sum(dangling_sum, ctx.dangling_sum);
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();

    double dangling_sum = ctx.dangling_sum;

    typename FRAG_T::template inner_vertex_array_t<double> next_results;
    next_results.Init(inner_vertices);

    size_t graph_vnum = frag.GetTotalVerticesNum();

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      auto& degree = ctx.degree;
      auto& results = ctx.results;

      for (auto v : inner_vertices) {
        if (degree[v] != 0) {
          results[v] *= degree[v];
        }
      }
      return;
    }

    double new_dangling = 0.0;
    double base =
        (1.0 - ctx.delta) / graph_vnum + ctx.delta * dangling_sum / graph_vnum;

    for (auto& u : inner_vertices) {
      // Get all incoming message's total
      if (ctx.degree[u] == 0) {
        next_results[u] = base;
        new_dangling += base;
      } else {
        double cur = 0;
        auto es = frag.GetIncomingAdjList(u);
        for (auto& e : es) {
          cur += ctx.results[e.get_neighbor()];
        }
        cur = (ctx.delta * cur + base) / ctx.degree[u];
        next_results[u] = cur;
      }
    }

    for (auto u : inner_vertices) {
      ctx.results.SetValue(u, next_results[u]);
    }

    Sum(new_dangling, ctx.dangling_sum);
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_PAGERANK_PAGERANK_AUTO_H_
