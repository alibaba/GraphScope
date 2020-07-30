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

#ifndef ANALYTICAL_ENGINE_BENCHMARKS_APPS_PAGERANK_PAGERANK_H_
#define ANALYTICAL_ENGINE_BENCHMARKS_APPS_PAGERANK_PAGERANK_H_

#include <iomanip>
#include <limits>

#include "grape/grape.h"

namespace gs {

namespace benchmarks {

template <typename FRAG_T>
class PageRankContext : public grape::VertexDataContext<FRAG_T, double> {
 public:
  using vid_t = typename FRAG_T::vid_t;

  explicit PageRankContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, double>(fragment, true),
        result(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, double delta,
            int max_round) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();
    this->delta = delta;
    this->max_round = max_round;
    degree.Init(inner_vertices, 0);
    result.Init(vertices, 0.0);
    next_result.Init(vertices);
    step = 0;
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (degree[v] == 0) {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << result[v] << std::endl;
      } else {
        os << frag.GetId(v) << " " << std::scientific << std::setprecision(15)
           << result[v] * degree[v] << std::endl;
      }
    }
    /*
        for (auto v : inner_vertices) {
          os << frag.GetId(v) << " " << result[v] << std::endl;
        }
    */
  }

  typename FRAG_T::template vertex_array_t<int> degree;
  typename FRAG_T::template vertex_array_t<double>& result;
  typename FRAG_T::template vertex_array_t<double> next_result;

  vid_t dangling_vnum = 0;
  int step = 0;
  int max_round = 0;
  double delta = 0;

  double dangling_sum = 0.0;
};

template <typename FRAG_T>
class PageRank : public grape::ParallelAppBase<FRAG_T, PageRankContext<FRAG_T>>,
                 public grape::ParallelEngine,
                 public grape::Communicator {
 public:
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(PageRank<FRAG_T>, PageRankContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    size_t graph_vnum = frag.GetTotalVerticesNum();
    messages.InitChannels(thread_num());

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

    for (auto u : inner_vertices) {
      if (ctx.degree[u] == 0) {
        ++ctx.dangling_vnum;
      }
    }

    double dangling_sum = p * static_cast<double>(ctx.dangling_vnum);

    Sum(dangling_sum, ctx.dangling_sum);

    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    double dangling_sum = ctx.dangling_sum;

    size_t graph_vnum = frag.GetTotalVerticesNum();

    ++ctx.step;
    if (ctx.step > ctx.max_round) {
      return;
    }

    double base =
        (1.0 - ctx.delta) / graph_vnum + ctx.delta * dangling_sum / graph_vnum;

    // process received ranks sent by other workers
    {
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, const double& msg) {
            ctx.result[u] = msg;
          });
    }

    // compute new ranks and send messages
    if (ctx.step != ctx.max_round) {
      ForEach(inner_vertices,
              [&ctx, base, &frag, &messages](int tid, vertex_t u) {
                if (ctx.degree[u] == 0) {
                  ctx.next_result[u] = base;
                } else {
                  double cur = 0;
                  auto es = frag.GetIncomingAdjList(u);
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
        if (ctx.degree[u] == 0) {
          ctx.next_result[u] = base;
        } else {
          double cur = 0;
          auto es = frag.GetIncomingAdjList(u);
          for (auto& e : es) {
            cur += ctx.result[e.get_neighbor()];
          }
          cur = (ctx.delta * cur + base) / ctx.degree[u];
          ctx.next_result[u] = cur;
        }
      });
    }

    ctx.result.Swap(ctx.next_result);

    double new_dangling = base * static_cast<double>(ctx.dangling_vnum);

    Sum(new_dangling, ctx.dangling_sum);

    messages.ForceContinue();
  }
};

}  // namespace benchmarks

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_BENCHMARKS_APPS_PAGERANK_PAGERANK_H_
