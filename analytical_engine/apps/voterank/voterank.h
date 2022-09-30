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

#ifndef ANALYTICAL_ENGINE_APPS_VOTERANK_VOTERANK_H_
#define ANALYTICAL_ENGINE_APPS_VOTERANK_VOTERANK_H_

#include <algorithm>
#include <tuple>
#include <vector>

#include "grape/grape.h"

#include "voterank/voterank_context.h"

namespace gs {

/**
 * @brief An implementation of VoteRank, the version in LDBC, which can work
 * on both directed and undirected graphs.
 *
 * This version of VoteRank inherits ParallelAppBase. Messages can be sent in
 * parallel with the evaluation process. This strategy improves performance by
 * overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */

template <typename FRAG_T>
class VoteRank : public grape::ParallelAppBase<FRAG_T, VoteRankContext<FRAG_T>>,
                 public grape::Communicator,
                 public grape::ParallelEngine {
 public:
  using vertex_t = typename FRAG_T::vertex_t;
  using vid_t = typename FRAG_T::vid_t;
  using oid_t = typename FRAG_T::oid_t;
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex;
  static constexpr bool need_split_edges = true;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  INSTALL_PARALLEL_WORKER(VoteRank<FRAG_T>, VoteRankContext<FRAG_T>, FRAG_T)

  VoteRank() {}
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    size_t graph_vnum = frag.GetTotalVerticesNum();
    messages.InitChannels(thread_num());

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    ctx.step = 0;

    // assign initial ranks and weights
    std::vector<size_t> edgeNums(thread_num(), 0);
    ForEach(inner_vertices,
            [&ctx, &edgeNums, &frag, &messages](int tid, vertex_t u) {
              edgeNums[tid] += frag.GetOutgoingAdjList(u).Size();
              ctx.rank[u] = 0;
              ctx.weight[u] = 1.0;
              ctx.scores[u] = 0.0;
              ctx.update.Insert(u);
              messages.SendMsgThroughIEdges<fragment_t, double>(
                  frag, u, ctx.weight[u], tid);
            });

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif

    size_t sumEdgeNum = 0;
    for (auto i : edgeNums) {
      sumEdgeNum += i;
    }
    Sum(sumEdgeNum, sumEdgeNum);

    ctx.avg_degree =
        static_cast<double>(sumEdgeNum) / static_cast<double>(graph_vnum);

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    ++ctx.step;
    if (ctx.step > ctx.num_of_nodes) {
      return;
    }

#ifdef PROFILING
    ctx.exec_time -= GetCurrentTime();
#endif

    // process received weights sent by other workers
    {
      messages.ParallelProcess<fragment_t, double>(
          thread_num(), frag,
          [&ctx, &frag](int tid, vertex_t u, const double& msg) {
            ctx.weight[u] = msg;
            auto es = frag.GetIncomingAdjList(u);
            for (auto& e : es) {
              auto v = e.get_neighbor();
              if (frag.IsInnerVertex(v)) {
                ctx.update.Insert(v);
              }
            }
          });
    }

#ifdef PROFILING
    ctx.preprocess_time += GetCurrentTime();
    ctx.exec_time -= GetCurrentTime();
#endif

    auto compare = [](std::tuple<double, size_t, vid_t>& lhs,
                      const std::tuple<double, size_t, vid_t>& rhs) {
      const double EPS = 1e-8;
      if (fabs(std::get<0>(lhs) - std::get<0>(rhs)) < EPS) {
        if (std::get<1>(rhs) < std::get<1>(lhs)) {
          lhs = rhs;
        }
      } else if (std::get<0>(lhs) < std::get<0>(rhs)) {
        lhs = rhs;
      }
    };

    //  pull weights from neighbors and compute new scores
    std::vector<std::tuple<double, size_t, vid_t>> max_scores(thread_num(),
                                                              {0, 0, {}});
    ForEach(inner_vertices,
            [&ctx, compare, &max_scores, &frag](int tid, vertex_t u) {
              if (ctx.update.Exist(u) && ctx.rank[u] == 0) {
                double cur = 0;
                auto es = frag.GetOutgoingAdjList(u);
                for (auto& e : es) {
                  cur += ctx.weight[e.get_neighbor()];
                }
                ctx.scores[u] = cur;
              }
              compare(max_scores[tid],
                      {ctx.scores[u], std::hash<oid_t>()(frag.GetId(u)),
                       frag.Vertex2Gid(u)});
            });

    ctx.update.Clear();

#ifdef PROFILING
    ctx.exec_time += GetCurrentTime();
    ctx.postprocess_time -= GetCurrentTime();
#endif
    for (auto& a : max_scores) {
      compare(ctx.max_score, a);
    }
    auto max_score = ctx.max_score;
    // select top node
    AllReduce(max_score, ctx.max_score, compare);
    const double EPS = 1e-8;
    if (std::get<0>(ctx.max_score) < EPS) {
      return;
    }

    // weaken the selected node and its out-neighbors
    vertex_t v;

    std::vector<vertex_t> update_vertices;
    if (frag.Gid2Vertex(std::get<2>(ctx.max_score), v)) {
      if (frag.IsInnerVertex(v)) {
        ctx.rank[v] = ctx.step;
        ctx.weight[v] = 0.0;
        ctx.scores[v] = 0.0;
        update_vertices.emplace_back(v);
      }
      auto es = frag.GetOutgoingAdjList(v);
      for (auto& e : es) {
        auto u = e.get_neighbor();
        if (frag.IsInnerVertex(u)) {
          ctx.weight[u] -= 1 / ctx.avg_degree;
          ctx.weight[u] = std::max(ctx.weight[u], 0.0);
          update_vertices.emplace_back(u);
        }
      }
    }

    // send message
    if (ctx.step != ctx.num_of_nodes) {
      ForEach(update_vertices, [&ctx, &frag, &messages](int tid, vertex_t u) {
        messages.SendMsgThroughIEdges<fragment_t, double>(frag, u,
                                                          ctx.weight[u], tid);
        auto es = frag.GetIncomingAdjList(u);
        for (auto& e : es) {
          auto v = e.get_neighbor();
          if (frag.IsInnerVertex(v)) {
            ctx.update.Insert(v);
          }
        }
      });
    }

    ctx.max_score = {0.0, 0, {}};

#ifdef PROFILING
    ctx.postprocess_time += GetCurrentTime();
#endif
    messages.ForceContinue();
  }
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_APPS_VOTERANK_VOTERANK_H_
