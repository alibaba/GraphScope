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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_H_

#include <vector>

#include "grape/grape.h"

#include "apps/centrality/katz/katz_centrality_context.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"

namespace gs {
/**
 * @brief The Katz centrality of a vertex is a measure of centrality in a
 * graph. It is used to measure the relative degree of influence of an actor
 * within a social network.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class KatzCentrality
    : public grape::ParallelAppBase<FRAG_T, KatzCentralityContext<FRAG_T>>,
      public grape::ParallelEngine,
      public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(KatzCentrality<FRAG_T>, KatzCentralityContext<FRAG_T>,
                          FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using edata_t = typename fragment_t::edata_t;
  using vid_t = typename FRAG_T::vid_t;

  bool CheckTerm(const fragment_t& frag, context_t& ctx, int thrd_num) {
    auto inner_vertices = frag.InnerVertices();
    std::vector<double> thread_local_sum(thrd_num, 0.0);
    std::vector<double> thread_local_delta_sum(thrd_num, 0.0);
    double local_sum = 0.0, local_delta_sum = 0.0;
    double global_sum = 0, global_delta_sum = 0;

    ForEach(inner_vertices.begin(), inner_vertices.end(),
            [&thread_local_sum, &thread_local_delta_sum, &ctx](int tid,
                                                               vertex_t v) {
              thread_local_sum[tid] += ctx.x[v] * ctx.x[v];
              thread_local_delta_sum[tid] +=
                  std::fabs(ctx.x[v] - ctx.x_last[v]);
            });

    for (int tid = 0; tid < thrd_num; tid++) {
      local_sum += thread_local_sum[tid];
      local_delta_sum += thread_local_delta_sum[tid];
    }

    Sum(local_sum, global_sum);
    Sum(local_delta_sum, global_delta_sum);

    VLOG(1) << "[step - " << ctx.curr_round << " ] Diff: " << global_delta_sum;
    if (global_delta_sum < frag.GetTotalVerticesNum() * ctx.tolerance ||
        ctx.curr_round >= ctx.max_round) {
      VLOG(1) << "Katz terminates after " << ctx.curr_round
              << " iterations. Diff: " << global_delta_sum;
      ctx.global_sum = global_sum;
      return true;
    }
    return false;
  }

  void pullAndSend(const fragment_t& frag, context_t& ctx,
                   message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    if (frag.directed()) {
      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [this, &ctx, &frag, &messages](int tid, vertex_t v) {
            if (!filterByDegree(frag, ctx, v)) {
              auto es = frag.GetIncomingAdjList(v);
              ctx.x[v] = 0;
              for (auto& e : es) {
                // do the multiplication y^T = Alpha * x^T A - Beta
                double edata = 1.0;
                vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
                    [&](auto& e, auto& data) {
                      data = static_cast<double>(e.get_data());
                    })(e, edata);
                ctx.x[v] += ctx.x_last[e.get_neighbor()] * edata;
              }
              ctx.x[v] = ctx.x[v] * ctx.alpha + ctx.beta;
              messages.Channels()[tid].SendMsgThroughOEdges(frag, v, ctx.x[v]);
            }
          });
    } else {
      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [this, &ctx, &frag, &messages](int tid, vertex_t v) {
            if (!filterByDegree(frag, ctx, v)) {
              auto es = frag.GetOutgoingAdjList(v);
              ctx.x[v] = 0;
              for (auto& e : es) {
                // do the multiplication y^T = Alpha * x^T A - Beta
                double edata = 1.0;
                vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
                    [&](auto& e, auto& data) {
                      data = static_cast<double>(e.get_data());
                    })(e, edata);
                ctx.x[v] += ctx.x_last[e.get_neighbor()] * edata;
              }
              ctx.x[v] = ctx.x[v] * ctx.alpha + ctx.beta;
              messages.Channels()[tid].SendMsgThroughOEdges(frag, v, ctx.x[v]);
            }
          });
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    pullAndSend(frag, ctx, messages);

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }
    ctx.curr_round++;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    int thrd_num = thread_num();
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;

    // we only sum inner vertices, so put CheckTerm before GetMessage is fine
    if (CheckTerm(frag, ctx, thrd_num)) {
      auto global_sum = ctx.global_sum;

      CHECK_GT(global_sum, 0);
      if (ctx.normalized) {
        auto inner_vertices = frag.InnerVertices();
        double s = 1.0 / std::sqrt(global_sum);
        ForEach(inner_vertices.begin(), inner_vertices.end(),
                [&x, &s](int tid, vertex_t v) { x[v] *= s; });
      }
      return;
    }
    messages.ParallelProcess<fragment_t, double>(
        thrd_num, frag, [&x](int tid, vertex_t v, double msg) { x[v] = msg; });
    x_last.Swap(x);

    pullAndSend(frag, ctx, messages);

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }
    ctx.curr_round++;
  }

  bool filterByDegree(const fragment_t& frag, context_t& ctx, vertex_t v) {
    int degree = frag.GetLocalOutDegree(v);
    if (frag.directed()) {
      degree += frag.GetLocalInDegree(v);
    }
    if (degree > ctx.degree_threshold) {
      return true;
    }
    return false;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_H_
