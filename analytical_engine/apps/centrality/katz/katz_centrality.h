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

#include "grape/grape.h"

#include "apps/centrality/katz/katz_centrality_context.h"

#include "core/app/app_base.h"
#include "core/utils/app_utils.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief The Katz centrality of a vertex is a measure of centrality in a
 * graph. It is used to measure the relative degree of influence of an actor
 * within a social network.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class KatzCentrality : public AppBase<FRAG_T, KatzCentralityContext<FRAG_T>>,
                       public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(KatzCentrality<FRAG_T>, KatzCentralityContext<FRAG_T>,
                         FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using edata_t = typename fragment_t::edata_t;
  using vid_t = typename FRAG_T::vid_t;

  bool CheckTerm(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();
    double frag_sum = 0.0, frag_delta_sum = 0.0;

    for (auto& v : inner_vertices) {
      frag_sum += ctx.x[v] * ctx.x[v];
      frag_delta_sum += std::fabs(ctx.x[v] - ctx.x_last[v]);
    }

    double total_sum = 0, total_delta_sum = 0;
    Sum(frag_sum, total_sum);
    Sum(frag_delta_sum, total_delta_sum);

    VLOG(1) << "[step - " << ctx.curr_round << " ] Diff: " << total_delta_sum;
    if (total_delta_sum < frag.GetTotalVerticesNum() * ctx.tolerance ||
        ctx.curr_round >= ctx.max_round) {
      VLOG(1) << "Katz terminates after " << ctx.curr_round
              << " iterations. Diff: " << total_delta_sum;
      ctx.total_sum = total_sum;
      return true;
    }
    return false;
  }

  void pullAndSend(const fragment_t& frag, context_t& ctx,
                   message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;

    for (auto& v : inner_vertices) {
      auto es = frag.GetIncomingAdjList(v);
      x[v] = 0;
      for (auto& e : es) {
        // do the multiplication y^T = Alpha * x^T A - Beta
        double edata = 1.0;
        static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
            [&](auto& e, auto& data) {
              data = static_cast<double>(e.get_data());
            })(e, edata);
        x[v] += x_last[e.get_neighbor()] * edata;
      }
      x[v] = x[v] * ctx.alpha + ctx.beta;
      messages.SendMsgThroughEdges(frag, v, ctx.x[v]);
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    pullAndSend(frag, ctx, messages);

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }
    ctx.curr_round++;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;

    // we only sum inner vertices, so put CheckTerm before GetMessage is fine
    if (CheckTerm(frag, ctx)) {
      auto inner_vertices = frag.InnerVertices();
      auto total_sum = ctx.total_sum;

      CHECK_GT(total_sum, 0);
      double s = 1.0 / std::sqrt(total_sum);
      for (auto& u : inner_vertices) {
        if (ctx.normalized) {
          x[u] *= s;
        }
      }

      return;
    }
    {
      double msg;
      vertex_t u;
      while (messages.GetMessage(frag, u, msg)) {
        x[u] = msg;
      }
    }

    x_last.Swap(x);

    pullAndSend(frag, ctx, messages);

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    }
    ctx.curr_round++;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_KATZ_KATZ_CENTRALITY_H_
