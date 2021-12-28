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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_EIGENVECTOR_EIGENVECTOR_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_EIGENVECTOR_EIGENVECTOR_CENTRALITY_H_

#include "grape/grape.h"

#include "apps/centrality/eigenvector/eigenvector_centrality_context.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Eigenvector centrality is a measure of the influence of a vertex in a
 * graph. Relative scores are assigned to all vertices in the graph based on the
 * concept that connections to high-scoring vertices contribute more to the
 * score of the vertex in question than equal connections to low-scoring
 * vertices
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class EigenvectorCentrality
    : public AppBase<FRAG_T, EigenvectorCentralityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(EigenvectorCentrality<FRAG_T>,
                         EigenvectorCentralityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using edata_t = typename fragment_t::edata_t;
  using vid_t = typename FRAG_T::vid_t;

  bool NormAndCheckTerm(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();
    double frag_sum = 0.0;

    for (auto& v : inner_vertices) {
      frag_sum += ctx.x[v] * ctx.x[v];
    }

    double total_sum = 0;
    Sum(frag_sum, total_sum);

    double norm = sqrt(total_sum);
    CHECK_GT(norm, 0);

    double local_delta_sum = 0, total_delta_sum = 0;
    for (auto& v : inner_vertices) {
      ctx.x[v] /= norm;
      local_delta_sum += std::abs(ctx.x[v] - ctx.x_last[v]);
    }

    Sum(local_delta_sum, total_delta_sum);
    VLOG(1) << "[step - " << ctx.curr_round << " ] Diff: " << total_delta_sum;
    if (total_delta_sum < frag.GetTotalVerticesNum() * ctx.tolerance ||
        ctx.curr_round >= ctx.max_round) {
      VLOG(1) << "Eigenvector centrality terminates after " << ctx.curr_round
              << " iterations. Diff: " << total_delta_sum;
      return true;
    }
    return false;
  }

  void Pull(const fragment_t& frag, context_t& ctx,
            message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;

    if (frag.directed()) {
      for (auto& v : inner_vertices) {
        x[v] = x_last[v];
        auto es = frag.GetIncomingAdjList(v);
        for (auto& e : es) {
          double edata = 1.0;
          static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
              [&](auto& e, auto& data) {
                data = static_cast<double>(e.get_data());
              })(e, edata);
          x[v] += x_last[e.get_neighbor()] * edata;
        }
      }
    } else {
      for (auto& v : inner_vertices) {
        x[v] = x_last[v];
        auto es = frag.GetOutgoingAdjList(v);
        for (auto& e : es) {
          double edata = 1.0;
          static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
              [&](auto& e, auto& data) {
                data = static_cast<double>(e.get_data());
              })(e, edata);
          x[v] += x_last[e.get_neighbor()] * edata;
        }
      }
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    Pull(frag, ctx, messages);
    auto inner_vertices = frag.InnerVertices();

    // call NormAndCheckTerm before send. because we normalize the vector 'x' in
    // the function.
    if (NormAndCheckTerm(frag, ctx))
      return;

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    } else {
      for (auto& v : inner_vertices) {
        messages.SendMsgThroughEdges(frag, v, ctx.x[v]);
      }
    }
    ctx.curr_round++;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;
    auto inner_vertices = frag.InnerVertices();

    {
      double msg;
      vertex_t u;
      while (messages.GetMessage(frag, u, msg)) {
        x[u] = msg;
      }
    }

    x_last.Swap(x);

    Pull(frag, ctx, messages);

    if (NormAndCheckTerm(frag, ctx))
      return;

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    } else {
      for (auto& v : inner_vertices) {
        messages.SendMsgThroughEdges(frag, v, x[v]);
      }
    }
    ctx.curr_round++;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_EIGENVECTOR_EIGENVECTOR_CENTRALITY_H_
