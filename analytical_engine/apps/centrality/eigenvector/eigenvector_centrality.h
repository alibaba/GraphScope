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

#include <vector>

#include "grape/grape.h"

#include "apps/centrality/eigenvector/eigenvector_centrality_context.h"

#include "core/app/app_base.h"
#include "core/utils/trait_utils.h"

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
    : public grape::ParallelAppBase<FRAG_T,
                                    EigenvectorCentralityContext<FRAG_T>>,
      public grape::ParallelEngine,
      public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(EigenvectorCentrality<FRAG_T>,
                          EigenvectorCentralityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using edata_t = typename fragment_t::edata_t;
  using vid_t = typename FRAG_T::vid_t;

  bool NormAndCheckTerm(const fragment_t& frag, context_t& ctx, int thrd_num) {
    auto inner_vertices = frag.InnerVertices();
    std::vector<double> thread_local_sum(thrd_num, 0.0);
    double local_sum = 0.0, global_sum = 0;
    ForEach(inner_vertices.begin(), inner_vertices.end(),
            [&thread_local_sum, &ctx](int tid, vertex_t v) {
              thread_local_sum[tid] += ctx.x[v] * ctx.x[v];
            });
    for (int tid = 0; tid < thrd_num; tid++) {
      local_sum += thread_local_sum[tid];
    }
    Sum(local_sum, global_sum);

    double norm = sqrt(global_sum);
    CHECK_GT(norm, 0);

    std::vector<double> thread_local_delta_sum(thrd_num, 0.0);
    double local_delta_sum = 0, global_delta_sum = 0;
    ForEach(inner_vertices.begin(), inner_vertices.end(),
            [&thread_local_delta_sum, &ctx, &norm](int tid, vertex_t v) {
              ctx.x[v] /= norm;
              thread_local_delta_sum[tid] += std::abs(ctx.x[v] - ctx.x_last[v]);
            });
    for (int tid = 0; tid < thrd_num; tid++) {
      local_delta_sum += thread_local_delta_sum[tid];
    }
    Sum(local_delta_sum, global_delta_sum);
    VLOG(1) << "[step - " << ctx.curr_round << " ] Diff: " << global_delta_sum;
    if (global_delta_sum < frag.GetTotalVerticesNum() * ctx.tolerance ||
        ctx.curr_round >= ctx.max_round) {
      VLOG(1) << "Eigenvector centrality terminates after " << ctx.curr_round
              << " iterations. Diff: " << global_delta_sum;
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
      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [&x, &x_last, &frag](int tid, vertex_t v) {
            auto es = frag.GetIncomingAdjList(v);
            x[v] = x_last[v];
            for (auto& e : es) {
              double edata = 1.0;
              vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
                  [&](auto& e, auto& data) {
                    data = static_cast<double>(e.get_data());
                  })(e, edata);
              x[v] += x_last[e.get_neighbor()] * edata;
            }
          });
    } else {
      ForEach(
          inner_vertices.begin(), inner_vertices.end(),
          [&x, &x_last, &frag](int tid, vertex_t v) {
            auto es = frag.GetOutgoingAdjList(v);
            x[v] = x_last[v];
            for (auto& e : es) {
              double edata = 1.0;
              vineyard::static_if<!std::is_same<edata_t, grape::EmptyType>{}>(
                  [&](auto& e, auto& data) {
                    data = static_cast<double>(e.get_data());
                  })(e, edata);
              x[v] += x_last[e.get_neighbor()] * edata;
            }
          });
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    int thrd_num = thread_num();
    messages.InitChannels(thread_num());
    Pull(frag, ctx, messages);
    auto inner_vertices = frag.InnerVertices();

    // call NormAndCheckTerm before send. because we normalize the vector 'x' in
    // the function.
    if (NormAndCheckTerm(frag, ctx, thrd_num))
      return;

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    } else {
      ForEach(inner_vertices.begin(), inner_vertices.end(),
              [&ctx, &frag, &messages](int tid, vertex_t v) {
                messages.Channels()[tid].SendMsgThroughOEdges(frag, v,
                                                              ctx.x[v]);
              });
    }
    ctx.curr_round++;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    int thrd_num = thread_num();
    auto& x = ctx.x;
    auto& x_last = ctx.x_last;
    auto inner_vertices = frag.InnerVertices();

    messages.ParallelProcess<fragment_t, double>(
        thrd_num, frag, [&x](int tid, vertex_t v, double msg) { x[v] = msg; });

    x_last.Swap(x);

    Pull(frag, ctx, messages);

    if (NormAndCheckTerm(frag, ctx, thrd_num))
      return;

    if (frag.fnum() == 1) {
      messages.ForceContinue();
    } else {
      ForEach(inner_vertices.begin(), inner_vertices.end(),
              [&ctx, &frag, &messages](int tid, vertex_t v) {
                messages.Channels()[tid].SendMsgThroughOEdges(frag, v,
                                                              ctx.x[v]);
              });
    }
    ctx.curr_round++;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_EIGENVECTOR_EIGENVECTOR_CENTRALITY_H_
